
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.Time;

namespace Kommander.System;

/// <summary>
/// Owns the per-partition cooldown and outstanding-transfer tables and executes one
/// leader-balancer planning pass per coordinator tick. Invoked exclusively on the
/// <see cref="RaftSystemCoordinator"/> single-consumer channel loop — no locking is
/// required for the owned dictionaries. Resets its tables on P0 leadership loss so
/// the next P0 leader always starts with a clean slate.
/// </summary>
internal sealed class LeaderBalancer
{
    private readonly Dictionary<int, DateTimeOffset> _balancerCooldowns = new();
    private readonly Dictionary<int, (string Target, DateTimeOffset Deadline)> _outstandingMoves = new();

    private readonly LoadReportStore loadReportStore;
    private readonly Func<string?> getLeaderNode;
    private readonly Func<ClusterMembership> getMembership;
    private readonly Func<HLCTimestamp> getHlcNow;
    private readonly Func<long> getSystemTerm;
    private readonly Dictionary<string, string> systemConfiguration;
    private readonly Action<string, TransferLeadershipSuggestionRequest> sendSuggestion;
    private readonly LivenessTable liveness;
    private readonly RaftConfiguration configuration;
    private readonly string localEndpoint;

    internal LeaderBalancer(
        LoadReportStore loadReportStore,
        Func<string?> getLeaderNode,
        Func<ClusterMembership> getMembership,
        Func<HLCTimestamp> getHlcNow,
        Func<long> getSystemTerm,
        Dictionary<string, string> systemConfiguration,
        Action<string, TransferLeadershipSuggestionRequest> sendSuggestion,
        LivenessTable liveness,
        RaftConfiguration configuration,
        string localEndpoint)
    {
        this.loadReportStore = loadReportStore;
        this.getLeaderNode = getLeaderNode;
        this.getMembership = getMembership;
        this.getHlcNow = getHlcNow;
        this.getSystemTerm = getSystemTerm;
        this.systemConfiguration = systemConfiguration;
        this.sendSuggestion = sendSuggestion;
        this.liveness = liveness;
        this.configuration = configuration;
        this.localEndpoint = localEndpoint;
    }

    /// <summary>Returns current count of outstanding suggestions. For test assertions only.</summary>
    internal int OutstandingMoveCountForTest => _outstandingMoves.Count;

    /// <summary>
    /// Executes one leader-balancer planning pass. Only acts when this node is the P0 leader
    /// and <c>EnableLeaderBalancer</c> is on. Must be called on the coordinator loop thread.
    /// </summary>
    internal Task RunPassAsync(CancellationToken cancellationToken)
    {
        if (!configuration.EnableLeaderBalancer)
            return Task.CompletedTask;

        string? leaderNode = getLeaderNode();

        if (!string.Equals(leaderNode, localEndpoint, StringComparison.Ordinal))
        {
            Diagnostics.KommanderMetrics.BalancerCountImbalance = 0.0;
            Diagnostics.KommanderMetrics.BalancerLoadImbalance = 0.0;
            _balancerCooldowns.Clear();
            _outstandingMoves.Clear();
            return Task.CompletedTask;
        }

        DateTimeOffset now = DateTimeOffset.UtcNow;
        HLCTimestamp hlcNow = getHlcNow();

        ClusterMembership membership = getMembership();
        TimeSpan ttl = configuration.LeaderBalancerReportTtl;

        loadReportStore.EvictStale(ttl, hlcNow);
        IReadOnlyList<NodeLoadReport> reports = loadReportStore.GetAll();

        HashSet<string> aliveEndpoints = new(StringComparer.Ordinal);
        foreach (ClusterMember m in membership.Members)
        {
            if (m.Role == ClusterMemberRole.Voter &&
                liveness.GetState(m.Endpoint) < MemberLivenessState.Suspect)
                aliveEndpoints.Add(m.Endpoint);
        }

        GlobalLeadershipView view = GlobalLeadershipView.Build(reports, membership.Members, aliveEndpoints, ttl, hlcNow);

        // Reconcile outstanding moves against the current view.
        List<int> toRemove = [];
        foreach ((int partitionId, (string target, DateTimeOffset deadline)) in _outstandingMoves)
        {
            if (view.PartitionOwner.TryGetValue(partitionId, out string? owner) &&
                string.Equals(owner, target, StringComparison.Ordinal))
            {
                Diagnostics.KommanderMetrics.BalancerMovesTotal.Add(1,
                    new KeyValuePair<string, object?>("outcome", "succeeded"));
                _balancerCooldowns[partitionId] = now + configuration.MoveCooldown;
                toRemove.Add(partitionId);
            }
            else if (now >= deadline)
            {
                Diagnostics.KommanderMetrics.BalancerMovesTotal.Add(1,
                    new KeyValuePair<string, object?>("outcome", "timed_out"));
                _balancerCooldowns[partitionId] = now + configuration.MoveCooldown;
                toRemove.Add(partitionId);
            }
        }
        foreach (int id in toRemove)
            _outstandingMoves.Remove(id);

        RaftSystemCoordinatorHelpers.UpdateImbalanceGauges(view);

        if (!view.IsComplete())
        {
            Diagnostics.KommanderMetrics.BalancerSkippedPassesTotal.Add(1);
            return Task.CompletedTask;
        }

        IReadOnlyDictionary<int, DateTimeOffset> cooldowns;
        if (_outstandingMoves.Count > 0)
        {
            Dictionary<int, DateTimeOffset> merged = new(_balancerCooldowns);
            DateTimeOffset farFuture = now + TimeSpan.FromDays(1);
            foreach (int pid in _outstandingMoves.Keys)
                merged.TryAdd(pid, farFuture);
            cooldowns = merged;
        }
        else
        {
            cooldowns = _balancerCooldowns;
        }

        int availableSlots = Math.Max(0, configuration.MaxConcurrentTransfers - _outstandingMoves.Count);
        if (availableSlots == 0)
            return Task.CompletedTask;

        RaftPartitionMap partitionMap;
        if (systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? mapJson))
        {
            partitionMap = global::System.Text.Json.JsonSerializer.Deserialize<RaftPartitionMap>(mapJson)
                           ?? new RaftPartitionMap { Partitions = [] };
        }
        else
        {
            partitionMap = new RaftPartitionMap { Partitions = [] };
        }

        int effectiveMaxMoves = Math.Min(configuration.MaxMovesPerPass, availableSlots);
        RaftConfiguration planConfig = effectiveMaxMoves == configuration.MaxMovesPerPass
            ? configuration
            : new RaftConfiguration
            {
                MaxMovesPerPass = effectiveMaxMoves,
                CountDeadband = configuration.CountDeadband,
                LoadImbalanceThreshold = configuration.LoadImbalanceThreshold,
                MinLeaderStabilityMs = configuration.MinLeaderStabilityMs,
                MoveCooldown = configuration.MoveCooldown,
                MaxConcurrentTransfers = configuration.MaxConcurrentTransfers,
            };

        IReadOnlyList<LeaderMove> moves =
            LeaderBalancePlanner.Plan(view, partitionMap, planConfig, cooldowns, now);

        long systemTerm = getSystemTerm();

        foreach (LeaderMove move in moves)
        {
            if (_outstandingMoves.Count >= configuration.MaxConcurrentTransfers)
                break;

            TransferLeadershipSuggestionRequest suggestion = new(
                move.PartitionId,
                systemTerm,
                hlcNow,
                localEndpoint,
                move.ToEndpoint);

            sendSuggestion(move.FromEndpoint, suggestion);

            _outstandingMoves[move.PartitionId] = (move.ToEndpoint, now + configuration.SuggestionTimeout);

            Diagnostics.KommanderMetrics.BalancerMovesTotal.Add(1,
                new KeyValuePair<string, object?>("outcome", "planned"));
        }

        return Task.CompletedTask;
    }
}
