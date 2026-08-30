namespace Kommander;

/// <summary>
/// What a <see cref="RaftConfiguration"/> option is for.
///
/// <para>The classification exists so that a new option cannot be added without someone deciding
/// which of these it is. That decision is the whole point: the fragility analysis found 117 public
/// options and observed that a safety guard behind an option is a guard that can be off. Naming the
/// kind does not remove the option, but it does make "this one can disable a fence" a fact the code
/// states rather than a fact a reader has to derive.</para>
/// </summary>
public enum RaftOptionKind
{
    /// <summary>A wrong value can break a Raft safety or durability guarantee, or leave the
    /// transport unauthenticated. Never tune one of these for throughput.</summary>
    Safety,

    /// <summary>A wrong value can wedge, livelock, or starve the cluster without breaking safety.
    /// Committed data stays correct; the cluster may stop making progress.</summary>
    Liveness,

    /// <summary>Tuning only. A wrong value costs latency, throughput, or memory.</summary>
    Performance,

    /// <summary>Identity, addressing, and topology. Set per deployment; no default is meaningful.</summary>
    Deployment,

    /// <summary>Observability only. No value changes what the protocol does.</summary>
    Diagnostics,

    /// <summary>Computed from other options; not settable.</summary>
    Derived
}

/// <summary>
/// One safety or liveness fence that the current configuration has turned off or weakened.
/// </summary>
/// <param name="Option">The option's property name on <see cref="RaftConfiguration"/>.</param>
/// <param name="Kind">Whether the fence protects safety or liveness.</param>
/// <param name="CurrentValue">The configured value, rendered for a log line.</param>
/// <param name="SafeValue">The value that keeps the fence on.</param>
/// <param name="Hazard">What can happen while the fence is off. Written for an operator reading
/// one startup log line, not for a reader of this file.</param>
/// <param name="IsShippedDefault">
/// <see langword="true"/> when this deviation is what a caller gets without setting anything. Those
/// are reported at Information — warning on every default startup would train operators to ignore
/// the message. A deviation someone chose is reported at Warning.
/// </param>
public sealed record RaftSafetyOptionDeviation(
    string Option,
    RaftOptionKind Kind,
    string CurrentValue,
    string SafeValue,
    string Hazard,
    bool IsShippedDefault);

/// <summary>
/// Classifies every <see cref="RaftConfiguration"/> option, and reports the ones whose current
/// value leaves a safety or liveness fence off.
///
/// <para><b>Why this exists.</b> The fragility analysis names configuration as one of the six root
/// causes: the option surface defines far more configurations than any suite runs, so a fence that
/// an option can disable will eventually run disabled somewhere, silently. The recommendation was
/// to make safety fences unconditional. Removing a public option is a breaking change for the
/// hosts that already set it, so the first step is the one that costs nothing and can ship now: a
/// node states at startup which fences it is running without. A deployment can then be audited
/// from its own logs instead of from a reading of this file.</para>
///
/// <para><b>What is NOT here.</b> A value that is merely outside its valid range belongs in
/// <see cref="RaftConfiguration.Validate"/>, which throws. This type reports valid configurations
/// that are less protected than the shipped one; it never throws and never changes a value.</para>
/// </summary>
public static class RaftSafetyOptionAudit
{
    /// <summary>
    /// Every public option on <see cref="RaftConfiguration"/>, by kind.
    ///
    /// <para>Kept exhaustive by a test that reflects over the type: an option added without an
    /// entry here fails that test, so the author has to decide what the option is for. That is the
    /// only enforcement mechanism available — nothing in C# can require the decision at the
    /// declaration.</para>
    /// </summary>
    public static IReadOnlyDictionary<string, RaftOptionKind> Classification { get; } =
        new Dictionary<string, RaftOptionKind>(StringComparer.Ordinal)
    {
        [nameof(RaftConfiguration.NodeName)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.NodeId)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.Host)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.Port)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.InitialPartitions)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.HttpScheme)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.GrpcScheme)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.TransportSecurity)] = RaftOptionKind.Safety,
        [nameof(RaftConfiguration.HttpAuthBearerToken)] = RaftOptionKind.Safety,
        [nameof(RaftConfiguration.HttpTimeout)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.HttpVersion)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.HeartbeatInterval)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.RecentHeartbeat)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.VotingTimeout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.CheckLeaderInterval)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.TimerInitialDelay)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.UpdateNodesInterval)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.StartElectionTimeout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.EndElectionTimeout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.StartElectionTimeoutIncrement)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.EndElectionTimeoutIncrement)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.ElectionTimeoutSeed)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.InvariantChecks)] = RaftOptionKind.Diagnostics,
        [nameof(RaftConfiguration.SlowRaftStateMachineLog)] = RaftOptionKind.Diagnostics,
        [nameof(RaftConfiguration.SlowRaftWALMachineLog)] = RaftOptionKind.Diagnostics,
        [nameof(RaftConfiguration.ReadIOThreads)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.WriteIOThreads)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxQueuedClientProposalsPerPartition)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.MaxWalQueueDepthPerPartition)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.MaxGlobalWalQueueDepth)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.MaxWalBatchSize)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.SqliteWalShardCount)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxWalGroupBatchPartitions)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.WalGroupCommitLingerMs)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.WalSingleFsyncCommit)] = RaftOptionKind.Safety,
        [nameof(RaftConfiguration.ApplicationDurabilityProvider)] = RaftOptionKind.Safety,
        [nameof(RaftConfiguration.MaxDrainQuantumControl)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxDrainQuantumReplication)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxDrainQuantumClient)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxDrainQuantumMaintenance)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.GrpcChannelsPerNode)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.GrpcEnableMultipleHttp2Connections)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.GrpcEnableSnapshotCompression)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.GrpcEnableAppendLogsCoalescing)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.GrpcAppendLogsMaxCoalesceBatch)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxOutboundQueueBytesPerPeer)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.EnableSharedExecutorPool)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.PartitionExecutorPoolSize)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.EnableQuiescence)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.QuiesceAfter)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.LeadershipBarrierTimeout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.SelfRepairPeerDownGrace)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.LeadershipConfirmationTimeout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.EnableCheckQuorum)] = RaftOptionKind.Safety,
        [nameof(RaftConfiguration.CheckQuorumIntervalMultiplier)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.BackfillEnabled)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.BackfillThreshold)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.FollowerSaturationBackoff)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxBackfillEntriesPerRound)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxBackfillBytesPerRound)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.BackfillNoProgressPauseCap)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.BackfillNoProgressAnchorFallbackShips)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.SnapshotReceiveSessionTtl)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.SnapshotMaxPendingSessions)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.SnapshotMaxPendingBytes)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.AllowLegacySnapshotSenders)] = RaftOptionKind.Safety,
        [nameof(RaftConfiguration.SnapshotTransferStepTimeout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.SnapshotRescueMaxConsecutiveCycles)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.SnapshotRescueProbeInterval)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.SnapshotExportRetryCacheMaxBytes)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.MaxPreAuthRequestBodyBytes)] = RaftOptionKind.Safety,
        [nameof(RaftConfiguration.LearnerPromotionLag)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.LearnerPromotionStableWindow)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.GossipInterval)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.GossipFanout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.PingTimeout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.IndirectPingFanout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.SuspicionTimeout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.DeadMemberEvictionGrace)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.EnableAutoRejoin)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.PingInterval)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.EnableLeaderBalancer)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.EnableLoadReports)] = RaftOptionKind.Diagnostics,
        [nameof(RaftConfiguration.LoadReportsEnabled)] = RaftOptionKind.Derived,
        [nameof(RaftConfiguration.LeaderBalancerReportInterval)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.LeaderBalancerInterval)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.LeaderBalancerReportTtl)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.CountDeadband)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.LoadImbalanceThreshold)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MinLeaderStabilityMs)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MoveCooldown)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxMovesPerPass)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxConcurrentTransfers)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.LeaderBalancerOpsWeight)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.LeaderBalancerQueueWeight)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.SuggestionTimeout)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.EnableSlowNodeAvoidance)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.SlowNodeMultiplier)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.SlowNodeFloorMs)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.SlowNodeMinSamples)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.SlowNodeObservationTtl)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.SlowNodeEnterPasses)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.SlowNodeExitPasses)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.ReplicationFactor)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.EnablePlacementRebalancer)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.PlacementPassEnabled)] = RaftOptionKind.Derived,
        [nameof(RaftConfiguration.PlacementPassInterval)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxReplicaMovesPerPass)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxConcurrentReplicaTransfers)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxConcurrentReplicaRepairs)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.DecommissionDrainTimeout)] = RaftOptionKind.Liveness,
        [nameof(RaftConfiguration.ReplicaCountDeadband)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.Zone)] = RaftOptionKind.Deployment,
        [nameof(RaftConfiguration.CompactEveryOperations)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.CompactNumberEntries)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.MaxEntriesPerCompaction)] = RaftOptionKind.Performance,
        [nameof(RaftConfiguration.CompactionLiveReplicaLagBudget)] = RaftOptionKind.Liveness,
    };

    /// <summary>
    /// Returns every fence the given configuration leaves off or weakened, most protective concern
    /// first. An empty result means every fence this audit knows about is on.
    ///
    /// <para>Each entry names the option, the value in force, the value that keeps the fence on,
    /// and the hazard. A deviation is never an error: several are legitimate, and one
    /// (<see cref="RaftConfiguration.EnableCheckQuorum"/>) is the shipped default. The point is
    /// that the choice is visible.</para>
    /// </summary>
    public static IReadOnlyList<RaftSafetyOptionDeviation> Inspect(RaftConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        List<RaftSafetyOptionDeviation> deviations = [];

        // ── Safety ────────────────────────────────────────────────────────────

        RaftTransportSecurityOptions security = configuration.TransportSecurity;

        if (security.NodeAuthenticationMode == RaftNodeAuthenticationMode.Disabled)
        {
            deviations.Add(new(
                "TransportSecurity.NodeAuthenticationMode",
                RaftOptionKind.Safety,
                "Disabled",
                "SharedSecret or MutualTls",
                "Peers are not authenticated: anything that can reach the transport can send "
                + "AppendEntries and vote requests, which is enough to rewrite committed state.",
                IsShippedDefault: true));
        }

        if (!security.RequireTls)
        {
            deviations.Add(new(
                "TransportSecurity.RequireTls",
                RaftOptionKind.Safety,
                "false",
                "true",
                "Replication traffic may travel in cleartext, so log payloads and any shared "
                + "secret on the wire are readable by anything on the path.",
                IsShippedDefault: false));
        }

        if (security.AllowInsecureCertificateValidation)
        {
            deviations.Add(new(
                "TransportSecurity.AllowInsecureCertificateValidation",
                RaftOptionKind.Safety,
                "true",
                "false",
                "Peer certificates are not validated, so TLS proves nothing about who the peer is.",
                IsShippedDefault: false));
        }

        if (configuration.AllowLegacySnapshotSenders)
        {
            deviations.Add(new(
                nameof(RaftConfiguration.AllowLegacySnapshotSenders),
                RaftOptionKind.Safety,
                "true",
                "false",
                "Snapshot chunks that carry no leader term or endpoint are accepted, so a chunk "
                + "from a superseded leader cannot be fenced by term.",
                IsShippedDefault: false));
        }

        if (!configuration.EnableCheckQuorum)
        {
            deviations.Add(new(
                nameof(RaftConfiguration.EnableCheckQuorum),
                RaftOptionKind.Safety,
                "false",
                "true",
                "A leader that has lost contact with its voters does not step down on its own, so "
                + "it keeps accepting writes that can never commit until it learns of a newer term.",
                IsShippedDefault: true));
        }

        if (configuration.ApplicationDurabilityProvider is null)
        {
            deviations.Add(new(
                nameof(RaftConfiguration.ApplicationDurabilityProvider),
                RaftOptionKind.Safety,
                "null",
                "an IApplicationDurabilityProvider instance",
                "Compaction is bounded by the WAL checkpoint alone. An application that has not "
                + "durably applied an entry below that checkpoint cannot replay it after a crash.",
                IsShippedDefault: true));
        }

        // ── Liveness ──────────────────────────────────────────────────────────

        if (!configuration.BackfillEnabled)
        {
            deviations.Add(new(
                nameof(RaftConfiguration.BackfillEnabled),
                RaftOptionKind.Liveness,
                "false",
                "true",
                "A follower that falls behind is never repaired, by backfill or by snapshot. It "
                + "stays behind until an operator intervenes.",
                IsShippedDefault: false));
        }

        if (configuration.SnapshotRescueMaxConsecutiveCycles <= 0)
        {
            deviations.Add(new(
                nameof(RaftConfiguration.SnapshotRescueMaxConsecutiveCycles),
                RaftOptionKind.Liveness,
                configuration.SnapshotRescueMaxConsecutiveCycles.ToString(),
                "a positive cycle count",
                "The rescue convergence breaker is off. A follower that returns below the "
                + "compaction floor after every install drives an unbounded export loop on the leader.",
                IsShippedDefault: false));
        }

        if (configuration.SnapshotRescueProbeInterval <= TimeSpan.Zero)
        {
            deviations.Add(new(
                nameof(RaftConfiguration.SnapshotRescueProbeInterval),
                RaftOptionKind.Liveness,
                configuration.SnapshotRescueProbeInterval.ToString(),
                "a positive interval",
                "A tripped rescue breaker never probes again, so a follower whose environment "
                + "recovers is not re-seeded without operator action.",
                IsShippedDefault: false));
        }

        if (configuration.SnapshotExportRetryCacheMaxBytes <= 0)
        {
            deviations.Add(new(
                nameof(RaftConfiguration.SnapshotExportRetryCacheMaxBytes),
                RaftOptionKind.Liveness,
                configuration.SnapshotExportRetryCacheMaxBytes.ToString(),
                "a positive byte budget",
                "Every retry of a failed snapshot send re-runs the export, so the leader's answer "
                + "to memory pressure is to repeat its most allocation-hungry operation.",
                IsShippedDefault: false));
        }

        if (configuration.CompactionLiveReplicaLagBudget <= 0)
        {
            deviations.Add(new(
                nameof(RaftConfiguration.CompactionLiveReplicaLagBudget),
                RaftOptionKind.Liveness,
                configuration.CompactionLiveReplicaLagBudget.ToString(),
                "a positive entry budget",
                "Compaction ignores how far a live, acking follower has replicated, so an ordinary "
                + "pass can force a healthy follower into snapshot dependence.",
                IsShippedDefault: false));
        }

        if (configuration.MaxOutboundQueueBytesPerPeer <= 0)
        {
            deviations.Add(new(
                nameof(RaftConfiguration.MaxOutboundQueueBytesPerPeer),
                RaftOptionKind.Liveness,
                configuration.MaxOutboundQueueBytesPerPeer.ToString(),
                "a positive byte budget",
                "There is no per-peer outbound bound, so an unreachable peer can grow the "
                + "dispatcher's queue until the node runs out of memory.",
                IsShippedDefault: false));
        }

        return deviations;
    }
}
