using System.Diagnostics;
using Kommander;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Covers the holey-log election hole: the unanchored live-propose broadcast can write a lone high
/// entry over a gap on a behind follower, so the raw WAL max id can advertise log freshness the
/// node does not have. The Raft §5.4.1 comparison assumes contiguous logs — comparing raw max ids
/// let a node missing an arbitrary committed range win an election, silently skip the hole in the
/// inherited-entry drain, and then serve as leader with an incomplete consumer projection for its
/// whole tenure (a leader is never backfilled).
///
/// <list type="bullet">
///   <item>Candidates advertise the contiguous-presence position (id and its term), never the raw
///         max id.</item>
///   <item>Voters compare a candidate against their own contiguous-presence position.</item>
///   <item>Promotion refuses to publish when the WAL has a hole below the max id (defense in
///         depth when the facade tracks presence) — but only while a voter peer could win the
///         term instead, and only a bounded number of times over the same hole. A sole voter,
///         or a node that keeps winning elections over the same hole, truncates the orphaned
///         tail (rows no replica could ever apply) and serves: an unbounded refusal would leave
///         the partition permanently leaderless.</item>
///   <item>The inherited-entry drain detects a hole instead of advancing over it, and the barrier
///         completion reverts the promotion instead of publishing over an incomplete drain.</item>
/// </list>
/// </summary>
public class TestHoleyLogElection
{
    // ── stubs ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// IRaftPartitionHost that records consumer applies / leadership publishes and captures every
    /// outbound transport message, with a configurable peer list.
    /// </summary>
    private sealed class CapturingHost : IRaftPartitionHost
    {
        public RaftConfiguration Config { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
            // Keep the drain-retry loops short: these tests seed genuine holes, so the loops
            // always run to their bound before refusing/escaping.
            LeadershipBarrierTimeout = TimeSpan.FromMilliseconds(300),
        };

        public int PartitionId { get; init; } = 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => Config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes { get; set; } = [];

        /// <summary>Ordered log of event labels produced by the callbacks.</summary>
        public List<string> EventLog { get; } = [];

        /// <summary>Every outbound transport message, in send order.</summary>
        public List<RaftResponderRequest> Outbound { get; } = [];

        /// <summary>
        /// SWIM verdict per endpoint; anything absent reads Alive, matching the production default.
        /// </summary>
        public Dictionary<string, MemberLivenessState> Liveness { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) =>
            Liveness.TryGetValue(endpoint, out MemberLivenessState state) ? state : MemberLivenessState.Alive;

        /// <summary>
        /// Frozen monotonic tick source, so a test can age a refusal streak past the peer-down grace
        /// without sleeping. Left null the host uses the real stopwatch and behaves as before.
        /// </summary>
        public long? MonotonicTicks { get; set; }

        public long GetMonotonicTimestamp() => MonotonicTicks ?? Stopwatch.GetTimestamp();

        /// <summary>Freezes the clock (if it is not frozen yet) and moves it forward.</summary>
        public void AdvanceMonotonic(TimeSpan delta) =>
            MonotonicTicks = (MonotonicTicks ?? Stopwatch.GetTimestamp()) + (long)(delta.TotalSeconds * Stopwatch.Frequency);

        public HLCTimestamp GetLastNodeActivity(string ep, int p) => HLCTimestamp.Zero;
        public HLCTimestamp GetLastNodeHearthbeat(string ep, int p) => HLCTimestamp.Zero;
        public void UpdateLastHeartbeat(string ep, int p, HLCTimestamp t) { }
        public void UpdateLastNodeActivity(string ep, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string ep, RaftResponderRequest req) => Outbound.Add(req);

        public Task InvokeLeaderChanged(int p, string leader)
        {
            if (leader == LocalEndpoint)
                EventLog.Add($"LeaderChanged:{leader}");
            return Task.CompletedTask;
        }

        public Task<bool> InvokeReplicationReceived(int p, RaftLog log)
        {
            EventLog.Add($"Applied:{log.Id}");
            return Task.FromResult(true);
        }

        public Task<bool> InvokeSystemReplicationReceived(int p, RaftLog log)
        {
            EventLog.Add($"SystemApplied:{log.Id}");
            return Task.FromResult(true);
        }

        public void InvokeReplicationError(int p, RaftLog log)
        {
            EventLog.Add($"ApplyError:{log.Id}");
        }

        public IRaftStateMachineTransfer? StateMachineTransfer => null;
        public IRaftSystemStateTransfer? SystemStateTransfer => null;
        public Task<SnapshotResponse> SendInstallSnapshotAsync(RaftNode n, SnapshotRequest r, CancellationToken ct)
            => Task.FromResult(new SnapshotResponse(false));
    }

    /// <summary>
    /// WAL facade simulating a log with a hole: the raw max id (a lone high entry from the
    /// unanchored live-propose broadcast) leads the contiguous-presence frontier. Presence
    /// tracking is opt-in — leaving <see cref="PresentId"/> at -1 models a facade that does not
    /// track presence, which exercises the legacy raw-max fallback and the drain-level detection.
    /// </summary>
    private sealed class HoleyWalFacade : IRaftWalFacade
    {
        public List<RaftLog> Entries { get; } = [];

        public long RawMaxLog { get; set; }

        public long CommitIndexValue { get; set; }

        public long PresentId { get; set; } = -1;

        public long PresentTermValue { get; set; } = -1;

        /// <summary>Models entries buffered above an unfilled gap (see IRaftWalFacade.HasPresenceGap).</summary>
        public bool HasPresenceGapValue { get; set; }

        public bool HasPresenceGap() => HasPresenceGapValue;

        public long LastEntryTerm { get; set; }

        private long _nextId = 1;

        public void SeedNextId(long nextId) => _nextId = nextId;

        public long GetCommitIndex() => CommitIndexValue;
        public long GetPresentIndex() => PresentId;
        public long GetPresentTerm() => PresentTermValue;
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(RawMaxLog);
        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(LastEntryTerm);
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);

        public ValueTask<List<RaftLog>> GetRangeAsync(long start, int max)
            => ValueTask.FromResult(Entries
                .Where(l => l.Id >= start && l.Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint)
                .OrderBy(l => l.Id).Take(max).ToList());

        public ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long start, int max)
            => ValueTask.FromResult(Entries.Where(l => l.Id >= start).OrderBy(l => l.Id).Take(max).ToList());

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
        {
            // Mirror RaftWriteAhead.EnqueuePropose: assign contiguous ids and stamp the term.
            foreach (RaftLog log in logs)
            {
                log.Id = _nextId++;
                log.Term = term;
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            if (maxId > RawMaxLog)
                RawMaxLog = maxId;
            return new(null!, 1L, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term, logIndex: maxId);
        }

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
        {
            foreach (RaftLog log in logs)
            {
                if (log.Type == RaftLogType.Proposed)
                    log.Type = RaftLogType.Committed;
            }

            long maxId = logs.Count > 0 ? logs.Max(l => l.Id) : 0;
            CommitIndexValue = maxId;
            Entries.AddRange(logs);

            return new(null!, 2L, WALWriteOperationType.LeaderCommit, (1, logs), logIndex: maxId);
        }

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
            => ValueTask.FromResult<IReadOnlyList<RaftLog>>([]);
        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;

        /// <summary>Every truncation boundary requested, in call order.</summary>
        public List<long> TruncateCalls { get; } = [];

        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId)
        {
            // Mirror RaftWriteAhead.TruncateLogsAfterAsync: delete above the boundary, clamp the
            // presence frontier, report the post-truncation max.
            TruncateCalls.Add(afterLogId);
            Entries.RemoveAll(l => l.Id > afterLogId);
            if (RawMaxLog > afterLogId)
                RawMaxLog = afterLogId;
            if (PresentId > afterLogId)
                PresentId = afterLogId;
            return ValueTask.FromResult(RawMaxLog);
        }

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
            => new(_ => { }, 3L, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp t = default, string? ep = null, long term = -1)
            => logs is null ? null : EnqueuePropose(term, logs, t, autoCommit: false);

        public void NotifyCommitted() { }
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];
        public void TryComplete(ulong correlationId, RaftResponse response) => Completed.Add((correlationId, response));
    }

    private static RaftWalCompletion ProposeCompletion(int partitionId, long logIndex, RaftOperationStatus status = RaftOperationStatus.Success) =>
        new(partitionId, OperationId: 1L, Term: -1L,
            MinLogIndex: -1L, MaxLogIndex: logIndex,
            WALWriteOperationType.LeaderPropose, status);

    private static RaftWalCompletion CommitCompletion(int partitionId, long minLogIndex, long maxLogIndex, RaftOperationStatus status = RaftOperationStatus.Success) =>
        new(partitionId, OperationId: 2L, Term: -1L,
            MinLogIndex: minLogIndex, MaxLogIndex: maxLogIndex,
            WALWriteOperationType.LeaderCommit, status);

    // ── candidates advertise the contiguous position ──────────────────────────

    /// <summary>
    /// A campaigning node with a WAL hole (contiguous through 3, lone high entry at 10) must
    /// advertise position (3, its term), NOT the raw max — otherwise its fresh-looking index would
    /// out-vote peers that actually hold the missing committed range.
    /// </summary>
    [Fact]
    public async Task Candidate_AdvertisesContiguousPosition_NotRawMaxId()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 9, PresentId = 3, PresentTermValue = 7 };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        RaftResponderRequest votesMsg = Assert.Single(host.Outbound,
            m => m.Type == RaftResponderRequestType.RequestVotes);
        Assert.NotNull(votesMsg.RequestVotesRequest);
        Assert.Equal(3, votesMsg.RequestVotesRequest!.MaxLogId);
        Assert.Equal(7, votesMsg.RequestVotesRequest.LastLogTerm);
    }

    // ── voters compare against their contiguous position ──────────────────────

    /// <summary>
    /// A voter with a WAL hole (contiguous through 3, lone high entry at 10) must grant a vote to
    /// a candidate whose contiguous log reaches 5: the candidate genuinely holds more of the log
    /// than this voter, and denying it (as the raw-max comparison did) would keep electable nodes
    /// out while the holey node campaigns.
    /// </summary>
    [Fact]
    public async Task Voter_WithWalHole_GrantsCandidateAheadOfContiguousPosition()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1 };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 5, remoteMaxLogId: 5, ts, preVote: false, remoteLastLogTerm: 1);

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
    }

    /// <summary>
    /// The voter still denies a candidate genuinely behind its contiguous position — the fix
    /// narrows what the voter advertises about itself; it must not weaken §5.4.1.
    /// </summary>
    [Fact]
    public async Task Voter_DeniesCandidateBehindContiguousPosition()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1 };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 5, remoteMaxLogId: 2, ts, preVote: false, remoteLastLogTerm: 1);

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
    }

    // ── the §5.4.1 missing-term fallback is symmetric ─────────────────────────

    /// <summary>
    /// A voter whose OWN last-log term is unreadable must not grant index-blind. The dangerous
    /// shape is a high index with no term — reachable when the presence frontier lands on a
    /// compacted checkpoint boundary, whose stored term reads -1 and is clamped to 0. The old
    /// comparison short-circuited on the remote term only, so it evaluated <c>5 != 0</c> then
    /// <c>5 &lt; 0</c> (false) and granted, with the index never examined. This voter holds a log
    /// through 10; the candidate holds 2 and must be denied.
    /// </summary>
    [Fact]
    public async Task Voter_WithUnreadableLocalTerm_DeniesCandidateBehindOnIndex()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 10, PresentTermValue = 0 };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 5, remoteMaxLogId: 2, ts, preVote: false, remoteLastLogTerm: 5);

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
    }

    /// <summary>
    /// The same voter still grants when the candidate is at or ahead of it on index. The fallback
    /// only removes the term from the comparison; it must not deny a candidate that is genuinely
    /// as up to date, or a node with a compacted boundary term could never help elect anyone.
    /// </summary>
    [Fact]
    public async Task Voter_WithUnreadableLocalTerm_StillGrantsCandidateAtOrAheadOnIndex()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 10, PresentTermValue = 0 };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 5, remoteMaxLogId: 10, ts, preVote: false, remoteLastLogTerm: 5);

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
    }

    /// <summary>
    /// A genuinely empty voter is unaffected: its index is 0, so no candidate is behind it and the
    /// grant still goes out. The fallback must not turn an empty node into a vote sink.
    /// </summary>
    [Fact]
    public async Task Voter_WithEmptyLog_StillGrants()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 0, LastEntryTerm = 0, PresentId = 0, PresentTermValue = 0 };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 5, remoteMaxLogId: 5, ts, preVote: false, remoteLastLogTerm: 5);

        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
    }

    /// <summary>
    /// The pre-vote probe shares the comparison, so it must deny the same candidate. A pre-vote
    /// grant that the real vote would refuse only churns elections.
    /// </summary>
    [Fact]
    public async Task PreVoter_WithUnreadableLocalTerm_DeniesCandidateBehindOnIndex()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 10, PresentTermValue = 0 };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 5, remoteMaxLogId: 2, ts, preVote: true, remoteLastLogTerm: 5);

        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.Vote);
    }

    // ── promotion refuses to publish over a hole ──────────────────────────────

    /// <summary>
    /// Defense in depth: even if a holey node somehow wins (e.g. an append raced the election),
    /// promotion must refuse to publish while a voter peer exists that could hold the missing
    /// range — serving would fix an incomplete consumer projection for the whole tenure, because
    /// a leader is never backfilled. The refusal hands the term to the peer.
    /// </summary>
    [Fact]
    public async Task Promotion_WithWalHole_IsRefusedAndRevertsToFollower()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 3 };
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 10, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ]);
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // Win a real election with the peer's vote so the promotion runs with a voter peer visible.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await Assert.ThrowsAsync<RaftException>(() =>
            sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3));

        Assert.NotEqual("node-a", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Empty(wal.TruncateCalls);
    }

    /// <summary>
    /// The refusal must be bounded, or it is its own wedge: a hole is only ever repaired by a
    /// serving leader's backfill, and the refusal is what prevents a leader from existing. Each
    /// election win over the SAME hole is quorum evidence that no reachable voter holds a fresher
    /// contiguous log (a fresher voter denies the vote and wins the term itself). After the bound,
    /// the node truncates the orphaned tail above the contiguous frontier — rows no replica could
    /// ever apply — and serves.
    /// </summary>
    [Fact]
    public async Task Promotion_WithWalHole_RepeatedRefusals_TruncateOrphanedTailAndServe()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 3 };
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 10, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ]);
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // Three consecutive wins over the same hole are refused: a fresher peer still has every
        // chance to win one of these terms.
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
            await Assert.ThrowsAsync<RaftException>(() =>
                sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3));
            Assert.Equal(RaftNodeState.Follower, sm.NodeState);
            Assert.Empty(wal.TruncateCalls);
        }

        // The fourth win escapes: truncate above the contiguous frontier (3) and publish.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3);

        long truncateBoundary = Assert.Single(wal.TruncateCalls);
        Assert.Equal(3, truncateBoundary);
        Assert.DoesNotContain(wal.Entries, l => l.Id == 10);
        Assert.DoesNotContain("Applied:10", host.EventLog);

        Assert.Equal("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Contains("LeaderChanged:node-a", host.EventLog);
    }

    // ── the peer-down grace on the destructive self-repair ────────────────────

    /// <summary>
    /// The refusal count measures elections, not outages. At a 2 s election timeout the budget is
    /// spent in seconds while a restarting voter is tens of seconds away — and that voter is
    /// exactly the one that may hold the missing range, because a reachable holder would have
    /// denied this node's vote and won the term itself. Truncating inside that window is how the
    /// split-nemesis run lost committed writes cluster-wide: every survivor self-repaired before
    /// the holder came back. While a voter peer is not Alive the truncation must therefore wait out
    /// the peer-down grace, no matter how many terms this node wins meanwhile.
    /// </summary>
    [Fact]
    public async Task Promotion_WithWalHole_DownVoterPeer_DefersTruncationDuringTheGrace()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 3 };
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 10, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ]);

        // node-b is up and votes; node-c is the restarting holder of the missing range.
        CapturingHost host = new() { Nodes = [new RaftNode("node-b"), new RaftNode("node-c")] };
        host.Liveness["node-c"] = MemberLivenessState.Dead;
        host.MonotonicTicks = Stopwatch.GetTimestamp();   // freeze: the grace cannot elapse in this loop

        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // Far past both caps (3, and 12 with a fresher live voter known): the count alone would
        // have truncated many terms ago.
        for (int attempt = 1; attempt <= 15; attempt++)
        {
            await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
            await Assert.ThrowsAsync<RaftException>(() =>
                sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3));
            Assert.Equal(RaftNodeState.Follower, sm.NodeState);
            Assert.Empty(wal.TruncateCalls);
        }

        Assert.NotEqual("node-a", host.Leader);
        Assert.Contains(wal.Entries, l => l.Id == 10);
    }

    /// <summary>
    /// The grace is a bound, never a hold: a peer that never returns must not keep the partition
    /// leaderless forever — that is the wedge the refusal caps exist to break. Once the grace
    /// elapses the gate self-repairs with the peer still down, exactly as it does when every voter
    /// peer is Alive.
    /// </summary>
    [Fact]
    public async Task Promotion_WithWalHole_DownVoterPeer_TruncatesOnceTheGraceExpires()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 3 };
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 10, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ]);

        CapturingHost host = new() { Nodes = [new RaftNode("node-b"), new RaftNode("node-c")] };
        host.Liveness["node-c"] = MemberLivenessState.Dead;
        host.MonotonicTicks = Stopwatch.GetTimestamp();

        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // The count budget is spent here; only the grace still holds the truncation back.
        for (int attempt = 1; attempt <= 5; attempt++)
        {
            await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
            await Assert.ThrowsAsync<RaftException>(() =>
                sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3));
            Assert.Empty(wal.TruncateCalls);
        }

        host.AdvanceMonotonic(host.Config.SelfRepairPeerDownGrace + TimeSpan.FromSeconds(1));

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3);

        long truncateBoundary = Assert.Single(wal.TruncateCalls);
        Assert.Equal(3, truncateBoundary);
        Assert.DoesNotContain(wal.Entries, l => l.Id == 10);

        Assert.Equal("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
    }

    /// <summary>
    /// The committed-drain gate takes the same grace: its escape delivers past the gap with
    /// <c>skipGaps</c>, which marks the missing ids applied forever, so it is destructive in the
    /// same way the tail truncation is. Here the frontier (5) sits above the last entry the drain
    /// can reach (3), and a voter peer is down.
    /// </summary>
    [Fact]
    public async Task Promotion_DrainBelowFrontier_DownVoterPeer_DefersSkipGapsDrainDuringTheGrace()
    {
        // RawMaxLog == PresentId keeps the completeness gate out of the way: the drain gate is the
        // one under test, and it runs first.
        HoleyWalFacade wal = new() { RawMaxLog = 3, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 5 };
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ]);

        CapturingHost host = new() { Nodes = [new RaftNode("node-b"), new RaftNode("node-c")] };
        host.Liveness["node-c"] = MemberLivenessState.Dead;
        host.MonotonicTicks = Stopwatch.GetTimestamp();

        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // Past the cap of 3: the count alone would already have taken the skip-gaps escape.
        for (int attempt = 1; attempt <= 5; attempt++)
        {
            await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
            await Assert.ThrowsAsync<RaftException>(() =>
                sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3));
            Assert.Equal(RaftNodeState.Follower, sm.NodeState);
            Assert.NotEqual("node-a", host.Leader);
        }

        // Grace expired: the gate stops waiting and serves, as it must to avoid a permanent wedge.
        host.AdvanceMonotonic(host.Config.SelfRepairPeerDownGrace + TimeSpan.FromSeconds(1));

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3);

        Assert.Equal("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
    }

    /// <summary>
    /// A sole voter (e.g. the last survivor of graceful leaves) is never refused on a hole: no
    /// reachable node can ever supply the missing range, so an unbounded refusal would leave the
    /// partition permanently leaderless — the node re-elects itself into the same refusal every
    /// election timeout, forever. It truncates the orphaned tail immediately and serves, keeping
    /// every entry at or below the contiguous frontier.
    /// </summary>
    [Fact]
    public async Task Promotion_WithWalHole_SoleVoter_TruncatesOrphanedTailAndServes()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 10, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 3 };
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 10, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ]);
        CapturingHost host = new();   // Nodes stays empty: sole voter
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

        // The contiguous prefix was applied; the unreachable lone high entry was truncated, never
        // delivered.
        Assert.Contains("Applied:1", host.EventLog);
        Assert.Contains("Applied:2", host.EventLog);
        Assert.Contains("Applied:3", host.EventLog);
        Assert.DoesNotContain("Applied:10", host.EventLog);

        long truncateBoundary = Assert.Single(wal.TruncateCalls);
        Assert.Equal(3, truncateBoundary);
        Assert.DoesNotContain(wal.Entries, l => l.Id == 10);

        Assert.Equal("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Contains("LeaderChanged:node-a", host.EventLog);
    }

    // ── the inherited drain detects the hole and the barrier reverts ──────────

    /// <summary>
    /// When the facade does not track presence (the promotion gate cannot fire), the inherited
    /// drain itself must detect the hole: entries above the gap are NOT applied (silently skipping
    /// them was the corruption — they were marked applied forever), and the barrier completion
    /// reverts the promotion instead of publishing over an incomplete projection — provided a
    /// voter peer exists that could hold the missing entries.
    /// </summary>
    [Fact]
    public async Task BarrierPromotion_InheritedDrainHole_RevertsInsteadOfPublishing()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 3, CommitIndexValue = 0 };   // PresentId = -1: untracked
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
            new RaftLog { Id = 3, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },   // hole at 2
        ]);
        wal.SeedNextId(4);
        CapturingHost host = new();
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // Promote peerless (the single-voter path arms the barrier and self-commits): maxLog (3) >
        // commit frontier (0) arms the barrier; leadership is unpublished.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.NotEqual("node-a", host.Leader);

        // Drive the barrier no-op (id 4) through propose, then make a voter peer visible before
        // the commit completes: the hole verdict depends on whether a peer could hold the missing
        // entries at the moment the drain fails.
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 4));
        host.Nodes = [new RaftNode("node-b")];
        await sm.CompleteWalOperationAsync(CommitCompletion(host.PartitionId, minLogIndex: 4, maxLogIndex: 4));

        // Entry 1 (below the hole) may be applied; entry 3 (above it) must never be — applying it
        // would mark the missing entry 2 as covered forever.
        Assert.DoesNotContain("Applied:3", host.EventLog);

        // The promotion reverted: leadership never published, node back to Follower.
        Assert.NotEqual("node-a", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    /// <summary>
    /// The sole-voter escape: with NO voter peer to defer to (e.g. the last survivor of graceful
    /// leaves), refusing to serve over a hole would leave the partition permanently leaderless —
    /// the departed quorum took the missing entries with it. After the bounded drain retry the
    /// node publishes anyway, delivering every entry it DOES hold past the gap, so only the
    /// genuinely absent entries are lost rather than the whole suffix.
    /// </summary>
    [Fact]
    public async Task BarrierPromotion_InheritedDrainHole_SoleVoterServesAfterBoundedWait()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 3, CommitIndexValue = 0 };   // PresentId = -1: untracked
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
            new RaftLog { Id = 3, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },   // hole at 2
        ]);
        wal.SeedNextId(4);
        CapturingHost host = new();   // Nodes stays empty: sole voter throughout
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 4));
        await sm.CompleteWalOperationAsync(CommitCompletion(host.PartitionId, minLogIndex: 4, maxLogIndex: 4));

        // Both present inherited entries were delivered — the skip-gaps drain covers the suffix
        // past the unrecoverable hole at 2.
        Assert.Contains("Applied:1", host.EventLog);
        Assert.Contains("Applied:3", host.EventLog);

        // And leadership published: availability wins when no peer can have the missing entries.
        Assert.Equal("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Contains("LeaderChanged:node-a", host.EventLog);
    }

    /// <summary>
    /// A hole below an ORDINARY leader commit (not the promotion barrier) must also disqualify
    /// the leader: previously the incomplete inherited drain was ignored while the commit's own
    /// batch apply advanced the cursor over the withheld range, permanently orphaning it — the
    /// leader kept serving grants minted from a projection missing an arbitrary committed range.
    /// Now the batch is not delivered over the hole and the leader steps down.
    /// </summary>
    [Fact]
    public async Task OrdinaryCommit_InheritedDrainHole_StepsDownWithoutDeliveringBatch()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 0, CommitIndexValue = 0 };   // PresentId = -1: untracked
        wal.SeedNextId(1);
        CapturingHost host = new();
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // Promote over an empty WAL: no inherited tail, publishes immediately.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.Equal("node-a", host.Leader);

        // Manufacture the post-promotion hole: prior-term entries 1 and 3 land in the WAL (id 2
        // missing — the unanchored broadcast shape), then the leader proposes and commits its own
        // entry at id 4. The commit's inherited drain must hit the hole at 2.
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
            new RaftLog { Id = 3, Term = 0, Type = RaftLogType.Proposed, LogType = "t" },
        ]);
        wal.SeedNextId(4);

        (RaftOperationStatus status, _) = sm.ReplicateLogs(
            [new RaftLog { LogType = "t", LogData = [1] }], autoCommit: true);
        Assert.Equal(RaftOperationStatus.Pending, status);

        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 4));

        // Make a voter peer visible before the commit completes: the hole verdict depends on
        // whether a peer could hold the missing entries at the moment the drain fails.
        host.Nodes = [new RaftNode("node-b")];
        await sm.CompleteWalOperationAsync(CommitCompletion(host.PartitionId, minLogIndex: 4, maxLogIndex: 4));

        // Entry 3 (above the hole) and the committed batch (id 4) must not have been delivered —
        // delivering id 4 would advance the apply cursor over the withheld range.
        Assert.DoesNotContain("Applied:3", host.EventLog);
        Assert.DoesNotContain("Applied:4", host.EventLog);

        // The leader stepped down instead of continuing to serve from an incomplete projection.
        Assert.NotEqual("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    // ── the committed-drain guard is bounded and escalates ────────────────────

    /// <summary>
    /// The committed-drain refusal ("drain stopped below the frontier") must be bounded like the
    /// presence gate: when a voter majority shares the same hole — or shares a commit frontier
    /// poisoned past it — every winner refuses, and an unbounded refusal leaves the partition
    /// leaderless for the rest of the run (~18k refusals in Jepsen run 32690955741). Three
    /// consecutive same-shape refusals hand the term back; the fourth win escalates: the node
    /// delivers everything it does hold past the gap and serves.
    /// </summary>
    [Fact]
    public async Task Promotion_CommittedDrainBelowFrontier_BoundedRefusalsThenServe()
    {
        // Commit frontier (5) sits above the drainable prefix (1..3): the committed drain can
        // never reach it — the poisoned-frontier shape of observation 1.
        HoleyWalFacade wal = new() { RawMaxLog = 3, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 5 };
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ]);
        wal.SeedNextId(4);
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // Three consecutive wins over the same shape are refused (the granter advertises no
        // fresher position, so the default cap applies).
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
            RaftException ex = await Assert.ThrowsAsync<RaftException>(() =>
                sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3));
            Assert.Contains($"({attempt}/3)", ex.Message);
            Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        }

        // The fourth win escapes: the drainable prefix is delivered and leadership publishes.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        await sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3);

        Assert.Contains("Applied:1", host.EventLog);
        Assert.Contains("Applied:3", host.EventLog);
        Assert.Equal("node-a", host.Leader);
        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
    }

    /// <summary>
    /// While a fresher live voter is known (its vote-path messages advertised a contiguous
    /// position above ours), the escalation is deferred with a stretched cap: that voter can win
    /// the term and repair this node by backfill with nothing lost, so destructive self-repair
    /// must wait for it. The stretched cap is still finite — "known and alive" does not guarantee
    /// "will campaign".
    /// </summary>
    [Fact]
    public async Task Promotion_CommittedDrainBelowFrontier_FresherVoterKnown_DefersEscalation()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 3, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 5 };
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ]);
        wal.SeedNextId(4);
        CapturingHost host = new() { Nodes = [new RaftNode("node-b"), new RaftNode("node-c")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // Five consecutive wins: node-b's grant advertises position 9 (above our contiguous 3) and
        // is ignored for quorum — but recorded as freshness evidence; node-c's grant completes the
        // quorum. With the fresher voter known, the 4th and 5th wins still refuse (the default cap
        // of 3 would have escalated at the 4th).
        for (int attempt = 1; attempt <= 5; attempt++)
        {
            await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
            await sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 9);
            RaftException ex = await Assert.ThrowsAsync<RaftException>(() =>
                sm.ReceivedVoteAsync("node-c", sm.CurrentTerm, remoteMaxLogId: 3));
            Assert.Contains($"({attempt}/12)", ex.Message);
            Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        }
    }

    // ── stale-vote promotions are abandoned, not recorded as drain failures ───

    /// <summary>
    /// A quorum completed by a vote dispatched long after the election started (executor backlog:
    /// a 10,001ms ReceiveVote was observed in the nightlies) must not promote: the drain would
    /// target a frontier the stale round never saw, spin for its whole bound, and be recorded as a
    /// drain failure on a perfectly good log. The round is abandoned cheaply — no exception, no
    /// refusal counted, term handed back for a fresh election.
    /// </summary>
    [Fact]
    public async Task StaleQuorumVote_AbandonsPromotionInsteadOfDraining()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 3, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 3 };
        wal.Entries.AddRange(
        [
            new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 2, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
            new RaftLog { Id = 3, Term = 1, Type = RaftLogType.Committed, LogType = "t" },
        ]);
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);

        // Age the round past 2× the election timeout (the stub randomizes within 50..100ms).
        await Task.Delay(350, TestContext.Current.CancellationToken);

        // The quorum-completing vote arrives stale: no promotion, no exception, no leadership.
        await sm.ReceivedVoteAsync("node-b", sm.CurrentTerm, remoteMaxLogId: 3);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.NotEqual("node-a", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
    }

    // ── gapped nodes defer candidacy to a known fresher voter ─────────────────

    /// <summary>
    /// A node holding entries above an unfilled gap, with a fresher live voter known from the
    /// vote paths, must yield the election-timer round instead of campaigning: winning would only
    /// reach the promotion gates and refuse, and each refused term appends a new-term barrier
    /// no-op that raises this node's advertised last-log term above the complete peer's — locking
    /// the complete peer out of §5.4.1 for the rest of the run (the majority-hole wedge). Yielding
    /// quiets the churn so the complete peer's own pre-vote can reach quorum.
    /// </summary>
    [Fact]
    public async Task GappedFollower_WithFresherVoterKnown_DefersCandidacy()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 8, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 3, HasPresenceGapValue = true };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        // node-b's pre-vote probe advertises contiguous position 9 — recorded as freshness
        // evidence even though the probe itself may be granted or denied.
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 2, remoteMaxLogId: 9, ts, preVote: true, remoteLastLogTerm: 1);
        host.Outbound.Clear();

        // The election-timer tick on a follower with no heartbeat would normally open a pre-vote
        // round; with the gap and the fresher voter known, the round is deferred.
        await sm.CheckPartitionLeadershipAsync();
        Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// The deference is bounded: a fresher peer that never campaigns must not suppress this node
    /// forever. After the bounded rounds the node campaigns normally.
    /// </summary>
    [Fact]
    public async Task GappedFollower_CandidacyDeference_IsBounded()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 8, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 3, HasPresenceGapValue = true };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 2, remoteMaxLogId: 9, ts, preVote: true, remoteLastLogTerm: 1);
        host.Outbound.Clear();

        // Ten rounds defer; the eleventh campaigns (opens a pre-vote round).
        for (int round = 1; round <= 10; round++)
        {
            await sm.CheckPartitionLeadershipAsync();
            Assert.DoesNotContain(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
        }

        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }

    /// <summary>
    /// Control: without a known fresher voter, a gapped follower still campaigns — the deference
    /// must never suppress the only nodes left.
    /// </summary>
    [Fact]
    public async Task GappedFollower_WithoutFresherVoterKnown_StillCampaigns()
    {
        HoleyWalFacade wal = new() { RawMaxLog = 8, LastEntryTerm = 1, PresentId = 3, PresentTermValue = 1, CommitIndexValue = 3, HasPresenceGapValue = true };
        CapturingHost host = new() { Nodes = [new RaftNode("node-b")] };
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        await sm.CheckPartitionLeadershipAsync();
        Assert.Contains(host.Outbound, m => m.Type == RaftResponderRequestType.RequestVotes);
    }
}
