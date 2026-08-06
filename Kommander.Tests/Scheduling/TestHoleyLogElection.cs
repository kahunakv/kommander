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
///         depth when the facade tracks presence).</item>
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

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
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
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);

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

    // ── promotion refuses to publish over a hole ──────────────────────────────

    /// <summary>
    /// Defense in depth: even if a holey node somehow wins (e.g. every peer has the same hole, or
    /// an append raced the election), promotion must refuse to publish — serving would fix an
    /// incomplete consumer projection for the whole tenure, because a leader is never backfilled.
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
        CapturingHost host = new();
        RaftPartitionStateMachine sm = new(host, wal, new CapturingReplySink(), NullLogger<IRaft>.Instance);

        await Assert.ThrowsAsync<RaftException>(() => sm.ForceLeaderForTestingAsync(replyCorrelationId: null));

        Assert.NotEqual("node-a", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    // ── the inherited drain detects the hole and the barrier reverts ──────────

    /// <summary>
    /// When the facade does not track presence (the promotion gate cannot fire), the inherited
    /// drain itself must detect the hole: entries above the gap are NOT applied (silently skipping
    /// them was the corruption — they were marked applied forever), and the barrier completion
    /// reverts the promotion instead of publishing over an incomplete projection.
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

        // Promote: maxLog (3) > commit frontier (0) arms the barrier; leadership is unpublished.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);
        Assert.NotEqual("node-a", host.Leader);

        // Drive the barrier no-op (id 4) through propose + commit. The inherited drain runs at the
        // commit completion, hits the hole at 2, and the barrier must revert the promotion.
        await sm.CompleteWalOperationAsync(ProposeCompletion(host.PartitionId, logIndex: 4));
        await sm.CompleteWalOperationAsync(CommitCompletion(host.PartitionId, minLogIndex: 4, maxLogIndex: 4));

        // Entry 1 (below the hole) may be applied; entry 3 (above it) must never be — applying it
        // would mark the missing entry 2 as covered forever.
        Assert.DoesNotContain("Applied:3", host.EventLog);

        // The promotion reverted: leadership never published, node back to Follower.
        Assert.NotEqual("node-a", host.Leader);
        Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }
}
