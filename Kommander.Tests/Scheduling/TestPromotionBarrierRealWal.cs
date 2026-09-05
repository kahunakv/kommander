
using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Regression guard for the stale-GetMaxLog promotion gate, at the REAL WAL layer
/// (<see cref="RaftWriteAhead"/> + real read/write schedulers), which the synchronous
/// <c>InheritedTailWalFacade</c> stub in <c>TestLeadershipBarrier</c> structurally cannot cover.
///
/// <para>The hole: <see cref="RaftWriteAhead"/>'s in-memory frontiers (commit, presence) advance
/// at ENQUEUE time, but <c>GetMaxLogAsync</c> reads the backend through the read scheduler, which
/// does not queue behind pending writes. An inherited entry whose physical append is still queued
/// in the write scheduler (starved write worker on a loaded host) is therefore invisible to the
/// promotion gate's <c>maxLog</c> read. The old gate (<c>maxLog &lt;= commitFrontier</c>) then
/// concluded "no inherited tail", skipped the barrier and published leadership with the entry —
/// possibly committed on quorum — never applied for the entire tenure (a leader is never
/// backfilled). Downstream shape: Kahuna's promoted lock partition serving a released lock as
/// held (GA-only <c>TestLockFailoverCoherence</c> failure).</para>
///
/// <para>The test makes the race deterministic with a gated <see cref="IWAL"/> wrapper: the
/// physical append blocks until released, so the enqueue-side frontiers and the backend disagree
/// for as long as the test needs. The fixed gate derives the inherited-tail decision from the
/// presence frontier, which cannot under-report a queued append.</para>
/// </summary>
public class TestPromotionBarrierRealWal
{
    // ── stubs ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// IWAL wrapper whose physical writes block on <see cref="WriteGate"/>. Everything else
    /// delegates to the inner adapter. Blocking Write stalls the write-scheduler worker exactly
    /// like a starved worker on a loaded host: the enqueue-side frontiers have already advanced,
    /// but backend reads (which do not queue behind writes) still see the pre-append state.
    /// </summary>
    private sealed class GatedWal(IWAL inner) : IWAL
    {
        /// <summary>Open (set) = writes proceed; closed (reset) = physical writes stall.</summary>
        public ManualResetEventSlim WriteGate { get; } = new(initialState: true);

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            WriteGate.Wait(TimeSpan.FromSeconds(30));
            return inner.Write(logs);
        }

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)
        {
            WriteGate.Wait(TimeSpan.FromSeconds(30));
            return inner.Write(logs, sync);
        }

        public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        public long GetTermAt(int partitionId, long logIndex) => inner.GetTermAt(partitionId, logIndex);
        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);
        public long GetLastCheckpoint(int partitionId) => inner.GetLastCheckpoint(partitionId);
        public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key) => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) => inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public RaftOperationStatus TruncateProposedLogsAfter(int partitionId, long afterLogId) => inner.TruncateProposedLogsAfter(partitionId, afterLogId);

        public void Dispose()
        {
            WriteGate.Set();
            WriteGate.Dispose();
            inner.Dispose();
        }
    }

    /// <summary>
    /// IRaftPartitionHost recording, in order, every consumer apply and LeaderChanged(self) —
    /// the same observable surface as TestLeadershipBarrier's recording host.
    /// </summary>
    private sealed class RecordingHost : IRaftPartitionHost
    {
        public RaftConfiguration Config { get; } = new()
        {
            Host = "localhost",
            Port = 8004,
            InitialPartitions = 1,
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
        };

        public int PartitionId => 1;
        public string Leader { get; set; } = "";
        public string LocalEndpoint => "node-a";
        public int LocalNodeId => 1;
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public bool IsVoter(string endpoint) => true;
        public RaftConfiguration Configuration => Config;
        public HybridLogicalClock HybridLogicalClock { get; } = new();
        public IReadOnlyList<RaftNode> Nodes => [];

        public List<string> EventLog { get; } = [];

        public MemberLivenessState GetNodeLiveness(string endpoint) => MemberLivenessState.Alive;
        public HLCTimestamp GetLastNodeActivity(string ep, int p) => HLCTimestamp.Zero;
        public void UpdateLastNodeActivity(string ep, int p, HLCTimestamp t) { }
        public void EnqueueResponse(string ep, RaftResponderRequest req) { }

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

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }

    // ── the regression ────────────────────────────────────────────────────────

    /// <summary>
    /// THE regression: an inherited prior-term Proposed entry sits enqueued in the write scheduler
    /// (its commit broadcast was lost with the old leader; its physical append is stalled). The
    /// promoted node's presence frontier covers the entry but the backend max-log read does not.
    /// The barrier must arm anyway, and leadership must not publish until the entry has been
    /// applied through the consumer callback. On the old <c>maxLog &lt;= commitFrontier</c> gate
    /// this fails at the first assert: leadership publishes immediately, the entry is never
    /// applied, and the consumer serves a stale projection for the whole tenure.
    /// </summary>
    [Fact]
    public async Task PromotedLeader_WithInheritedTailStillQueuedInWriteScheduler_ArmsBarrierAndAppliesBeforePublishing()
    {
        const int partitionId = 1;

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9004,
            InitialPartitions = 0,
        };

        GatedWal gated = new(new InMemoryWAL(NullLogger<IRaft>.Instance));

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            gated,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        RaftPartition partition = new(
            manager,
            gated,
            partitionId,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        // Completions are queued and pumped from the test thread so the state machine keeps its
        // production single-caller discipline (in production the partition executor serializes them).
        ConcurrentQueue<RaftWalCompletion> completions = new();
        RaftWriteAhead writeAhead = new(manager, completions.Enqueue, partition, gated);

        RecordingHost host = new();
        RaftPartitionStateMachine sm = new(host, new RaftWalFacadeAdapter(writeAhead), new CapturingReplySink(), NullLogger<IRaft>.Instance);

        try
        {
            // Stall the write-scheduler worker BEFORE the append is enqueued, so the physical
            // write provably has not landed when the promotion gate reads the backend.
            gated.WriteGate.Reset();

            // Follower-side delivery of a prior-term entry whose commit broadcast was lost with
            // the old leader: it arrives Proposed and its append is enqueued — the presence
            // frontier advances NOW, the backend sees nothing until the gate opens.
            writeAhead.EnqueueProposeOrCommit([
                new RaftLog { Id = 1, Term = 0, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] }
            ]);

            // The disagreement under test, pinned explicitly: enqueue-advanced frontier covers the
            // entry; the scheduler-routed backend read does not (reads don't queue behind writes).
            Assert.Equal(1, writeAhead.GetPresentIndex());
            Assert.Equal(0, writeAhead.GetCommitIndex());
            Assert.Equal(0, await writeAhead.GetMaxLog());

            await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

            // The barrier MUST be armed: leadership unpublished, node state does not leak Leader,
            // nothing applied yet (the entry physically cannot have been applied — it is unwritten).
            Assert.NotEqual("node-a", host.Leader);
            Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
            Assert.DoesNotContain("LeaderChanged:node-a", host.EventLog);
            Assert.DoesNotContain(host.EventLog, e => e.StartsWith("Applied:"));

            // Release the stalled worker: the inherited append and the barrier no-op (id 2, queued
            // behind it in partition FIFO) land, and their completions drive the barrier commit.
            gated.WriteGate.Set();

            TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(10));
            long started = Stopwatch.GetTimestamp();

            while (!host.EventLog.Contains("LeaderChanged:node-a"))
            {
                if (Stopwatch.GetElapsedTime(started) > budget)
                    Assert.Fail($"Barrier never completed; events: [{string.Join(",", host.EventLog)}]");

                if (completions.TryDequeue(out RaftWalCompletion? completion))
                    await sm.CompleteWalOperationAsync(completion);
                else
                    await Task.Delay(5, TestContext.Current.CancellationToken);
            }

            // The inherited entry reached the consumer BEFORE leadership was published, and the
            // barrier no-op stayed consensus-internal.
            int appliedIdx = host.EventLog.IndexOf("Applied:1");
            int leaderChangedIdx = host.EventLog.IndexOf("LeaderChanged:node-a");
            Assert.True(appliedIdx >= 0, $"inherited entry must be applied; events: [{string.Join(",", host.EventLog)}]");
            Assert.True(appliedIdx < leaderChangedIdx,
                $"Applied:1 (idx {appliedIdx}) must precede LeaderChanged (idx {leaderChangedIdx}); events: [{string.Join(",", host.EventLog)}]");
            Assert.DoesNotContain("Applied:2", host.EventLog);

            Assert.Equal("node-a", host.Leader);
            Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        }
        finally
        {
            gated.WriteGate.Set();
            partition.Dispose();
            manager.Dispose();
        }
    }

    /// <summary>
    /// The restart-convergence payoff of the inherited-re-commit work (spec: "converge the
    /// persistent WAL commit frontier over inherited entries on the leader", T3): after a
    /// promotion commits an inherited prior-term Proposed band, the band's rows must be durably
    /// re-marked Committed — so a RESTART recomputes the commit frontier at the promoted value
    /// instead of regressing below the band, and the next promotion finds nothing inherited (the
    /// no-tail fast path publishes immediately, with no barrier round).
    ///
    /// <para>Before the durable re-commit, the band stayed Proposed on disk: restore reconstructed
    /// the frontier BELOW it, the consumer projection regressed, and — worse — the leader's
    /// backfill could never re-ship the band (the committed-only range read skipped it), wedging
    /// every follower behind a gap the leader did not believe existed.</para>
    ///
    /// <para>Runs on <see cref="RocksDbWAL"/> deliberately: <see cref="InMemoryWAL"/> returns its
    /// STORED instances from reads, so <c>EnqueueCommit</c>'s in-place type flip mutates the
    /// backend directly and masks a re-commit whose physical write never happens — the exact
    /// masking that let the empty-payload bug (feature 25016232 round 4) through every in-memory
    /// test. A serializing backend is the only honest witness for the row flip.</para>
    /// </summary>
    [Fact]
    public async Task RestartAfterInheritedCommit_FrontierDoesNotRegress()
    {
        const int partitionId = 1;

        RaftConfiguration config = new()
        {
            Host = "localhost",
            Port = 9014,
            InitialPartitions = 0,
        };

        string walDir = Path.Combine(Path.GetTempPath(), $"kommander-t3-{Guid.NewGuid():N}");
        Directory.CreateDirectory(walDir);
        RocksDbWAL wal = new(walDir, "wal", NullLogger<IRaft>.Instance, syncWrites: false);

        RaftManager manager = new(
            config,
            new StaticDiscovery([]),
            wal,
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        ((FairReadScheduler)manager.ReadScheduler).Start();
        ((FairWalScheduler)manager.WalScheduler).Start();

        RaftPartition partition = new(
            manager,
            wal,
            partitionId,
            startRange: 0,
            endRange: 0,
            NullLogger<IRaft>.Instance);

        ConcurrentQueue<RaftWalCompletion> completions = new();
        RaftWriteAhead writeAhead = new(manager, completions.Enqueue, partition, wal);

        RecordingHost host = new();
        RaftPartitionStateMachine sm = new(host, new RaftWalFacadeAdapter(writeAhead), new CapturingReplySink(), NullLogger<IRaft>.Instance);

        try
        {
            // Pre-crash history: 1..2 committed with durable markers; 3..5 arrive Proposed from a
            // prior term whose commit broadcast was lost with the old leader.
            writeAhead.EnqueueProposeOrCommit([
                new RaftLog { Id = 1, Term = 0, Type = RaftLogType.Committed, LogType = "t", LogData = [1] },
                new RaftLog { Id = 2, Term = 0, Type = RaftLogType.Committed, LogType = "t", LogData = [1] },
            ]);
            writeAhead.EnqueueProposeOrCommit([
                new RaftLog { Id = 3, Term = 0, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] },
                new RaftLog { Id = 4, Term = 0, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] },
                new RaftLog { Id = 5, Term = 0, Type = RaftLogType.Proposed, LogType = "t", LogData = [1] },
            ]);

            // Promote: the barrier commits the inherited band and (the fix under test) durably
            // re-marks rows 3..5 Committed. Pump completions until leadership publishes.
            await sm.ForceLeaderForTestingAsync(replyCorrelationId: null);

            TimeSpan budget = TestTimeouts.Scale(TimeSpan.FromSeconds(10));
            long started = Stopwatch.GetTimestamp();

            while (!host.EventLog.Contains("LeaderChanged:node-a"))
            {
                if (Stopwatch.GetElapsedTime(started) > budget)
                    Assert.Fail($"Barrier never completed; events: [{string.Join(",", host.EventLog)}]");

                if (completions.TryDequeue(out RaftWalCompletion? completion))
                    await sm.CompleteWalOperationAsync(completion);
                else
                    await Task.Delay(5, TestContext.Current.CancellationToken);
            }

            // Drain the re-commit's own completion and wait for the physical writes to settle:
            // rows 3..5 (re-marked) and 6 (the committed barrier no-op) must read Committed.
            while (Stopwatch.GetElapsedTime(started) < budget)
            {
                while (completions.TryDequeue(out RaftWalCompletion? completion))
                    await sm.CompleteWalOperationAsync(completion);

                List<RaftLog> committedTail = await writeAhead.GetRangeAsync(3, 10);
                if (committedTail.Count(l => l.Id is >= 3 and <= 6) == 4)
                    break;

                await Task.Delay(5, TestContext.Current.CancellationToken);
            }

            // ── "Restart": a fresh RaftWriteAhead over the SAME backend re-runs restore. ──
            RaftWriteAhead restarted = new(manager, _ => { }, partition, wal);
            IReadOnlyList<RaftLog> restoreLogs = await restarted.LoadRestoreLogsAsync();
            await restarted.CompleteRestoreAsync(restoreLogs);

            // THE acceptance assert: the frontier does not regress below the promoted value.
            // Pre-fix the band 3..5 read Proposed and restore stopped at 2.
            Assert.Equal(6, restarted.GetCommitIndex());

            // And the next promotion finds nothing inherited: a state machine restored over the
            // same backend publishes leadership IMMEDIATELY (no-tail fast path, no barrier).
            RecordingHost hostB = new();
            RaftPartitionStateMachine smB = new(hostB, new RaftWalFacadeAdapter(restarted), new CapturingReplySink(), NullLogger<IRaft>.Instance);
            await smB.ForceLeaderForTestingAsync(replyCorrelationId: null);

            Assert.Equal("node-a", hostB.Leader);
            Assert.Equal(RaftNodeState.Leader, smB.NodeState);
        }
        finally
        {
            partition.Dispose();
            manager.Dispose();

            try
            {
                Directory.Delete(walDir, recursive: true);
            }
            catch
            {
                // Best-effort temp cleanup; the OS temp dir reaps leftovers.
            }
        }
    }
}
