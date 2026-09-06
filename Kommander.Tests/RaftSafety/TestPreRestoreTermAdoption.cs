using Kommander;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Scheduling;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Regression tests for what a partition does with a term it learned <em>before</em> its WAL
/// restore completed (DST FINDING 5).
///
/// <para>The executor lets AppendEntries through while the restore is still running, so a restarting
/// follower adopts the live leader's term in memory before <c>CompleteRestoreAsync</c> has read the
/// log tail or the hard state. Those stored terms are older than the live one by construction — the
/// node crashed, and the cluster moved on. Writing either of them over the live term regressed it
/// (Raft §5.1). Under the debug-build policy the term setter threw inside Phase 2, the executor
/// caught and logged it, and the partition ran on with its restore permanently incomplete: votes
/// dropped, views refused, appends acked, elections won. The harness saw two followers and no
/// leader; the leader was the node it could not see.</para>
///
/// <para>Three things changed, each with a test here: stored terms are floors, the leader-check tick
/// is fenced until the restore completes (an unrestored campaign would overwrite the durable vote
/// record it has not read), and a Phase 2 failure now stops the partition instead of leaving it
/// half-alive.</para>
///
/// <para>In the cluster-integration collection because <see cref="RaftInvariants.Policy"/> is
/// process-wide and the first test depends on it being <see cref="RaftInvariantPolicy.Throw"/>.</para>
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestPreRestoreTermAdoption : IDisposable
{
    private readonly RaftInvariantPolicy originalPolicy = RaftInvariants.Policy;

    public void Dispose() => RaftInvariants.Policy = originalPolicy;

    /// <summary>
    /// A WAL facade whose stored terms are fixed by the test and whose Phase 1 can be parked.
    /// </summary>
    private sealed class StoredTermWal : IRaftWalFacade
    {
        private readonly TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Term the log tail reports.</summary>
        public long TailTerm { get; init; }

        /// <summary>Hard state the store reports, or null for none.</summary>
        public (long CurrentTerm, string? VotedFor)? HardState { get; init; }

        /// <summary>When true, Phase 1 waits for <see cref="ReleaseRestore"/>.</summary>
        public bool GateRestore { get; init; }

        /// <summary>When set, Phase 2 throws it.</summary>
        public Exception? FailCompleteRestoreWith { get; init; }

        public void ReleaseRestore() => gate.TrySetResult();

        public async ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
        {
            if (GateRestore)
                await gate.Task.ConfigureAwait(false);

            return [];
        }

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) =>
            FailCompleteRestoreWith is null ? ValueTask.CompletedTask : ValueTask.FromException(FailCompleteRestoreWith);

        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(TailTerm);
        public ValueTask<(long CurrentTerm, string? VotedFor)?> LoadHardStateAsync() => ValueTask.FromResult(HardState);
        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(0L);
        public ValueTask<long> TruncateLogsAfterAsync(long afterLogId) => ValueTask.FromResult(afterLogId);
        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) => ValueTask.FromResult(new List<RaftLog>());
        public ValueTask<long> GetAnyTermAtAsync(long logIndex) => ValueTask.FromResult(-1L);
        public ValueTask<long> GetLastCheckpointAsync() => ValueTask.FromResult(-1L);
        public long GetCommitIndex() => 0;
        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp ts, bool autoCommit) => MakeNoOp();
        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) => MakeNoOp();
        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) => MakeNoOp();
        public void NotifyCommitted() { }
        private static WALWriteOperation MakeNoOp() => new(_ => { }, 0, WALWriteOperationType.LeaderPropose, (0, []));
    }

    private sealed class RelaySink : IRaftOperationReplySink
    {
        internal RaftPartitionExecutor? Executor;
        public void TryComplete(ulong correlationId, RaftResponse response)
            => Executor?.DeliverReply(correlationId, response);
    }

    private static (RaftPartitionExecutor Executor, TestWalCompletionFences.StubHost Host) BuildExecutor(StoredTermWal wal)
    {
        TestWalCompletionFences.StubHost host = new(partitionId: 1);
        RelaySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);
        RaftPartitionExecutor executor = new(sm, partitionId: 1, slowThresholdMs: 0, NullLogger<IRaft>.Instance);
        sink.Executor = executor;
        executor.Start();
        return (executor, host);
    }

    /// <summary>
    /// The FINDING 5 shape at the state-machine level. The live leader's heartbeat teaches the node
    /// term 5 before the restore runs; the log tail and the hard state both say 3. The restore must
    /// complete, and the term must still be 5 afterwards.
    ///
    /// <para>Before the fix this threw <see cref="RaftInvariantViolationException"/> out of
    /// <c>CompleteRestoreAsync</c> under the <see cref="RaftInvariantPolicy.Throw"/> policy, and
    /// silently set the term back to 3 under <see cref="RaftInvariantPolicy.Log"/>.</para>
    /// </summary>
    [Fact]
    public async Task TermAdoptedBeforeRestore_IsNotRegressedByTheRestore()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Throw;

        TestWalCompletionFences.StubHost host = new(partitionId: 1);
        StoredTermWal wal = new() { TailTerm = 3, HardState = (3, "old-leader") };
        RaftPartitionStateMachine sm = new(host, wal, NoReplySink.Instance, NullLogger<IRaft>.Instance);

        // Phase 1 first, as the executor orders it: the restore is in flight when the append lands.
        IReadOnlyList<RaftLog> loaded = await sm.StartRestoreAsync();

        // A heartbeat from the cluster's live leader at a term above anything this node stored.
        await sm.AppendLogsAsync("live-leader", term: 5, host.HybridLogicalClock.SendOrLocalEvent(2), logs: null);
        Assert.Equal(5L, sm.CurrentTerm);

        await sm.CompleteRestoreAsync(loaded);

        Assert.Equal(5L, sm.CurrentTerm);

        // The restore ran to completion: a second call is the guarded no-op, not a second replay.
        await sm.CompleteRestoreAsync(loaded);
        Assert.Equal(5L, sm.CurrentTerm);
    }

    /// <summary>
    /// The stored terms are still honoured when they are the higher ones — a node that crashed after
    /// a term bump and restarted into a quiet cluster must not come back at the tail's lower term.
    /// </summary>
    [Fact]
    public async Task StoredTermAboveTheLiveOne_StillWins()
    {
        RaftInvariants.Policy = RaftInvariantPolicy.Throw;

        TestWalCompletionFences.StubHost host = new(partitionId: 1);
        StoredTermWal wal = new() { TailTerm = 2, HardState = (7, "node-b") };
        RaftPartitionStateMachine sm = new(host, wal, NoReplySink.Instance, NullLogger<IRaft>.Instance);

        IReadOnlyList<RaftLog> loaded = await sm.StartRestoreAsync();
        await sm.AppendLogsAsync("live-leader", term: 4, host.HybridLogicalClock.SendOrLocalEvent(2), logs: null);

        await sm.CompleteRestoreAsync(loaded);

        Assert.Equal(7L, sm.CurrentTerm);
    }

    /// <summary>
    /// The leader-check tick that arrives while Phase 1 is parked must start no campaign, and the
    /// same tick after the restore completes must. The peer is what the campaign would write to, so
    /// its absence from the enqueued requests is the proof.
    /// </summary>
    [Fact]
    public async Task CheckLeaderBeforeRestoreCompletes_StartsNoCampaign_AndDoesAfter()
    {
        StoredTermWal wal = new() { TailTerm = 0, GateRestore = true };
        (RaftPartitionExecutor executor, TestWalCompletionFences.StubHost host) = BuildExecutor(wal);

        try
        {
            host.Nodes = [new RaftNode("peer-node")];

            // Past the election timeout ([50, 100) ms on this host), so an unfenced tick campaigns.
            await Task.Delay(150, TestContext.Current.CancellationToken);

            await executor.Ask(new RaftRequest(RaftRequestType.CheckLeader), TestContext.Current.CancellationToken);

            Assert.False(executor.IsRestored);
            Assert.DoesNotContain(host.EnqueuedResponses, r => r.Item2.Type == RaftResponderRequestType.RequestVotes);

            wal.ReleaseRestore();
            await executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            await executor.Ask(new RaftRequest(RaftRequestType.CheckLeader), TestContext.Current.CancellationToken);

            Assert.Contains(host.EnqueuedResponses, r => r.Item2.Type == RaftResponderRequestType.RequestVotes);
        }
        finally
        {
            wal.ReleaseRestore();
            executor.Dispose();
        }
    }

    /// <summary>
    /// A Phase 2 failure stops the partition. Before the fix the restore task faulted and the
    /// executor kept serving: appends acked, ticks processed, restore never complete.
    /// </summary>
    [Fact]
    public async Task RestorePhase2Failure_StopsThePartition()
    {
        StoredTermWal wal = new() { FailCompleteRestoreWith = new InvalidOperationException("phase 2 refused") };
        (RaftPartitionExecutor executor, _) = BuildExecutor(wal);

        try
        {
            InvalidOperationException restoreFailure = await Assert.ThrowsAsync<InvalidOperationException>(
                () => executor.RestoreTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

            Assert.Equal("phase 2 refused", restoreFailure.Message);
            Assert.False(executor.IsRestored);

            // Every later post is refused loudly rather than served by a half-restored partition.
            InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => executor.Ask(new RaftRequest(RaftRequestType.CheckLeader), TestContext.Current.CancellationToken));

            Assert.Contains("stopping", refused.Message, StringComparison.Ordinal);
        }
        finally
        {
            executor.Dispose();
        }
    }

    private sealed class NoReplySink : IRaftOperationReplySink
    {
        public static readonly NoReplySink Instance = new();
        public void TryComplete(ulong correlationId, RaftResponse response) { }
    }
}
