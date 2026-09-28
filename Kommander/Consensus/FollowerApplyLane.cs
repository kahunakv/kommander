using System.Diagnostics;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.WAL.Data;

namespace Kommander.Consensus;

/// <summary>
/// Delivers a follower's committed entries to the consumer in executor turns of their own, after the
/// append that carried them has been acknowledged (<see cref="RaftConfiguration.FollowerApplyInOwnTurn"/>).
///
/// <para><b>Why.</b> A follower used to deliver a commit broadcast's entries inside the completion
/// of the append that carried them, before its ack. The consumer's callbacks (Kahuna's apply, about
/// 0.7 ms for a 136-entry batch on the CamusDB bank) then held the partition executor, and the next
/// proposal's append, which arrived meanwhile, waited behind them in the executor's queue: the
/// ~1 ms <c>follower.queue</c> that was the largest term of the round once the leader-side costs
/// were gone. Here the completion acks first and hands the entries to this lane, which delivers them
/// in maintenance-class turns of at most <see cref="RaftConfiguration.FollowerApplyTurnTime"/> (or
/// <see cref="RaftConfiguration.FollowerApplyTurnBudget"/> entries). An append that arrives while the
/// lane has work waits for one turn at most: the executor drains its replication queue before its
/// maintenance queue in every cycle. The bound is in time because what it protects is the append's
/// wait: with an entry bound, a cheap consumer paid an executor turn per few dozen entries for nothing
/// (−3.6% at 256-entry batches and 128 writers), and an expensive one still held the append for the
/// whole bound.</para>
///
/// <para><b>What does not change.</b> Delivery is still exactly-once and in log id order, and goes
/// through <see cref="LogApplicator.ApplyLogToConsumerAsync"/>, so the applied cursor still only
/// advances over delivered entries and the read-index and local-application waiters are completed
/// where it advances. The ack never carried the applied position (<see cref="FollowerAcks"/>), so
/// the leader sees nothing different. The applied cursor trailing the commit frontier is a state
/// every follower already had — a withheld drain leaves it there until a later drain — and every
/// other delivery path (the tick's retry drain, promotion, snapshot install, the apply hold) reads
/// the WAL from the cursor and is guarded by it, so an entry the lane still holds is never delivered
/// twice.</para>
///
/// <para><b>The backlog is a set of runs in id order.</b> Each run is the committed prefix of one
/// append (<c>Committed</c> or <c>CommittedCheckpoint</c> entries, consecutive ids, above the cursor).
/// A run above a gap is held until the gap fills: with pipelined proposals the commit broadcasts reach
/// a follower in quorum order, not log order, and the inline path re-read such a batch from the WAL
/// once the batch below it arrived. A turn delivers only the entry at cursor + 1 that the commit
/// frontier covers, so holding a run never lets delivery step over a gap; when the backlog cannot
/// reach the frontier, the turn falls back to the WAL drain. Entries the backlog holds are delivered
/// from memory, so a compaction that removes their rows cannot make the WAL drain step over them.</para>
///
/// <para><b>Backpressure.</b> A turn delivers at least one entry, and everything above
/// <see cref="BacklogHighWaterTurns"/> entry caps' worth of backlog. So a consumer slower than the commit
/// rate slows this executor, as the inline apply did, instead of letting the backlog grow without
/// bound. The system partition keeps the inline apply: its entries are the cluster's own
/// configuration, and it is not on any consumer's write path.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class FollowerApplyLane
{
    /// <summary>Backlog, in entry caps (<see cref="RaftConfiguration.FollowerApplyTurnBudget"/>), that a turn lets stand before it delivers the excess too.</summary>
    internal const int BacklogHighWaterTurns = 8;

    /// <summary>
    /// Hard bound on the backlog. Reached only if the head stops being deliverable while commits keep
    /// arriving; the backlog is then dropped and the WAL drain delivers from the cursor, as it did for
    /// any batch the inline fast path could not deliver.
    /// </summary>
    internal const int MaxBacklogEntries = 65_536;

    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly LogApplicator applier;
    private readonly Func<Action<RaftRequest>?> getPostToExecutor;

    /// <summary>A run of consecutive committed entries from one append; <see cref="Next"/> is the first undelivered one.</summary>
    private sealed class Run(List<RaftLog> logs)
    {
        public readonly List<RaftLog> Logs = logs;
        public int Next;
        public long FirstId => Logs[0].Id;
    }

    /// <summary>Held runs, ordered by <see cref="Run.FirstId"/>.</summary>
    private readonly List<Run> runs = [];

    /// <summary>Undelivered entries across <see cref="runs"/> (an upper bound: it counts entries a WAL drain delivered meanwhile until they are pruned).</summary>
    private int heldEntries;

    /// <summary>A turn is queued on the executor and has not started yet.</summary>
    private bool turnPosted;

    public FollowerApplyLane(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        LogApplicator applier,
        Func<Action<RaftRequest>?> getPostToExecutor)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.applier = applier;
        this.getPostToExecutor = getPostToExecutor;
    }

    /// <summary>
    /// Whether follower completions hand their entries to this lane. Off for the system partition,
    /// when <see cref="RaftConfiguration.FollowerApplyInOwnTurn"/> is off, and for a state machine
    /// with no executor to post turns to (unit-test hosts); the completion then applies inline.
    /// </summary>
    public bool Enabled =>
        host.PartitionId != RaftSystemConfig.SystemPartition
        && host.Configuration.FollowerApplyInOwnTurn
        && getPostToExecutor() is not null;

    /// <summary>A turn is queued on the executor and has not started yet.</summary>
    public bool HasPostedTurn => turnPosted;

    /// <summary>Entries held for delivery. Diagnostics and tests.</summary>
    public int BacklogCount => heldEntries;

    /// <summary>
    /// Takes the committed prefix of a follower append that has just been acknowledged, and queues a
    /// turn when there is anything to deliver. The <see cref="RaftLog"/> instances are retained; the
    /// list is not (the pending-operation envelope that owns it is pooled).
    /// </summary>
    public void Accept(List<RaftLog>? logs)
    {
        if (logs is { Count: > 0 } && !applier.ConsumerAppliesHeld)
        {
            List<RaftLog>? run = null;
            long applied = coreState.LastAppliedIndex;

            foreach (RaftLog log in logs)
            {
                // Proposed and other unresolved types are not deliverable yet; their commit arrives
                // in a later append. The inline fast path stopped here too.
                if (log.Type is not (RaftLogType.Committed or RaftLogType.CommittedCheckpoint))
                    break;

                if (log.Id <= applied)
                    continue;                   // applied already: a re-sent entry

                if (run is not null && log.Id != run[^1].Id + 1)
                {
                    Hold(run);                  // a gap inside the batch: each side is its own run
                    run = null;
                }

                (run ??= new List<RaftLog>(logs.Count)).Add(log);
            }

            if (run is not null)
                Hold(run);
        }

        Schedule();
    }

    /// <summary>Inserts a run in id order, or drops the backlog if it would pass <see cref="MaxBacklogEntries"/>.</summary>
    private void Hold(List<RaftLog> logs)
    {
        if (heldEntries + logs.Count > MaxBacklogEntries)
        {
            Clear();
            return;
        }

        Run run = new(logs);
        int index = runs.Count;
        while (index > 0 && runs[index - 1].FirstId > run.FirstId)
            index--;                            // usually none: commits mostly arrive in order

        runs.Insert(index, run);
        heldEntries += logs.Count;
    }

    /// <summary>
    /// One apply turn: delivers from the backlog for up to the turn time or entry cap, falls back to the WAL drain when
    /// the backlog cannot reach the commit frontier, and queues the next turn if it made progress and
    /// work remains. <paramref name="posted"/> is true for the turn this lane queued
    /// (<see cref="RaftRequestType.ApplyCommittedEntries"/>) and false for the tick's retry, which
    /// leaves a queued turn queued.
    /// </summary>
    public async Task RunTurnAsync(bool posted)
    {
        if (posted)
            turnPosted = false;

        // A leader delivers through its commit completions and the promotion drain, which already read
        // everything committed from the WAL; what is left here is at or below its cursor.
        if (coreState.NodeState == RaftNodeState.Leader)
        {
            Clear();
            return;
        }

        // Held (a pending re-seed, or the test hook): resuming drains from the WAL at the cursor.
        if (applier.ConsumerAppliesHeld)
            return;

        long startTicks = RoundStageInstrumentation.Stamp();
        long appliedBefore = coreState.LastAppliedIndex;
        long committed = wal.GetCommitIndex();

        // The turn ends at whichever bound comes first: its time (how long an append queued behind it
        // may wait) or its entry cap. The backlog above the high water is delivered regardless, so a
        // consumer slower than the commit rate slows the follower instead of growing the backlog.
        int entryCap = host.Configuration.FollowerApplyTurnBudget;
        if (entryCap <= 0)
            entryCap = int.MaxValue;

        long highWater = entryCap == int.MaxValue ? long.MaxValue : (long)entryCap * BacklogHighWaterTurns;
        long mustDeliver = Math.Max(0, heldEntries - highWater);

        TimeSpan turnTime = host.Configuration.FollowerApplyTurnTime;
        long deadline = turnTime > TimeSpan.Zero
            ? host.GetMonotonicTimestamp() + (long)(turnTime.TotalSeconds * Stopwatch.Frequency)
            : long.MaxValue;
        bool yielded = false;

        int delivered = 0;
        while (PeekNext() is { } log)
        {
            if (delivered >= mustDeliver
                && (delivered >= entryCap || (deadline != long.MaxValue && host.GetMonotonicTimestamp() >= deadline)))
            {
                yielded = true;
                break;
            }

            if (log.Id != coreState.LastAppliedIndex + 1 || log.Id > committed)
                break;

            runs[0].Next++;
            heldEntries--;
            await applier.ApplyLogToConsumerAsync(log).ConfigureAwait(false);
            delivered++;
        }

        // The backlog cannot reach the frontier (a hole just filled, or entries of batches it never
        // held became deliverable): drain from the WAL, bounded by what is left of the entry cap. A
        // no-op answer (withheld) leaves the cursor where it is; the next completion or tick retries.
        if (!yielded && delivered < entryCap && committed > coreState.LastAppliedIndex && !HeadIsNext())
        {
            long target = entryCap == int.MaxValue
                ? committed
                : Math.Min(committed, coreState.LastAppliedIndex + (entryCap - delivered));
            await applier.DrainCommittedAppliesAsync(target).ConfigureAwait(false);
        }

        if (coreState.LastAppliedIndex > appliedBefore)
        {
            RoundStageInstrumentation.Record(RoundStage.FollowerApply, startTicks);

            // Only a turn that moved the cursor queues the next one: a drain that withheld would
            // otherwise re-post itself forever.
            Schedule();
        }
    }

    /// <summary>Drops the backlog. The WAL drains deliver from the cursor.</summary>
    public void Clear()
    {
        runs.Clear();
        heldEntries = 0;
    }

    /// <summary>
    /// The lowest held entry above the applied cursor, after pruning entries a WAL drain delivered
    /// meanwhile. The first run holds it: runs are ordered by first id and each is consecutive, so once
    /// the first run is pruned to the cursor no later run can start below its next entry.
    /// </summary>
    private RaftLog? PeekNext()
    {
        long applied = coreState.LastAppliedIndex;

        while (runs.Count > 0)
        {
            Run head = runs[0];
            while (head.Next < head.Logs.Count && head.Logs[head.Next].Id <= applied)
            {
                head.Next++;
                heldEntries--;
            }

            if (head.Next < head.Logs.Count)
                return head.Logs[head.Next];

            runs.RemoveAt(0);
        }

        return null;
    }

    /// <summary>Whether the backlog holds the next id to deliver.</summary>
    private bool HeadIsNext() => PeekNext() is { } next && next.Id == coreState.LastAppliedIndex + 1;

    /// <summary>
    /// Queues a turn when there is deliverable work and none is queued: the backlog's head is next and
    /// committed, or the commit frontier is ahead of the cursor with a resolved row written at or above
    /// the next id (<see cref="IRaftWalFacade.GetReadableResolvedHighWater"/>, the gate the WAL drain
    /// applies itself; below it the drain would answer "not covered" without reading).
    /// </summary>
    private void Schedule()
    {
        if (turnPosted)
            return;

        long next = coreState.LastAppliedIndex + 1;
        long committed = wal.GetCommitIndex();
        bool deliverable = committed >= next
            && (HeadIsNext() || next <= wal.GetReadableResolvedHighWater());

        if (!deliverable)
            return;

        if (getPostToExecutor() is not { } post)
            return;

        turnPosted = true;

        try
        {
            post(new RaftRequest(RaftRequestType.ApplyCommittedEntries));
        }
        catch (InvalidOperationException)
        {
            // The executor is stopping and takes no new work. Nothing is lost: the entries are in the
            // WAL, and the restart's restore delivers them.
            turnPosted = false;
        }
    }
}
