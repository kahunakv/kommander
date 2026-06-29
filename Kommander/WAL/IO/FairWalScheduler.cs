using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.WAL.IO;

/// <summary>
/// Fair, partition-aware WAL write scheduler with cross-partition group commit.
///
/// <para>Design goals:</para>
/// <list type="bullet">
///   <item>FIFO write order within each partition — no operation is ever applied out
///       of submission order for the same partition.</item>
///   <item>Fair scheduling across partitions — a hot partition cannot starve others.
///       Each partition occupies at most one slot in the global ready-queue at a
///       time; partitions alternate in round-robin fashion as they produce work.</item>
///   <item><b>Cross-partition group commit:</b> when multiple partitions are ready
///       simultaneously, a single worker drains up to
///       <see cref="DefaultMaxGroupBatchPartitions"/> of them and issues ONE
///       <c>walAdapter.Write</c> call spanning all of them.  For RocksDB (which
///       shares a single WAL across all column families) this means one fsync
///       regardless of how many partitions are included — the dominant cost saving
///       in many-partition deployments.</item>
///   <item>Compatible writes are batched within each drain cycle to amortize
///       <c>walAdapter.Write</c> overhead.</item>
///   <item><b>Deferred group commit (opt-in):</b> group commit is otherwise purely
///       <em>opportunistic</em> — a worker fsyncs whatever happens to be ready the
///       instant it wakes. When a linger window is configured
///       (<c>RaftConfiguration.WalGroupCommitLingerMs</c>) the worker instead waits a
///       bounded, adaptive interval to let more partitions become ready and share the
///       fsync. This matters where ready partitions trickle in rather than arriving
///       together — notably the follower append path, whose appends are paced by
///       replication RPCs — so they would otherwise each force a near-solo fsync. See
///       <c>LingerForMoreReadyPartitions</c>.</item>
///   <item>Bounded per-partition queues with back-pressure: callers receive a
///       <see cref="BackpressureExceededException"/> when the per-partition depth
///       limit is reached rather than silently blocking indefinitely.</item>
///   <item>Graceful drain on <see cref="Stop"/> — all operations accepted before
///       <see cref="Stop"/> is called are guaranteed to complete (or error) before
///       worker threads exit.</item>
/// </list>
///
/// <para><b>Concurrency model:</b> up to <c>workerCount</c> workers can hold
/// write batches simultaneously.  The single-writer-per-partition invariant is
/// maintained by the <c>InFlight</c> flag: a partition included in one worker's
/// group batch cannot simultaneously appear in another worker's batch because its
/// ID is consumed from <c>_readyPartitions</c> (a drain-once queue) before
/// <c>InFlight</c> is set.</para>
/// </summary>
public sealed class FairWalScheduler : IRaftWalScheduler, IDisposable
{
    // ── Constants ──────────────────────────────────────────────────────────

    /// <summary>Default max operations drained from a partition in a single batch.</summary>
    public const int DefaultMaxBatchSize = 256;

    /// <summary>Default per-partition pending-queue depth limit.</summary>
    public const int DefaultMaxQueueDepth = 4096;

    /// <summary>
    /// Default maximum number of partitions coalesced into a single WAL write call.
    /// When multiple partitions are ready simultaneously a worker drains up to this
    /// many of them and issues one <c>walAdapter.Write</c> (one fsync for RocksDB).
    /// </summary>
    public const int DefaultMaxGroupBatchPartitions = 64;

    // ── Configuration ──────────────────────────────────────────────────────

    private readonly IWAL walAdapter;
    private readonly ILogger<IRaft> logger;
    private readonly int maxQueueDepthPerPartition;
    private readonly int maxGlobalQueueDepth;
    private readonly int maxBatchSize;
    private readonly int maxGroupBatchPartitions;

    /// <summary>
    /// Deferred group-commit linger window in <see cref="Stopwatch"/> ticks, or 0 when
    /// disabled. When &gt; 0 a worker that took its first ready partition keeps gathering
    /// more ready partitions (up to <see cref="maxGroupBatchPartitions"/>) until this much
    /// time elapses or a probe finds nothing newly ready — see the drain loop. 0 preserves
    /// the original purely-opportunistic batching.
    /// </summary>
    private readonly long lingerTicks;

    /// <summary>
    /// When true, the single-fsync commit fast path is active: a group batch whose logs are <b>all</b>
    /// per-entry <c>Committed</c> markers is written sync-off (no fsync of its own), riding the next
    /// durable write. Any batch containing a proposed entry, a <c>CommittedCheckpoint</c>, a rollback, or
    /// any other type is still written sync. Off (default) preserves byte-for-byte the prior always-sync
    /// behaviour. Wired from <see cref="RaftConfiguration.WalSingleFsyncCommit"/>.
    /// </summary>
    private readonly bool lazyCommitMarkers;

    // Total pending-or-in-flight operations across all partitions.
    // Checked against maxGlobalQueueDepth (when > 0) in Enqueue;
    // decremented in ProcessGroupBatch after each batch write completes.
    private int _globalQueueDepth;

    // ── Per-partition state ────────────────────────────────────────────────

    private sealed class PartitionState
    {
        /// <summary>Pending operations in submission order.</summary>
        public readonly Queue<WALWriteOperation> Ops = new();

        /// <summary>Guards <see cref="Ops"/>, <see cref="Scheduled"/>, <see cref="InFlight"/>, and <see cref="Depth"/>.</summary>
        public readonly object Lock = new();

        /// <summary>
        /// True when this partition's id is already in <see cref="FairWalScheduler._readyPartitions"/>.
        /// At most one copy of the id is ever present in the ready-queue for a given partition.
        /// </summary>
        public bool Scheduled;

        /// <summary>
        /// True while a worker thread is executing a WAL write that includes this partition.
        /// A concurrent <see cref="Enqueue"/> that sees <c>InFlight=true</c> must NOT
        /// add the partition to <c>_readyPartitions</c>; the post-I/O lock section
        /// will re-schedule once the write finishes.
        /// </summary>
        public bool InFlight;

        /// <summary>Current number of pending-or-in-flight operations.</summary>
        public int Depth;

        /// <summary>
        /// Per-partition EWMA of enqueue-to-durable latency in milliseconds.
        /// Updated by the worker thread after each successful Write batch.
        /// </summary>
        public readonly PartitionWaitAccumulator CommitWait = new();
    }

    private readonly ConcurrentDictionary<int, PartitionState> _partitions = new();

    // ── Global ready-queue ─────────────────────────────────────────────────

    // Partition IDs that have at least one pending operation waiting to be drained.
    // Bounded to 2× the max possible concurrent partitions to apply light
    // back-pressure at the signalling layer as well.
    private readonly BlockingCollection<int> _readyPartitions;

    // ── Worker threads ─────────────────────────────────────────────────────

    private readonly Thread[] _workers;
    private readonly CancellationTokenSource _cts = new();
    private volatile bool _stopping;
    private bool _started;

    // ── Observability ──────────────────────────────────────────────────────

    private long _totalBatchesWritten;
    private long _totalSyncBatchesWritten;
    private long _totalOperationsCompleted;
    private long _totalPartitionsBatched;

    /// <summary>
    /// Total number of <c>walAdapter.Write</c> calls dispatched to the storage adapter.
    /// Each group-commit batch counts as one call regardless of how many partitions it spans,
    /// so this is the count most directly correlated with fsync pressure on RocksDB.
    /// </summary>
    public long TotalBatchesWritten => Interlocked.Read(ref _totalBatchesWritten);

    /// <summary>
    /// Number of group batches dispatched with an fsync (sync=true) — the true fsync count on a durable
    /// backend. Equals <see cref="TotalBatchesWritten"/> when the single-fsync fast path is off (every
    /// batch fsyncs). With the fast path on, commit-only batches are written sync-off and excluded here,
    /// so this drops toward ~1× per committed write while <see cref="TotalBatchesWritten"/> stays ~2×
    /// (the number of <c>walAdapter.Write</c> calls is unchanged; only how many fsync differs).
    /// </summary>
    public long TotalSyncBatchesWritten => Interlocked.Read(ref _totalSyncBatchesWritten);

    /// <summary>Total number of individual operations completed.</summary>
    public long TotalOperationsCompleted => Interlocked.Read(ref _totalOperationsCompleted);

    /// <summary>
    /// Total number of partitions summed across every group batch. Divided by
    /// <see cref="TotalBatchesWritten"/> this gives the mean partitions-per-fsync — the
    /// batch density the deferred group-commit linger is designed to raise. Comparing this
    /// ratio with the linger on vs off shows directly whether the window is coalescing more
    /// partitions per fsync (the intended effect) or whether batches were already dense
    /// (in which case latency is queue-wait-bound, not batch-density-bound).
    /// </summary>
    public long TotalPartitionsBatched => Interlocked.Read(ref _totalPartitionsBatched);

    // ── Construction ──────────────────────────────────────────────────────

    /// <param name="walAdapter">Synchronous storage adapter invoked on worker threads.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="workerCount">
    /// Number of worker threads.  Allows up to this many partitions to flush
    /// concurrently.  Defaults to <see cref="Environment.ProcessorCount"/>.
    /// </param>
    /// <param name="maxQueueDepthPerPartition">
    /// Per-partition soft back-pressure limit.  Defaults to
    /// <see cref="DefaultMaxQueueDepth"/>.
    /// </param>
    /// <param name="maxBatchSize">
    /// Maximum operations drained from a partition in a single storage flush.
    /// Defaults to <see cref="DefaultMaxBatchSize"/>.
    /// </param>
    /// <param name="maxGlobalQueueDepth">
    /// Global cap on total pending-or-in-flight operations across all partitions.
    /// 0 or negative disables the global cap (only per-partition limits apply).
    /// </param>
    /// <param name="maxGroupBatchPartitions">
    /// Maximum number of partitions coalesced into a single <c>walAdapter.Write</c>
    /// call (one fsync for RocksDB).  Defaults to
    /// <see cref="DefaultMaxGroupBatchPartitions"/>.
    /// </param>
    /// <param name="groupCommitLingerMs">
    /// Deferred group-commit linger window in milliseconds (see
    /// <c>RaftConfiguration.WalGroupCommitLingerMs</c>). 0 (default) keeps the original
    /// opportunistic batching; &gt; 0 lets a worker briefly wait to gather more ready
    /// partitions into one fsync, bailing early under low load.
    /// </param>
    /// <param name="lazyCommitMarkers">
    /// Single-fsync commit fast path (see <c>RaftConfiguration.WalSingleFsyncCommit</c>). When true, a
    /// group batch whose logs are all per-entry <c>Committed</c> markers is written sync-off so it rides
    /// the next durable write instead of forcing its own fsync. Off (default) keeps every batch sync.
    /// </param>
    public FairWalScheduler(
        IWAL walAdapter,
        ILogger<IRaft> logger,
        int workerCount = 0,
        int maxQueueDepthPerPartition = DefaultMaxQueueDepth,
        int maxBatchSize = DefaultMaxBatchSize,
        int maxGlobalQueueDepth = 0,
        int maxGroupBatchPartitions = DefaultMaxGroupBatchPartitions,
        int groupCommitLingerMs = 0,
        bool lazyCommitMarkers = false)
    {
        this.walAdapter = walAdapter;
        this.logger = logger;
        this.maxQueueDepthPerPartition = maxQueueDepthPerPartition;
        this.maxGlobalQueueDepth = maxGlobalQueueDepth > 0 ? maxGlobalQueueDepth : 0;
        this.maxBatchSize = maxBatchSize <= 0 ? DefaultMaxBatchSize : maxBatchSize;
        this.maxGroupBatchPartitions = maxGroupBatchPartitions <= 0 ? DefaultMaxGroupBatchPartitions : maxGroupBatchPartitions;
        this.lingerTicks = groupCommitLingerMs > 0
            ? (long)(groupCommitLingerMs / 1000.0 * global::System.Diagnostics.Stopwatch.Frequency)
            : 0;
        this.lazyCommitMarkers = lazyCommitMarkers;

        if (workerCount <= 0)
            workerCount = Math.Max(1, Environment.ProcessorCount);

        _readyPartitions = new BlockingCollection<int>(boundedCapacity: workerCount * 64);
        _workers = new Thread[workerCount];
    }

    // ── IRaftWalScheduler ──────────────────────────────────────────────────

    /// <inheritdoc/>
    /// <exception cref="BackpressureExceededException">
    /// Thrown when the per-partition pending queue is at or above
    /// <see cref="maxQueueDepthPerPartition"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the scheduler has been stopped or not yet started.
    /// </exception>
    public void Enqueue(WALWriteOperation operation)
    {
        if (_stopping)
            throw new InvalidOperationException("FairWalScheduler: scheduler is stopping; no new operations accepted.");

        if (!_started)
            throw new InvalidOperationException("FairWalScheduler: call Start() before Enqueue().");

        int partitionId = operation.Logs.PartitionId;
        PartitionState state = _partitions.GetOrAdd(partitionId, _ => new PartitionState());

        lock (state.Lock)
        {
            if (state.Depth >= maxQueueDepthPerPartition)
                throw new BackpressureExceededException(partitionId, state.Depth);

            if (maxGlobalQueueDepth > 0)
            {
                int globalDepth = Interlocked.Increment(ref _globalQueueDepth);
                if (globalDepth > maxGlobalQueueDepth)
                {
                    Interlocked.Decrement(ref _globalQueueDepth);
                    throw new BackpressureExceededException(partitionId, globalDepth);
                }
            }

            operation.EnqueueTicks = global::System.Diagnostics.Stopwatch.GetTimestamp();
            state.Ops.Enqueue(operation);
            state.Depth++;

            // Schedule the partition in the global ready-queue only if it is not
            // already scheduled AND no worker is currently executing I/O for it.
            // If InFlight=true a worker is mid-write; that worker's post-I/O lock
            // section will re-schedule once the write finishes.
            if (!state.Scheduled && !state.InFlight)
            {
                state.Scheduled = true;
                // TryAdd with a short timeout so we don't block the caller.
                // If the ready-queue is full (workers are overloaded), we still
                // accept the op but wake a worker via a forced add.
                if (!_readyPartitions.TryAdd(partitionId))
                    _readyPartitions.Add(partitionId, _cts.Token);
            }
        }
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// <summary>Starts the worker threads.  Must be called exactly once before <see cref="Enqueue"/>.</summary>
    public void Start()
    {
        if (_started)
            return;

        _started = true;
        KommanderMetrics.RegisterScheduler(this);

        for (int i = 0; i < _workers.Length; i++)
        {
            int workerId = i;
            _workers[i] = new Thread(() => WorkerLoop(workerId))
            {
                IsBackground = true,
                Name = $"FairWalScheduler-{workerId}",
            };
            _workers[i].Start();
        }
    }

    /// <summary>
    /// Returns a best-effort snapshot of per-partition pending-or-in-flight depths.
    /// Used by <see cref="KommanderMetrics"/> to populate the
    /// <c>raft.wal.queue_depth</c> observable gauge.  No lock is held during
    /// iteration; values are approximate (suitable for metrics).
    /// </summary>
    internal IEnumerable<(int partitionId, int depth)> SnapshotPartitionDepths()
    {
        foreach (KeyValuePair<int, PartitionState> kv in _partitions)
            yield return (kv.Key, kv.Value.Depth);
    }

    /// <summary>
    /// Returns the current pending-or-in-flight depth for a single partition, or 0 if the
    /// partition has no queued work. The value is approximate (no lock held) and suitable
    /// for advisory load scoring only.
    /// </summary>
    public int GetPartitionDepth(int partitionId) =>
        _partitions.TryGetValue(partitionId, out PartitionState? state) ? state.Depth : 0;

    /// <summary>
    /// Returns the current EWMA enqueue-to-durable commit-wait latency in milliseconds
    /// for the given partition, or <c>0</c> if no write batch has yet completed for it.
    /// The value is approximate and suitable for advisory saturation detection only.
    /// </summary>
    public double GetPartitionCommitWaitMs(int partitionId) =>
        _partitions.TryGetValue(partitionId, out PartitionState? state) ? state.CommitWait.CurrentWaitMs() : 0.0;

    /// <summary>
    /// Stops the scheduler.
    ///
    /// <para>When called, no new operations are accepted.  All operations that were
    /// already <see cref="Enqueue"/>d before this call will be processed and their
    /// <c>OnComplete</c> callbacks fired before worker threads exit.  Callers should
    /// wait for all expected completions before assuming the system is quiescent.</para>
    /// </summary>
    public void Stop()
    {
        if (!_started)
            return; // Start() was never called; nothing to stop.

        _stopping = true;

        // Give workers a signal to drain remaining items and then exit.
        // We do this by cancelling after a best-effort drain pass.
        // Workers complete in-flight batches and then re-check the queue.
        // Once all queues are empty and cancellation is signalled, they exit.
        _cts.Cancel();

        foreach (Thread worker in _workers)
            worker.Join();
    }

    // ── Worker ─────────────────────────────────────────────────────────────

    private void WorkerLoop(int workerId)
    {
        // Per-worker reusable scratch space — never crosses iteration or worker boundaries.
        List<int> partitionGroup = new(maxGroupBatchPartitions);
        List<(int PartitionId, List<WALWriteOperation> Ops)> groupBatches = new(maxGroupBatchPartitions);

        // Pre-allocated per-partition operation lists: one slot per possible group-batch partition.
        // Cleared and reused each iteration instead of allocating a new List per partition per flush.
        List<List<WALWriteOperation>> opListPool = new(maxGroupBatchPartitions);
        for (int i = 0; i < maxGroupBatchPartitions; i++)
            opListPool.Add(new List<WALWriteOperation>(maxBatchSize));

        // Pre-allocated log-groups list passed to walAdapter.Write — cleared and reused each flush.
        List<(int, List<RaftLog>)> logGroups = new(maxGroupBatchPartitions);

        CancellationToken token = _cts.Token;

        while (true)
        {
            // Block until at least one partition is ready.
            int partitionId;
            try
            {
                partitionId = _readyPartitions.Take(token);
            }
            catch (OperationCanceledException)
            {
                // Drain remaining items before exiting.
                DrainRemaining(partitionGroup, groupBatches, opListPool, logGroups);
                break;
            }
            catch (ObjectDisposedException)
            {
                // Collection was disposed during shutdown; exit immediately.
                break;
            }

            // Gather additional ready partitions — the group-commit coalescing step.
            // Up to maxGroupBatchPartitions−1 extra partitions join this worker's batch,
            // all sharing the same Write call (one fsync for RocksDB).
            partitionGroup.Clear();
            partitionGroup.Add(partitionId);

            // (1) Opportunistic sweep: take whatever is *already* ready, non-blocking.
            while (partitionGroup.Count < maxGroupBatchPartitions && _readyPartitions.TryTake(out int extraId))
                partitionGroup.Add(extraId);

            // (2) Deferred linger (opt-in): if the batch is not yet full and a window is
            //     configured, briefly wait for more partitions to become ready so they
            //     share this fsync instead of forcing their own. Adaptive and bounded:
            //     we bail the instant a probe finds nothing newly ready (so sequential /
            //     low-overlap load pays at most one probe, not the whole window), and the
            //     total wait never exceeds lingerTicks. This is the lever for paths whose
            //     ready partitions trickle in (e.g. follower appends paced by replication
            //     RPCs) rather than arriving all at once like a leader's local proposes.
            if (lingerTicks > 0 && partitionGroup.Count < maxGroupBatchPartitions)
                LingerForMoreReadyPartitions(partitionGroup, token);

            ProcessGroupBatch(partitionGroup, groupBatches, opListPool, logGroups);
        }
    }

    /// <summary>Per-probe wait while lingering. BlockingCollection timeouts have ~1 ms
    /// granularity, so this is the smallest useful slice; the cumulative wait is bounded by
    /// <see cref="lingerTicks"/>, not by this value.</summary>
    private const int LingerProbeMs = 1;

    /// <summary>
    /// Deferred group-commit: after the opportunistic sweep, keep pulling newly-ready
    /// partitions into <paramref name="partitionGroup"/> so they share this worker's single
    /// fsync, until the batch is full, the linger window (<see cref="lingerTicks"/>) elapses,
    /// or a probe finds nothing newly ready.
    ///
    /// <para>The early bail on an empty probe is what makes this <b>adaptive</b>:
    /// <see cref="System.Collections.Concurrent.BlockingCollection{T}.TryTake(out T,int,CancellationToken)"/>
    /// returns immediately while partitions are already queued (a dense burst is gathered at
    /// full speed) but blocks up to one short probe when the queue drains — and a probe that
    /// comes back empty means the arrival burst has paused, so we stop rather than wait out
    /// the whole window. Sequential / low-overlap load therefore pays at most one probe.</para>
    ///
    /// <para>Only <em>when</em> the group fsync fires changes; never <em>whether</em> a write
    /// is durable before its completion callback runs. Per-partition ordering and the
    /// <c>Scheduled</c>/<c>InFlight</c> ready-queue discipline are untouched.</para>
    /// </summary>
    private void LingerForMoreReadyPartitions(List<int> partitionGroup, CancellationToken token)
    {
        long deadline = global::System.Diagnostics.Stopwatch.GetTimestamp() + lingerTicks;

        while (partitionGroup.Count < maxGroupBatchPartitions
               && global::System.Diagnostics.Stopwatch.GetTimestamp() < deadline)
        {
            int extraId;
            try
            {
                if (!_readyPartitions.TryTake(out extraId, LingerProbeMs, token))
                    break; // queue paused within the probe → burst over, stop lingering.
            }
            catch (OperationCanceledException)
            {
                break; // shutdown: process what we have; WorkerLoop's next Take drains the rest.
            }
            catch (ObjectDisposedException)
            {
                break;
            }

            partitionGroup.Add(extraId);
        }
    }

    /// <summary>
    /// Core group-commit routine.
    ///
    /// <para>Phase 1 — lock each partition, drain up to <see cref="maxBatchSize"/>
    /// ops, and mark <c>InFlight=true</c>.  The partition's slot in
    /// <c>_readyPartitions</c> has already been consumed (the caller dequeued it),
    /// so no other worker can race to the same partition.</para>
    ///
    /// <para>Phase 2 — issue a single <c>walAdapter.Write</c> spanning all drained
    /// partitions.  For RocksDB this is one <c>db.Write</c> / one fsync regardless
    /// of partition count.</para>
    ///
    /// <para>Phase 3 — per-partition post-write: run <c>FollowerAppend</c>
    /// truncation, fire <c>OnComplete</c> callbacks, decrement depth, clear
    /// <c>InFlight</c>, and re-schedule if new ops arrived during the write.</para>
    /// </summary>
    private void ProcessGroupBatch(
        List<int> partitionIds,
        List<(int PartitionId, List<WALWriteOperation> Ops)> groupBatches,
        List<List<WALWriteOperation>> opListPool,
        List<(int, List<RaftLog>)> logGroups)
    {
        groupBatches.Clear();

        // ── Phase 1: drain ops per partition, mark InFlight ────────────────
        foreach (int pid in partitionIds)
        {
            if (!_partitions.TryGetValue(pid, out PartitionState? state))
                continue;

            // Reuse the pre-allocated list for this group-batch slot; clear before filling.
            // Grow on demand so DrainRemaining (which drains without a partition-count cap) never
            // overflows the pool. Once grown to the high-water mark, no further allocation occurs.
            if (groupBatches.Count >= opListPool.Count)
                opListPool.Add(new List<WALWriteOperation>(maxBatchSize));
            List<WALWriteOperation> pidBatch = opListPool[groupBatches.Count];
            pidBatch.Clear();

            lock (state.Lock)
            {
                while (pidBatch.Count < maxBatchSize && state.Ops.TryDequeue(out WALWriteOperation? op))
                    pidBatch.Add(op);

                // Clear Scheduled (the id has been consumed from _readyPartitions)
                // and raise InFlight so Enqueue cannot re-add this partition while
                // the combined Write is in progress.
                state.Scheduled = false;
                state.InFlight  = true;
            }

            if (pidBatch.Count > 0)
            {
                groupBatches.Add((pid, pidBatch));
            }
            else
            {
                // Nothing to write; clear InFlight immediately.
                lock (state.Lock)
                    state.InFlight = false;
            }
        }

        if (groupBatches.Count == 0)
            return;

        // ── Phase 2: single cross-partition WAL write ──────────────────────
        RaftOperationStatus status;

        try
        {
            // Reuse the worker-owned logGroups list — cleared here, populated below.
            logGroups.Clear();

            // Single-fsync fast path: the batch may skip its own fsync only if EVERY log in it is a
            // per-entry Committed marker (already quorum-durable from its propose fsync, so a lost marker
            // is reconstructible). Any proposed entry, CommittedCheckpoint (the durable recovery anchor),
            // rollback, or other type forces a sync write — and that sync write also flushes any sync-off
            // markers batched alongside it. Off by default ⇒ always sync ⇒ byte-for-byte prior behaviour.
            bool sync = !lazyCommitMarkers;

            foreach ((_, List<WALWriteOperation> ops) in groupBatches)
                foreach (WALWriteOperation op in ops)
                {
                    logGroups.Add(op.Logs);
                    if (!sync && !AllCommittedMarkers(op.Logs.Logs))
                        sync = true;
                }

            status = walAdapter.Write(logGroups, sync);
            Interlocked.Increment(ref _totalBatchesWritten);
            if (sync)
                Interlocked.Increment(ref _totalSyncBatchesWritten);
            Interlocked.Add(ref _totalPartitionsBatched, groupBatches.Count);
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[FairWalScheduler] Group WAL write error ({PartitionCount} partitions): {Message}",
                groupBatches.Count, ex.Message);
            status = RaftOperationStatus.Errored;
        }

        // Stamp completion time once for all ops in this group batch.
        long doneAtTicks = global::System.Diagnostics.Stopwatch.GetTimestamp();
        double ticksToMs = 1000.0 / global::System.Diagnostics.Stopwatch.Frequency;

        KommanderMetrics.WalBatchesTotal.Add(1);

        // ── Phase 3: per-partition post-write cleanup ──────────────────────
        foreach ((int pid, List<WALWriteOperation> pidBatch) in groupBatches)
        {
            KommanderMetrics.WalBatchSize.Record(pidBatch.Count);

            // Record commit-wait latency for this partition's batch (enqueue→durable).
            // Average across all ops so each batch contributes one observation to the EWMA,
            // regardless of batch size, giving consistent per-partition decay behaviour.
            if (_partitions.TryGetValue(pid, out PartitionState? waitState))
            {
                // One volatile read decides whether to attribute per-op latency to its
                // phase for the double-fsync measurement; off by default, no cost in prod.
                bool instrument = WalPhaseInstrumentation.Enabled;
                double totalWaitMs = 0;
                foreach (WALWriteOperation op in pidBatch)
                {
                    double opWaitMs = (doneAtTicks - op.EnqueueTicks) * ticksToMs;
                    totalWaitMs += opWaitMs;
                    if (instrument)
                        WalPhaseInstrumentation.RecordDurable(op.Type, opWaitMs);
                }
                waitState.CommitWait.RecordWaitMs(totalWaitMs / pidBatch.Count);
            }

            if (status == RaftOperationStatus.Success)
            {
                foreach (WALWriteOperation op in pidBatch)
                {
                    if (op.Type == WALWriteOperationType.FollowerAppend && op.LogIndex > 0)
                        walAdapter.TruncateLogsAfter(pid, op.LogIndex);
                }
            }

            foreach (WALWriteOperation op in pidBatch)
            {
                try
                {
                    op.OnComplete(BuildCompletion(op, status));
                    Interlocked.Increment(ref _totalOperationsCompleted);
                    KommanderMetrics.WalOperationsTotal.Add(1);
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        "[FairWalScheduler] OnComplete callback for partition {PartitionId} op {OperationId} threw: {Message}",
                        pid, op.OperationId, ex.Message);
                }
            }

            if (maxGlobalQueueDepth > 0)
                Interlocked.Add(ref _globalQueueDepth, -pidBatch.Count);

            if (!_partitions.TryGetValue(pid, out PartitionState? state))
                continue;

            lock (state.Lock)
            {
                state.Depth   -= pidBatch.Count;
                state.InFlight = false;

                if (state.Ops.Count > 0 && !state.Scheduled)
                {
                    // Only mark Scheduled=true when the partition ID is actually
                    // inserted into the ready-queue.  If the bounded queue is full,
                    // TryAdd returns false; leaving Scheduled=false lets the next
                    // Enqueue call re-schedule the partition rather than stranding it.
                    if (_readyPartitions.TryAdd(pid))
                        state.Scheduled = true;
                }
            }
        }
    }

    /// <summary>
    /// After the CTS fires, drain any partitions that were already in
    /// the ready-queue but not yet processed.
    /// </summary>
    private void DrainRemaining(
        List<int> partitionGroup,
        List<(int PartitionId, List<WALWriteOperation> Ops)> groupBatches,
        List<List<WALWriteOperation>> opListPool,
        List<(int, List<RaftLog>)> logGroups)
    {
        // Drain everything still in the ready-queue as one group-commit batch.
        partitionGroup.Clear();
        while (_readyPartitions.TryTake(out int pid))
            partitionGroup.Add(pid);

        if (partitionGroup.Count > 0)
            ProcessGroupBatch(partitionGroup, groupBatches, opListPool, logGroups);

        // Also sweep all partition states for any items that were enqueued
        // but whose partitionId was not yet in the ready-queue (should not
        // happen in normal operation, but guards against race at shutdown).
        // IMPORTANT: skip partitions that are currently in-flight (InFlight=true).
        // The worker executing that batch will clear InFlight in its post-I/O
        // lock section and re-add the partition to _readyPartitions if items
        // remain; this worker's own DrainRemaining first phase (TryTake loop)
        // will then pick it up, preserving the single-writer-per-partition
        // invariant even during shutdown.
        foreach (KeyValuePair<int, PartitionState> kv in _partitions)
        {
            PartitionState state = kv.Value;
            bool shouldProcess;
            lock (state.Lock)
                shouldProcess = state.Ops.Count > 0 && !state.InFlight;

            if (shouldProcess)
            {
                partitionGroup.Clear();
                partitionGroup.Add(kv.Key);
                ProcessGroupBatch(partitionGroup, groupBatches, opListPool, logGroups);
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// <summary>
    /// True when every log in <paramref name="logs"/> is a per-entry <see cref="RaftLogType.Committed"/>
    /// marker — the only type the single-fsync fast path may write sync-off. Notably excludes
    /// <see cref="RaftLogType.CommittedCheckpoint"/> (the durable recovery anchor, which must always fsync)
    /// and every first-durability type (Proposed/ProposedCheckpoint) and rollback marker. An empty list is
    /// not eligible (returns false), so it can never spuriously suppress a fsync.
    /// </summary>
    private static bool AllCommittedMarkers(List<RaftLog> logs)
    {
        if (logs.Count == 0)
            return false;

        foreach (RaftLog log in logs)
        {
            if (log.Type != RaftLogType.Committed)
                return false;
        }
        return true;
    }

    private static RaftWalCompletion BuildCompletion(WALWriteOperation op, RaftOperationStatus status)
    {
        List<RaftLog> logs = op.Logs.Logs;
        long minIndex = -1;
        if (logs.Count > 0)
        {
            minIndex = logs[0].Id;
            for (int i = 1; i < logs.Count; i++)
                if (logs[i].Id < minIndex) minIndex = logs[i].Id;
        }

        return new RaftWalCompletion(
            PartitionId: op.Logs.PartitionId,
            OperationId: op.OperationId,
            Term: op.Term,
            MinLogIndex: minIndex,
            MaxLogIndex: op.LogIndex,
            OperationType: op.Type,
            Status: status
        );
    }

    // ── IDisposable ────────────────────────────────────────────────────────

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        // Join all workers before disposing managed resources so that no worker
        // thread can touch _readyPartitions after it is disposed.
        if (!_stopping)
            Stop();
        _cts.Dispose();
        _readyPartitions.Dispose();
    }
}

/// <summary>
/// Thrown by <see cref="FairWalScheduler.Enqueue"/> when a partition's pending-write
/// queue has reached the configured depth limit.
/// </summary>
public sealed class BackpressureExceededException : Exception
{
    public int PartitionId { get; }
    public int CurrentDepth { get; }

    public BackpressureExceededException(int partitionId, int currentDepth)
        : base($"FairWalScheduler: partition {partitionId} queue depth {currentDepth} exceeded limit.")
    {
        PartitionId = partitionId;
        CurrentDepth = currentDepth;
    }
}
