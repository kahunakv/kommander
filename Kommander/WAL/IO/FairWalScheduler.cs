using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Scheduling;
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
///       bounded, evidence-gated interval to let more partitions become ready and share
///       the fsync — but only while some other partition has queued work that must
///       become ready soon; with nothing to wait for it flushes immediately.
///       This matters where ready partitions trickle in rather than arriving
///       together — notably the follower append path, whose appends are paced by
///       replication RPCs — so they would otherwise each force a near-solo fsync. See
///       <c>LingerForMoreReadyPartitions</c>.</item>
///   <item><b>Return gathering (a store that serializes its synced writes):</b> a synced write is
///       not started while another one is in progress that carries partitions expected back, and
///       after it ends the next waits a short, bounded moment for those partitions. Without this, partitions
///       written in a closed loop split into two groups that take turns on the engine, and every
///       round costs two syncs: the other group's and its own. See
///       <c>GatherForSharedSync</c>.</item>
///   <item>Bounded per-partition queues with back-pressure: callers receive a
///       <see cref="BackpressureExceededException"/> when the per-partition depth
///       limit is reached rather than silently blocking indefinitely.</item>
///   <item>Graceful drain on <see cref="Stop"/> — all operations accepted before
///       <see cref="Stop"/> is called are guaranteed to complete (or error) before
///       worker threads exit.</item>
///   <item>Cheap idle workers: the ready-queue is a <see cref="ParkedWorkerQueue{T}"/>. A worker
///       with nothing to write is parked on its own event, and an enqueue that makes a partition
///       ready wakes one worker, or none while a worker is in its short spin for the next id. The
///       queue used to be a <c>BlockingCollection</c>, where every idle worker spun before it
///       parked on every take; at ten thousand small batches a second that was two thirds of
///       the workers' CPU.</item>
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

    /// <summary>
    /// Tick source for enqueue stamps, the group-commit linger window, and completion latency.
    /// Comes from <see cref="RaftConfiguration.TickSource"/>, so a deterministic run measures WAL
    /// latency on simulated ticks instead of the process clock.
    /// </summary>
    private readonly Kommander.Time.IMonotonicTickSource tickSource;
    private readonly ILogger<IRaft> logger;
    private readonly int maxQueueDepthPerPartition;
    private readonly int maxGlobalQueueDepth;
    private readonly int maxBatchSize;
    private readonly int maxGroupBatchPartitions;

    /// <summary>
    /// Deferred group-commit linger window in <see cref="Stopwatch"/> ticks, or 0 when
    /// disabled. When &gt; 0 a worker that took its first ready partition keeps gathering
    /// more ready partitions (up to <see cref="maxGroupBatchPartitions"/>) until this much
    /// time elapses or no partition outside its group has queued work left to wait for —
    /// see <see cref="LingerForMoreReadyPartitions"/>. 0 preserves the original
    /// purely-opportunistic batching.
    /// </summary>
    private readonly long lingerTicks;

    /// <summary>
    /// True when the linger is enabled, so <see cref="_queuedOperations"/> is maintained.
    /// Kept conditional so a linger-off scheduler stays byte-for-byte the prior behaviour,
    /// with no extra interlocked traffic on the hot enqueue path.
    /// </summary>
    private readonly bool trackQueuedOps;

    /// <summary>
    /// When true, the single-fsync commit fast path is active: a group batch whose logs are <b>all</b>
    /// per-entry <c>Committed</c> markers is written sync-off (no fsync of its own), riding the next
    /// durable write. Any batch containing a proposed entry, a <c>CommittedCheckpoint</c>, a rollback, or
    /// any other type is still written sync, and so is any operation flagged
    /// <see cref="WALWriteOperation.RequiresSync"/>: a <c>Committed</c> row the enqueuing partition has
    /// never held is a first-durability write whatever its type says, and only that partition can
    /// tell. Off (default) preserves byte-for-byte the prior always-sync behaviour. Wired from
    /// <see cref="RaftConfiguration.WalSingleFsyncCommit"/>.
    /// </summary>
    private readonly bool lazyCommitMarkers;

    /// <summary>
    /// A bound for how long a batch that holds only lazy commit markers may wait for its partition's
    /// next write before it is written on its own, for a caller that turns the deferral on. See
    /// <see cref="TryDeferMarkerBatch"/>.
    /// </summary>
    public const int DefaultMarkerDeferralMs = 2;

    /// <summary>
    /// The deferral bound on the tick source; zero means marker batches are never held. Zero unless
    /// the constructor is given a bound, because the deferral is only right when nothing waits for a
    /// marker's completion. The manager gives one with the single-fsync fast path, where the leader's
    /// commit takes effect when the propose quorum is durable
    /// (<c>WalCompletionRouter.CommitAtQuorumAsync</c>). Before that existed the consumer was told
    /// about a commit at the marker's completion; a consumer that waits for that before it sends
    /// more work then waited for a write its own next proposal was meant to carry, and was released
    /// only by the bound: measured on one, the deferral cut the round from 8.9 ms to 5.9 ms and
    /// still lost a third of the throughput.
    /// </summary>
    private readonly long markerDeferralTicks;

    /// <summary>
    /// Synced <c>walAdapter.Write</c> calls in progress across all workers. A sync-off write issued
    /// while one of them runs does not return until the engine finishes that sync, so this is what
    /// tells <see cref="TryDeferMarkerBatch"/> that writing a marker batch now would cost a sync.
    /// </summary>
    private int _syncWritesInFlight;

    /// <summary>
    /// Ids of partitions that hold a deferred marker batch (<see cref="PartitionState.DeferredMarkers"/>).
    /// An id may be stale: the partition's next batch takes the markers without touching this queue.
    /// The reader checks the partition under its lock.
    /// </summary>
    private readonly ConcurrentQueue<int> _deferredMarkerPartitions = new();

    private long _totalMarkerBatchesDeferred;

    /// <summary>
    /// Number of times a batch of lazy commit markers was held back to ride its partition's next
    /// write instead of being written on its own behind another partition's sync.
    /// </summary>
    public long TotalMarkerBatchesDeferred => Interlocked.Read(ref _totalMarkerBatchesDeferred);

    // ── Return gathering ───────────────────────────────────────────────────

    /// <summary>
    /// Default upper bound of the return window: how long a synced write may be held for partitions
    /// that are expected back. See <see cref="GatherForSharedSync"/>.
    /// </summary>
    public static readonly TimeSpan DefaultReturnWindow = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// A return window below this is not worth a wait: waking the held worker costs tens of
    /// microseconds, which is then most of what the wait could save.
    /// </summary>
    private static readonly TimeSpan MinReturnWindow = TimeSpan.FromMicroseconds(100);

    /// <summary>
    /// The longest a worker waits for a synced write in progress before it writes anyway, as a
    /// multiple of the usual synced write and never less than <see cref="MinInFlightWaitCap"/>. The
    /// wait itself costs nothing on a store that serializes its writers (the write would wait inside
    /// the engine instead); the cap only keeps a stalled device, or an error in this accounting, from
    /// turning a wait into a hang.
    /// </summary>
    private const int InFlightWaitCapSyncWrites = 8;

    private static readonly TimeSpan MinInFlightWaitCap = TimeSpan.FromMilliseconds(10);

    /// <summary>A partition that was waited for in vain needs at most this many quick returns in a row to be expected again.</summary>
    private const int MaxQuickReturnsNeeded = 32;

    /// <summary>Expected returns that came, in a row, after which one earlier miss is forgiven (the need halves).</summary>
    private const int ReturnsToForgiveMiss = 8;

    /// <summary>
    /// True when this scheduler gathers returning partitions: the store serializes its synced writes
    /// (<see cref="IWAL.SerializesSyncedWrites"/>), worker threads run the writes, and the caller gave
    /// a window. Otherwise every field below stays at its initial value and no path reads it.
    /// </summary>
    private readonly bool returnGathering;

    /// <summary>Upper bound of the return window on the tick source; the window in use is <see cref="ReturnWindowTicks"/>.</summary>
    private readonly long maxReturnWindowTicks;

    private readonly long minReturnWindowTicks;

    private readonly long minInFlightWaitCapTicks;

    /// <summary>
    /// Moving average of how long a synced engine write takes when no other synced write is in
    /// progress: the cost of one sync, which is what a gathered partition saves. Zero until the first
    /// such write. Plain racy updates; it sizes a wait and never decides what is written.
    /// </summary>
    private long _syncWriteEwmaTicks;

    /// <summary>
    /// Synced writes that carry at least one partition expected back, and that are in the engine or
    /// have left it without those partitions being marked yet. A gathering worker waits while this
    /// is not zero. It is apart from <see cref="_syncWritesInFlight"/> for two reasons. That one
    /// counts every synced write, and a write nobody is expected back from is not worth a wait. And
    /// it goes down as soon as the engine answers: a worker released at that point would see nobody
    /// expected back yet and write at once.
    /// </summary>
    private int _unsettledSyncWrites;

    /// <summary>Partitions with <see cref="PartitionState.AwaitedReturn"/> set.</summary>
    private int _awaitedReturns;

    /// <summary>Tick at which the partitions of the last synced write stop being waited for.</summary>
    private long _returnDeadlineTicks;

    /// <summary>
    /// Ids of partitions that are, or were, expected back, at most once each
    /// (<see cref="PartitionState.AwaitedQueued"/>). Read only by <see cref="ExpireAwaitedReturns"/>,
    /// which needs to find the ones that did not come without a scan of every partition.
    /// </summary>
    private readonly ConcurrentQueue<int> _awaitedPartitions = new();

    /// <summary>
    /// Bumped, with a full fence, whenever what a gathering worker waits for can have changed: a
    /// synced write settled, or the last expected partition came back. The worker reads it before it decides to
    /// wait and sleeps only while it is unchanged (<c>ParkedWorkerQueue.TryTakeWhile</c>).
    /// </summary>
    private long _gatherEpoch;

    private long _totalReturnsAwaited;
#if KOMMANDER_THREAD_FREE
    // The thread-free build has no worker to gather, so these three stay zero there.
#pragma warning disable CS0649
#endif
    private long _totalReturnGathers;
    private long _totalReturnsMissed;
    private long _totalGatherHandoffs;
#if KOMMANDER_THREAD_FREE
#pragma warning restore CS0649
#endif

#if !KOMMANDER_THREAD_FREE
    /// <summary>Guards <see cref="_gathererWorker"/> (writes), <see cref="_gathererHeld"/> and <see cref="_gatherHandoff"/>.</summary>
    private readonly object _gatherLock = new();

    /// <summary>Slot index of the worker that gathers, or -1. One worker gathers at a time.</summary>
    private int _gathererWorker = -1;

    /// <summary>Partitions the gathering worker holds, with those handed to it and not yet taken.</summary>
    private int _gathererHeld;

    /// <summary>
    /// Partition ids another worker took from the ready-queue while one gathers, given to the
    /// gathering worker so they are written with its batch and not in a write of their own.
    /// </summary>
    private readonly List<int> _gatherHandoff = [];
#endif

    /// <summary>
    /// Times a worker held a batch back: for a synced write in progress, or for partitions expected
    /// back. Against <see cref="TotalSyncBatchesWritten"/> it shows how often the gathering engages.
    /// </summary>
    public long TotalReturnGathers => Interlocked.Read(ref _totalReturnGathers);

    /// <summary>Times a partition was marked as expected back after a synced write.</summary>
    public long TotalReturnsAwaited => Interlocked.Read(ref _totalReturnsAwaited);

    /// <summary>
    /// Times a partition that a batch was held for did not come back inside the window. Each one
    /// cost that batch the rest of the window; a rate that stays high means the window does not fit
    /// the workload.
    /// </summary>
    public long TotalReturnsMissed => Interlocked.Read(ref _totalReturnsMissed);

    /// <summary>Times a worker gave the partitions it had taken to the gathering worker.</summary>
    public long TotalGatherHandoffs => Interlocked.Read(ref _totalGatherHandoffs);

    /// <summary>Test seam: the moving average of a synced write, in milliseconds.</summary>
    internal double SyncWriteAverageMs => Volatile.Read(ref _syncWriteEwmaTicks) * 1000.0 / tickSource.Frequency;

    /// <summary>
    /// Test seam: sets the moving average of a synced write and stops real writes from moving it, so
    /// a test with a store that answers at once gets a return window of a known size.
    /// </summary>
    internal void SeedSyncWriteAverageForTesting(TimeSpan duration)
    {
        _syncWriteAveragePinned = true;
        Volatile.Write(ref _syncWriteEwmaTicks, (long)(duration.TotalSeconds * tickSource.Frequency));
    }

    /// <summary>Test seam: true once a test set the average; real writes then no longer move it.</summary>
    private volatile bool _syncWriteAveragePinned;

    /// <summary>
    /// Test seam: gives a partition the history of one that returns at once, so the next synced write
    /// that carries its work marks it as expected back. Saves a test the rounds that build it.
    /// </summary>
    internal void ExpectReturnsForTesting(int partitionId)
    {
        PartitionState state = _partitions.GetOrAdd(partitionId, static (_, source) => new PartitionState(source), tickSource);

        lock (state.Lock)
            state.QuickReturns = state.QuickReturnsNeeded;
    }

    /// <summary>
    /// Test-only: receives a Write event per operation after a successful batch write, and a Truncate
    /// event after a successful proposed-tail truncation, so the TLA+ trace validator can keep its
    /// model of the disk in step with this store (see <see cref="Diagnostics.IWalTraceSink"/>).
    /// Null in production. Set once, by the manager, before the scheduler runs.
    /// </summary>
    internal Diagnostics.IWalTraceSink? TraceSink { get; set; }

    /// <summary>
    /// True when this scheduler owns no worker threads and a caller drives every flush through
    /// <see cref="PumpOnce"/> or <see cref="PumpUntilIdle"/>.
    /// <para>Only a deterministic simulation sets this. Writes stay deferred rather than running
    /// inline, which is the point: "the write completed" becomes an explicit, schedulable event
    /// instead of something that happens whenever a worker thread is next scheduled. The
    /// completion path is unchanged — it still reports through
    /// <c>WALWriteOperation.OnComplete</c> — so nothing in the consensus path can tell the
    /// difference except when the callback arrives.</para>
    /// </summary>
    private readonly bool manualExecution;

    // Total pending-or-in-flight operations across all partitions.
    // Checked against maxGlobalQueueDepth (when > 0) in Enqueue;
    // decremented in ProcessGroupBatch after each batch write completes.
    private int _globalQueueDepth;

    /// <summary>
    /// Operations enqueued but not yet drained into any worker's write batch. Maintained only
    /// when <see cref="trackQueuedOps"/> — the linger's evidence gate reads it (minus the
    /// gathering group's own depths) to decide whether any partition outside the group still
    /// has work that is guaranteed to become ready soon. Unlike <see cref="_globalQueueDepth"/>
    /// (pending-or-in-flight, for back-pressure), this counter is decremented at drain time:
    /// an op inside another worker's in-flight write is <b>not</b> evidence of a future arrival
    /// (its partition only re-schedules if more ops remain queued behind it), so it must not
    /// hold the gate open.
    /// </summary>
    private int _queuedOperations;

    // ── Per-partition state ────────────────────────────────────────────────

    private sealed class PartitionState
    {
        /// <summary>Pending operations in submission order.</summary>
        public readonly Queue<WALWriteOperation> Ops = new();

        /// <summary>
        /// Guards <see cref="Ops"/>, <see cref="Scheduled"/>, <see cref="InFlight"/>, <see cref="Depth"/> and
        /// the deferred-marker fields.
        /// </summary>
        public readonly object Lock = new();

        /// <summary>
        /// Lazy commit-marker operations a worker drained and put back, in submission order, to be
        /// written with this partition's next batch. They precede everything in <see cref="Ops"/>,
        /// still count in <see cref="Depth"/>, and are not complete: their callbacks fire only after
        /// the write that carries them. Null until the partition first defers.
        /// </summary>
        public List<WALWriteOperation>? DeferredMarkers;

        /// <summary>Tick at which <see cref="DeferredMarkers"/> went from empty to non-empty.</summary>
        public long DeferredSinceTicks;

        /// <summary>True while this partition's id is in <see cref="_deferredMarkerPartitions"/>.</summary>
        public bool DeferredQueued;

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
        /// Tick at which the last synced write that carried sync work of this partition ended, until
        /// the partition is back (<see cref="BackSyncOps"/> reached <see cref="DoneSyncOps"/>); then
        /// zero. The distance between the two is the partition's return time.
        /// </summary>
        public long LastSyncDoneTicks;

        /// <summary>
        /// Operations with <see cref="WALWriteOperation.CountsAsSyncWork"/> that the last synced write
        /// carried for this partition: one for a single back-to-back writer, several when several
        /// writers share the partition.
        /// </summary>
        public int DoneSyncOps;

        /// <summary>
        /// Such operations enqueued since that write ended. The partition is back when there are as
        /// many as the write carried. Counting the first one only would release a held batch when one
        /// of several writers is back, and the others would miss it: the split this removes, inside
        /// one partition.
        /// </summary>
        public int BackSyncOps;

        /// <summary>
        /// True from the end of a synced write of this partition until the partition is back or the
        /// window passed, when its history says it comes back at once. Counted in
        /// <see cref="_awaitedReturns"/>.
        /// </summary>
        public bool AwaitedReturn;

        /// <summary>True while this partition's id is in <see cref="_awaitedPartitions"/>.</summary>
        public bool AwaitedQueued;

        /// <summary>
        /// Returns inside the window, in a row. The evidence that the partition is written in a closed
        /// loop: its next write is caused by the completion of the previous one.
        /// </summary>
        public int QuickReturns;

        /// <summary>
        /// <see cref="QuickReturns"/> needed before the partition is expected back. One at first; a
        /// partition that was waited for in vain doubles it, so a partition that only sometimes
        /// returns at once costs a wait less and less often.
        /// </summary>
        public int QuickReturnsNeeded = 1;

        /// <summary>Expected returns that came since the last miss; see <see cref="ReturnsToForgiveMiss"/>.</summary>
        public int ReturnsSinceMiss;

        /// <summary>
        /// <see cref="WALWriteOperation.EnqueueTicks"/> of the oldest operation in the batch a worker is
        /// currently writing for this partition, or 0 while none is in flight. Together with the head of
        /// <see cref="Ops"/> this names the oldest uncompleted operation, which is what the stall signal
        /// (<see cref="GetPartitionOldestPendingWriteAgeMs"/>) ages. Guarded by <see cref="Lock"/>.
        /// </summary>
        public long InFlightOldestEnqueueTicks;

        /// <summary>
        /// Per-partition EWMA of enqueue-to-durable latency in milliseconds.
        /// Updated by the worker thread after each successful Write batch.
        /// </summary>
        public readonly PartitionWaitAccumulator CommitWait;

        public PartitionState(Kommander.Time.IMonotonicTickSource tickSource)
        {
            CommitWait = new PartitionWaitAccumulator(tickSource: tickSource);
        }
    }

    private readonly ConcurrentDictionary<int, PartitionState> _partitions = new();

    /// <summary>
    /// Node-wide EWMA of enqueue-to-durable latency, fed once per group batch with the mean wait
    /// across every operation in that batch — regardless of which partitions contributed to it.
    ///
    /// <para><b>Why this is not the mean of the per-partition accumulators.</b> The fsync is shared:
    /// one <c>walAdapter.Write</c> covers every partition in the group, so the batch is the unit at
    /// which the device is actually observed. Averaging the per-partition EWMAs would instead weight
    /// an idle partition's frozen last value equally with a hot partition's live one, and those
    /// frozen values never age (see <see cref="PartitionWaitAccumulator"/>).</para>
    ///
    /// <para><b>Why it exists at all.</b> Follower appends enqueue here too, so this is the one
    /// commit-wait figure a node can report when it leads no partition. Per-partition figures ride
    /// the load report only for led partitions, which makes them useless for judging a node that is
    /// a candidate to *receive* leadership.</para>
    /// </summary>
    private readonly PartitionWaitAccumulator _nodeCommitWait;

    // ── Global ready-queue ─────────────────────────────────────────────────

    /// <summary>
    /// Ids of partitions with at least one pending operation waiting to be drained, and the workers
    /// parked for them.
    ///
    /// <para>Unbounded on purpose. A partition is in it at most once (<see cref="PartitionState.Scheduled"/>),
    /// so its length is bounded by the number of partitions, not by the write rate; the bound on
    /// pending work is the per-partition depth limit. The earlier bounded collection capped the
    /// number of distinct ready partitions instead: a full one blocked <see cref="Enqueue"/> under
    /// the partition lock, and left a partition that could not be put back after a write to be
    /// rescheduled by its next enqueue.</para>
    /// </summary>
    private readonly ParkedWorkerQueue<int> _readyPartitions;

    // ── Worker threads ─────────────────────────────────────────────────────

    private readonly Thread[] _workers;
    private volatile bool _stopping;
    private bool _started;

    // ── Observability ──────────────────────────────────────────────────────

    private long _totalBatchesWritten;
    private long _totalSyncBatchesWritten;
    private long _totalOperationsCompleted;
    private long _totalPartitionsBatched;
#if KOMMANDER_THREAD_FREE
    // The thread-free build has no linger, so these two stay zero there.
#pragma warning disable CS0649
#endif
    private long _totalLingerWaits;
    private long _totalLingerPartitionsGathered;
#if KOMMANDER_THREAD_FREE
#pragma warning restore CS0649
#endif

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

    /// <summary>
    /// Number of timed linger waits entered: the linger was enabled, window remained, and at
    /// least one partition outside the gathering group had queued-but-undrained work. This is
    /// the contract of the evidence gate — under closed-loop or sequential load (where the next
    /// op arrives only after the current flush completes) this counter must stay at zero,
    /// because a wait there stalls the very flush the next op depends on. The 3-partition
    /// throughput investigation that motivated the gate lacked exactly this figure.
    /// </summary>
    public long TotalLingerWaits => Interlocked.Read(ref _totalLingerWaits);

    /// <summary>
    /// Partitions added to a group batch by the linger — arrivals during or immediately after a
    /// timed wait, i.e. the coalescing the opportunistic sweep alone would have missed. Divided
    /// by <see cref="TotalLingerWaits"/> this gives the linger's hit rate.
    /// </summary>
    public long TotalLingerPartitionsGathered => Interlocked.Read(ref _totalLingerPartitionsGathered);

    /// <summary>
    /// Times a parked worker was woken for a ready partition. Each one is a thread hand-off with an OS
    /// wake cost; against <see cref="TotalBatchesWritten"/> it gives the hand-offs per engine write.
    /// </summary>
    public long WorkerWakeups => _readyPartitions.Wakeups;

    /// <summary>
    /// Wake-ups after which the worker found no partition ready. A worker takes every ready partition
    /// into its group, so a second worker woken for the same burst finds nothing.
    /// </summary>
    public long EmptyWorkerWakeups => _readyPartitions.EmptyWakeups;

    /// <summary>Test seam: workers parked, or about to park. Exact only while the scheduler is quiet.</summary>
    internal int IdleWorkerCount => _readyPartitions.IdleWorkerCount;

    /// <summary>
    /// Test seam: workers inside the linger's timed wait, where an arrival wakes them first.
    /// <see cref="TotalLingerWaits"/> is counted just before a worker enters that wait, so a test
    /// that needs the worker to be the one woken waits for this instead.
    /// </summary>
    internal int LingeringWorkerCount => _readyPartitions.TimedWaiterCount;

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
    /// opportunistic batching; &gt; 0 lets a worker wait — only while another partition
    /// has queued work that is guaranteed to become ready soon — to gather more ready
    /// partitions into one fsync. When nothing can arrive, the worker flushes
    /// immediately and the window costs nothing.
    /// </param>
    /// <param name="lazyCommitMarkers">
    /// Single-fsync commit fast path (see <c>RaftConfiguration.WalSingleFsyncCommit</c>). When true, a
    /// group batch whose logs are all per-entry <c>Committed</c> markers is written sync-off so it rides
    /// the next durable write instead of forcing its own fsync. Off (default) keeps every batch sync.
    /// </param>
    /// <param name="maxSpinBeforePark">
    /// Upper bound of how long one idle worker polls the ready-queue before it parks. Null means the
    /// default (20 µs); <see cref="TimeSpan.Zero"/> disables the spin, so every enqueue that finds no
    /// busy worker is a kernel wake. See <see cref="RaftConfiguration.IdleWorkerMaxSpinBeforePark"/>.
    /// </param>
    /// <param name="returnWindow">
    /// Upper bound of the return window (see <c>RaftConfiguration.WalGroupCommitReturnWindow</c>). Null
    /// or zero, the default here, turns return gathering off. It is also off, whatever this says, for
    /// a store that does not serialize its synced writes and in manual execution.
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
        bool lazyCommitMarkers = false,
        Kommander.Time.IMonotonicTickSource? tickSource = null,
        bool manualExecution = false,
        int markerDeferralMs = 0,
        TimeSpan? maxSpinBeforePark = null,
        TimeSpan? returnWindow = null)
    {
        this.manualExecution = manualExecution;
        this.tickSource = tickSource ?? Kommander.Time.SystemMonotonicTickSource.Instance;
        this._nodeCommitWait = new PartitionWaitAccumulator(tickSource: this.tickSource);
        this.walAdapter = walAdapter;
        this.logger = logger;
        this.maxQueueDepthPerPartition = maxQueueDepthPerPartition;
        this.maxGlobalQueueDepth = maxGlobalQueueDepth > 0 ? maxGlobalQueueDepth : 0;
        this.maxBatchSize = maxBatchSize <= 0 ? DefaultMaxBatchSize : maxBatchSize;
        this.maxGroupBatchPartitions = maxGroupBatchPartitions <= 0 ? DefaultMaxGroupBatchPartitions : maxGroupBatchPartitions;
        this.lingerTicks = groupCommitLingerMs > 0
            ? (long)(groupCommitLingerMs / 1000.0 * global::System.Diagnostics.Stopwatch.Frequency)
            : 0;
        this.trackQueuedOps = this.lingerTicks > 0;
        this.lazyCommitMarkers = lazyCommitMarkers;

        // Deferral needs worker threads: it relies on another worker's write being in progress, and
        // on that worker's completion to release what was held. Manual mode writes inline, one batch
        // at a time, so there is never a sync in progress to step around.
        this.markerDeferralTicks = MarkerDeferralTicks(lazyCommitMarkers, manualExecution, markerDeferralMs, this.tickSource.Frequency);

        // Gathering waits on the wall clock for an arrival, which a caller-driven scheduler must never
        // do, and it only pays on a store where a second synced write waits for the first.
        TimeSpan window = returnWindow ?? TimeSpan.Zero;
        this.returnGathering = !manualExecution && window > TimeSpan.Zero && walAdapter.SerializesSyncedWrites;
        this.maxReturnWindowTicks = returnGathering ? Math.Max(1, (long)(window.TotalSeconds * this.tickSource.Frequency)) : 0;
        this.minReturnWindowTicks = Math.Max(1, (long)(MinReturnWindow.TotalSeconds * this.tickSource.Frequency));
        this.minInFlightWaitCapTicks = Math.Max(1, (long)(MinInFlightWaitCap.TotalSeconds * this.tickSource.Frequency));

        if (workerCount <= 0)
            workerCount = Math.Max(1, Environment.ProcessorCount);

        // Manual mode has no workers: the ready-queue is then only a queue, pumped by the caller.
        _workers = new Thread[manualExecution ? 0 : workerCount];
        _readyPartitions = new ParkedWorkerQueue<int>(_workers.Length, maxSpinBeforePark);

#if KOMMANDER_THREAD_FREE
        // The thread-free build has no worker threads. RaftConfiguration.Validate already refuses
        // the threaded configuration, so this only guards a direct construction.
        if (!manualExecution)
            throw new PlatformNotSupportedException(
                "FairWalScheduler: the thread-free build (KOMMANDER_THREAD_FREE) supports manual execution only.");
#endif
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

        // The factory is static and the tick source travels as the state argument. A capturing lambda
        // here would read `tickSource` off this instance, which the compiler cannot cache in a static
        // field, so every admission — including the steady state where the partition already exists —
        // allocated a delegate. Dictionary admission semantics are unchanged; two callers that race to
        // create the same missing partition can still both construct a PartitionState, and one loses.
        PartitionState state = _partitions.GetOrAdd(
            partitionId,
            static (_, source) => new PartitionState(source),
            tickSource);

        bool scheduled = false;
        bool lastAwaitedReturned = false;

        lock (state.Lock)
        {
            if (state.Depth >= maxQueueDepthPerPartition)
            {
                KommanderMetrics.WalBackpressureRejectionsTotal.Add(1);
                throw new BackpressureExceededException(partitionId, state.Depth);
            }

            if (maxGlobalQueueDepth > 0)
            {
                int globalDepth = Interlocked.Increment(ref _globalQueueDepth);
                if (globalDepth > maxGlobalQueueDepth)
                {
                    Interlocked.Decrement(ref _globalQueueDepth);
                    KommanderMetrics.WalBackpressureRejectionsTotal.Add(1);
                    throw new BackpressureExceededException(partitionId, globalDepth);
                }
            }

            operation.EnqueueTicks = tickSource.GetTimestamp();
            state.Ops.Enqueue(operation);
            state.Depth++;

            if (returnGathering && CountsAsSyncWork(operation))
            {
                operation.CountsAsSyncWork = true;
                lastAwaitedReturned = NoteReturn(state, operation.EnqueueTicks);
            }

            // Increment AFTER Depth++: the interlocked full fence orders the two stores, so a
            // linger gate that observes the new queued count also observes the new depth and
            // can never spuriously report work outside a group that only this partition holds.
            // (The reverse skew — new depth, stale count — merely makes a linger flush early.)
            if (trackQueuedOps)
                Interlocked.Increment(ref _queuedOperations);

            // Schedule the partition in the global ready-queue only if it is not
            // already scheduled AND no worker is currently executing I/O for it.
            // If InFlight=true a worker is mid-write; that worker's post-I/O lock
            // section will re-schedule once the write finishes.
            if (!state.Scheduled && !state.InFlight)
            {
                state.Scheduled = true;
                _readyPartitions.Publish(partitionId);
                scheduled = true;
            }
        }

        // Everyone a gathering worker waited for is back: it writes now. Outside the partition lock,
        // which that worker takes to drain. When this partition's id was just published too, that
        // worker takes it: it is awake for the signal and looks at the ready-queue before anything
        // else. A second worker woken for the id could take it first and write this partition on
        // its own, beside the batch that was held for it.
        bool gathererTakesIt = lastAwaitedReturned && SignalGatherer();

        // Manual mode writes here, outside the partition lock, so the operation is durable and its
        // completion callback delivered before this call returns.
        if (manualExecution)
            DrainInline();
        else if (scheduled && !gathererTakesIt)
        {
            // The wake comes after the id is published, which is all the handshake needs, and outside
            // the partition lock, which the woken worker takes first thing to drain this partition.
            _readyPartitions.WakeOne();
        }
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// <summary>
    /// Starts the worker threads.  Must be called exactly once before <see cref="Enqueue"/>.
    /// <para>In manual mode there are no worker threads to start; the caller drives every flush
    /// through <see cref="PumpOnce"/> or <see cref="PumpUntilIdle"/>. The scheduler is still
    /// registered for metrics and still accepts work.</para>
    /// </summary>
    public void Start()
    {
        if (_started)
            return;

        _started = true;
        KommanderMetrics.RegisterScheduler(this);

#if !KOMMANDER_THREAD_FREE
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
#endif
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
    /// <para>One ordering IS guaranteed: a batch leaves the depth before its first completion
    /// callback fires (Phase 3b of <see cref="ProcessGroupBatch"/>), so a caller that reads the depth
    /// after its own ack never counts the write it was acked for. The volatile read keeps that
    /// guarantee visible to a reader on another thread.</para>
    /// </summary>
    public int GetPartitionDepth(int partitionId) =>
        _partitions.TryGetValue(partitionId, out PartitionState? state) ? Volatile.Read(ref state.Depth) : 0;

    /// <inheritdoc />
    public double GetPartitionOldestPendingWriteAgeMs(int partitionId) =>
        _partitions.TryGetValue(partitionId, out PartitionState? state)
            ? OldestPendingWriteAgeMs(state, tickSource.GetTimestamp())
            : 0;

    /// <summary>
    /// Age of the oldest uncompleted operation across every partition this scheduler serves — the
    /// node-wide durable-write stall signal (a group batch holds one engine write for all of them, so
    /// one stalled fsync ages every partition in it alike). <c>0</c> when nothing is pending.
    /// </summary>
    public double GetOldestPendingWriteAgeMs()
    {
        long now = tickSource.GetTimestamp();
        double oldest = 0;

        foreach (PartitionState state in _partitions.Values)
        {
            double age = OldestPendingWriteAgeMs(state, now);
            if (age > oldest)
                oldest = age;
        }

        return oldest;
    }

    /// <summary>Per-partition snapshot of <see cref="GetPartitionOldestPendingWriteAgeMs"/> for the metrics gauge.</summary>
    internal IEnumerable<(int PartitionId, double AgeMs)> SnapshotPartitionOldestPendingWriteAges()
    {
        long now = tickSource.GetTimestamp();
        foreach (KeyValuePair<int, PartitionState> kv in _partitions)
            yield return (kv.Key, OldestPendingWriteAgeMs(kv.Value, now));
    }

    private double OldestPendingWriteAgeMs(PartitionState state, long nowTicks)
    {
        long oldest;
        lock (state.Lock)
        {
            // An in-flight batch always predates anything still queued behind it.
            // Deferred markers were drained before anything queued now, so they are older still.
            oldest = state.InFlightOldestEnqueueTicks != 0
                ? state.InFlightOldestEnqueueTicks
                : state.DeferredMarkers is { Count: > 0 } deferred ? deferred[0].EnqueueTicks
                : state.Ops.TryPeek(out WALWriteOperation? head) ? head.EnqueueTicks : 0;
        }

        return oldest == 0 ? 0 : Math.Max(0, (nowTicks - oldest) * 1000.0 / tickSource.Frequency);
    }

    /// <summary>
    /// Returns the current EWMA enqueue-to-durable commit-wait latency in milliseconds
    /// for the given partition, or <c>0</c> if no write batch has yet completed for it.
    /// The value is approximate and suitable for advisory saturation detection only.
    /// </summary>
    public double GetPartitionCommitWaitMs(int partitionId) =>
        _partitions.TryGetValue(partitionId, out PartitionState? state) ? state.CommitWait.CurrentWaitMs() : 0.0;

    /// <summary>
    /// Returns the node-wide EWMA enqueue-to-durable commit-wait latency in milliseconds, across
    /// every partition this node writes for — led or followed. <c>0</c> when no group batch has
    /// completed yet, which a caller must read as <b>unknown</b> and not as <b>fast</b>: pair it
    /// with <see cref="GetNodeCommitWaitSamples"/> and <see cref="GetNodeCommitWaitAgeMs"/>.
    /// </summary>
    public double GetNodeCommitWaitMs() => _nodeCommitWait.CurrentWaitMs();

    /// <summary>
    /// Number of group batches that fed <see cref="GetNodeCommitWaitMs"/>. <c>0</c> means the node
    /// has written nothing yet, so its commit-wait figure carries no information at all.
    /// </summary>
    public long GetNodeCommitWaitSamples() => _nodeCommitWait.SampleCount;

    /// <summary>
    /// Milliseconds since the last group batch fed <see cref="GetNodeCommitWaitMs"/>, or <c>0</c>
    /// when there has been none. The EWMA decays per sample, so a node that goes quiet holds its
    /// last figure indefinitely; this is how a consumer detects that.
    /// </summary>
    public double GetNodeCommitWaitAgeMs() => _nodeCommitWait.AgeMs();

    /// <summary>
    /// Stops the scheduler.
    ///
    /// <para>When called, no new operations are accepted.  All operations that were
    /// already <see cref="Enqueue"/>d before this call will be processed and their
    /// <c>OnComplete</c> callbacks fired before worker threads exit, however deep a partition's
    /// queue is: each worker drains until nothing is left that it can claim (see
    /// <c>DrainRemaining</c>).  Callers should wait for all expected completions before assuming
    /// the system is quiescent.</para>
    /// </summary>
    public void Stop()
    {
        if (!_started)
            return; // Start() was never called; nothing to stop.

        _stopping = true;

        // Manual mode has no worker to drain the tail, so this thread drains it. Without this a
        // shutdown would strand every queued write with its completion callback unfired, and the
        // partition waiting on that callback would never learn the write failed.
        if (manualExecution)
        {
            PumpUntilIdle();
            return;
        }

        // Wake every parked or lingering worker. Each finishes the batch it holds, sees the request on
        // its next take, drains what is still queued (DrainRemaining) and exits.
        _readyPartitions.RequestStop();

        foreach (Thread worker in _workers)
            worker.Join();
    }

    // ── Worker ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs every ready group batch on the calling thread. Called by <see cref="Enqueue"/> in
    /// manual mode so a write is durable, and its completion callback delivered, before the
    /// enqueuing call returns.
    ///
    /// <para><b>Why inline and not deferred.</b> A deferred write deadlocks a single-threaded
    /// driver whenever a partition operation awaits its own write: the driver is inside the
    /// executor drain, and the write pump it would need runs after that drain returns. Making the
    /// write inline removes the whole class of deadlock. The cost is that "the write is slow" can
    /// no longer be expressed by holding the completion here — a simulation models that in the
    /// storage device instead, which is where the latency belongs.</para>
    ///
    /// <para>Re-entrancy is not a concern: a completion callback posts into a partition executor
    /// queue, never back into this scheduler.</para>
    /// </summary>
    private void DrainInline() => PumpUntilIdle();

    /// <summary>
    /// Runs at most one group batch on the calling thread. Returns true when a batch ran.
    /// Manual mode only; throws otherwise, because pumping a scheduler that also has worker
    /// threads would break the single-executor-per-partition discipline those threads rely on.
    /// </summary>
    /// <remarks>
    /// The batch is assembled exactly as a worker thread assembles it, including the
    /// opportunistic sweep for more ready partitions, so group-commit behaviour is the real one.
    /// The linger step is deliberately skipped: it waits on wall-clock time, which a
    /// deterministic run must never do, and its only effect is which partitions share an fsync.
    /// </remarks>
    public bool PumpOnce()
    {
        if (!manualExecution)
            throw new InvalidOperationException(
                "FairWalScheduler.PumpOnce requires manual mode; this scheduler owns worker threads.");

        if (!_readyPartitions.TryDequeue(out int partitionId))
            return false;

        List<int> partitionGroup = new(maxGroupBatchPartitions) { partitionId };
        List<(int PartitionId, List<WALWriteOperation> Ops)> groupBatches = new(maxGroupBatchPartitions);

        List<List<WALWriteOperation>> opListPool = new(maxGroupBatchPartitions);
        for (int i = 0; i < maxGroupBatchPartitions; i++)
            opListPool.Add(new List<WALWriteOperation>(maxBatchSize));

        List<(int, List<RaftLog>)> logGroups = new(maxGroupBatchPartitions);
        Dictionary<int, long> acknowledgedPresence = new(maxGroupBatchPartitions);

        while (partitionGroup.Count < maxGroupBatchPartitions && _readyPartitions.TryDequeue(out int extraId))
            partitionGroup.Add(extraId);

        ProcessGroupBatch(partitionGroup, groupBatches, opListPool, logGroups, acknowledgedPresence);
        return true;
    }

    /// <summary>
    /// Runs group batches until nothing is ready, and returns how many ran.
    ///
    /// <para>A completing write can enqueue the next one, so this is a loop rather than a single
    /// pass. <paramref name="maxBatches"/> bounds it: a workload that feeds itself forever would
    /// otherwise never return, and a simulation wants that reported as a stuck step rather than
    /// as a hang.</para>
    /// </summary>
    public int PumpUntilIdle(int maxBatches = 10_000)
    {
        int batches = 0;

        while (batches < maxBatches && PumpOnce())
            batches++;

        return batches;
    }

#if !KOMMANDER_THREAD_FREE
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

        // Pre-allocated per-partition acknowledged-presence fold passed alongside — cleared and reused.
        Dictionary<int, long> acknowledgedPresence = new(maxGroupBatchPartitions);

        while (true)
        {
            // Park until at least one partition is ready. False means Stop ran: drain what is
            // still queued, then exit.
            if (!_readyPartitions.Take(workerId, handOn: false, out int partitionId))
            {
                DrainRemaining(partitionGroup, groupBatches, opListPool, logGroups, acknowledgedPresence);
                break;
            }

            // Gather additional ready partitions — the group-commit coalescing step.
            // Up to maxGroupBatchPartitions−1 extra partitions join this worker's batch,
            // all sharing the same Write call (one fsync for RocksDB).
            partitionGroup.Clear();
            partitionGroup.Add(partitionId);

            // (1) Opportunistic sweep: take whatever is *already* ready, non-blocking.
            while (partitionGroup.Count < maxGroupBatchPartitions && _readyPartitions.TryDequeue(out int extraId))
                partitionGroup.Add(extraId);

            // (2) Deferred linger (opt-in): if the batch is not yet full and a window is
            //     configured, wait briefly for more partitions to become ready so they
            //     share this fsync instead of forcing their own. The wait is evidence-
            //     gated: it runs only while some partition OUTSIDE this group has queued
            //     work — work guaranteed to become ready once its current write finishes —
            //     and the total wait never exceeds lingerTicks. When nothing can arrive
            //     (e.g. closed-loop load, where the next op is caused by this very flush's
            //     completion) the worker flushes immediately and the linger costs nothing.
            //     This is the lever for paths whose ready partitions trickle in (e.g.
            //     follower appends paced by replication RPCs) rather than arriving all at
            //     once like a leader's local proposes.
            if (lingerTicks > 0 && partitionGroup.Count < maxGroupBatchPartitions)
                LingerForMoreReadyPartitions(partitionGroup, workerId);

            // (3) Return gathering (a store that serializes its synced writes): hold the batch while
            //     a synced write of partitions that are expected back is in progress, and then for
            //     those partitions, so they all share one sync. False means another worker gathers and
            //     took these partitions into its batch: this worker has nothing to write.
            if (returnGathering && !GatherForSharedSync(partitionGroup, workerId))
                continue;

            // This worker is about to be busy with the engine. An id it leaves behind — the group
            // is full, or the id arrived as the linger ended — needs another worker now: its producer
            // may have counted on this one (it was spinning or lingering when the id was published).
            _readyPartitions.HandOnIfQueued();

            ProcessGroupBatch(partitionGroup, groupBatches, opListPool, logGroups, acknowledgedPresence);
        }
    }
#endif

#if !KOMMANDER_THREAD_FREE
    // Group-commit linger: only the worker loop lingers, and the thread-free build has no worker
    // loop. Manual mode writes each batch when it is enqueued.
    /// <summary>
    /// Deferred group-commit: after the opportunistic sweep, wait for more partitions to
    /// become ready so they share this worker's single fsync, until the batch is full, the
    /// linger window (<see cref="lingerTicks"/>) elapses, or no partition outside the group
    /// has queued work left to wait for.
    ///
    /// <para><b>The wait is evidence-gated, not speculative.</b> Partitions already in the
    /// group cannot re-enter the ready-queue while gathered (their <c>Scheduled</c> flag is
    /// still set until the drain phase clears it), so the only possible arrivals are OTHER
    /// partitions. A timed wait therefore runs only while at least one of those has
    /// queued-but-undrained work (<see cref="OtherPartitionsHaveQueuedWork"/>): such a
    /// partition is either already in the ready-queue (taken instantly) or queued behind
    /// another worker's in-flight write, which re-schedules it on completion — a concrete
    /// arrival to wait for. With no such partition the worker flushes immediately and pays
    /// nothing. The previous implementation instead probed blindly with a fixed 1 ms timeout
    /// and bailed on an empty probe; on a small partition count under closed-loop load the
    /// batch could never fill AND the next op could only arrive after this flush completed,
    /// so every flush blocked ~1–2 ms (kernel timed-wait granularity) for nothing — measured
    /// as a 44% throughput loss on a 3-partition cluster, with 1 ms and 2 ms windows
    /// indistinguishable because the wait was probe-bounded, not window-bounded.</para>
    ///
    /// <para>Each timed wait is bounded by the <em>remaining</em> window, so the configured
    /// linger is honoured exactly; an arrival ends the wait immediately. Kernel timed-wait
    /// granularity still rounds a sub-millisecond remainder up to ~1 ms.</para>
    ///
    /// <para>While it waits the worker is registered with the ready-queue as a timed waiter, and
    /// an enqueue wakes a timed waiter before any parked worker: the arrival is exactly what this
    /// worker waits for. Another worker that returns to the queue at that moment can still take the
    /// id first; the wait then resumes for what is left of the window.</para>
    ///
    /// <para>Only <em>when</em> the group fsync fires changes; never <em>whether</em> a write
    /// is durable before its completion callback runs. Per-partition ordering and the
    /// <c>Scheduled</c>/<c>InFlight</c> ready-queue discipline are untouched.</para>
    /// </summary>
    private void LingerForMoreReadyPartitions(List<int> partitionGroup, int workerId)
    {
        long deadline = tickSource.GetTimestamp() + lingerTicks;

        while (partitionGroup.Count < maxGroupBatchPartitions)
        {
            long remainingTicks = deadline - tickSource.GetTimestamp();
            if (remainingTicks <= 0)
                break;

            if (!OtherPartitionsHaveQueuedWork(partitionGroup))
                break; // nothing outside the group can become ready → flushing now is strictly better.

            int waitMs = (int)Math.Ceiling(remainingTicks * 1000.0 / tickSource.Frequency);

            Interlocked.Increment(ref _totalLingerWaits);

            if (!_readyPartitions.TryTake(workerId, waitMs, out int extraId, out bool timedOut))
            {
                // Timed out: the expected arrival was slower than the budget. A stop request reads as
                // a timeout too: process what we have; WorkerLoop's next take drains the rest.
                if (timedOut)
                    break;

                // Woken, and another worker took the id first. Wait again for what is left.
                continue;
            }

            partitionGroup.Add(extraId);
            Interlocked.Increment(ref _totalLingerPartitionsGathered);

            // Grab whatever arrived alongside without waiting again; the loop then re-checks
            // the remaining window and whether anything else is worth waiting for.
            while (partitionGroup.Count < maxGroupBatchPartitions && _readyPartitions.TryDequeue(out int moreId))
            {
                partitionGroup.Add(moreId);
                Interlocked.Increment(ref _totalLingerPartitionsGathered);
            }
        }
    }

    /// <summary>
    /// True when at least one partition outside <paramref name="partitionGroup"/> has operations
    /// enqueued but not yet drained into any worker's write batch — i.e. a concrete future
    /// ready-queue arrival the linger can gather. Computed as <see cref="_queuedOperations"/>
    /// minus the group's own depths; for group partitions depth equals their queued count,
    /// because their ready-queue slot is held by this worker so none of their ops can be in
    /// flight. Lock-free and approximate: the reads race with concurrent enqueues and drains,
    /// so a stale answer occasionally makes the linger wait once more or flush a little early.
    /// Both are timing effects only — durability and ordering never depend on this answer.
    /// </summary>
    private bool OtherPartitionsHaveQueuedWork(List<int> partitionGroup)
    {
        int queuedElsewhere = Volatile.Read(ref _queuedOperations);
        if (queuedElsewhere <= 0)
            return false;

        foreach (int pid in partitionGroup)
        {
            if (_partitions.TryGetValue(pid, out PartitionState? state))
                queuedElsewhere -= state.Depth;
        }

        return queuedElsewhere > 0;
    }

    /// <summary>
    /// Return gathering: holds this worker's batch so that partitions written in a closed loop share
    /// one sync instead of taking turns on the engine. Returns false when another worker gathers and
    /// took this worker's partitions into its batch; the caller then has nothing to write.
    ///
    /// <para><b>The problem.</b> A closed-loop writer sends its next write when the previous one is
    /// acknowledged, so a partition comes back a moment after the synced write that carried it ends.
    /// A worker that writes whatever is ready at once starts the next synced write in that moment,
    /// with the partitions that became ready during the previous sync. The returning partitions miss
    /// it, wait for it inside the engine, and are written by the write after it. The partitions
    /// then form two groups that alternate for ever, and every round costs two syncs. Measured on
    /// one node with four partitions: each engine write took 6 ms where one sync takes 3 ms.</para>
    ///
    /// <para><b>What this does.</b> Two waits, both before the batch is drained, so the partitions
    /// this worker already holds keep collecting operations meanwhile:</para>
    /// <list type="number">
    ///   <item>While a synced write that carries partitions expected back is in progress
    ///       (<see cref="_unsettledSyncWrites"/>). The store serializes its writers, so this batch
    ///       would wait for that sync inside the engine with its content already fixed. Waiting here
    ///       takes about the same time and keeps the batch open for those partitions. A write nobody
    ///       is expected back from is not waited for: there the engine's own queue is the faster
    ///       hand-over, and a load that is not a closed loop behaves as it did before.</item>
    ///   <item>After that write ended, until every partition it carried that is expected back has
    ///       come (<see cref="_awaitedReturns"/>), or the return window passed
    ///       (<see cref="_returnDeadlineTicks"/>).</item>
    /// </list>
    ///
    /// <para><b>Who is expected back.</b> A partition whose last returns were inside the window
    /// (<see cref="PartitionState.QuickReturns"/>); see <see cref="NoteSyncedWriteDone"/>. It is back
    /// when as many operations were enqueued for it as its last synced write carried
    /// (<see cref="NoteReturn"/>), so several writers on one partition are gathered like several
    /// partitions. A partition that sends one write and stops is never waited for. A partition that was waited for and did
    /// not come is not expected again until it has returned at once several times in a row, twice
    /// as many after each miss (<see cref="ExpireAwaitedReturns"/>), so a wrong guess costs one
    /// window and then less and less often.</para>
    ///
    /// <para><b>Bounds.</b> The window is half of the usual synced write and at most the configured
    /// bound (<see cref="ReturnWindowTicks"/>): a wait never costs more than half of what it can
    /// save, and a store whose sync is cheap gets no window and no wait at all. The first wait ends
    /// when the write in progress ends; it is capped (<see cref="InFlightWaitCapTicks"/>) so that a
    /// stalled device degrades to the old behaviour, this batch waiting inside the engine, and never
    /// to a hang. A full batch and a stop request end both waits.</para>
    ///
    /// <para><b>One worker gathers.</b> Two workers that each held part of the ready partitions would
    /// write one after the other, and the second part would pay two syncs: the very split this
    /// removes. A worker that finds another one gathering gives it its partitions
    /// (<see cref="TryBecomeGatherer"/>) and goes back to the ready-queue.</para>
    ///
    /// <para>Lazy commit markers ride for free: a partition held here keeps its markers queued, and
    /// they are drained with the propose that follows them, in the same engine write.</para>
    ///
    /// <para>Only <em>when</em> a batch is written changes. What is written, whether it is synced,
    /// the order inside a partition and the order of completions are untouched.</para>
    /// </summary>
    private bool GatherForSharedSync(List<int> partitionGroup, int workerId)
    {
        long window = ReturnWindowTicks();

        // No synced write measured yet, or one too cheap to share: write at once.
        if (window == 0)
            return true;

        // Another worker gathers: these partitions belong in its batch, whatever this worker would
        // decide for itself. The usual case is the last expected partition, taken here a moment
        // before the gathering worker woke for it: with nothing left to wait for, this worker would
        // write it alone, beside the batch that was held for it.
        if (Volatile.Read(ref _gathererWorker) >= 0 && TryHandOff(partitionGroup))
            return false;

        bool gathering = false;
        bool waitedForReturns = false;
        long hardDeadlineTicks = 0;

        try
        {
            while (!_stopping && partitionGroup.Count < maxGroupBatchPartitions)
            {
                // Read before the state it guards: a change after this read ends the sleep below
                // at once, and the loop then reads the new state.
                long epoch = Volatile.Read(ref _gatherEpoch);
                long nowTicks = tickSource.GetTimestamp();
                long waitUntilTicks;

                if (Volatile.Read(ref _unsettledSyncWrites) > 0)
                {
                    waitUntilTicks = long.MaxValue; // until it settles; capped below
                }
                else if (Volatile.Read(ref _awaitedReturns) > 0)
                {
                    waitUntilTicks = Volatile.Read(ref _returnDeadlineTicks);

                    if (nowTicks >= waitUntilTicks)
                    {
                        // The window passed with partitions still out. When this worker held its
                        // batch for them, the guess was wrong and they have to prove themselves
                        // again. Otherwise they are leftovers of a load that stopped, which cost
                        // nobody a wait.
                        ExpireAwaitedReturns(nowTicks, window, missed: waitedForReturns);
                        break;
                    }

                    waitedForReturns = true;
                }
                else
                    break;

                if (hardDeadlineTicks == 0)
                    hardDeadlineTicks = nowTicks + InFlightWaitCapTicks() + window;

                if (waitUntilTicks > hardDeadlineTicks)
                    waitUntilTicks = hardDeadlineTicks;

                if (nowTicks >= waitUntilTicks)
                    break;

                if (!gathering)
                {
                    if (!TryBecomeGatherer(partitionGroup, workerId, out bool handedOff))
                    {
                        if (handedOff)
                            return false;

                        // The gathering worker has no room for these partitions: write them now.
                        break;
                    }

                    gathering = true;
                    Interlocked.Increment(ref _totalReturnGathers);
                }

                int waitMs = (int)Math.Clamp(
                    Math.Ceiling((waitUntilTicks - nowTicks) * 1000.0 / tickSource.Frequency), 1, int.MaxValue);

                bool took = _readyPartitions.TryTakeWhile(
                    workerId,
                    waitMs,
                    (Scheduler: this, Epoch: epoch),
                    static wait => Volatile.Read(ref wait.Scheduler._gatherEpoch) == wait.Epoch,
                    out int extraId,
                    out _);

                if (took)
                {
                    partitionGroup.Add(extraId);

                    while (partitionGroup.Count < maxGroupBatchPartitions && _readyPartitions.TryDequeue(out int moreId))
                        partitionGroup.Add(moreId);
                }

                TakeHandedOffPartitions(partitionGroup, release: false);
            }
        }
        finally
        {
            // Under the lock that a hand-off takes: a worker either gave its partitions before this
            // and they are written with this batch, or finds nobody gathering and gathers itself.
            if (gathering)
                TakeHandedOffPartitions(partitionGroup, release: true);
        }

        return true;
    }

    /// <summary>
    /// Makes the calling worker the one that gathers, or, when another worker already is, gives that
    /// worker the caller's partitions. Returns true when the caller gathers. Otherwise
    /// <paramref name="handedOff"/> says whether the partitions were given away (the caller's group
    /// is then empty) or the gathering worker had no room for them (the caller writes them itself).
    ///
    /// <para>A partition given away stays scheduled: its id has only moved from one worker's hands
    /// to another's, and the gathering worker drains it as if it had dequeued the id itself. The
    /// room check reads a count the gathering worker refreshes after each take, so a batch can pass
    /// <see cref="maxGroupBatchPartitions"/> by the few ids of one take; the batch lists grow on
    /// demand.</para>
    /// </summary>
    private bool TryBecomeGatherer(List<int> partitionGroup, int workerId, out bool handedOff)
    {
        handedOff = false;

        lock (_gatherLock)
        {
            if (_gathererWorker < 0)
            {
                Volatile.Write(ref _gathererWorker, workerId);
                _gathererHeld = partitionGroup.Count;
                return true;
            }
        }

        handedOff = TryHandOff(partitionGroup);
        return false;
    }

    /// <summary>
    /// Gives the caller's partitions to the gathering worker, when there is one and it has room.
    /// True when they were given; the caller's group is then empty.
    ///
    /// <para>No wake: the gathering worker does not act on these now. It takes them at its next
    /// wake, and at the latest when it stops gathering, in the critical section that ends its turn,
    /// so a partition given here is always written with its batch.</para>
    /// </summary>
    private bool TryHandOff(List<int> partitionGroup)
    {
        lock (_gatherLock)
        {
            if (_gathererWorker < 0 || _gathererHeld + partitionGroup.Count > maxGroupBatchPartitions)
                return false;

            _gatherHandoff.AddRange(partitionGroup);
            _gathererHeld += partitionGroup.Count;
        }

        partitionGroup.Clear();
        Interlocked.Increment(ref _totalGatherHandoffs);
        return true;
    }

    /// <summary>
    /// Moves the partitions other workers handed over into the gathering worker's group, and with
    /// <paramref name="release"/> ends its turn as the gathering worker in the same critical section.
    /// </summary>
    private void TakeHandedOffPartitions(List<int> partitionGroup, bool release)
    {
        lock (_gatherLock)
        {
            if (_gatherHandoff.Count > 0)
            {
                partitionGroup.AddRange(_gatherHandoff);
                _gatherHandoff.Clear();
            }

            if (release)
            {
                Volatile.Write(ref _gathererWorker, -1);
                _gathererHeld = 0;
            }
            else
                _gathererHeld = partitionGroup.Count;
        }
    }

    /// <summary>
    /// Stops expecting the partitions whose window passed. Called by a worker that found the return
    /// deadline passed with partitions still out.
    ///
    /// <para>With <paramref name="missed"/> the caller held a batch for them until the deadline: each
    /// of them then has to prove itself again, its run of quick returns starts over and the run it
    /// needs doubles, up to <see cref="MaxQuickReturnsNeeded"/>. Without it nobody waited (the
    /// partitions stopped writing and the next batch came later), so nothing is charged: the late
    /// return, if it comes, ends the run by itself.</para>
    ///
    /// <para>A partition whose own window is still open (a later synced write ended after the one
    /// this deadline came from) is kept.</para>
    /// </summary>
    private void ExpireAwaitedReturns(long nowTicks, long window, bool missed)
    {
        // One pass over what is queued now: an id put back below is not looked at again in this call.
        for (int remaining = _awaitedPartitions.Count; remaining > 0; remaining--)
        {
            if (!_awaitedPartitions.TryDequeue(out int pid))
                break;

            if (!_partitions.TryGetValue(pid, out PartitionState? state))
                continue;

            bool keepQueued = false;

            lock (state.Lock)
            {
                if (!state.AwaitedReturn)
                {
                    // It came back; the id was left here for this pass to drop.
                    state.AwaitedQueued = false;
                }
                else if (nowTicks - state.LastSyncDoneTicks < window)
                {
                    keepQueued = true;
                }
                else
                {
                    state.AwaitedReturn = false;
                    state.AwaitedQueued = false;
                    Interlocked.Decrement(ref _awaitedReturns);

                    if (missed)
                    {
                        state.QuickReturns = 0;
                        state.ReturnsSinceMiss = 0;
                        state.QuickReturnsNeeded = Math.Min(state.QuickReturnsNeeded * 2, MaxQuickReturnsNeeded);
                        Interlocked.Increment(ref _totalReturnsMissed);
                        KommanderMetrics.WalReturnsMissedTotal.Add(1);
                    }
                }
            }

            if (keepQueued)
                _awaitedPartitions.Enqueue(pid);
        }
    }

    /// <summary>
    /// The longest a gathering worker waits for a synced write in progress: several usual synced
    /// writes, and never less than <see cref="MinInFlightWaitCap"/>.
    /// </summary>
    private long InFlightWaitCapTicks() =>
        Math.Max(minInFlightWaitCapTicks, Volatile.Read(ref _syncWriteEwmaTicks) * InFlightWaitCapSyncWrites);
#endif

    // ── Return gathering: bookkeeping shared with the write path ───────────

    /// <summary>
    /// The return window in use, on the tick source: half of the usual synced write, at most the
    /// configured bound, and zero (no gathering) when that is below <see cref="MinReturnWindow"/> or
    /// no synced write was measured yet.
    ///
    /// <para>Half, because a partition that is waited for and comes saves one sync, and the batch
    /// that waited for it must not pay more than a part of that. It also turns the gathering off by
    /// itself on a store whose sync is cheap, where the engine's own writer queue hands over faster
    /// than a parked worker wakes.</para>
    /// </summary>
    private long ReturnWindowTicks()
    {
        long window = Math.Min(maxReturnWindowTicks, Volatile.Read(ref _syncWriteEwmaTicks) / 2);
        return window >= minReturnWindowTicks ? window : 0;
    }

    /// <summary>
    /// True when <paramref name="op"/>, taken alone, makes its batch pay a sync: the same rule the
    /// write path applies to the whole batch, without the metadata operations, which are not part of
    /// a partition's write loop.
    /// </summary>
    private bool CountsAsSyncWork(WALWriteOperation op)
    {
        if (op.Type is WALWriteOperationType.HardState or WALWriteOperationType.HlcFloor or WALWriteOperationType.AcknowledgedPresence)
            return false;

        return !lazyCommitMarkers || op.RequiresSync || !AllCommittedMarkers(op.Logs.Logs);
    }

    /// <summary>
    /// Records, under the partition lock, that sync work was enqueued for a partition. When that makes
    /// as many operations as the partition's last synced write carried, the partition is back. Returns
    /// true when it was the last partition a gathering worker waited for; the caller then signals
    /// that worker once it has released the lock.
    ///
    /// <para><b>All of them, not the first.</b> With several writers on one partition, each sends
    /// its next operation when its own previous one is acknowledged. A partition that counted as back
    /// at the first of them would let the held batch go with one writer in it and leave the others
    /// to the write after: two groups of writers that take turns, each round two syncs, inside one
    /// partition.</para>
    ///
    /// <para>A return inside the window adds to the partition's run of quick returns; a later one
    /// ends the run, and the partition is not waited for until it has a new one.</para>
    /// </summary>
    private bool NoteReturn(PartitionState state, long nowTicks)
    {
        long doneTicks = state.LastSyncDoneTicks;

        // Nothing is out: the partition is back already, or no synced write of it has ended yet.
        if (doneTicks == 0)
            return false;

        // Some operations of the last write are still out.
        if (++state.BackSyncOps < state.DoneSyncOps)
            return false;

        state.LastSyncDoneTicks = 0;

        long window = ReturnWindowTicks();

        if (window > 0 && nowTicks - doneTicks <= window)
        {
            if (state.QuickReturns < MaxQuickReturnsNeeded)
                state.QuickReturns++;
        }
        else
            state.QuickReturns = 0;

        if (!state.AwaitedReturn)
            return false;

        state.AwaitedReturn = false;

        // An expected return that came. Enough of them in a row take back one doubling.
        if (state.QuickReturnsNeeded > 1 && ++state.ReturnsSinceMiss >= ReturnsToForgiveMiss)
        {
            state.QuickReturnsNeeded /= 2;
            state.ReturnsSinceMiss = 0;
        }

        return Interlocked.Decrement(ref _awaitedReturns) == 0;
    }

    /// <summary>
    /// Records, under the partition lock and before any completion of the batch fires, that a synced
    /// write that carried sync work of a partition ended, and decides whether the partition is
    /// expected back. Returns true when it is.
    ///
    /// <para><b>Before the completions</b>, because a completion is what makes the partition come
    /// back: stamped after, the return could be enqueued first and would be measured against the
    /// write before this one.</para>
    ///
    /// <para>What the partition has queued at this moment was enqueued before the write ended, so it
    /// is not a return of what the write carried: with several writers on the partition it is the
    /// other group of them. The partition is still waited for; its queued operations and the
    /// returning ones then go out together.</para>
    ///
    /// <para>A partition whose earlier write is still not all back when this one ends does not write
    /// in a closed loop at this pace: its run of quick returns ends here. A partition whose batch
    /// carried only markers or metadata is not in a write loop here, and its stamp is left
    /// alone.</para>
    /// </summary>
    private bool NoteSyncedWriteDone(int partitionId, PartitionState state, List<WALWriteOperation> pidBatch, long doneAtTicks, long window)
    {
        int carriedSyncOps = 0;

        foreach (WALWriteOperation op in pidBatch)
        {
            if (op.CountsAsSyncWork)
                carriedSyncOps++;
        }

        if (carriedSyncOps == 0)
            return false;

        if (state.LastSyncDoneTicks != 0)
            state.QuickReturns = 0;

        state.LastSyncDoneTicks = doneAtTicks;
        state.DoneSyncOps = carriedSyncOps;
        state.BackSyncOps = 0;

        bool expected = window > 0 && state.QuickReturns >= state.QuickReturnsNeeded;

        if (state.AwaitedReturn)
        {
            // Still expected from an earlier write that a worker did not wait out (a full batch, a
            // stop request). The expectation moves to this write, or ends with the run above.
            if (!expected)
            {
                state.AwaitedReturn = false;
                Interlocked.Decrement(ref _awaitedReturns);
            }

            return expected;
        }

        if (!expected)
            return false;

        state.AwaitedReturn = true;
        Interlocked.Increment(ref _awaitedReturns);
        Interlocked.Increment(ref _totalReturnsAwaited);

        if (!state.AwaitedQueued)
        {
            state.AwaitedQueued = true;
            _awaitedPartitions.Enqueue(partitionId);
        }

        return true;
    }

    /// <summary>
    /// Feeds the moving average of a synced write with one that ran with no other synced write in
    /// progress, so the sample is the cost of the sync and not of a wait behind another one.
    /// </summary>
    private void NoteSyncWriteDuration(long ticks)
    {
        if (_syncWriteAveragePinned)
            return;

        long average = Volatile.Read(ref _syncWriteEwmaTicks);
        Volatile.Write(ref _syncWriteEwmaTicks, average == 0 ? ticks : average + (ticks - average) / 8);
    }

    /// <summary>
    /// Tells the gathering worker, if there is one, that what it waits for changed. The epoch is
    /// bumped first, with a full fence: a worker that is about to sleep and is not registered yet
    /// reads it after it registered and does not sleep (<c>ParkedWorkerQueue.TryTakeWhile</c>).
    /// </summary>
    /// <returns>
    /// True when a worker was gathering. That worker then looks at the ready-queue before it does
    /// anything else, and if it leaves without taking what is there it passes the wake on
    /// (<c>HandOnIfQueued</c> in the worker loop), so a caller that just published an id need not
    /// wake another worker for it.
    /// </returns>
    private bool SignalGatherer()
    {
        Interlocked.Increment(ref _gatherEpoch);

#if !KOMMANDER_THREAD_FREE
        int gatherer = Volatile.Read(ref _gathererWorker);

        if (gatherer >= 0)
        {
            _readyPartitions.WakeTimedWaiter(gatherer);
            return true;
        }
#endif

        return false;
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
    /// <para>Phase 3 — post-write, in three passes. Pass 3a records every commit-wait
    /// observation (per partition and the single node-level sample for the batch).
    /// Pass 3b decrements depth and clears the in-flight stall age for every partition
    /// of the group. Pass 3c fires <c>OnComplete</c> callbacks, clears <c>InFlight</c>,
    /// and re-schedules if new ops arrived during the write.
    /// The order is a contract: a callback releases an awaiting caller, and that caller
    /// may read the accumulators and the backlog signals immediately, so no observation
    /// may be recorded, and no backlog released, after any callback of the batch has fired.
    /// <c>InFlight</c> is the exception and must outlive the callbacks, or a second worker
    /// could complete the partition's next batch out of submission order.</para>
    /// </summary>
    private void ProcessGroupBatch(
        List<int> partitionIds,
        List<(int PartitionId, List<WALWriteOperation> Ops)> groupBatches,
        List<List<WALWriteOperation>> opListPool,
        List<(int, List<RaftLog>)> logGroups,
        Dictionary<int, long> acknowledgedPresence)
    {
        groupBatches.Clear();

        // Return gathering: partitions of this batch that have sync work in it and the history of a
        // closed loop. With at least one, the batch's synced write is one other workers wait for.
        int partitionsExpectedBack = 0;

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

            int takenFromQueue = 0;
            bool tookSyncWork = false;
            bool deferred = false;
            bool announceDeferral = false;

            lock (state.Lock)
            {
                // Markers held back earlier were submitted before anything queued now: they go first.
                long deferredSinceTicks = 0;
                if (state.DeferredMarkers is { Count: > 0 } held)
                {
                    pidBatch.AddRange(held);
                    held.Clear();
                    deferredSinceTicks = state.DeferredSinceTicks;
                }

                while (pidBatch.Count < maxBatchSize && state.Ops.TryDequeue(out WALWriteOperation? op))
                {
                    pidBatch.Add(op);
                    takenFromQueue++;

                    if (op.CountsAsSyncWork)
                        tookSyncWork = true;
                }

                if (tookSyncWork && state.QuickReturns >= state.QuickReturnsNeeded)
                    partitionsExpectedBack++;

                // Clear Scheduled (the id has been consumed from _readyPartitions).
                state.Scheduled = false;

                if (TryDeferMarkerBatch(state, pidBatch, deferredSinceTicks))
                {
                    deferred = true;
                    announceDeferral = !state.DeferredQueued;
                    state.DeferredQueued = true;
                    pidBatch.Clear();
                }
                else
                {
                    // Raise InFlight so Enqueue cannot re-add this partition while the combined
                    // Write is in progress.
                    state.InFlight = true;

                    // Ops dequeue in submission order, so the first op of the batch is its oldest: the
                    // stall signal ages it until the post-write section below clears it.
                    state.InFlightOldestEnqueueTicks = pidBatch.Count > 0 ? pidBatch[0].EnqueueTicks : 0;
                }
            }

            // Drained ops leave the linger gate's evidence pool here, not at completion: once out of
            // the queue they can no longer cause a future ready-queue arrival by themselves. Markers
            // taken back from a deferral left it when they were first drained.
            if (trackQueuedOps && takenFromQueue > 0)
                Interlocked.Add(ref _queuedOperations, -takenFromQueue);

            if (deferred)
            {
                Interlocked.Increment(ref _totalMarkerBatchesDeferred);

                if (announceDeferral)
                    _deferredMarkerPartitions.Enqueue(pid);

                // The sync this batch stepped around may have ended between the check and the
                // announcement. Its worker then found nothing to release, so release it here: the
                // id is announced before this read, and that worker reads the queue after its
                // decrement, so one of the two always sees the other.
                // This worker may still hold other partitions to write, so the ids need a worker
                // of their own.
                if (Volatile.Read(ref _syncWritesInFlight) == 0)
                    RescheduleDeferredMarkers(engineIdle: true, wake: true);

                continue;
            }

            if (pidBatch.Count > 0)
                groupBatches.Add((pid, pidBatch));
            else
            {
                // Nothing to write; clear InFlight immediately.
                lock (state.Lock)
                {
                    state.InFlight = false;
                    state.InFlightOldestEnqueueTicks = 0;
                }
            }
        }

        if (groupBatches.Count == 0)
            return;

        // ── Phase 1.5: proposed-tail truncation, BEFORE the batch write ────
        // One truncate at the minimum FollowerAppend index per partition, run before the batch's
        // rows land. Running it AFTER the write (as this originally did) breaks the sequential
        // equivalence its comment claimed: executed one op at a time, an op's truncation runs
        // BEFORE later ops write their rows, so those rows survive — but write-everything-then-
        // truncate-at-the-batch-min deleted Proposed rows the SAME batch had just written
        // (a fresh propose coalesced behind a lower-anchored append), silently discarding
        // entries whose presence/commit bookkeeping had already advanced at enqueue. Truncating
        // first keeps the pre-existing-orphan cleanup (those rows predate every op in the batch,
        // so the batch minimum is exactly their sequential fate) while every row written by this
        // batch survives it; a genuine orphan among them is cleaned by a later batch's pass.
        // Two guards keep the sweep from deleting live data (it deletes with no log line and no
        // frontier bookkeeping, so anything it takes is silently unrecoverable):
        //
        //   1. Only an op that writes a first-durability row (Proposed/ProposedCheckpoint) may
        //      define a cutoff. Such an op is a live tail append, and "the tail ends at X ⇒
        //      Proposed rows above X are a dead term's orphans" is the sweep's founding claim. A
        //      pure resolution op — a commit-marker re-ship of an OLD id — says nothing about the
        //      tail: with pipelined proposals the commit of id k always races proposals k+1.., so
        //      letting its low max id drive the cutoff deleted freshly-acked quorum rows above it.
        //      That is precisely what manufactured the shared hole at index 6 on four of five
        //      voters in Jepsen run 32690955741 (a duplicate commit-marker batch for the committed
        //      prefix, LogIndex 5, arriving after proposes 6..8 had landed) and left the partition
        //      permanently leaderless.
        //
        //   2. The cutoff never goes below the enqueue-time accepted-id floor stamped on the ops
        //      (see WALWriteOperation.TruncateFloor): a row this process accepted may back a
        //      quorum ack and is certified by the in-memory frontiers, which are never re-read
        //      from disk. This also covers a late proposal RETRY (a Proposed-carrying op whose max
        //      id sits below already-accepted higher rows). Floors are monotone per partition, so
        //      the batch maximum is the latest knowledge.
        //
        // Pre-restart orphans (Proposed rows of a dead term left above the durable tail by a
        // crash) sit above the floor whenever recovery could not chain presence over them, and
        // are still swept; rows the floor covers are certified by the in-memory frontiers and
        // never touched.
        foreach ((int pid, List<WALWriteOperation> pidBatch) in groupBatches)
        {
            long minTruncateIndex = long.MaxValue;
            long acceptedFloor = -1;

            foreach (WALWriteOperation op in pidBatch)
            {
                if (op.Type != WALWriteOperationType.FollowerAppend)
                    continue;

                if (op.TruncateFloor > acceptedFloor)
                    acceptedFloor = op.TruncateFloor;

                if (op.LogIndex > 0 && op.LogIndex < minTruncateIndex && ContainsFirstDurabilityRow(op.Logs.Logs))
                    minTruncateIndex = op.LogIndex;
            }

            if (minTruncateIndex != long.MaxValue)
            {
                long cut = Math.Max(minTruncateIndex, acceptedFloor);
                RaftOperationStatus truncated = walAdapter.TruncateProposedLogsAfter(pid, cut);
                if (TraceSink is { } truncateTrace && truncated == RaftOperationStatus.Success)
                    truncateTrace.Record(new(pid, Diagnostics.WalTraceEventKind.Truncate, Max: cut));
            }
        }

        // ── Phase 2: single cross-partition WAL write ──────────────────────
        RaftOperationStatus status;
        long writeStartTicks = tickSource.GetTimestamp();

        // Whether the batch was written with its own fsync. Each completion carries it, because the
        // follower's reported durable frontier must not count a resolution that rode sync-off and
        // is not on disk yet (see RaftWriteAhead.MarkResolutionWritten).
        bool batchSynced = true;

        // Whether this batch is counted in _syncWritesInFlight, so every exit below gives it back.
        bool syncWriteCounted = false;

        // Whether this batch is counted in _unsettledSyncWrites (return gathering), given back once
        // its partitions are marked in Phase 3b; and whether its synced write started with no other
        // one in progress, which makes its duration a sample of one sync.
        bool unsettledCounted = false;
        bool syncWriteAlone = false;
        bool syncWritten = false;

        try
        {
            // Reuse the worker-owned logGroups list — cleared here, populated below.
            logGroups.Clear();
            acknowledgedPresence.Clear();

            // Single-fsync fast path: the batch may skip its own fsync only if EVERY log in it is a
            // per-entry Committed marker (already quorum-durable from its propose fsync, so a lost marker
            // is reconstructible). Any proposed entry, CommittedCheckpoint (the durable recovery anchor),
            // rollback, or other type forces a sync write — and that sync write also flushes any sync-off
            // markers batched alongside it. So does an operation flagged RequiresSync: a Committed row
            // the partition never held is not a marker but the row's first and only durable write, and
            // the type cannot tell the two apart (a follower that receives a propose broadcast after the
            // leader committed it, or a backfilled committed range, sees the row typed Committed on first
            // sight). Off by default ⇒ always sync ⇒ byte-for-byte prior behaviour.
            bool sync = !lazyCommitMarkers;

            foreach ((int pid, List<WALWriteOperation> ops) in groupBatches)
                foreach (WALWriteOperation op in ops)
                {
                    // Hard-state writes are metadata, not log rows: run each on this worker ahead of
                    // the batch, with its own status, so a vote grant or a term adoption never blocks
                    // the partition executor on the engine. Ordered before the batch write so a term
                    // this batch's rows were written under is on disk no later than the rows.
                    if (op.Type is WALWriteOperationType.HardState or WALWriteOperationType.HlcFloor)
                    {
                        op.MetadataStatus = PersistMetadata(pid, op);
                        continue;
                    }

                    // The acknowledged-presence fold: per partition, the LAST value in submission
                    // order wins, because each operation's value already describes the whole frontier
                    // its batch establishes (not just its own rows) and a regression correction must
                    // be able to lower it. The backend assigns the folded value in the same batch as
                    // the rows (IWAL.Write, three-argument overload).
                    if (op.Type == WALWriteOperationType.AcknowledgedPresence)
                    {
                        acknowledgedPresence[pid] = op.MetadataValue;
                        continue;                       // carries no rows
                    }

                    if (op.AcknowledgedPresence >= 0)
                        acknowledgedPresence[pid] = op.AcknowledgedPresence;

                    logGroups.Add(op.Logs);
                    if (!sync && (op.RequiresSync || !AllCommittedMarkers(op.Logs.Logs)))
                        sync = true;
                }

            batchSynced = sync;

            // A batch holding only a correction still writes: the value it carries is what keeps a
            // crash from reporting a failed range as lost acknowledged entries.
            if (logGroups.Count > 0 || acknowledgedPresence.Count > 0)
            {
                if (sync)
                {
                    syncWriteAlone = Interlocked.Increment(ref _syncWritesInFlight) == 1;
                    syncWriteCounted = true;
                    syncWritten = true;

                    // Only a write that carries partitions expected back is worth another worker's
                    // wait: that worker keeps its batch open for them. For any other write the
                    // engine's own writer queue hands over faster than a parked worker wakes, so
                    // nobody is asked to wait and the load behaves as it did without gathering.
                    if (partitionsExpectedBack > 0)
                    {
                        Interlocked.Increment(ref _unsettledSyncWrites);
                        unsettledCounted = true;
                    }
                }

                status = walAdapter.Write(logGroups, sync, acknowledgedPresence.Count > 0 ? acknowledgedPresence : null);

                if (TraceSink is { } writeTrace && status == RaftOperationStatus.Success)
                    TraceBatchWrite(writeTrace, groupBatches, sync);
                Interlocked.Increment(ref _totalBatchesWritten);
                if (sync)
                {
                    Interlocked.Increment(ref _totalSyncBatchesWritten);
                    KommanderMetrics.WalSyncBatchesTotal.Add(1);
                }
                Interlocked.Add(ref _totalPartitionsBatched, groupBatches.Count);
            }
            else
                status = RaftOperationStatus.Success;
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[FairWalScheduler] Group WAL write error ({PartitionCount} partitions): {Message}",
                groupBatches.Count, ex.Message);
            status = RaftOperationStatus.Errored;
        }

        // The engine is done with this batch. When that was the last sync in progress, a marker
        // batch held back behind it can be written now without waiting on anything.
        bool engineIdle = syncWriteCounted && Interlocked.Decrement(ref _syncWritesInFlight) == 0;

        // Stamp completion time once for all ops in this group batch.
        long doneAtTicks = tickSource.GetTimestamp();
        double ticksToMs = 1000.0 / tickSource.Frequency;

        if (returnGathering && syncWriteAlone && status == RaftOperationStatus.Success)
            NoteSyncWriteDuration(doneAtTicks - writeStartTicks);

        // Round stages (off by default): one stopwatch stamp per batch, carried on each completion
        // so the executor can time the completion's trip back to the partition.
        long durableStageTicks = RoundStageInstrumentation.Stamp();

        KommanderMetrics.WalBatchesTotal.Add(1);

        // The engine's answer time for this batch, success or failure alike. The commit-wait EWMA
        // below is fed by completed batches too, but as an average it hides a single slow fsync;
        // the histogram keeps the max readable from a scrape, and a batch that errored is still a
        // measurement of how long the engine held the worker.
        KommanderMetrics.WalWriteDurationMs.Record(
            (doneAtTicks - writeStartTicks) * ticksToMs,
            status == RaftOperationStatus.Success ? KommanderMetrics.WalWriteResultOk : KommanderMetrics.WalWriteResultErrored);

        // ── Phase 3a: measure the batch before anyone learns it completed ────
        // Every commit-wait observation (per partition and node-wide) is recorded here, before
        // Phase 3b fires a single OnComplete. A callback releases the caller's await, and that
        // caller may read the accumulators at once — a load report built right after an ack
        // must already include the batch it was acked for. Recording after the callbacks let a
        // reader see the ack and a sample count one short of the writes it observed.
        //
        // Node-wide wait is accumulated across every partition in this group batch and recorded
        // once, so one shared fsync yields exactly one node-level observation.
        double groupTotalWaitMs = 0;
        int groupOpCount = 0;

        // One volatile read decides whether to attribute per-op latency to its
        // phase for the double-fsync measurement; off by default, no cost in prod.
        bool instrument = WalPhaseInstrumentation.Enabled;

        foreach ((int pid, List<WALWriteOperation> pidBatch) in groupBatches)
        {
            KommanderMetrics.WalBatchSize.Record(pidBatch.Count);

            // Record commit-wait latency for this partition's batch (enqueue→durable).
            // Average across all ops so each batch contributes one observation to the EWMA,
            // regardless of batch size, giving consistent per-partition decay behaviour.
            if (!_partitions.TryGetValue(pid, out PartitionState? waitState))
                continue;

            double totalWaitMs = 0;
            foreach (WALWriteOperation op in pidBatch)
            {
                double opWaitMs = (doneAtTicks - op.EnqueueTicks) * ticksToMs;
                totalWaitMs += opWaitMs;
                if (instrument)
                    WalPhaseInstrumentation.RecordDurable(op.Type, opWaitMs);
                if (durableStageTicks != 0)
                    RecordWalStage(op.Type, opWaitMs);
            }
            waitState.CommitWait.RecordWaitMs(totalWaitMs / pidBatch.Count);

            groupTotalWaitMs += totalWaitMs;
            groupOpCount += pidBatch.Count;
        }

        if (groupOpCount > 0)
            _nodeCommitWait.RecordWaitMs(groupTotalWaitMs / groupOpCount);

        // ── Phase 3b: release the batch's backlog before anyone learns it completed ──
        // Same contract as 3a, for the backlog signals. The batch is finished with the engine, so its
        // operations are no longer "pending or in flight" and no longer age the stall signal. Both
        // were released after the callbacks, so a caller woken by its ack could read depth 1 (and a
        // non-zero oldest-pending age) for a queue that held nothing but the write it was just acked
        // for — a sequential writer reported a backlog it never had, and a load report built right
        // after an ack carried it to the balancer. Every partition of the group is released before
        // the first callback of ANY partition fires: one ack can wake a reader of another partition.
        // InFlight deliberately stays raised until 3c; see there.
        bool returnExpectations = returnGathering && syncWritten && status == RaftOperationStatus.Success;
        long returnWindowTicks = returnExpectations ? ReturnWindowTicks() : 0;
        bool anyReturnExpected = false;

        foreach ((int pid, List<WALWriteOperation> pidBatch) in groupBatches)
        {
            if (maxGlobalQueueDepth > 0)
                Interlocked.Add(ref _globalQueueDepth, -pidBatch.Count);

            if (!_partitions.TryGetValue(pid, out PartitionState? releasedState))
                continue;

            lock (releasedState.Lock)
            {
                releasedState.Depth -= pidBatch.Count;
                releasedState.InFlightOldestEnqueueTicks = 0;

                // Return gathering: stamp the end of the write and decide whether the partition is
                // expected back. Here, before any callback, for the reason NoteSyncedWriteDone gives.
                if (returnExpectations && NoteSyncedWriteDone(pid, releasedState, pidBatch, doneAtTicks, returnWindowTicks))
                    anyReturnExpected = true;
            }
        }

        // The batch is settled: a worker that held its own batch back for this write can go on, and
        // now finds the partitions it is to wait for. The deadline is published before the count
        // goes down, so that worker never reads the deadline of an earlier write.
        if (anyReturnExpected)
            Volatile.Write(ref _returnDeadlineTicks, doneAtTicks + returnWindowTicks);

        if (unsettledCounted)
            Interlocked.Decrement(ref _unsettledSyncWrites);

        // Every synced write can change what a gathering worker waits for: the count above, who is
        // expected back, the deadline. The signal costs one interlocked step when nobody gathers.
        if (returnExpectations || unsettledCounted)
            SignalGatherer();

        // ── Phase 3c: per-partition completion and re-scheduling ─────────────
        // InFlight is cleared only AFTER the partition's callbacks: clearing it earlier would let
        // another worker drain, write and complete the partition's next batch while these callbacks
        // are still running, delivering completions out of submission order.
        foreach ((int pid, List<WALWriteOperation> pidBatch) in groupBatches)
        {
            foreach (WALWriteOperation op in pidBatch)
            {
                try
                {
                    op.OnComplete(BuildCompletion(op, status, batchSynced, durableStageTicks));
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

            if (!_partitions.TryGetValue(pid, out PartitionState? state))
                continue;

            lock (state.Lock)
            {
                state.InFlight = false;

                if (state.Ops.Count > 0 && !state.Scheduled)
                {
                    // No parked worker is woken: the thread running this batch goes back to the
                    // ready-queue as soon as the batch returns (a worker's next take, a pump's next
                    // pass, the shutdown drain) and takes the id itself. Waking one here only
                    // produced a worker that found the queue already swept. A lingering worker is
                    // woken: this id is the arrival its evidence gate made it wait for.
                    state.Scheduled = true;
                    _readyPartitions.EnqueueFromWorker(pid);
                }
            }
        }

        // After this batch's own callbacks, so the order of its completions is as it always was.
        // The callbacks are also what brings each partition's next propose, which takes its held
        // markers with it; what is still held here has no write of its own coming. No parked
        // worker is woken, for the reason above.
        RescheduleDeferredMarkers(engineIdle, wake: false);
    }

    private static long MarkerDeferralTicks(bool lazyCommitMarkers, bool manualExecution, int markerDeferralMs, long ticksPerSecond) =>
        lazyCommitMarkers && !manualExecution && markerDeferralMs > 0
            ? (long)(markerDeferralMs / 1000.0 * ticksPerSecond)
            : 0;

    /// <summary>
    /// Decides, under the partition lock, whether the batch just drained for a partition is held back
    /// instead of written, and holds it back when so. Returns true when the batch was moved to
    /// <see cref="PartitionState.DeferredMarkers"/>; the caller then must not write it.
    ///
    /// <para><b>Why.</b> A batch that holds only lazy commit markers is written sync-off, which costs
    /// microseconds on an idle engine. The engine serializes its writers, though: while another
    /// partition's synced write is in progress, a sync-off write waits for that sync to end. The
    /// marker batch then holds its partition in flight for a whole sync, and the partition's next
    /// propose, which is usually enqueued a moment after the markers, waits behind it and then pays
    /// its own sync. With four partitions on one engine a round took two syncs instead of one.</para>
    ///
    /// <para><b>What changes.</b> Only when the markers are written. A held batch stays first in the
    /// partition's order and is written by the partition's next batch, in the same engine write as
    /// the propose that follows it, or on its own once no sync is in progress or
    /// the bound passed. Its operations are not complete while held:
    /// every callback still fires after the write that carried its rows, with that write's sync
    /// flag, so nothing downstream learns of a marker that is not in the store.</para>
    ///
    /// <para><b>What may be held.</b> Leader commit operations whose rows are all per-entry
    /// <c>Committed</c> markers and that do not require a sync: exactly what the fast path would
    /// write sync-off. A batch with anything else in it is written at once, as before, and takes
    /// earlier held markers with it. Nothing is held when no sync is in progress (the write is
    /// cheap), when the partition has more queued (the batch size cap cut the drain short), or
    /// while the scheduler stops.</para>
    /// </summary>
    private bool TryDeferMarkerBatch(PartitionState state, List<WALWriteOperation> pidBatch, long deferredSinceTicks)
    {
        if (markerDeferralTicks == 0 || _stopping || pidBatch.Count == 0 || state.Ops.Count > 0)
            return false;

        if (Volatile.Read(ref _syncWritesInFlight) == 0)
            return false;

        long nowTicks = tickSource.GetTimestamp();

        // Markers that were already held keep their first deferral time: the bound is on the wait of
        // the oldest marker, not on each pass through here.
        if (deferredSinceTicks != 0 && nowTicks - deferredSinceTicks >= markerDeferralTicks)
            return false;

        foreach (WALWriteOperation op in pidBatch)
        {
            if (op.Type != WALWriteOperationType.LeaderCommit || op.RequiresSync || !AllCommittedMarkers(op.Logs.Logs))
                return false;
        }

        state.DeferredMarkers ??= new List<WALWriteOperation>(pidBatch.Count);
        state.DeferredMarkers.AddRange(pidBatch);
        state.DeferredSinceTicks = deferredSinceTicks != 0 ? deferredSinceTicks : nowTicks;

        return true;
    }

    /// <summary>
    /// Puts partitions that hold a deferred marker batch back in the ready-queue, so a worker drains
    /// them and decides again: with <paramref name="engineIdle"/> every such partition (its markers
    /// can be written at once), otherwise only those held for the bound.
    /// Called by a worker when it defers and after each batch it writes.
    ///
    /// <para><b>Nothing stays held for ever.</b> A batch is held only while a synced write is in
    /// progress. The worker of that write calls this when the write ends. If other synced writes
    /// are still in progress then, a young batch stays held, and each of those writes calls this
    /// again when it ends. The last one to end passes <paramref name="engineIdle"/>.</para>
    /// </summary>
    /// <param name="engineIdle">True when no synced write is in progress.</param>
    /// <param name="wake">
    /// Whether to wake a parked worker for the ids put back. False when the caller returns to the
    /// ready-queue right after and takes them itself; a lingering worker is woken either way. One
    /// wake covers all of them: a worker takes every ready id into its group.
    /// </param>
    private void RescheduleDeferredMarkers(bool engineIdle, bool wake)
    {
#if !KOMMANDER_THREAD_FREE
        // The thread-free build runs in manual mode only, where nothing is ever held.
        if (_deferredMarkerPartitions.IsEmpty)
            return;

        long nowTicks = tickSource.GetTimestamp();
        bool scheduledAny = false;

        // One pass over what is queued now: an id put back below is not looked at again in this call.
        for (int remaining = _deferredMarkerPartitions.Count; remaining > 0; remaining--)
        {
            if (!_deferredMarkerPartitions.TryDequeue(out int pid))
                break;

            if (!_partitions.TryGetValue(pid, out PartitionState? state))
                continue;

            bool keepQueued = false;

            lock (state.Lock)
            {
                if (state.DeferredMarkers is not { Count: > 0 })
                {
                    // The partition's next batch already took them.
                    state.DeferredQueued = false;
                    continue;
                }

                bool due = engineIdle || _stopping || nowTicks - state.DeferredSinceTicks >= markerDeferralTicks;

                if (!due)
                {
                    keepQueued = true;
                }
                else if (state.Scheduled || state.InFlight)
                {
                    // A worker is about to drain the partition, or will re-schedule it: that drain
                    // takes the markers.
                    state.DeferredQueued = false;
                }
                else
                {
                    state.Scheduled = true;
                    state.DeferredQueued = false;
                    _readyPartitions.Publish(pid);
                    scheduledAny = true;
                }
            }

            if (keepQueued)
                _deferredMarkerPartitions.Enqueue(pid);
        }

        if (scheduledAny)
        {
            if (wake)
                _readyPartitions.WakeOne();
            else
                _readyPartitions.WakeTimedWaiter();
        }
#endif
    }

#if !KOMMANDER_THREAD_FREE
    // Shutdown drain of the worker loop, which the thread-free build does not have.
    /// <summary>
    /// The shutdown drain of one worker: writes everything this worker can claim, in as many passes
    /// as it takes, and returns when nothing is left to claim. Runs after <see cref="Stop"/> closed
    /// admission, once the worker finished the batch it held.
    ///
    /// <para><b>Why it loops.</b> A batch takes at most <see cref="maxBatchSize"/> operations of a
    /// partition, and the batch's tail puts the partition back in the ready-queue when more are
    /// queued. A drain that made one pass and returned left that id in the queue with no worker to
    /// take it: every operation beyond the first few batches kept its callback unfired, and whoever
    /// waited on it waited for ever. Each pass here drains at least one operation of every partition
    /// it takes and admission is closed, so the loop ends.</para>
    ///
    /// <para><b>Why a partition is claimed through the ready-queue.</b> Only the thread that
    /// dequeued a partition's id may drain it; that is what keeps one writer per partition. The
    /// sweep below therefore does not process a partition it finds with work: it schedules it, under
    /// the partition lock and only when it is neither scheduled nor in flight, and the dequeue that
    /// follows decides which worker gets it. Processing it directly, as this once did, let a second
    /// draining worker take the same partition's id from the queue meanwhile and clear its in-flight
    /// flag under the first.</para>
    ///
    /// <para><b>Why returning with partitions still in flight is enough.</b> A partition in flight
    /// belongs to a worker that is inside its write. That worker puts the partition back when the
    /// write ends, finds the stop request on its next take, and runs this drain itself. So every
    /// id that is published during shutdown is published by a worker that drains afterwards.</para>
    /// </summary>
    private void DrainRemaining(
        List<int> partitionGroup,
        List<(int PartitionId, List<WALWriteOperation> Ops)> groupBatches,
        List<List<WALWriteOperation>> opListPool,
        List<(int, List<RaftLog>)> logGroups,
        Dictionary<int, long> acknowledgedPresence)
    {
        while (true)
        {
            // Schedule every partition that holds work nobody owns. In normal operation there is
            // none: an operation is scheduled when it is enqueued. This covers held commit markers,
            // which wait for a write that will not come now, and any partition a race at shutdown
            // left unscheduled.
            foreach (KeyValuePair<int, PartitionState> kv in _partitions)
            {
                PartitionState state = kv.Value;

                lock (state.Lock)
                {
                    bool hasWork = state.Ops.Count > 0 || state.DeferredMarkers is { Count: > 0 };

                    if (hasWork && !state.Scheduled && !state.InFlight)
                    {
                        state.Scheduled = true;
                        _readyPartitions.Publish(kv.Key);
                    }
                }
            }

            // Take everything ready as one group-commit batch. No partition-count cap here, unlike
            // the worker loop: ProcessGroupBatch grows its scratch lists on demand.
            partitionGroup.Clear();
            while (_readyPartitions.TryDequeue(out int pid))
                partitionGroup.Add(pid);

            if (partitionGroup.Count == 0)
                return;

            ProcessGroupBatch(partitionGroup, groupBatches, opListPool, logGroups, acknowledgedPresence);
        }
    }
#endif

    // ── Helpers ────────────────────────────────────────────────────────────

    /// <summary>
    /// True when every log in <paramref name="logs"/> is a per-entry <see cref="RaftLogType.Committed"/>
    /// marker — the only type the single-fsync fast path may write sync-off. Notably excludes
    /// <see cref="RaftLogType.CommittedCheckpoint"/> (the durable recovery anchor, which must always fsync)
    /// and every first-durability type (Proposed/ProposedCheckpoint) and rollback marker. An empty list is
    /// not eligible (returns false), so it can never spuriously suppress a fsync.
    /// </summary>
    /// <summary>
    /// True when <paramref name="logs"/> contains at least one first-durability row
    /// (<see cref="RaftLogType.Proposed"/> or <see cref="RaftLogType.ProposedCheckpoint"/>) — the
    /// mark of a live tail append. Only such an operation may drive the proposed-tail truncation
    /// cutoff; see the guard notes at the call site.
    /// </summary>
    private static bool ContainsFirstDurabilityRow(List<RaftLog> logs)
    {
        foreach (RaftLog log in logs)
        {
            if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                return true;
        }
        return false;
    }

    /// <summary>
    /// Records one Write event per log-carrying operation of a successful batch, in submission order
    /// per partition, before any completion fires: the validator applies the rows to its disk model
    /// at write time, so a crash between the write and the completion is modelled as the store sees it.
    /// </summary>
    private static void TraceBatchWrite(
        Diagnostics.IWalTraceSink sink, List<(int, List<WALWriteOperation>)> groupBatches, bool synced)
    {
        foreach ((int pid, List<WALWriteOperation> ops) in groupBatches)
            foreach (WALWriteOperation op in ops)
            {
                if (op.Type is WALWriteOperationType.HardState or WALWriteOperationType.HlcFloor or WALWriteOperationType.AcknowledgedPresence)
                    continue;

                List<RaftLog> logs = op.Logs.Logs;
                Diagnostics.WalTraceRow[] rows = new Diagnostics.WalTraceRow[logs.Count];
                for (int i = 0; i < logs.Count; i++)
                    rows[i] = new(logs[i].Id, logs[i].Type);

                sink.Record(new(pid, Diagnostics.WalTraceEventKind.Write, Rows: rows, Synced: synced));
            }
    }

    /// <summary>
    /// True when the resolved rows of <paramref name="logs"/> cover every id in
    /// [<paramref name="min"/>, <paramref name="max"/>]. One pass with no allocation when the resolved
    /// rows are in ascending order without gaps, which is the common case; otherwise counts the
    /// distinct resolved ids.
    /// </summary>
    private static bool ResolvedFillSpan(List<RaftLog> logs, long min, long max)
    {
        long previous = -1;
        bool ordered = true;
        foreach (RaftLog log in logs)
        {
            if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                continue;
            if (previous >= 0 && log.Id != previous && log.Id != previous + 1)
            {
                ordered = false;
                break;
            }
            previous = log.Id;
        }

        if (ordered)
            return true;

        HashSet<long> distinct = [];
        foreach (RaftLog log in logs)
        {
            if (log.Type is not (RaftLogType.Proposed or RaftLogType.ProposedCheckpoint))
                distinct.Add(log.Id);
        }
        return distinct.Count == max - min + 1;
    }

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

    private RaftOperationStatus PersistMetadata(int partitionId, WALWriteOperation op)
    {
        try
        {
            bool ok = op.Type == WALWriteOperationType.HardState
                ? walAdapter.PersistHardState(partitionId, op.Term, op.VotedFor)
                : walAdapter.PersistHlcFloor(partitionId, op.MetadataValue);

            return ok ? RaftOperationStatus.Success : RaftOperationStatus.Errored;
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[FairWalScheduler] {Type} metadata write for partition {PartitionId} (term {Term}, vote {VotedFor}, value {Value}) threw: {Message}",
                op.Type, partitionId, op.Term, op.VotedFor ?? "(none)", op.MetadataValue, ex.Message);
            return RaftOperationStatus.Errored;
        }
    }

    /// <summary>
    /// Records the enqueue → durable wait of a round write as its
    /// <see cref="RoundStageInstrumentation"/> stage. The wait is the scheduler's own measurement,
    /// on its tick source (the stopwatch in production), so it matches <c>WalPhaseInstrumentation</c>.
    /// </summary>
    private static void RecordWalStage(WALWriteOperationType type, double waitMs)
    {
        switch (type)
        {
            case WALWriteOperationType.LeaderPropose:
                RoundStageInstrumentation.RecordMs(RoundStage.LeaderWal, waitMs);
                break;

            case WALWriteOperationType.LeaderCommit:
                RoundStageInstrumentation.RecordMs(RoundStage.LeaderCommitWal, waitMs);
                break;

            case WALWriteOperationType.FollowerAppend:
                RoundStageInstrumentation.RecordMs(RoundStage.FollowerWal, waitMs);
                break;
        }
    }

    private static RaftWalCompletion BuildCompletion(WALWriteOperation op, RaftOperationStatus status, bool synced, long durableStageTicks = 0)
    {
        if (op.Type is WALWriteOperationType.HardState or WALWriteOperationType.HlcFloor)
            return new RaftWalCompletion(op.Logs.PartitionId, op.OperationId, op.Term, -1, -1, op.Type, op.MetadataStatus, MetadataValue: op.MetadataValue);

        List<RaftLog> logs = op.Logs.Logs;
        long minIndex = -1;
        long writtenMax = -1;
        long resolvedMax = -1;
        long resolvedMin = -1;

        // The ids a batch carries are ascending in every enqueue path (the propose allocator
        // counts up, the commit/rollback/follower paths order by id), so one pass that checks each
        // id is the previous one or its successor proves the batch fills its own span without
        // allocating. A batch that fails the check is ordered and de-duplicated below; if it still
        // fills the span (an unordered but complete batch) the ids are dropped again.
        bool contiguous = true;
        for (int i = 0; i < logs.Count; i++)
        {
            long id = logs[i].Id;

            if (logs[i].Type is not (RaftLogType.Proposed or RaftLogType.ProposedCheckpoint))
            {
                if (id > resolvedMax)
                    resolvedMax = id;
                if (resolvedMin < 0 || id < resolvedMin)
                    resolvedMin = id;
            }

            if (i == 0)
            {
                minIndex = id;
                writtenMax = id;
                continue;
            }

            long previous = logs[i - 1].Id;
            if (id != previous && id != previous + 1)
                contiguous = false;
            if (id < minIndex) minIndex = id;
            if (id > writtenMax) writtenMax = id;
        }

        long[]? sparseIds = null;
        if (!contiguous)
        {
            sparseIds = logs.Select(static log => log.Id).Distinct().Order().ToArray();
            if (sparseIds.Length == writtenMax - minIndex + 1)
                sparseIds = null;
        }

        // The resolved ids get the same treatment: allocation-free when they fill their own span,
        // which a pure marker batch in id order always does.
        long[]? resolvedSparseIds = null;
        if (resolvedMin >= 0 && !ResolvedFillSpan(logs, resolvedMin, resolvedMax))
            resolvedSparseIds = logs
                .Where(static log => log.Type is not (RaftLogType.Proposed or RaftLogType.ProposedCheckpoint))
                .Select(static log => log.Id).Distinct().Order().ToArray();

        return new RaftWalCompletion(
            PartitionId: op.Logs.PartitionId,
            OperationId: op.OperationId,
            Term: op.Term,
            MinLogIndex: minIndex,
            MaxLogIndex: op.LogIndex,
            OperationType: op.Type,
            Status: status,
            WrittenMaxLogIndex: writtenMax,
            SparseLogIds: sparseIds,
            Synced: synced,
            ResolvedMaxLogIndex: resolvedMax,
            ResolvedMinLogIndex: resolvedMin,
            ResolvedSparseLogIds: resolvedSparseIds
        )
        {
            DurableStageTicks = durableStageTicks
        };
    }

    // ── IDisposable ────────────────────────────────────────────────────────

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        // Join all workers before disposing managed resources so that no worker
        // thread can touch _readyPartitions after it is disposed.
        if (!_stopping)
            Stop();
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
