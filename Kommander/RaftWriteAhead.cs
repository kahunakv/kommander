
using System.Buffers;
using System.Diagnostics;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Logging;
using Kommander.Support.Collections;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Manages the write-ahead log (WAL) for a Raft partition. Provides functionality for
/// recovering logs, proposing new operations, committing or rolling back changes,
/// and managing Raft log compaction.
/// </summary>
public sealed class RaftWriteAhead
{
    private readonly RaftManager manager;

    private readonly Action<RaftWalCompletion> onComplete;

    private readonly RaftPartition partition;

    private readonly IWAL walAdapter;

    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// A private dictionary to store and manage collections of <see cref="RaftLog"/> objects
    /// categorized by their respective actions (<see cref="RaftLogAction"/>). This dictionary
    /// is utilized to track operations such as propose, commit, and rollback for logs during
    /// the write-ahead process in the Raft consensus algorithm.
    ///
    /// By grouping logs based on their actions we can enqueue the write operations in the
    /// persistence (rocksdb or sqlite) in a more efficient way.
    /// </summary>
    private readonly SmallDictionary<RaftLogAction, List<RaftLog>> plan = new(3);

    /// <summary>
    /// The order <see cref="EnqueueProposeOrCommit"/> flattens the write plan: proposes first,
    /// resolutions last, so within one physical batch the resolved row state always wins over a
    /// stale duplicate Proposed copy of the same id (last put wins). See the note at the flatten
    /// loop for why iterating the reused dictionary directly was order-nondeterministic.
    /// </summary>
    private static readonly RaftLogAction[] PlanFlattenOrder =
        [RaftLogAction.Propose, RaftLogAction.Rollback, RaftLogAction.Commit];
    
    private readonly int compactEveryOperations;
    
    private readonly int compactNumberEntries;

    private readonly int maxEntriesPerCompaction;

    private bool recovered;

    // ── Soft restore floor (the "soft checkpoint") ───────────────────────────────────────────────
    // A durable CommittedCheckpoint (the HARD checkpoint) is the compaction anchor: entries below
    // it are removed, so it must stay conservative (it is fenced by the application-durability
    // floor and by follower retention). The application-durability floor itself, however, can sit
    // far ABOVE the last hard checkpoint — exactly the state a blocked compaction leaves behind —
    // and every entry at or below that floor is already durably applied by the application.
    // Redelivering those entries on restore is pure replay cost: it made cold-restart time
    // proportional to workload runtime instead of to the application's flush lag.
    //
    // The three fields below carry the floor from LoadRestoreLogsAsync (phase 1) to
    // CompleteRestoreAsync (phase 2). They are written once in phase 1 and read once in phase 2;
    // the executor handoff between the phases orders the accesses.
    //
    // restoreDeliveryFloor: committed CONSUMER entries with id <= this are not redelivered via
    // OnLogRestored. System-partition coordinator entries are exempt — the coordinator rebuilds
    // its in-memory state (roster, partition map) from replay, and the application floor says
    // nothing about that state. -1 = no floor (no provider, or "no opinion").
    private long restoreDeliveryFloor = -1;

    // restoreSoftFloorIndex/Term: when > 0, the restore read was NARROWED to start at
    // restoreSoftFloorIndex + 1 and the frontier scan must be seeded as if a CommittedCheckpoint
    // sat at this position — the floor certifies its whole prefix committed (the application can
    // only durably apply entries that were delivered as committed, and delivery is contiguous).
    // Only set when the WAL was proven to still hold the entry at the floor, so the (term, index)
    // freshness pair advertised after restore describes a real on-disk entry.
    private long restoreSoftFloorIndex = -1;

    private long restoreSoftFloorTerm;

    private long proposeIndex = 1;

    private long commitIndex = 1;

    // Out-of-order resolved (Committed/RolledBack) ids buffered until the gap below them fills,
    // so the follower's commit frontier (commitIndex) only ever advances over a contiguous prefix.
    // The unanchored live-propose broadcast delivers entries out of order under load; without this
    // buffer the frontier either overshoots a hole (applying entries before their predecessors,
    // which slows WriteOperationCompleted) or — if advanced only on an exact match — freezes at the
    // first reordered entry. Touched only on the partition's serialized executor path.
    private readonly SortedSet<long> pendingResolved = new();

    /// <summary>Throttle for <see cref="LogStaleProposedSkipped"/>: executor thread only.</summary>
    private long lastStaleProposedLogTicks;
    private int suppressedStaleProposedLogs;

    /// <summary>
    /// Running count of stale-duplicate skips since this instance was constructed, as opposed to
    /// the per-window <see cref="suppressedStaleProposedLogs"/> which resets on every emitted line.
    /// Surfaced through <see cref="GetStaleProposedSkippedCount"/> so a harness can read the total
    /// rather than reconstructing it by parsing throttled log lines — "fired occasionally" versus
    /// "fired constantly" is the distinction that matters, and only a running total answers it from
    /// a single observation. In memory, so it starts again at zero on every restart: it is a floor,
    /// not a lifetime total, and the log lines are the record that survives a crash. Written on the
    /// executor thread, read from arbitrary threads; see the accessor for the read discipline.
    /// </summary>
    private long staleProposedSkipped;

    // Reused across follower appends to collect this batch's resolved ids; applied to the frontier
    // only after the WAL enqueue succeeds, so a backpressure rejection needs no frontier rollback.
    private readonly List<long> resolvedThisBatch = [];

    // ── Contiguous-presence frontier ─────────────────────────────────────────────────────────────
    // The next slot after the largest id L such that every id through L is durably present in the
    // WAL (any entry type — Proposed, Committed, RolledBack, checkpoints). Unlike the raw max id
    // (Keys.Max), this can never overshoot a hole: the unanchored live-propose broadcast can write
    // a lone high entry over a gap on a behind follower, and the raw max would then advertise log
    // freshness this node does not have. Election freshness (Raft §5.4.1) assumes the candidate's
    // index describes a contiguous log — comparing raw max ids lets a node with holes win an
    // election while missing committed entries, and then serve with an incomplete consumer
    // projection. Advanced on the partition's serialized executor path, like commitIndex.
    private long presentIndex = 1;

    /// <summary>Term of the entry at the presence frontier (<see cref="presentIndex"/> − 1), so a
    /// candidate advertises a (term, index) pair that describes the same log position. 0 when the
    /// frontier has no entry (empty log).</summary>
    private long presentTerm;

    // Out-of-order present ids (with their terms) buffered until the gap below them fills — the
    // presence analog of pendingResolved.
    private readonly SortedDictionary<long, long> pendingPresent = new();

    // High-water mark of ids ever buffered in pendingPresent, maintained on insert and never
    // lowered. Feeds the accepted-id floor stamped on follower-append operations: a stale-high
    // value only raises the floor, which suppresses deletions — always the safe direction.
    private long pendingPresentHighWater = -1;

    private int operations;

    private long walOperationSequence;

    private int compactionInFlight;

    /// <summary>
    /// Application-supplied floor: compaction will not truncate committed entries at or above this
    /// id, even when the checkpoint has advanced past it. Used to retain a WAL tail for
    /// point-in-time recovery. Default <see cref="long.MaxValue"/> means no extra retention
    /// (truncate to checkpoint). A value &lt;= 0 is normalized to <see cref="long.MaxValue"/>
    /// (no protection), NOT to 0 — a zero floor would suppress all compaction by collapsing
    /// <c>effectiveFloor</c> to 0 and triggering the early-return, which is never the desired
    /// behaviour when a caller has not yet computed its protected index.
    /// </summary>
    private long minRetainIndex = long.MaxValue;

    /// <summary>
    /// Registry of active composable retention holds, keyed by an opaque monotonically-increasing
    /// token so that multiple holds may sit at the same index without colliding. Guarded by
    /// <see cref="holdsLock"/>. The effective hold floor (min over the values, or
    /// <see cref="long.MaxValue"/> when empty) is republished to <see cref="holdFloor"/> after every
    /// mutation so the compaction pass can read it lock-free.
    /// <para>
    /// This is the composable counterpart to <see cref="minRetainIndex"/>: several independent
    /// consumers (PITR horizon, backup capture, …) each hold a floor and the WAL retains down to the
    /// minimum of all of them. In-memory only — resets on process restart, same durability contract
    /// as <see cref="minRetainIndex"/>.
    /// </para>
    /// </summary>
    private readonly Dictionary<long, long> retentionHolds = new();

    private readonly object holdsLock = new();

    private long nextHoldToken;

    /// <summary>
    /// Cached minimum of <see cref="retentionHolds"/> (<see cref="long.MaxValue"/> when no holds are
    /// active). Published with a volatile write under <see cref="holdsLock"/> and read lock-free by
    /// <see cref="RunCompactionPassAsync"/>, mirroring the volatile-floor pattern of
    /// <see cref="minRetainIndex"/>.
    /// </summary>
    private long holdFloor = long.MaxValue;

    /// <summary>
    /// Live-replica retention floor as last published by the leader's heartbeat round: the lowest
    /// log index a live, acking follower still needs, or <see cref="long.MaxValue"/> when no
    /// follower constrains retention. See <see cref="SetLiveReplicaRetentionFloor"/>. Volatile
    /// pattern as <see cref="minRetainIndex"/>.
    /// </summary>
    private long liveReplicaRetentionFloor = long.MaxValue;

    /// <summary>
    /// When <see cref="liveReplicaRetentionFloor"/> was last published (monotonic ticks; 0 = never).
    /// A value older than <see cref="liveReplicaFloorStalenessTicks"/> is ignored by compaction:
    /// the publisher runs only on an active leader's heartbeat round, so expiry — rather than a
    /// clear-on-step-down hook at every leader→follower transition site — is what keeps a
    /// stepped-down, quiesced, or crashed-mid-publish leader from pinning retention forever.
    /// </summary>
    private long liveReplicaFloorPublishedTicks;

    /// <summary>
    /// Staleness bound for the published live-replica floor: ten heartbeat intervals, floored at
    /// 30 s. An active leader republishes every round, so a fresh value is always present where the
    /// hold matters; anything older means the publisher stopped. Not readonly only for
    /// <see cref="SetLiveReplicaFloorStalenessForTesting"/>.
    /// </summary>
    private long liveReplicaFloorStalenessTicks;

    /// <summary>Test-only: overrides the staleness bound so expiry is testable without waiting 30 s.</summary>
    internal void SetLiveReplicaFloorStalenessForTesting(TimeSpan window) =>
        liveReplicaFloorStalenessTicks = (long)(window.TotalSeconds * Stopwatch.Frequency);

    // Test-only handle for WaitForCompactionIdleAsync; not a production synchronization point.
    private Task? compactionPassTask;

    private int compactionPassCount;

    private int effectiveCompactionPassCount;

    /// <summary>
    /// 1 once the "compaction has no checkpoint to compact to" line has been logged for this
    /// partition. Rearmed (back to 0) as soon as a pass observes a checkpoint, so the message is
    /// one-shot per stall rather than one-shot per process — a partition that checkpoints and then
    /// stops checkpointing reports again instead of staying silent for the process lifetime.
    /// </summary>
    private int awaitingFirstCheckpointReported;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="onComplete">
    /// Callback invoked by the scheduler when a WAL write completes (or errors).
    /// Must not block; the owning partition executor routes the completion back
    /// to <see cref="RaftPartitionStateMachine.CompleteWalOperationAsync"/>.
    /// </param>
    /// <param name="partition"></param>
    /// <param name="walAdapter"></param>
    public RaftWriteAhead(RaftManager manager, Action<RaftWalCompletion> onComplete, RaftPartition partition, IWAL walAdapter)
    {
        this.manager = manager;
        this.onComplete = onComplete;
        this.logger = manager.Logger;
        this.partition = partition;
        this.walAdapter = walAdapter;
        
        this.compactEveryOperations = manager.Configuration.CompactEveryOperations;
        this.compactNumberEntries = manager.Configuration.GetEffectiveCompactNumberEntries();
        this.maxEntriesPerCompaction = manager.Configuration.GetEffectiveMaxEntriesPerCompaction();
        this.operations = compactEveryOperations > 0 ? compactEveryOperations : 0;
        this.liveReplicaFloorStalenessTicks = (long)(Math.Max(
            30_000, 10 * manager.Configuration.HeartbeatInterval.TotalMilliseconds) * Stopwatch.Frequency / 1000);
    }

    /// <summary>
    /// Publishes the live-replica retention floor (see <see cref="Scheduling.IRaftWalFacade.SetLiveReplicaRetentionFloor"/>).
    /// Called from the leader's heartbeat round on the partition executor; read lock-free by
    /// <see cref="RunCompactionPassAsync"/>. Values &lt;= 0 are normalized to
    /// <see cref="long.MaxValue"/> (no protection), mirroring <see cref="SetMinRetainIndex"/>.
    /// The publish timestamp is written after the floor, so a torn observation errs toward
    /// treating the value as stale — never toward applying a stale one as fresh.
    /// </summary>
    public void SetLiveReplicaRetentionFloor(long floor)
    {
        Volatile.Write(ref liveReplicaRetentionFloor, floor <= 0 ? long.MaxValue : floor);
        Volatile.Write(ref liveReplicaFloorPublishedTicks, Stopwatch.GetTimestamp());
    }

    /// <summary>
    /// Called after a commit or follower-append WAL operation persists successfully.
    /// Each operation decrements the counter by one (not per log entry in the batch).
    /// When the counter reaches zero, starts a compaction pass without waiting for it.
    /// </summary>
    public void NotifyCommitted()
    {
        if (compactEveryOperations <= 0)
            return;

        int remaining = Interlocked.Decrement(ref operations);

        if (remaining <= 0)
        {
            Interlocked.Exchange(ref operations, compactEveryOperations);
            Compact();
        }
    }

    /// <summary>
    /// Phase 1 of the nonblocking restore: reads all persisted log entries from WAL
    /// storage through the I/O scheduler.  Returns the raw list so the caller can
    /// deliver it back to the partition executor for replay under the single-owner
    /// guarantee (correctness rule 1).
    /// <para>
    /// When an <see cref="IApplicationDurabilityProvider"/> is configured, the floor it reports
    /// re-anchors the read in both directions. A floor <b>below</b> the last checkpoint widens the
    /// read down to <c>floor + 1</c>, so committed entries the application has not durably applied
    /// are redelivered on replay. A floor <b>above</b> the checkpoint is a <b>soft checkpoint</b>:
    /// entries at or below it are already durably applied by the application, so (on non-system
    /// partitions, and only when the WAL still holds the entry at the floor) the read is
    /// <b>narrowed</b> to start at <c>floor + 1</c> and <see cref="CompleteRestoreAsync"/> seeds
    /// the frontier scan from the floor. Unlike the hard checkpoint, the soft floor removes
    /// nothing: the skipped prefix stays in the WAL for follower backfill and compaction keeps its
    /// own conservative fences. This bounds cold-restart replay by the application's flush lag
    /// instead of by workload runtime.
    /// </para>
    /// <para>
    /// The system partition is never narrowed: the system coordinator rebuilds its in-memory
    /// roster/partition-map from replayed <c>_RaftSystem</c> entries, and the application floor
    /// certifies nothing about coordinator state.
    /// </para>
    /// </summary>
    public async ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
    {
        if (recovered)
            return [];

        // Consulted before the read: at restart the provider must answer from the application's
        // durable storage, so there is nothing to "re-assert" first (unlike the in-memory
        // SetMinRetainIndex/AcquireRetentionHold floors, which reset on restart).
        long durablyApplied = manager.Configuration.ApplicationDurabilityProvider
            ?.GetDurablyAppliedIndex(partition.PartitionId) ?? -1;

        long lastCheckpoint = -1;
        long replayFloor = -1;
        long softFloorTerm = -1;

        List<RaftLog> logs = await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () =>
        {
            if (durablyApplied < 0)
                return walAdapter.ReadLogs(partition.PartitionId);

            lastCheckpoint = walAdapter.GetLastCheckpoint(partition.PartitionId);
            replayFloor = durablyApplied + 1;

            // Floor below the checkpoint: widen. Unbounded range read from the floor — the same
            // seek-from-id path the backends already implement, returning every entry (any type)
            // with id >= replayFloor.
            if (lastCheckpoint > 0 && replayFloor < lastCheckpoint)
                return walAdapter.ReadLogsRange(partition.PartitionId, replayFloor);

            // Floor above the checkpoint (including "no checkpoint at all", the state a
            // durability-fenced compaction leaves behind): narrow the read to the tail above the
            // soft floor. Requires proof that the entry AT the floor is still durably present —
            // its term seeds the post-restore freshness pair — otherwise fall through to the
            // conservative full read (the WAL and the application have diverged; frontier
            // reconstruction then proceeds from what the WAL actually holds). Any entry type
            // counts: under WalSingleFsyncCommit a durably applied entry can persist as Proposed
            // with its lazy commit marker lost, yet the application floor proves it committed.
            if (durablyApplied > 0
                && durablyApplied > lastCheckpoint
                && partition.PartitionId != RaftSystemConfig.SystemPartition)
            {
                softFloorTerm = walAdapter.GetTermAt(partition.PartitionId, durablyApplied);
                if (softFloorTerm > 0)
                    return walAdapter.ReadLogsRange(partition.PartitionId, replayFloor);

                softFloorTerm = -1;
            }

            // Historical checkpoint-anchored read (also the no-provider path above): with no
            // checkpoint, ReadLogs reads from the beginning.
            return walAdapter.ReadLogs(partition.PartitionId);
        }).ConfigureAwait(false);

        // Committed consumer entries at or below the floor are durably applied and are not
        // redelivered, on every read path (the widened and narrowed reads exclude them by
        // construction; the full reads skip them at delivery).
        restoreDeliveryFloor = durablyApplied;

        if (softFloorTerm > 0)
        {
            restoreSoftFloorIndex = durablyApplied;
            restoreSoftFloorTerm = softFloorTerm;
            manager.Logger.LogInfoRestoreNarrowedBySoftFloor(
                manager.LocalEndpoint, partition.PartitionId, durablyApplied, lastCheckpoint, logs.Count);
            KommanderMetrics.RecordRestoreNarrowedBySoftFloor(partition.PartitionId);
        }
        else if (durablyApplied > 0 && durablyApplied > lastCheckpoint
                 && partition.PartitionId != RaftSystemConfig.SystemPartition)
        {
            manager.Logger.LogWarnRestoreSoftFloorEntryMissing(
                manager.LocalEndpoint, partition.PartitionId, durablyApplied, lastCheckpoint);
        }

        if (replayFloor > 0 && lastCheckpoint > 0 && replayFloor < lastCheckpoint)
            manager.Logger.LogInfoRestoreWidenedByDurabilityFloor(manager.LocalEndpoint, partition.PartitionId, replayFloor, lastCheckpoint);

        if (logs.Count > 0)
            manager.Logger.LogInfoRecoveredLogs(manager.LocalEndpoint, partition.PartitionId, logs.Count);

        return logs;
    }

    /// <summary>
    /// Phase 2 of the nonblocking restore: replays the loaded log entries by invoking
    /// the application replication callbacks and updating the WAL commit index.
    /// Must be called on the partition executor thread (single-owner guarantee).
    /// For P0, each entry is dispatched by log type: <c>_RaftSystem</c> entries go to
    /// <c>InvokeSystemLogRestored</c>; all other types go to <c>InvokeLogRestored</c>
    /// (consumer).  After replay completes, P0 fires both <c>InvokeSystemRestoreFinished</c>
    /// and <c>InvokeRestoreFinished</c> so the coordinator and the consumer each observe
    /// restore completion; non-P0 partitions fire only <c>InvokeRestoreFinished</c>.
    /// </summary>
    public async ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs)
    {
        if (recovered)
            return;

        recovered = true;

        manager.InvokeRestoreStarted(partition.PartitionId);

        // ── Reconstruct the commit frontier ───────────────────────────────────────────────
        // logs is sorted ascending by id and begins at the last durable CommittedCheckpoint — or
        // below it, at the application-durability floor + 1, when a configured
        // IApplicationDurabilityProvider reported a floor under the checkpoint — or ABOVE it, at
        // the soft-checkpoint floor + 1, when the floor sits over the checkpoint and the read was
        // narrowed (see LoadRestoreLogsAsync; the seed below re-anchors the scan for that case).
        // Extra pre-checkpoint entries never move the frontier: they sit below the checkpoint,
        // which certifies its whole prefix and jumps the contiguous frontier over them regardless.
        // We scan once to derive three quantities used below.
        long maxLogId = 0;              // highest durable id (any type) — the propose cursor floor
        long lastResolvedCommitted = 0; // id of the last Committed/CommittedCheckpoint seen (legacy path)
        long contiguousCommitted = 0;   // highest id of an unbroken committed prefix (fast path)
        long contiguousPresent = 0;     // highest id of an unbroken present prefix (any type)
        long contiguousPresentTerm = 0; // term of the entry at contiguousPresent
        bool any = false;

        // A narrowed read (soft checkpoint, see LoadRestoreLogsAsync) starts ABOVE the floor, so
        // the scan would never chain from id 1 (or from a checkpoint entry — none is in the read).
        // Seed every cursor as if a CommittedCheckpoint sat at the floor: the application's durable
        // floor certifies its whole prefix committed, exactly the certificate a checkpoint entry
        // carries, and the floor entry's term was proven on-disk before narrowing was chosen.
        if (restoreSoftFloorIndex > 0)
        {
            maxLogId = restoreSoftFloorIndex;
            lastResolvedCommitted = restoreSoftFloorIndex;
            contiguousCommitted = restoreSoftFloorIndex;
            contiguousPresent = restoreSoftFloorIndex;
            contiguousPresentTerm = restoreSoftFloorTerm;
            any = true;
        }

        // The persisted last-checkpoint id only advances when the checkpoint's whole prefix was
        // contiguously present at land time (see RocksDbWAL.VerifyCheckpointPrefixPresent). A
        // CommittedCheckpoint ROW above this floor therefore landed over a replication gap this
        // node never backfilled before the crash: it certifies nothing here, and letting it jump
        // the frontiers would advertise election freshness for — and mark as applied — entries
        // this node does not hold. Such a row extends the chains only contiguously, like any
        // other entry.
        long certifiedCheckpointFloor = await GetLastCheckpointAsync().ConfigureAwait(false);

        foreach (RaftLog log in logs)
        {
            any = true;
            if (log.Id > maxLogId)
                maxLogId = log.Id;

            // A checkpoint certifies its whole prefix (it is the durable recovery anchor), so it
            // may jump the presence frontier — but only when the persisted floor covers it (see
            // above); every other type only extends an unbroken run — a missing id (e.g. a hole
            // left by an out-of-order append) stops it, exactly like the committed frontier below.
            if (log.Type == RaftLogType.CommittedCheckpoint && log.Id <= certifiedCheckpointFloor
                ? log.Id > contiguousPresent
                : log.Id == contiguousPresent + 1)
            {
                contiguousPresent = log.Id;
                contiguousPresentTerm = log.Term;
            }

            switch (log.Type)
            {
                case RaftLogType.CommittedCheckpoint:
                    lastResolvedCommitted = log.Id;
                    // A checkpoint certifies the whole prefix ≤ its id is committed (it is the durable
                    // recovery anchor), so it may jump the contiguous frontier — again only when the
                    // persisted floor attests its prefix was present; an over-gap row extends the
                    // chain contiguously like a plain Committed entry.
                    if (log.Id <= certifiedCheckpointFloor
                        ? log.Id > contiguousCommitted
                        : log.Id == contiguousCommitted + 1)
                        contiguousCommitted = log.Id;
                    break;

                case RaftLogType.Committed:
                    lastResolvedCommitted = log.Id;
                    // Extend the contiguous prefix only across an unbroken run; a gap — a Proposed entry
                    // whose lazy commit marker was lost on the fast path, or any missing id — stops it.
                    if (log.Id == contiguousCommitted + 1)
                        contiguousCommitted = log.Id;
                    break;

                case RaftLogType.Proposed:
                case RaftLogType.ProposedCheckpoint:
                case RaftLogType.RolledBack:
                case RaftLogType.RolledBackCheckpoint:
                    break;

                default:
                    throw new NotImplementedException();
            }
        }

        if (manager.Configuration.WalSingleFsyncCommit)
        {
            // Single-fsync fast path: the per-entry Committed marker is written lazily, so a crash can
            // leave a durable Proposed prefix whose markers were lost. The on-disk types are no longer a
            // reliable committed/uncommitted boundary, so reconstruct conservatively:
            //   commitIndex  = highest CONTIGUOUS committed prefix + 1 (the safe lower bound). Entries
            //                  above the first gap are NOT treated as committed here; the true frontier
            //                  above this is re-supplied by the leader on reconnect (follower) or by
            //                  re-commit once a current-term entry commits on election (leader).
            //   proposeIndex = maxLogId + 1, so the durable Proposed tail is PRESERVED — a later propose
            //                  never reuses its ids, which would overwrite an acked-but-lazily-committed
            //                  entry (data loss). Recovery never treats that tail as committed, so an
            //                  unacknowledged-but-not-durable write is never promoted.
            commitIndex = contiguousCommitted + 1;
            proposeIndex = maxLogId + 1;
        }
        else
        {
            // Legacy path (commit markers always durable ⇒ the committed prefix is contiguous and
            // complete). Byte-for-byte the prior behaviour: the frontier is the last committed id, and a
            // proposed-but-uncommitted tail is discarded (a later propose reuses its ids).
            commitIndex = any ? lastResolvedCommitted + 1 : await GetMaxLog().ConfigureAwait(false) + 1;
            proposeIndex = any ? lastResolvedCommitted + 1 : commitIndex;
        }

        // Presence frontier: the last id of the unbroken durable prefix (any type), independent of
        // the commit-marker reconstruction above. With no entries, mirror the commit frontier (a
        // term of 0 makes the freshness comparison fall back to index-only, the legacy ordering).
        presentIndex = any ? contiguousPresent + 1 : commitIndex;
        presentTerm = any ? contiguousPresentTerm : 0;

        // ── Floor the frontier at the durable checkpoint ─────────────────────────────────
        // A durable checkpoint certifies its whole prefix as committed, so no reconstruction may
        // publish a frontier below it. The scans above depend on the checkpoint LOG ENTRY being in
        // the restore read; when it is absent — a fully compacted partition whose boundary survives
        // only in backend metadata, or a truncated tail — an empty or short list reconstructs a
        // near-zero frontier. The node then reports commit frontier 0 in every ack, the leader
        // anchors backfill at 1 below its own compaction floor, and only a snapshot install can
        // repair it (the "anchored at 1" producer in the Caraxes soak wedge). Seeding from the
        // recorded boundary — exactly what a snapshot install would do — makes the restored node
        // report the truth instead. Runs before replay: entries below the boundary are certified
        // committed, so replaying them under the raised frontier is correct.
        long restoredCheckpoint = certifiedCheckpointFloor;
        if (restoredCheckpoint > 0 && commitIndex <= restoredCheckpoint)
        {
            long checkpointTerm = await GetAnyTermAtAsync(restoredCheckpoint).ConfigureAwait(false);
            manager.Logger.LogWarnRestoreFrontierBelowCheckpoint(
                manager.LocalEndpoint, partition.PartitionId, commitIndex - 1, restoredCheckpoint);
            SeedCommitFrontierFromSnapshot(restoredCheckpoint, Math.Max(checkpointTerm, 0));
        }

        // ── Replay the committed prefix to the application ─────────────────────────────────
        // Apply only committed data entries strictly below the reconstructed frontier. Checkpoints carry
        // no application payload (their state is restored via the snapshot transfer); entries at or above
        // the frontier are deferred to leader re-supply / re-commit.
        long softFloorSkipped = 0;

        foreach (RaftLog log in logs)
        {
            // P0 checkpoint entries may carry a serialized system-configuration snapshot
            // (RaftSystemConfig.CheckpointLogType). Deliver it in log order so the coordinator
            // rebuilds the membership roster and partition map even when compaction removed the
            // original config delta entries below this checkpoint; deltas above it are replayed
            // afterwards and overwrite snapshot values in commit order. Payload-free checkpoints
            // (older WALs, non-P0 partitions) fall through to the skip below, as before.
            if (log.Type == RaftLogType.CommittedCheckpoint
                && partition.PartitionId == RaftSystemConfig.SystemPartition
                && log.LogType == RaftSystemConfig.CheckpointLogType
                && log.LogData is { Length: > 0 })
            {
                try
                {
                    await manager.InvokeSystemLogRestored(partition.PartitionId, log).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    manager.Logger.LogError("[{Endpoint}/{PartitionId}] {Message}\n{Stacktrace}", manager.LocalEndpoint, partition.PartitionId, ex.Message, ex.StackTrace);
                }

                continue;
            }

            if (log.Type != RaftLogType.Committed || log.Id >= commitIndex)
                continue;

            // Promotion-barrier no-ops are consensus-internal (see RaftSystemConfig
            // .LeadershipBarrierLogType): they persist in the WAL like any committed entry but
            // must never reach a consumer, on restore or anywhere else.
            if (log.LogType == RaftSystemConfig.LeadershipBarrierLogType)
                continue;

            // Soft restore floor: the application durably applied every committed entry at or
            // below the floor, so consumer redelivery is skipped. The narrowed read excludes these
            // entries by construction; this filter covers the full-read paths (the system
            // partition, and the WAL-diverged fallback). System coordinator entries are exempt:
            // the coordinator's in-memory state is rebuilt only from replay.
            if (log.Id <= restoreDeliveryFloor
                && !(partition.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType))
            {
                softFloorSkipped++;
                continue;
            }

            try
            {
                if (partition.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
                {
                    if (!await manager.InvokeSystemLogRestored(partition.PartitionId, log).ConfigureAwait(false))
                        manager.InvokeReplicationError(partition.PartitionId, log);
                }
                else
                {
                    if (!await manager.InvokeLogRestored(partition.PartitionId, log).ConfigureAwait(false))
                        manager.InvokeReplicationError(partition.PartitionId, log);
                }
            }
            catch (Exception ex)
            {
                manager.Logger.LogError("[{Endpoint}/{PartitionId}] {Message}\n{Stacktrace}", manager.LocalEndpoint, partition.PartitionId, ex.Message, ex.StackTrace);

                manager.InvokeReplicationError(partition.PartitionId, log);
            }
        }

        if (softFloorSkipped > 0)
            manager.Logger.LogInfoRestoreSkippedBelowSoftFloor(
                manager.LocalEndpoint, partition.PartitionId, softFloorSkipped, restoreDeliveryFloor);

        if (partition.PartitionId == RaftSystemConfig.SystemPartition)
        {
            // Fire both signals so the system coordinator and the consumer each learn
            // restore is complete. For non-P0 partitions only the consumer signal fires.
            manager.InvokeSystemRestoreFinished(partition.PartitionId);
            manager.InvokeRestoreFinished(partition.PartitionId);
        }
        else
            manager.InvokeRestoreFinished(partition.PartitionId);
    }

    /// <summary>
    /// Proposes a batch of logs in the current term for processing by the Raft consensus protocol.
    /// Logs are assigned unique indices and associated with the current term, then enqueued for replication.
    /// </summary>
    /// <param name="contextSelf"></param>
    /// <param name="term">
    ///     The current term in the Raft consensus protocol used to associate with the logs.
    /// </param>
    /// <param name="logs">
    ///     A list of logs to be proposed. If the list is null or empty, the method will return immediately with a success status and no index update.
    /// </param>
    /// <returns>
    /// A tuple containing the operation status and the index of the last proposed log.
    /// If the proposal succeeds, the status will be <see cref="RaftOperationStatus.Success"/> and the index will reflect the latest proposed index.
    /// If the operation fails, the status will indicate the specific error, and the index will return as -1.
    /// </returns>
    /// <exception cref="Exception">
    /// May be thrown for unexpected errors during the proposal process or queuing for replication.
    /// </exception>
    public Task<(RaftOperationStatus, long)> Propose(long term, List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return Task.FromResult((RaftOperationStatus.Success, -1L));

        WALWriteOperation operation = EnqueuePropose(term, logs, default, false);

        // Synchronous: EnqueuePropose hands off to the WAL scheduler without awaiting. No async
        // state machine or extra Task.FromResult round-trip — just wrap the already-known result.
        return Task.FromResult((RaftOperationStatus.Pending, operation.LogIndex));
    }

    /// <summary>
    /// Returns <paramref name="logs"/> in ascending <see cref="RaftLog.Id"/> order. The common
    /// case — input already non-decreasing (callers build batches in id order, and freshly
    /// proposed logs share a placeholder id) — returns the original list with no allocation. Only
    /// when the input is genuinely out of order does it fall back to a stably sorted copy.
    /// <para>
    /// The fallback uses <see cref="Enumerable.OrderBy{TSource,TKey}(IEnumerable{TSource},Func{TSource,TKey})"/>,
    /// which is a stable sort: entries with equal ids keep their relative order. This is
    /// load-bearing on the propose path, where unassigned entries can share a placeholder id and
    /// must retain insertion order so they receive sequential indices deterministically. The
    /// already-sorted check uses a strict <c>&gt;</c> comparison, so equal-id runs are treated as
    /// sorted and likewise keep their original order — identical to the stable sort. The result is
    /// only read by callers, never structurally mutated.
    /// </para>
    /// <remarks>
    /// Exposed as <see langword="internal"/> (not <see langword="private"/>) so the
    /// <c>Kommander.MicroBenchmarks</c> project can measure the allocation-free fast path against
    /// the previous <c>OrderBy(...).ToArray()</c> cost on real code.
    /// </remarks>
    internal static IReadOnlyList<RaftLog> OrderById(List<RaftLog> logs)
    {
        for (int i = 1; i < logs.Count; i++)
        {
            if (logs[i - 1].Id > logs[i].Id)
                return logs.OrderBy(static log => log.Id).ToArray();
        }

        return logs;
    }

    public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit)
    {
        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;

        // Index-allocation invariant (Raft §5.3): a leader appends only at lastLogIndex + 1.
        // An allocator at or below the highest durably present id would stamp this proposal onto
        // an index that is already occupied — the reissued slot then commits a second value at
        // the same index on whichever replicas happen to have a hole there (the Log Matching
        // violation of Jepsen run 31805148040). The monotonic follower arms and the promotion
        // seeding should make this unreachable; if it fires anyway, self-heal past the presence
        // frontier and say so loudly rather than issue a colliding id.
        if (presentIndex >= 0 && proposeIndex < presentIndex)
        {
            logger.LogError("[{Endpoint}/{Partition}] Propose allocator at {ProposeIndex} is BELOW the presence frontier {PresentIndex} — refusing to reissue occupied indices; skipping forward.",
                manager.LocalEndpoint, partition.PartitionId, proposeIndex, presentIndex);
            proposeIndex = presentIndex;
        }

        // Snapshot mutable state before mutation so we can roll back atomically if the scheduler
        // rejects the operation (e.g. BackpressureExceededException). The id/term snapshots use
        // pooled value buffers (no GC references): rented and returned within this call, never
        // exposed, so they add no steady-state allocation on the propose hot path.
        long savedProposeIndex = proposeIndex;
        long[] savedIds   = ArrayPool<long>.Shared.Rent(count);
        long[] savedTerms = ArrayPool<long>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedIds[i]   = log.Id;
                savedTerms[i] = log.Term;
                log.Id   = proposeIndex++;
                log.Term = term;
            }

            WALWriteOperation operation = new(
                onComplete,
                Interlocked.Increment(ref walOperationSequence),
                WALWriteOperationType.LeaderPropose,
                (partition.PartitionId, logs),
                timestamp,
                term: term,
                autoCommit: autoCommit,
                logIndex: proposeIndex
            );

            try
            {
                manager.WalScheduler.Enqueue(operation);
            }
            catch
            {
                // Restore all mutations so the caller observes no state change.
                proposeIndex = savedProposeIndex;
                for (int i = 0; i < count; i++)
                {
                    ordered[i].Id   = savedIds[i];
                    ordered[i].Term = savedTerms[i];
                }
                throw;
            }

            // Enqueue succeeded: the proposed entries are on their way to durable storage, so the
            // presence frontier advances over them. A leader writes sequential ids, but a node
            // that recently followed can still carry a hole below — the frontier buffers over it.
            for (int i = 0; i < count; i++)
                AdvancePresenceFrontier(ordered[i].Id, ordered[i].Term);

            // Count the durable phase from the producer side (inert unless instrumentation
            // is enabled): one LeaderPropose enqueue per single-round committed write.
            WalPhaseInstrumentation.RecordEnqueued(WALWriteOperationType.LeaderPropose);

            return operation;
        }
        finally
        {
            ArrayPool<long>.Shared.Return(savedIds);
            ArrayPool<long>.Shared.Return(savedTerms);
        }
    }

    /// <summary>
    /// Commits a list of Raft log entries by updating their type to indicate they are committed.
    /// Processes the logs in ascending order of their IDs and updates the commit index.
    /// </summary>
    /// <param name="logs">
    /// A list of Raft log entries to commit. If null or empty, the method returns success with a commit index of -1.
    /// </param>
    /// <returns>
    /// A tuple containing the operation status and the last committed log index.
    /// The operation status is an instance of <see cref="RaftOperationStatus"/> indicating success or failure.
    /// If the operation fails, the commit index will be -1.
    /// </returns>
    public Task<(RaftOperationStatus, long)> Commit(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return Task.FromResult((RaftOperationStatus.Success, -1L));

        long lastCommitIndex = -1;
        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;
        RaftLogType[] savedTypes = ArrayPool<RaftLogType>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedTypes[i] = log.Type;

                switch (log.Type)
                {
                    case RaftLogType.Proposed:
                        log.Type = RaftLogType.Committed;
                        lastCommitIndex = log.Id;
                        break;

                    case RaftLogType.ProposedCheckpoint:
                        log.Type = RaftLogType.CommittedCheckpoint;
                        lastCommitIndex = log.Id;
                        break;

                    case RaftLogType.Committed:
                    case RaftLogType.CommittedCheckpoint:
                    case RaftLogType.RolledBack:
                    case RaftLogType.RolledBackCheckpoint:
                    default:
                        break;
                }
            }

            try
            {
                WALWriteOperation operation = EnqueueCommitPrepared(logs, lastCommitIndex);

                // Gap-buffered, post-enqueue frontier advance — same rationale as EnqueueCommit:
                // the frontier must never certify past an unresolved id.
                for (int i = 0; i < count; i++)
                {
                    if (savedTypes[i] is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                        AdvanceCommitFrontier(ordered[i].Id);
                }

                return Task.FromResult((RaftOperationStatus.Pending, operation.LogIndex));
            }
            catch
            {
                for (int i = 0; i < count; i++)
                    ordered[i].Type = savedTypes[i];
                throw;
            }
        }
        finally
        {
            ArrayPool<RaftLogType>.Shared.Return(savedTypes);
        }
    }

    public WALWriteOperation EnqueueCommit(List<RaftLog> logs)
    {
        long lastCommitIndex = -1;
        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;
        RaftLogType[] savedTypes = ArrayPool<RaftLogType>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedTypes[i] = log.Type;

                switch (log.Type)
                {
                    case RaftLogType.Proposed:
                        log.Type = RaftLogType.Committed;
                        lastCommitIndex = log.Id;
                        break;

                    case RaftLogType.ProposedCheckpoint:
                        log.Type = RaftLogType.CommittedCheckpoint;
                        lastCommitIndex = log.Id;
                        break;
                }
            }

            try
            {
                WALWriteOperation operation = EnqueueCommitPrepared(logs, lastCommitIndex);

                // Advance the frontier only AFTER a successful enqueue, and only through the
                // gap-buffered path. The old arms jumped the frontier monotonically to the highest
                // committed id, which certified a "committed through N" that skipped every
                // unresolved id below N — on a node whose log has a hole (the poisoned-quorum
                // shape), a promotion-barrier no-op commit then poisoned the commit frontier past
                // the hole, and every later promotion refused forever on a committed drain that
                // could never reach it (the Jepsen majority-hole leaderless wedge, observation 1
                // of run 32690955741). AdvanceCommitFrontier keeps the two properties the old
                // comment wanted — a lower id never drags the frontier backwards (below-frontier
                // ids are ignored, so the inherited-tail re-commit stays a no-op) — while an
                // over-gap id is buffered until the gap resolves instead of certifying it away.
                // Ascending order (OrderById) keeps the buffered drain cheap.
                for (int i = 0; i < count; i++)
                {
                    if (savedTypes[i] is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                        AdvanceCommitFrontier(ordered[i].Id);
                }

                return operation;
            }
            catch
            {
                for (int i = 0; i < count; i++)
                    ordered[i].Type = savedTypes[i];
                throw;
            }
        }
        finally
        {
            ArrayPool<RaftLogType>.Shared.Return(savedTypes);
        }
    }

    private WALWriteOperation EnqueueCommitPrepared(List<RaftLog> logs, long lastCommitIndex)
    {
        WALWriteOperation operation = new(
            onComplete,
            Interlocked.Increment(ref walOperationSequence),
            WALWriteOperationType.LeaderCommit,
            (partition.PartitionId, logs),
            logIndex: lastCommitIndex
        );

        manager.WalScheduler.Enqueue(operation);

        // The second durable phase of a committed write: pairs with the LeaderPropose
        // enqueue above, so enqueues-per-write ≈ 2 confirms the two-fsync structure.
        WalPhaseInstrumentation.RecordEnqueued(WALWriteOperationType.LeaderCommit);

        return operation;
    }

    /// <summary>
    /// Rolls back a list of Raft logs by updating their types to indicate a rollback operation
    /// and processing them through the Write-Ahead Log (WAL) adapter.
    /// </summary>
    /// <param name="logs">
    /// A list of Raft logs to be rolled back. If the list is null or empty, no rollback is performed.
    /// </param>
    /// <returns>
    /// A tuple containing the operation status and the index of the last processed log:
    /// - <see cref="RaftOperationStatus.Success"/> and -1 if the rollback is completed successfully or no logs were provided.
    /// - The relevant <see cref="RaftOperationStatus"/> and -1 in case of an error.
    /// </returns>
    public Task<(RaftOperationStatus, long)> Rollback(List<RaftLog>? logs)
    {
        if (logs is null || logs.Count == 0)
            return Task.FromResult((RaftOperationStatus.Success, -1L));

        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;
        RaftLogType[] savedTypes = ArrayPool<RaftLogType>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedTypes[i] = log.Type;

                switch (log.Type)
                {
                    case RaftLogType.Proposed:
                    {
                        log.Type = RaftLogType.RolledBack;

                        //RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Rollback(partition.PartitionId, log));
                    }
                    break;

                    case RaftLogType.ProposedCheckpoint:
                    {
                        log.Type = RaftLogType.RolledBackCheckpoint;

                        //RaftOperationStatus status = await manager.WriteThreadPool.EnqueueTask(() => walAdapter.Rollback(partition.PartitionId, log));
                    }
                    break;
                }
            }

            try
            {
                WALWriteOperation operation = EnqueueRollbackPrepared(logs);

                // Same rollback-resolves-the-frontier rule as EnqueueRollback.
                for (int i = 0; i < count; i++)
                {
                    if (savedTypes[i] is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                        AdvanceCommitFrontier(ordered[i].Id);
                }

                return Task.FromResult((RaftOperationStatus.Pending, operation.LogIndex));
            }
            catch
            {
                for (int i = 0; i < count; i++)
                    ordered[i].Type = savedTypes[i];
                throw;
            }
        }
        finally
        {
            ArrayPool<RaftLogType>.Shared.Return(savedTypes);
        }
    }

    public WALWriteOperation EnqueueRollback(List<RaftLog> logs)
    {
        IReadOnlyList<RaftLog> ordered = OrderById(logs);
        int count = ordered.Count;
        RaftLogType[] savedTypes = ArrayPool<RaftLogType>.Shared.Rent(count);

        try
        {
            for (int i = 0; i < count; i++)
            {
                RaftLog log = ordered[i];
                savedTypes[i] = log.Type;

                switch (log.Type)
                {
                    case RaftLogType.Proposed:
                        log.Type = RaftLogType.RolledBack;
                        break;

                    case RaftLogType.ProposedCheckpoint:
                        log.Type = RaftLogType.RolledBackCheckpoint;
                        break;
                }
            }

            try
            {
                WALWriteOperation operation = EnqueueRollbackPrepared(logs);

                // A rollback RESOLVES its ids, exactly like a commit — the frontier tracks the
                // resolved prefix, not the committed one (the follower append path already counts
                // RolledBack rows in resolvedThisBatch). Under the old monotonic EnqueueCommit
                // jump this advance was implicit: the next commit above a rolled-back band jumped
                // the frontier over it. With the gap-buffered commit advance, skipping rollbacks
                // here left the leader's frontier stuck below every rolled-back id forever while
                // the apply path advanced over the resolved band — the CommitMonotonicity
                // commit-below-applied violations of CI run 33195170707 (Scenario08/09 pause
                // chaos, where proposal-timeout rollbacks are constant).
                for (int i = 0; i < count; i++)
                {
                    if (savedTypes[i] is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                        AdvanceCommitFrontier(ordered[i].Id);
                }

                return operation;
            }
            catch
            {
                for (int i = 0; i < count; i++)
                    ordered[i].Type = savedTypes[i];
                throw;
            }
        }
        finally
        {
            ArrayPool<RaftLogType>.Shared.Return(savedTypes);
        }
    }

    private WALWriteOperation EnqueueRollbackPrepared(List<RaftLog> logs)
    {
        WALWriteOperation operation = new(
            onComplete,
            Interlocked.Increment(ref walOperationSequence),
            WALWriteOperationType.LeaderRollback,
            (partition.PartitionId, logs)
        );

        manager.WalScheduler.Enqueue(operation);

        return operation;
    }

    /// <summary>
    /// Retrieves the highest log index recorded in the Write-Ahead Log (WAL) for a specific partition.
    /// This method queries the WAL adapter and returns the maximum log index for the partition.
    /// </summary>
    /// <returns>
    /// The maximum log index currently recorded for the specified partition.
    /// </returns>
    public async Task<long> GetMaxLog()
    {
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId),
            static s => s.walAdapter.GetMaxLog(s.PartitionId)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the highest log id known to be committed, read from the in-memory
    /// <c>commitIndex</c> (which is the next-commit slot, so the committed id is one less).
    /// Unlike <see cref="GetMaxLog"/> this excludes proposed-but-uncommitted tail entries,
    /// so the leader can seed its backfill cursor on election without shipping uncommitted logs.
    /// Synchronous: it reads an in-memory counter, no WAL/scheduler round-trip.
    /// </summary>
    public long GetCommitIndex() => commitIndex - 1;

    /// <summary>
    /// Records that an incoming Proposed/ProposedCheckpoint row was skipped because its id is
    /// already resolved locally (the stale-duplicate guard). Observability requested by the
    /// index-reissue investigation: the skip is the only trace that a stale duplicate arrived,
    /// and it is exactly the event that used to regress the propose allocator — so it must be
    /// visible in a run's logs, throttled to one line a second.
    /// </summary>
    private void LogStaleProposedSkipped(long id)
    {
        // Counted before the throttle: the log line is sampled, the counter is not.
        staleProposedSkipped++;

        long now = global::System.Diagnostics.Stopwatch.GetTimestamp();

        if (lastStaleProposedLogTicks != 0 && (now - lastStaleProposedLogTicks) < global::System.Diagnostics.Stopwatch.Frequency)
        {
            suppressedStaleProposedLogs++;
            return;
        }

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("[{Endpoint}/{Partition}] Skipped stale Proposed duplicate of resolved id {Id} (suppressedSinceLastLine={Suppressed})",
                manager.LocalEndpoint, partition.PartitionId, id, suppressedStaleProposedLogs);

        lastStaleProposedLogTicks = now;
        suppressedStaleProposedLogs = 0;
    }

    /// <summary>
    /// Number of incoming <c>Proposed</c>/<c>ProposedCheckpoint</c> rows this partition has refused
    /// to write because their id was already resolved locally — the stale-duplicate guard — since
    /// this partition last started.
    /// </summary>
    /// <remarks>
    /// <para>A non-zero count is NOT a fault: under a partition a deposed leader keeps broadcasting
    /// in-flight proposals and the heartbeat-driven proposal retry can race its own commit, so
    /// duplicates of resolved ids arrive legitimately. What the number is for is telling apart
    /// "fired occasionally" from "fired constantly" when a hole-below-the-frontier defect is being
    /// chased — those point at opposite causes.</para>
    /// <para><b>Zero is not evidence the guard never fired.</b> The count is in memory and restarts
    /// at zero with the process, so under a crash-fault workload a node that skipped duplicates and
    /// was then killed reports 0 — measured on the Jepsen kill jobs, where this reads BELOW the
    /// number of log lines the same run emitted. Treat it as a floor since the last start. The
    /// throttled <c>"Skipped stale Proposed duplicate"</c> log lines are the record that survives a
    /// restart; this counter is the one that is cheap to read.</para>
    /// <para>Read with <see cref="Volatile.Read(ref long)"/> because the writer is the partition
    /// executor thread and callers (diagnostics endpoints, tests) are not; the value is monotonic
    /// between restarts, so a slightly stale read only ever under-reports.</para>
    /// </remarks>
    public long GetStaleProposedSkippedCount() => Volatile.Read(ref staleProposedSkipped);

    /// <summary>
    /// Seeds the propose-id ALLOCATOR at promotion: a new leader appends at
    /// <c>lastLogIndex + 1</c> (Raft §5.3), so the allocator is set to exactly
    /// <paramref name="nextId"/> — one above the promotion-time presence frontier / commit
    /// frontier maximum. Both directions matter: a follower stint can leave the allocator LOW
    /// (an unresolved prior-term band moved it backwards before the arms became monotonic, or
    /// legitimately when the band was the tail) — stamping from there reissues durably occupied
    /// indices and commits two values at one index; and stale high proposes later truncated away
    /// can leave it HIGH — stamping from there opens a permanent hole below the new entry. The
    /// promotion hole-gate has already proven the log contiguous through the seed point, which is
    /// what makes the exact (non-monotonic) set safe HERE and nowhere else.
    /// </summary>
    public void SeedProposeAllocator(long nextId)
    {
        proposeIndex = nextId;
    }

    /// <summary>
    /// Advances the contiguous commit frontier to absorb a resolved (Committed/RolledBack) id.
    /// An id below the frontier is a duplicate replay (ignored); an id above it sits over an
    /// unfilled gap and is buffered until the gap closes; an id that fills the next slot advances
    /// the frontier and then drains any buffered successors that have now become contiguous.
    /// </summary>
    private void AdvanceCommitFrontier(long id)
    {
        if (id < commitIndex)
            return;

        if (id > commitIndex)
        {
            pendingResolved.Add(id);
            return;
        }

        commitIndex = id + 1;

        while (pendingResolved.Count > 0)
        {
            long next = pendingResolved.Min;
            if (next < commitIndex)
            {
                pendingResolved.Remove(next);   // stale duplicate already covered by the frontier
                continue;
            }
            if (next > commitIndex)
                break;                          // gap remains: stop draining

            pendingResolved.Remove(next);
            commitIndex = next + 1;
        }
    }

    /// <summary>
    /// Returns the highest log id known to be durably present with no holes below it (any entry
    /// type). This is the log position a node may advertise for election freshness: unlike
    /// <see cref="GetMaxLog"/>, it can never overshoot a gap left by an out-of-order append, so a
    /// node missing a committed range cannot look fresher than a node that actually holds it.
    /// Synchronous: reads an in-memory counter, no WAL/scheduler round-trip.
    /// </summary>
    public long GetPresentIndex() => presentIndex - 1;

    /// <summary>
    /// True when this node holds durable entries buffered ABOVE an unfilled gap — the lone-high
    /// shape the unanchored live-propose broadcast leaves on a behind follower. A node in this
    /// state knows its own log is missing a range some peer may hold; the election path uses it
    /// to defer candidacy to a known fresher voter instead of churning elections it would refuse
    /// at the promotion gates. Synchronous: reads an in-memory collection count.
    /// </summary>
    public bool HasPresenceGap() => pendingPresent.Count > 0;

    /// <summary>
    /// Returns the term of the log entry at <see cref="GetPresentIndex"/> — the pair a candidate
    /// or voter uses for the Raft §5.4.1 comparison. 0 when the log is empty at the frontier.
    /// </summary>
    public long GetPresentTerm() => presentTerm;

    /// <summary>
    /// Advances the contiguous presence frontier to absorb a durably written entry (any type).
    /// The presence analog of <see cref="AdvanceCommitFrontier"/>: an id below the frontier is a
    /// duplicate re-ship (ignored), an id above it sits over an unfilled gap and is buffered with
    /// its term until the gap closes, and an id that fills the next slot advances the frontier and
    /// drains any buffered successors that have become contiguous.
    /// </summary>
    private void AdvancePresenceFrontier(long id, long term)
    {
        if (id < presentIndex)
        {
            // The legacy (two-fsync) recovery path discards the proposed tail and lets a later
            // propose reuse its ids. An overwrite of the frontier entry itself must refresh the
            // advertised term so the (term, index) freshness pair keeps describing the real entry.
            if (id == presentIndex - 1)
                presentTerm = term;
            return;
        }

        if (id > presentIndex)
        {
            pendingPresent[id] = term;
            if (id > pendingPresentHighWater)
                pendingPresentHighWater = id;
            return;
        }

        presentIndex = id + 1;
        presentTerm = term;

        DrainPendingPresent();
    }

    /// <summary>
    /// Undoes the optimistic frontier advance for a WAL write that COMPLETED with a failure
    /// (e.g. a full disk). The enqueue paths advance the presence/commit frontiers as soon as the
    /// scheduler accepts an operation — correct in the steady state — but a write that later
    /// fails leaves the frontiers certifying entries that are not on disk. That is a Raft §5.4.1
    /// safety hazard: the node advertises election freshness for entries it does not hold, can
    /// win an election with a short log, and acks heartbeats with a commit frontier the leader
    /// then trusts to skip backfill.
    ///
    /// <para>The regression is deliberately conservative: both clamps go to the failed range's
    /// LOWEST id, so a durable entry above the range stops being advertised too. Under-advertising
    /// durable entries is always safe — it can only lose elections and invite re-shipment
    /// (idempotent puts) — while over-advertising undoes quorum. A follower heals through
    /// backfill: the leader sees the low ack and re-ships, and the re-enqueue re-advances the
    /// frontier. A leader heals on restart, when <see cref="CompleteRestoreAsync"/> recomputes
    /// the frontiers from on-disk contiguity.</para>
    ///
    /// <para>The propose ALLOCATOR (<c>proposeIndex</c>) is deliberately NOT regressed: it must
    /// stay monotonic (see <see cref="SeedProposeAllocator"/>) or a later propose reissues ids a
    /// pipelined sibling already occupies. The failed ids become a hole this node's frontiers now
    /// truthfully report.</para>
    ///
    /// <para>Runs on the partition's serialized executor path — the single writer of the frontier
    /// fields — like every other frontier mutation. The WAL term read for the regressed freshness
    /// pair degrades to 0 (the "empty log" term) on any read failure, which only makes this node
    /// less electable, never more.</para>
    /// </summary>
    public async ValueTask RegressFrontiersAfterFailedWriteAsync(long minLogIndex, long maxLogIndex, bool regressPresence, bool regressCommit)
    {
        if (minLogIndex < 0)
            return;

        if (maxLogIndex < minLogIndex)
            maxLogIndex = minLogIndex;

        // Drop the failed range from the over-gap buffers first, so a buffered id can never
        // re-certify an entry the disk refused when the frontier later drains over it.
        for (long id = minLogIndex; id <= maxLogIndex; id++)
        {
            if (regressPresence)
                pendingPresent.Remove(id);
            if (regressCommit)
                pendingResolved.Remove(id);
        }

        bool commitRegressed = regressCommit && commitIndex > minLogIndex;
        if (commitRegressed)
            commitIndex = minLogIndex;

        bool presenceRegressed = regressPresence && presentIndex > minLogIndex;
        if (presenceRegressed)
        {
            presentIndex = minLogIndex;

            // The entry now at the frontier is the durable one below the failed range; re-read
            // its term so the advertised (term, index) freshness pair describes a real on-disk
            // entry instead of the entry that failed to persist.
            long termAtFrontier = 0;
            if (minLogIndex - 1 > 0)
            {
                try
                {
                    long storedTerm = await GetAnyTermAtAsync(minLogIndex - 1).ConfigureAwait(false);
                    if (storedTerm > 0)
                        termAtFrontier = storedTerm;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(
                        "[{Endpoint}/{Partition}] Could not re-read the term at {Index} after a failed WAL write; advertising term 0 at the regressed frontier: {Message}",
                        manager.LocalEndpoint, partition.PartitionId, minLogIndex - 1, ex.Message);
                }
            }

            presentTerm = termAtFrontier;
        }

        if (commitRegressed || presenceRegressed)
            logger.LogError(
                "[{Endpoint}/{Partition}] WAL write of range [{Min},{Max}] failed after the frontiers advanced optimistically; regressed commit frontier to {CommitIndex} and presence frontier to {PresentIndex} so this node stops advertising entries that are not on disk",
                manager.LocalEndpoint, partition.PartitionId, minLogIndex, maxLogIndex, commitIndex - 1, presentIndex - 1);
    }

    /// <summary>
    /// Seeds the in-memory commit/propose frontier to a freshly installed snapshot boundary at
    /// <paramref name="snapshotIndex"/>. A snapshot means every id through the boundary is durably committed
    /// (the prefix is compacted away), so the frontier — which a fresh or lagging follower otherwise leaves at
    /// its stale value — must jump to it. Without this, <see cref="GetCommitIndex"/> reports a value below the
    /// boundary (the compacted prefix reads as an unfilled gap), which stalls both consumer delivery
    /// (<c>DrainCommittedAppliesAsync</c> bounds on the frontier) and the follower's backfill-progress report.
    /// Any over-gap ids buffered in <c>pendingResolved</c> that the boundary now covers are drained. Runs on the
    /// partition executor, the single writer of these fields.
    /// </summary>
    /// <summary>
    /// Advances the in-memory commit frontier over a §5.4.2-proven inherited entry at delivery
    /// time, ahead of its lazy durable re-commit marker (see
    /// <see cref="Scheduling.IRaftWalFacade.MarkInheritedCommitted"/>). Monotonic and gap-buffered
    /// like every resolution: the later re-commit enqueue's own advance is then a no-op.
    /// </summary>
    public void MarkInheritedCommitted(long id) => AdvanceCommitFrontier(id);

    /// <summary>
    /// Absorbs a prefix that is proven resolved by OTHER bookkeeping (the applied cursor, capped by
    /// contiguous presence) into the commit frontier, then drains any buffered resolutions that
    /// became contiguous. Called at promotion: a follower stint can leave the in-memory frontier
    /// below ids the node has already delivered — the applied cursor only ever advances over
    /// delivered or resolution-visited rows, so applied∧present ⇒ resolved — and a LEADER never
    /// receives the re-ship/backfill that heals a follower's frontier, so without this the
    /// promotion freezes the dip for the whole tenure: every new commit buffers above the
    /// phantom gap, the advertised frontier pins, and the CommitMonotonicity oracle fires (CI run
    /// 33195170707). The old monotonic <see cref="EnqueueCommit"/> jump absorbed such prefixes
    /// implicitly; the gap-buffered frontier needs the explicit, bounded reconciliation. Never
    /// moves the frontier backwards, and callers must never pass an id beyond the contiguous
    /// presence frontier.
    /// </summary>
    public void AbsorbResolvedPrefix(long throughId)
    {
        if (throughId + 1 <= commitIndex)
            return;

        commitIndex = throughId + 1;

        while (pendingResolved.Count > 0 && pendingResolved.Min < commitIndex)
            pendingResolved.Remove(pendingResolved.Min);
        while (pendingResolved.Count > 0 && pendingResolved.Min == commitIndex)
        {
            long next = pendingResolved.Min;
            pendingResolved.Remove(next);
            commitIndex = next + 1;
        }
    }

    public void SeedCommitFrontierFromSnapshot(long snapshotIndex, long snapshotTerm = 0)
    {
        long target = snapshotIndex + 1;
        if (target > commitIndex)
            commitIndex = target;
        if (target > proposeIndex)
            proposeIndex = target;

        // The snapshot boundary certifies its whole prefix, so the presence frontier jumps too —
        // otherwise a snapshot-installed follower would advertise a stale (pre-snapshot) log
        // position for election freshness and could never be elected despite being caught up.
        if (target > presentIndex)
        {
            presentIndex = target;
            presentTerm = snapshotTerm;
        }

        // Drop buffered resolved ids now covered by the boundary, then absorb any that have become contiguous.
        while (pendingResolved.Count > 0 && pendingResolved.Min < commitIndex)
            pendingResolved.Remove(pendingResolved.Min);
        while (pendingResolved.Count > 0 && pendingResolved.Min == commitIndex)
        {
            long next = pendingResolved.Min;
            pendingResolved.Remove(next);
            commitIndex = next + 1;
        }

        DrainPendingPresent();
    }

    /// <summary>
    /// Drops buffered present ids already covered by the frontier, then absorbs any that have
    /// become contiguous. Shared by the snapshot seed and (indirectly) the frontier advance.
    /// </summary>
    private void DrainPendingPresent()
    {
        while (pendingPresent.Count > 0)
        {
            long next = -1;
            long nextTerm = 0;
            foreach (KeyValuePair<long, long> kv in pendingPresent)
            {
                next = kv.Key;
                nextTerm = kv.Value;
                break;                          // SortedDictionary: first key is the minimum
            }

            if (next < presentIndex)
            {
                pendingPresent.Remove(next);
                continue;
            }
            if (next > presentIndex)
                break;

            pendingPresent.Remove(next);
            presentIndex = next + 1;
            presentTerm = nextTerm;
        }
    }

    /// <summary>
    /// Removes every log entry with id &gt; <paramref name="afterLogId"/> and returns the
    /// post-truncation max log id.
    /// <para>
    /// Atomicity is provided by the WAL backend, not by the scheduler: this calls the single
    /// <see cref="IWAL.TruncateLogsAfterAndGetMax"/> operation, which performs the delete and the
    /// max-read under one acquisition of the backend's per-partition write guard. Scheduling it on the
    /// <see cref="IRaftReadScheduler"/> only serializes it against other reads; it does <b>not</b>
    /// exclude the WAL-scheduler write path (a separate thread pool), so the backend-level guard is
    /// what prevents a concurrent <c>FollowerAppend</c> from re-growing the tail between the two steps.
    /// </para>
    /// <para>No-op-safe: if <paramref name="afterLogId"/> is at or above the current max, the
    /// log is unchanged and the current max is returned.</para>
    /// </summary>
    public async ValueTask<long> TruncateLogsAfterAsync(long afterLogId)
    {
        long maxLogId = await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId, afterLogId),
            static s =>
            {
                (RaftOperationStatus _, long m) = s.walAdapter.TruncateLogsAfterAndGetMax(s.PartitionId, s.afterLogId);
                return m;
            }).ConfigureAwait(false);

        // The truncation removed every WAL entry above afterLogId, so any buffered out-of-order
        // resolution above it now points at an absent entry. Drop them; the leader's contiguous
        // backfill re-delivers (and re-buffers) the gap. Safe to mutate here without a lock: the
        // repair caller runs inside the partition's serialized executor message, so no concurrent
        // FollowerAppend is touching pendingResolved.
        while (pendingResolved.Count > 0 && pendingResolved.Max > afterLogId)
            pendingResolved.Remove(pendingResolved.Max);

        // Same for the presence frontier: clamp it to the truncation boundary and drop buffered
        // ids above it. The term at the new frontier is re-read from the surviving entry so the
        // advertised (term, index) pair stays consistent — truncation is a rare repair path, so
        // the extra read is not a hot-path cost.
        while (pendingPresent.Count > 0)
        {
            long maxPending = -1;
            foreach (KeyValuePair<long, long> kv in pendingPresent)
                maxPending = kv.Key;            // SortedDictionary: last key is the maximum
            if (maxPending <= afterLogId)
                break;
            pendingPresent.Remove(maxPending);
        }

        if (presentIndex > afterLogId + 1)
        {
            presentIndex = Math.Max(afterLogId, 0) + 1;
            presentTerm = afterLogId > 0 ? await GetAnyTermAtAsync(afterLogId).ConfigureAwait(false) : 0;
            if (presentTerm < 0)
                presentTerm = 0;                // boundary entry compacted/absent: legacy index-only ordering
        }

        return maxLogId;
    }

    /// <summary>
    /// Atomically installs a snapshot boundary: stamps a durable <c>CommittedCheckpoint</c> at
    /// <paramref name="snapshotIndex"/> with term <paramref name="lastIncludedTerm"/>, retaining the
    /// suffix above it when the stored term matches and truncating it when it conflicts. Delegates the
    /// atomicity to the single backend <see cref="IWAL.InstallSnapshotBoundary"/> op and installs durably
    /// (<c>sync: true</c>) — a snapshot boundary is a committed-state boundary and must survive a crash.
    /// <para>Scheduled on the read thread so it serializes against reads; the backend's per-partition
    /// guard is what excludes the WAL-scheduler write path, exactly as <see cref="TruncateLogsAfterAsync"/>
    /// relies on. Returns whether the suffix was truncated.</para>
    /// </summary>
    public async ValueTask<(RaftOperationStatus Status, bool SuffixTruncated)> InstallSnapshotBoundaryAsync(
        long snapshotIndex, long lastIncludedTerm)
    {
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId, snapshotIndex, lastIncludedTerm),
            static s => s.walAdapter.InstallSnapshotBoundary(
                s.PartitionId, s.snapshotIndex, s.lastIncludedTerm, sync: true)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the id of the last <c>CommittedCheckpoint</c> WAL entry for this partition, or
    /// -1 when no checkpoint exists.  Scheduled on the read thread so it does not race with WAL writes.
    /// </summary>
    public async ValueTask<long> GetLastCheckpointAsync()
    {
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId),
            static s => s.walAdapter.GetLastCheckpoint(s.PartitionId)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Retrieves the current term of the Raft log for the specified partition.
    /// This term represents the latest term recognized by the Write-Ahead Log (WAL).
    /// </summary>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains the current term of the Raft log.
    /// </returns>
    public async Task<long> GetCurrentTerm()
    {
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId),
            static s => s.walAdapter.GetCurrentTerm(s.PartitionId)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Persists this partition's Raft hard state <c>(currentTerm, votedFor)</c> via the backend's metadata
    /// store, namespaced by partition. Durability rides the backend's existing WAL fsync cadence (no
    /// dedicated fsync) per the chosen lighter guarantee, so the very last write can be lost on power
    /// failure. The metadata call is a fast, internally-synchronized in-process write, so it runs inline on
    /// the partition executor rather than through the I/O scheduler.
    /// </summary>
    public void PersistHardState(long currentTerm, string? votedFor) =>
        walAdapter.PersistHardState(partition.PartitionId, currentTerm, votedFor);

    /// <summary>
    /// Reads this partition's persisted hard state, or <see langword="null"/> when none has been written
    /// yet (fresh node or legacy WAL). Used on restore to seed <c>currentTerm</c> and the vote record
    /// instead of inferring the term from the log tail.
    /// </summary>
    public (long CurrentTerm, string? VotedFor)? LoadHardState() =>
        walAdapter.TryGetHardState(partition.PartitionId, out long term, out string? votedFor)
            ? (term, votedFor)
            : null;

    /// <summary>
    /// Processes a list of Raft log entries by proposing or committing them based on their type and ID.
    /// This method validates the logs, ensures ordering, handles outdated logs, and performs necessary actions
    /// such as proposing, committing, or skipping logs as required. This is typically used by replica nodes.
    /// </summary>
    /// <param name="logs">
    /// A list of Raft log entries to be processed. The logs can be of various types, including proposed or committed logs.
    /// If the list is null or empty, no operations are performed, and a success status with an index of -1 is returned.
    /// </param>
    /// <returns>
    /// A tuple containing the operation status and the highest index reached during the process.
    /// The operation status indicates whether the process succeeded, encountered errors, or other specific conditions.
    /// The index represents the maximum of the propose or commit index after processing.
    /// </returns>
    /// <exception cref="NotImplementedException">
    /// Thrown if execution reaches functionality that has not yet been implemented.
    /// </exception>
    public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1)
    {
        if (logs is null || logs.Count == 0)
            return null;

        bool allOutdated = true;

        IReadOnlyList<RaftLog> orderedLogs = OrderById(logs);

        foreach (RaftLog log in orderedLogs)
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed or RaftLogType.ProposedCheckpoint when log.Id < (proposeIndex - 1): 
                    /*logger.LogWarning(
                        "[{Endpoint}/{Partition}] Proposed log #{Id} is not the expected #{ProposeIndex}",
                        manager.LocalEndpoint, 
                        partition.PartitionId, 
                        log.Id, 
                        proposeIndex
                    );*/
                    break;
                
                case RaftLogType.Committed or RaftLogType.CommittedCheckpoint when log.Id < (commitIndex - 1):
                    /*logger.LogWarning(
                        "[{Endpoint}/{Partition}] Committed log #{Id} is not the expected #{CommitIndex}",
                        manager.LocalEndpoint, 
                        partition.PartitionId, 
                        log.Id, 
                        commitIndex
                    );*/
                    break;
                
                default:
                    allOutdated = false;
                    break;
            }
        }

        if (allOutdated)
        {
            /*logger.LogWarning(
                "[{Endpoint}/{Partition}] All replicated indexes are included already in the log Min={Min} Max={Max}",
                manager.LocalEndpoint, 
                partition.PartitionId,
                logs.Min(log => log.Id),
                logs.Max(log => log.Id)
            );
            
            return (RaftOperationStatus.Success, Math.Min(proposeIndex, commitIndex));*/
        }
        
        // Snapshot proposeIndex before the mutation loop so a backpressure rejection from
        // WalScheduler.Enqueue can be rolled back. The commit frontier is intentionally NOT
        // advanced in the loop: resolved ids are collected and applied only after a successful
        // enqueue (below), so a rejection needs no frontier rollback.
        long savedProposeIndex = proposeIndex;

        resolvedThisBatch.Clear();

        // Reuse internal lists
        foreach (KeyValuePair<RaftLogAction, List<RaftLog>> keyValue in plan)
            keyValue.Value.Clear();

        foreach (RaftLog log in orderedLogs)
        {
            switch (log.Type)
            {
                case RaftLogType.Proposed: /* when log.Id >= proposeIndex: */
                {
                    // A locally RESOLVED id must never be written as Proposed again. Resolution
                    // (commit or rollback) is terminal; the only sender of a Proposed copy for a
                    // resolved id is a stale duplicate — a deposed leader's in-flight broadcast or
                    // a proposal retry that raced its own commit. Writing it would regress the
                    // on-disk row from Committed back to Proposed while the in-memory frontier
                    // (which never re-reads rows) stays past it — and the write pipeline's
                    // post-append TruncateProposedLogsAfter then silently DELETES the regressed
                    // row, leaving a permanent hole below the advertised frontier: the follower
                    // reports itself caught up, the leader never backfills, and the apply drain
                    // blocks on the absent row forever (the Jepsen frozen-replica residue).
                    // Checks both the contiguous frontier and the resolved-above-gap buffer, so a
                    // resolved-but-buffered id is protected too.
                    if (log.Id < commitIndex || pendingResolved.Contains(log.Id))
                    {
                        LogStaleProposedSkipped(log.Id);
                        break;
                    }

                    if (plan.TryGetValue(RaftLogAction.Propose, out List<RaftLog> proposeActions))
                        proposeActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Propose, [log]);

                    logger.LogDebugProposedLogs(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    // MONOTONIC, never a plain assignment: proposeIndex is the id ALLOCATOR the
                    // node stamps client writes from when it is (or becomes) leader. A low-id
                    // Proposed row — an unresolved band from an earlier term, a stale duplicate
                    // that predates the resolved-id guard — must never drag it backwards: a
                    // regressed allocator makes a later leader reissue indices that are already
                    // durably occupied elsewhere, committing two different values at one index
                    // (the Jepsen Log Matching violation, run 31805148040 p2/211..218). Every
                    // sibling frontier in this file is monotonic; this was the one that was not.
                    if (log.Id + 1 > proposeIndex)
                        proposeIndex = log.Id + 1;
                }
                break;

                case RaftLogType.RolledBack: /* when log.Id >= proposeIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Rollback, out List<RaftLog> rollbackActions))
                        rollbackActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Rollback, [log]);

                    logger.LogDebugRolledbackLog(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    resolvedThisBatch.Add(log.Id);
                }
                break;    

                case RaftLogType.Committed: /* when log.Id >= commitIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog> commitActions))
                        commitActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Commit, [log]);
                
                    logger.LogDebugCommittedLogs(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    resolvedThisBatch.Add(log.Id);
                }
                break;    

                case RaftLogType.ProposedCheckpoint: /* when log.Id >= proposeIndex: */
                {
                    // Same resolved-id guard as the Proposed case above: a resolved checkpoint id
                    // must never regress to ProposedCheckpoint on disk.
                    if (log.Id < commitIndex || pendingResolved.Contains(log.Id))
                    {
                        LogStaleProposedSkipped(log.Id);
                        break;
                    }

                    if (plan.TryGetValue(RaftLogAction.Propose, out List<RaftLog> proposeActions))
                        proposeActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Propose, [log]);

                    logger.LogDebugProposedCheckpointLog(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    // Monotonic for the same reason as the Proposed arm above.
                    if (log.Id + 1 > proposeIndex)
                        proposeIndex = log.Id + 1;
                } 
                break;

                case RaftLogType.RolledBackCheckpoint: /* when log.Id >= commitIndex: */
                {
                    if (plan.TryGetValue(RaftLogAction.Rollback, out List<RaftLog> rollbackActions))
                        rollbackActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Rollback, [log]);

                    logger.LogDebugRolledBackCheckpointLog(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    resolvedThisBatch.Add(log.Id);
                } 
                break;

                case RaftLogType.CommittedCheckpoint: /* when log.Id >= commitIndex:*/
                {
                    if (plan.TryGetValue(RaftLogAction.Commit, out List<RaftLog> commitActions))
                        commitActions.Add(log);
                    else
                        plan.Add(RaftLogAction.Commit, [log]);

                    logger.LogDebugCommittedCheckpointLog(manager.LocalEndpoint, partition.PartitionId, log.Id);

                    resolvedThisBatch.Add(log.Id);
                } 
                break;

                default:
                    break;
            }
        }

        List<RaftLog> logsToWrite = new(orderedLogs.Count);

        // Track the maximum id while flattening the plan groups so we avoid a second
        // LINQ Max() pass over logsToWrite. The max over the concatenated groups equals
        // the max over logsToWrite, so logIndex is identical to the previous code.
        long maxLogId = -1;

        // EXPLICIT flatten order — proposes first, resolutions last. The physical write applies
        // the flattened list in order and the last put for a key wins, so when one batch carries
        // both a (stale duplicate) Proposed copy and the resolution of the same id, the resolved
        // row must be what lands. Iterating `plan` directly made the order an accident of which
        // action key was inserted first in this instance's lifetime (keys are never removed from
        // the reused dictionary) — a partition whose first-ever batch was a commit marker would
        // flatten Commit before Propose forever, letting a same-batch duplicate leave the row
        // Proposed, where the post-append truncation can silently delete it.
        foreach (RaftLogAction action in PlanFlattenOrder)
        {
            if (!plan.TryGetValue(action, out List<RaftLog> group))
                continue;

            for (int i = 0; i < group.Count; i++)
            {
                RaftLog log = group[i];
                logsToWrite.Add(log);
                if (log.Id > maxLogId)
                    maxLogId = log.Id;
            }
        }

        if (logsToWrite.Count == 0)
            return null;

        // Accepted-id floor for the scheduler's proposed-tail truncation: everything at or below
        // the contiguous presence frontier, everything ever buffered over a gap, and this batch
        // itself has been accepted by this process — a commit-marker re-ship or a late proposal
        // retry whose own max id sits BELOW these rows must not delete them (they may already back
        // quorum acks; the frontiers certifying them are never re-read from disk). Computed on the
        // partition's serialized executor path, the single writer of the presence fields.
        long acceptedFloor = Math.Max(presentIndex - 1, Math.Max(pendingPresentHighWater, maxLogId));

        WALWriteOperation operation = new(
            onComplete,
            Interlocked.Increment(ref walOperationSequence),
            WALWriteOperationType.FollowerAppend,
            (partition.PartitionId, logsToWrite),
            timestamp,
            endpoint,
            term,
            logIndex: maxLogId,
            truncateFloor: acceptedFloor
        );

        try
        {
            manager.WalScheduler.Enqueue(operation);
        }
        catch
        {
            proposeIndex = savedProposeIndex;
            throw;
        }

        // Enqueue succeeded: advance the commit frontier over this batch's resolved ids. Ascending
        // order (orderedLogs is sorted) keeps the drain cheap; AdvanceCommitFrontier still buffers
        // any id that sits above an unfilled gap so the frontier never overshoots a hole.
        foreach (long id in resolvedThisBatch)
            AdvanceCommitFrontier(id);

        // Every written entry (any type) is now durably present; the presence frontier likewise
        // buffers over any gap, so a lone high entry from the unanchored live-propose broadcast
        // never inflates this node's advertised log freshness.
        foreach (RaftLog log in logsToWrite)
            AdvancePresenceFrontier(log.Id, log.Term);

        // Follower-side durable phase. Followers fsync on the propose quorum's critical
        // path, so this phase's latency is measured symmetrically with the leader's.
        WalPhaseInstrumentation.RecordEnqueued(WALWriteOperationType.FollowerAppend);

        return operation;
    }

    public Task<(RaftOperationStatus, long)> ProposeOrCommit(List<RaftLog>? logs)
    {
        WALWriteOperation? operation = EnqueueProposeOrCommit(logs);
        return Task.FromResult(operation is null
            ? (RaftOperationStatus.Success, Math.Max(proposeIndex, commitIndex))
            : (RaftOperationStatus.Pending, operation.LogIndex));
    }

    /// <summary>
    /// Retrieves a range of log entries from the Write-Ahead Log (WAL) starting from the specified log index.
    /// </summary>
    /// <param name="startLogIndex">
    /// The index of the first log entry to be retrieved. Only log entries from this index onward will be returned.
    /// </param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains a list of <see cref="RaftLog"/> objects
    /// corresponding to the logs retrieved from the specified range.
    /// </returns>
    /// <exception cref="RaftException">
    /// Thrown if the thread pool is not started or disposed while attempting to retrieve logs asynchronously.
    /// </exception>
    public async Task<List<RaftLog>> GetRange(long startLogIndex)
    {
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId, startLogIndex),
            static s => s.walAdapter.ReadLogsRange(s.PartitionId, s.startLogIndex)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the term of the single entry at <paramref name="logIndex"/>, or <c>-1</c> if
    /// no entry with that id exists.  All entry types (Proposed, Committed, etc.) are included
    /// so a Log Matching Property check is correct even when the anchor entry is uncommitted.
    /// </summary>
    public async ValueTask<long> GetAnyTermAtAsync(long logIndex)
    {
        // Scalar term lookup: the backend reads a single term (point key/row) instead of
        // materializing a full RaftLog and its payload just to discard everything but Term.
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId, logIndex),
            static s => s.walAdapter.GetTermAt(s.PartitionId, s.logIndex)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> committed log entries with id ≥
    /// <paramref name="startLogIndex"/>, sorted ascending. The bound is pushed to the storage
    /// engine so that a follower far behind the leader does not cause a full tail scan.
    /// Uncommitted (proposed/rolled-back) entries within the returned batch are filtered out.
    ///
    /// <para><b>Compaction floor handling:</b> if the leader has already compacted past
    /// <paramref name="startLogIndex"/>, the requested prefix no longer exists, so this method
    /// returns an empty list rather than a batch that would advance the follower over a gap.
    /// The leader never ships a non-contiguous range — log-shipping always starts at the
    /// follower's <c>lastCommitIndexes + 1</c>, which keeps the follower's log contiguous by
    /// construction (the follower append path does not itself enforce a prev-entry match).
    /// On the empty result, <c>SendHeartbeat</c> initiates a snapshot transfer to the follower
    /// (when a state-machine transfer is registered and a checkpoint exists); the follower then
    /// resumes normal log shipping from the snapshot index.</para>
    /// </summary>
    public async ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries)
    {
        List<RaftLog> all = await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId, startLogIndex, maxEntries),
            static s => s.walAdapter.ReadLogsRange(s.PartitionId, s.startLogIndex, s.maxEntries)
        ).ConfigureAwait(false);

        // Filter out any uncommitted entries (proposed/rolled-back) within the bounded batch.
        // The storage layer already capped the row count, so no further size check is needed.
        // `all` is a fresh list uniquely owned by this call (every backend's ReadLogsRange
        // materializes a new list, and the RaftLog references it holds are not mutated here), so
        // compact the committed entries in place with a write index and trim the tail — this
        // returns the same list and backing array instead of allocating a second List<RaftLog>.
        int write = 0;
        for (int read = 0; read < all.Count; read++)
        {
            RaftLog log = all[read];
            if (log.Type != RaftLogType.Committed && log.Type != RaftLogType.CommittedCheckpoint)
                continue;
            if (write != read)
                all[write] = log;
            write++;
        }
        if (write < all.Count)
            all.RemoveRange(write, all.Count - write);
        return all;
    }

    /// <summary>
    /// Reads up to <paramref name="maxEntries"/> log entries of ANY type (Proposed,
    /// Committed, RolledBack, etc.) with id ≥ <paramref name="startLogIndex"/>.
    /// Unlike <see cref="GetRangeAsync"/>, Proposed/RolledBack entries are not filtered out.
    /// </summary>
    public async ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long startLogIndex, int maxEntries)
    {
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId, startLogIndex, maxEntries),
            static s => s.walAdapter.ReadLogsRange(s.PartitionId, s.startLogIndex, s.maxEntries)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Byte-budgeted variant of <see cref="GetRangeAllTypesAsync(long, int)"/>: the storage engine
    /// also stops once adding the next entry would exceed <paramref name="maxBytes"/> of payload,
    /// while always returning at least one entry when one exists. Bounds the materialized
    /// allocation of the leader-backfill read, which an entry count alone does not.
    /// </summary>
    public async ValueTask<List<RaftLog>> GetRangeAllTypesAsync(long startLogIndex, int maxEntries, long maxBytes)
    {
        return await manager.ReadScheduler.EnqueueTask(
            partition.PartitionId,
            (walAdapter, partition.PartitionId, startLogIndex, maxEntries, maxBytes),
            static s => s.walAdapter.ReadLogsRange(s.PartitionId, s.startLogIndex, s.maxEntries, s.maxBytes)
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Starts log compaction for this partition if no pass is already running.
    /// Returns immediately without waiting for the pass to finish.
    /// <para>
    /// The overlap skip is logged at Trace: it is a silent return that fires often under load, and
    /// "the trigger fired but no pass ran" is otherwise indistinguishable from "the trigger never
    /// fired" when diagnosing a WAL that is not shrinking.
    /// </para>
    /// </summary>
    public void Compact()
    {
        if (Interlocked.CompareExchange(ref compactionInFlight, 1, 0) != 0)
        {
            logger.LogTraceCompactionAlreadyInFlight(manager.LocalEndpoint, partition.PartitionId);
            return;
        }

        compactionPassTask = RunCompactionPassAsync();
    }

    /// <summary>
    /// Waits for the in-flight compaction pass to complete. For tests only.
    /// </summary>
    internal Task WaitForCompactionIdleAsync() => compactionPassTask ?? Task.CompletedTask;

    /// <summary>
    /// Number of compaction passes that actually started. For tests only.
    /// </summary>
    internal int CompactionPassCount => Volatile.Read(ref compactionPassCount);

    /// <summary>
    /// Number of invoked passes that got past both retention gates and reached
    /// <c>CompactLogsOlderThan</c>. The difference against <see cref="CompactionPassCount"/> is the
    /// invoked-vs-effective gap that makes a stalled checkpoint visible. For tests only; the same
    /// split is published as the <c>raft.wal.compaction_passes_total</c> metric.
    /// </summary>
    internal int EffectiveCompactionPassCount => Volatile.Read(ref effectiveCompactionPassCount);

    /// <summary>
    /// Sets the minimum WAL index that compaction must not truncate below, regardless of the
    /// checkpoint position. Used by point-in-time recovery consumers to protect a retained tail.
    /// <para>
    /// This setter is synchronous and thread-safe via a volatile write; after it returns the next
    /// compaction pass observes the new value without any scheduling round-trip.
    /// </para>
    /// <para>
    /// Values &lt;= 0 are normalized to <see cref="long.MaxValue"/> (no protection). This prevents
    /// a caller that has not yet computed its protected index (e.g. first tick, empty PITR window)
    /// from accidentally disabling compaction by passing 0.
    /// </para>
    /// <para>
    /// The floor is in-memory and resets to <see cref="long.MaxValue"/> on process restart.
    /// Consumers must re-assert it after every node start before relying on PITR for that node.
    /// </para>
    /// </summary>
    public void SetMinRetainIndex(long index) =>
        Volatile.Write(ref minRetainIndex, index <= 0 ? long.MaxValue : index);

    /// <summary>
    /// Acquires a composable retention hold: the WAL retains committed entries down to
    /// <paramref name="index"/> for this partition until the returned handle is disposed. Unlike
    /// <see cref="SetMinRetainIndex"/> (a single last-writer-wins floor), any number of holds may be
    /// active at once and the effective floor is the <b>minimum</b> across all of them, so
    /// independent consumers never clobber one another. <see cref="SetMinRetainIndex"/> remains an
    /// orthogonal floor composed in as the degenerate single-value case.
    /// <para>
    /// Semantics: N concurrent holds ⇒ floor = min(index over holds); disposing one recomputes the
    /// floor to the min of the rest; zero holds ⇒ no hold protection (<see cref="long.MaxValue"/>).
    /// An <paramref name="index"/> &lt;= 0 is normalized to <see cref="long.MaxValue"/> (a hold that
    /// contributes no protection), mirroring <see cref="SetMinRetainIndex"/>.
    /// </para>
    /// <para>
    /// Thread-safe and synchronous: the recomputed floor is published with a volatile write before
    /// the call returns, so the next compaction pass observes it with no scheduling round-trip.
    /// In-memory — resets on process restart; consumers must re-assert holds after every node start.
    /// </para>
    /// <para>
    /// <see cref="IDisposable.Dispose"/> on the returned handle is idempotent: it releases exactly
    /// one hold and repeated calls are no-ops.
    /// </para>
    /// </summary>
    public IDisposable AcquireRetentionHold(long index)
    {
        long normalized = index <= 0 ? long.MaxValue : index;

        long token;
        lock (holdsLock)
        {
            token = nextHoldToken++;
            retentionHolds[token] = normalized;
            RecomputeHoldFloorLocked();
        }

        return new RetentionHold(this, token);
    }

    /// <summary>
    /// Releases the hold identified by <paramref name="token"/> and republishes the hold floor.
    /// Idempotent — a token already removed (or never present) is ignored. Invoked only from
    /// <see cref="RetentionHold.Dispose"/>.
    /// </summary>
    private void ReleaseRetentionHold(long token)
    {
        lock (holdsLock)
        {
            if (retentionHolds.Remove(token))
                RecomputeHoldFloorLocked();
        }
    }

    /// <summary>
    /// Recomputes <see cref="holdFloor"/> as the minimum of the active holds (<see cref="long.MaxValue"/>
    /// when none) and publishes it with a volatile write. Caller must hold <see cref="holdsLock"/>.
    /// </summary>
    private void RecomputeHoldFloorLocked()
    {
        long min = long.MaxValue;
        foreach (long held in retentionHolds.Values)
        {
            if (held < min)
                min = held;
        }

        Volatile.Write(ref holdFloor, min);
    }

    /// <summary>Current retention floor. Diagnostics/tests only; <see cref="long.MaxValue"/> means unset.</summary>
    internal long MinRetainIndex => Volatile.Read(ref minRetainIndex);

    /// <summary>
    /// Live-replica retention floor as last published (before the budget clamp and the staleness
    /// check applied at compaction time). Diagnostics/tests only; <see cref="long.MaxValue"/> means
    /// no follower constrains retention.
    /// </summary>
    internal long LiveReplicaRetentionFloor => Volatile.Read(ref liveReplicaRetentionFloor);

    /// <summary>
    /// Effective retention floor actually applied by compaction (before composing with the
    /// checkpoint): the minimum of the legacy <see cref="SetMinRetainIndex"/> floor and the
    /// min-of-holds floor. <see cref="long.MaxValue"/> means no extra retention. Diagnostics/tests only.
    /// </summary>
    internal long EffectiveRetentionFloor => Math.Min(Volatile.Read(ref minRetainIndex), Volatile.Read(ref holdFloor));

    /// <summary>
    /// Handle returned by <see cref="AcquireRetentionHold"/>. Disposing releases exactly one hold;
    /// the <see cref="Interlocked.Exchange(ref int, int)"/> guard makes repeated disposal a no-op so
    /// a double-dispose can never release a later hold that happens to reuse bookkeeping.
    /// </summary>
    private sealed class RetentionHold : IDisposable
    {
        private readonly RaftWriteAhead owner;
        private readonly long token;
        private int disposed;

        public RetentionHold(RaftWriteAhead owner, long token)
        {
            this.owner = owner;
            this.token = token;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
                owner.ReleaseRetentionHold(token);
        }
    }

    /// <summary>
    /// Runs one compaction pass: resolves the truncation floor, then drains removable entries below
    /// it. Never throws — a failed pass is logged and the in-flight flag released.
    /// <para>
    /// The floor is <c>min(lastCheckpoint, minRetainIndex, holdFloor, durabilityFloor)</c>, so a
    /// pass can legitimately do nothing. Every such outcome is reported: the two gates log at Debug
    /// and count into <c>raft.wal.compaction_passes_total</c> under their own <c>outcome</c> tag,
    /// and the no-checkpoint gate additionally emits one Information line per stall. This is
    /// deliberate — both gates used to return above the first log statement, which made a WAL that
    /// never shrinks produce no evidence at any level, so "the trigger never fired", "nothing has
    /// checkpointed", and "a retention floor is holding" were indistinguishable in a post-mortem.
    /// </para>
    /// </summary>
    private async Task RunCompactionPassAsync()
    {
        Interlocked.Increment(ref compactionPassCount);

        try
        {
            long lastCheckpoint = await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () =>
                walAdapter.GetLastCheckpoint(partition.PartitionId)
            ).ConfigureAwait(false);

            if (lastCheckpoint <= 0)
            {
                KommanderMetrics.RecordCompactionPass(partition.PartitionId, KommanderMetrics.CompactionOutcome.NoCheckpoint);
                logger.LogDebugCompactionSkippedNoCheckpoint(manager.LocalEndpoint, partition.PartitionId, lastCheckpoint);

                // One line per stall at Information: the Debug line above can fire on every commit,
                // but a deployment whose application never checkpoints needs exactly one visible
                // statement that compaction is gated on something other than CompactEveryOperations.
                if (Interlocked.Exchange(ref awaitingFirstCheckpointReported, 1) == 0)
                    logger.LogInfoCompactionAwaitingFirstCheckpoint(manager.LocalEndpoint, partition.PartitionId);

                return;
            }

            // A checkpoint exists again: rearm the one-shot so a later stall is reported rather
            // than swallowed by the flag set during an earlier one.
            Volatile.Write(ref awaitingFirstCheckpointReported, 0);

            // Application-durability floor: entries the application has not durably applied must
            // never be truncated, even below the checkpoint — restart replay needs them (see
            // LoadRestoreLogsAsync). CompactLogsOlderThan deletes strictly below its floor, so
            // durablyApplied + 1 fences exactly the unapplied suffix while still allowing the
            // durably-applied prefix (id <= durablyApplied) to be removed. Applies on leaders and
            // followers alike, including the startup window before the consumer's first flush tick
            // (the provider reads its persisted floor, so nothing needs re-asserting).
            long durabilityFloor = long.MaxValue;
            bool clampedByDurabilityFloor = false;

            IApplicationDurabilityProvider? durabilityProvider = manager.Configuration.ApplicationDurabilityProvider;
            if (durabilityProvider is not null)
            {
                long durablyApplied = durabilityProvider.GetDurablyAppliedIndex(partition.PartitionId);
                if (durablyApplied >= 0)
                {
                    durabilityFloor = durablyApplied + 1;
                    clampedByDurabilityFloor = durabilityFloor < lastCheckpoint;
                    KommanderMetrics.RecordDurabilityFloorLag(
                        partition.PartitionId,
                        Math.Max(0, lastCheckpoint - durabilityFloor));
                }
            }

            // Live-replica lag budget: while a live, acking follower's replicated position sits
            // below the checkpoint AND within the configured budget, hold the floor at that
            // position so ordinary backfill can still serve the follower. Compacting past a
            // responsive replica forces it into snapshot dependence, and a leader that does so on
            // its ordinary cadence re-creates the below-floor condition faster than any snapshot
            // rescue can repair it — the rescue then loops forever (the Caraxes
            // bank-optimistic-45m-p finding). The budget bounds the retention: a follower more
            // than lagBudget entries behind is beyond what backfill catch-up can plausibly close
            // before the next pass and is left to the snapshot path, so a slow-forever replica
            // cannot grow the WAL without bound. The staleness window bounds trust in the
            // publisher: only an active leader keeps the value fresh.
            long liveReplicaFloor = long.MaxValue;
            long lagBudget = manager.Configuration.CompactionLiveReplicaLagBudget;
            if (lagBudget > 0)
            {
                long publishedTicks = Volatile.Read(ref liveReplicaFloorPublishedTicks);
                long publishedFloor = Volatile.Read(ref liveReplicaRetentionFloor);
                if (publishedTicks != 0
                    && Stopwatch.GetTimestamp() - publishedTicks <= liveReplicaFloorStalenessTicks
                    && publishedFloor < lastCheckpoint)
                {
                    liveReplicaFloor = Math.Max(publishedFloor, lastCheckpoint - lagBudget);
                    KommanderMetrics.RecordCompactionHeldByLiveReplica(
                        partition.PartitionId, lastCheckpoint - liveReplicaFloor);
                }
            }

            // Compose the legacy single floor with the min-of-holds floor, the
            // application-durability floor, and the live-replica floor: compaction must retain
            // below whichever protected index is lowest across all consumers.
            long retainIndexFloor = Volatile.Read(ref minRetainIndex);
            long activeHoldFloor = Volatile.Read(ref holdFloor);
            long retainFloor = Math.Min(
                Math.Min(Math.Min(retainIndexFloor, activeHoldFloor), durabilityFloor), liveReplicaFloor);
            long effectiveFloor = Math.Min(lastCheckpoint, retainFloor);

            if (effectiveFloor <= 0)
            {
                KommanderMetrics.RecordCompactionPass(partition.PartitionId, KommanderMetrics.CompactionOutcome.FloorNotPositive);

                // Guarded so the floor-source attribution is not computed when Debug is off.
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebugCompactionSkippedFloorNotPositive(
                        manager.LocalEndpoint,
                        partition.PartitionId,
                        effectiveFloor,
                        DescribeFloorSource(effectiveFloor, lastCheckpoint, retainIndexFloor, activeHoldFloor, durabilityFloor, liveReplicaFloor),
                        lastCheckpoint,
                        retainIndexFloor,
                        activeHoldFloor,
                        durabilityFloor);

                return;
            }

            Interlocked.Increment(ref effectiveCompactionPassCount);
            KommanderMetrics.RecordCompactionPass(partition.PartitionId, KommanderMetrics.CompactionOutcome.Effective);

            logger.LogInfoCompactionStarted(manager.LocalEndpoint, partition.PartitionId, effectiveFloor);

            // Scheduled on ReadScheduler, not WalScheduler — compaction deletes must not
            // contend with the write path on the WAL scheduler.
            // All drain batches run inside a single WAL compaction call so durable backends
            // commit one transaction / db.Write per pass instead of one per batch.
            int removedTotal = await manager.ReadScheduler.EnqueueTask(partition.PartitionId, () =>
            {
                (RaftOperationStatus status, int removed) = walAdapter.CompactLogsOlderThan(
                    partition.PartitionId,
                    effectiveFloor,
                    compactNumberEntries,
                    maxEntriesPerCompaction);

                return status == RaftOperationStatus.Success ? removed : 0;
            }).ConfigureAwait(false);

            logger.LogInfoCompactionFinished(manager.LocalEndpoint, partition.PartitionId, removedTotal, effectiveFloor);

            // A clamped pass that removed nothing has fully drained the durably-applied prefix and
            // is now blocked waiting on the application's flusher. Surface it loudly: a stalled
            // flusher otherwise grows the WAL without bound and silently.
            if (clampedByDurabilityFloor && removedTotal == 0)
            {
                KommanderMetrics.RecordCompactionBlockedByDurabilityFloor(partition.PartitionId);
                logger.LogWarnCompactionBlockedByDurabilityFloor(
                    manager.LocalEndpoint,
                    partition.PartitionId,
                    durabilityFloor - 1,
                    lastCheckpoint);
            }
        }
        catch (Exception ex)
        {
            KommanderMetrics.RecordCompactionPass(partition.PartitionId, KommanderMetrics.CompactionOutcome.Failed);

            logger.LogError(
                ex,
                "[{Endpoint}/{Partition}] Compaction failed",
                manager.LocalEndpoint,
                partition.PartitionId);
        }
        finally
        {
            Interlocked.Exchange(ref compactionInFlight, 0);
        }
    }

    /// <summary>
    /// Names which consumer's floor the composed <paramref name="effectiveFloor"/> came from, for the
    /// skip log. Ties are resolved in the order checkpoint → durability floor → retention holds →
    /// <see cref="SetMinRetainIndex"/>: the earlier a source appears, the more likely it is the one an
    /// operator can act on, and reporting a single name keeps the message actionable where printing
    /// four numbers alone would not.
    /// </summary>
    private static string DescribeFloorSource(
        long effectiveFloor,
        long lastCheckpoint,
        long retainIndexFloor,
        long activeHoldFloor,
        long durabilityFloor,
        long liveReplicaFloor)
    {
        if (effectiveFloor == lastCheckpoint)
            return "checkpoint";
        if (effectiveFloor == durabilityFloor)
            return "durability_floor";
        if (effectiveFloor == activeHoldFloor)
            return "retention_hold";
        if (effectiveFloor == retainIndexFloor)
            return "min_retain_index";
        if (effectiveFloor == liveReplicaFloor)
            return "live_replica_lag_budget";
        return "unknown";
    }
}
