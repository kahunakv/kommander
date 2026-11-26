# WAL Write Durability & the Single-Fsync Commit Fast Path — Developer Guide

This guide explains how a write becomes **durable** on the persistent WAL backends (RocksDB, SQLite),
and the settings that reduce the cost of the disk syncs. **Group commit** raises throughput. The
**single-fsync commit fast path** lowers latency. The guide also covers a full disk, storage corruption
and lost writes.

---

## Table of contents

1. [Summary](#summary)
2. [Two fsyncs per committed write](#two-fsyncs-per-committed-write)
3. [Group commit (throughput)](#group-commit-throughput)
4. [The single-fsync fast path (latency)](#the-single-fsync-fast-path-latency)
5. [The lazy commit marker](#the-lazy-commit-marker)
6. [Crash and recovery](#crash-and-recovery)
7. [Configuration](#configuration)
8. [Full-disk (ENOSPC) recovery](#full-disk-enospc-recovery)
9. [Storage corruption and lost writes](#storage-corruption-and-lost-writes)
10. [Observability](#observability)
11. [Code map](#code-map)
12. [Invariants you must not break](#invariants-you-must-not-break)
13. [Testing](#testing)
14. [Glossary](#glossary)

---

## Summary

A Raft write is durable when a **quorum of nodes has it on disk**. Kommander commits in two phases,
**propose** and **commit**, and on a durable backend each phase issues an `fsync`. With the fast path
off, both are on the critical path of a write.

```
   client write ──► leader PROPOSE  (fsync #1)  ──► replicate, wait for quorum
                 ──► leader COMMIT   (fsync #2)  ──► ack client
```

- **Group commit** packs many concurrent writes into one `fsync`. It is always on and raises throughput.
  `WalGroupCommitLingerMs` tunes it.
- **The single-fsync fast path** (`WalSingleFsyncCommit`, on by default) acknowledges the client as soon
  as a quorum has the entry on disk, which is the Raft commit point. The commit marker is written lazily
  and rides a later sync. This lowers latency and roughly halves the syncs per write.

```
   fast path off:  PROPOSE(fsync) ──► quorum ──► COMMIT(fsync) ──► ack     (2 fsyncs on the path)
   fast path on:   PROPOSE(fsync) ──► quorum ──► ack ; commit marker rides next sync   (1)
```

---

## Two fsyncs per committed write

- **Propose.** The leader appends the entry as `Proposed`, syncs it and replicates it. Followers append
  it durably and acknowledge. The entry is committed in the Raft sense once a quorum acknowledges.
- **Commit.** The leader writes a per-entry `Committed` marker, an in-place update of the same row. With
  the fast path off it syncs the marker before it acknowledges the client. Followers write and sync the
  replicated commit too.

```
  leader: append Proposed(id=N) ─ fsync ─► replicate to followers
  followers: append Proposed(id=N) ─ fsync ─► ack
  leader: quorum reached ─► write Committed(id=N) ─ fsync ─► ack client
```

`RaftWriteAhead` enqueues the two phases as separate `WALWriteOperation`s. The two syncs of one write
cannot coalesce, since the commit operation is created after the propose reaches quorum. The second sync
is redundant for durability. The marker lets the WAL describe its committed prefix on restart.

`RocksDbWAL` and `SqliteWAL` sync by default (`syncWrites: true`, which is RocksDB
`WriteOptions.SetSync(true)` and SQLite `PRAGMA synchronous=FULL`). `InMemoryWAL` does not sync.

---

## Group commit (throughput)

When several partitions have writes ready, one `FairWalScheduler` worker drains up to
`MaxWalGroupBatchPartitions` of them and up to `MaxWalBatchSize` operations from each. It then issues one
`walAdapter.Write`, which is one `fsync` on RocksDB. A batch can mix one partition's propose with
another's commit.

```
  partitions ready:  P1(commit)  P2(propose)  P3(commit)  P4(propose)  …
                          └──────────── one walAdapter.Write ───────────┘
                                         = ONE fsync (RocksDB)
```

By default a worker syncs the work that is ready when it wakes. Work that arrives staggered, as on the
follower append path, then gets a near-solo sync for each arrival. `WalGroupCommitLingerMs` makes the
worker wait a bounded, adaptive interval for more partitions. The wait ends as soon as a probe finds no
newly ready partition. The linger raises batch density and tightens the latency tail. It cannot shorten
a write that waits for two serial syncs.

---

## The single-fsync fast path (latency)

`WalSingleFsyncCommit` applies to the `autoCommit` single-round path on the leader and the followers. The
explicit two-phase (`!AutoCommit`) path has a separate durable commit and does not change.

When the propose quorum lands, the leader advances its in-memory commit frontier and releases the client
ticket **before** it enqueues the commit marker. The client waits for one sync plus the quorum round-trip.

```
  propose quorum reached
     ├─► advance localCommittedIndex to this entry      (in memory)
     ├─► mark proposal Committed ─► client ticket releases   ← ack happens here
     └─► enqueue the Committed marker write (lazy)
```

A single-voter leader is a quorum by itself, and the fast path applies there too. In a 3-node loopback
RocksDB benchmark (mixed workload, concurrency 64) the flag lowered the set p50 by roughly 18% and the
syncs per committed write from ~2 to ~1.

The fast path preserves the guarantee that **a write acknowledged to the client is durable on a quorum,
and crash recovery yields the same committed prefix**.

- **Ack timing.** A follower acknowledges a propose after its WAL write is durable. A propose quorum
  therefore means that a quorum has the entry on disk.
- **Lazy marker.** A marker lost in a crash only demotes an entry to `Proposed` on disk. The entry was
  quorum-durable from its propose sync and can be recovered (see [Crash and recovery](#crash-and-recovery)).
- **The recovery anchor is durable.** `CommittedCheckpoint` is always a sync write, on both the
  checkpoint-commit and snapshot-install paths.

### The leader's propose sync overlaps the followers'

With `FanOutBeforeLocalWrite` on (the default), the leader sends a proposal to its followers as soon as
its own `Proposed` write is **queued**, before it is durable. The syncs then overlap.

```
   serial:      leader PROPOSE(fsync) ──► send ──► follower APPEND(fsync) ──► ack ──► quorum
   overlapped:  leader PROPOSE(fsync) ─────────────────────────────┐
                send ──► follower APPEND(fsync) ──► ack ───────────┴──► quorum
```

The quorum counts the leader after its own write is durable (`RaftProposalQuorum.LocalDurable`). Two
follower acks are not enough while the leader's copy is queued. Work that runs at quorum (the commit
marker, the leader's applies, backfill reads up to the commit index) therefore finds the batch on the
leader's disk. If the leader's write fails, the followers may already have the batch and a later leader
can commit it. The caller gets `ProposalOutcomeUnknown`. The promotion barrier uses the serial order.

### The follower acks before it applies

With `FollowerApplyInOwnTurn` on (the default), a follower acknowledges an append before it delivers the
entries that append committed to the application (`OnReplicationReceived`). It delivers them afterwards
in separate executor turns (`FollowerApplyLane`) of at most `FollowerApplyTurnTime` (100 µs), in the
maintenance class. The next proposal's append is in the replication class and goes first.

```
   before:  [commit k completion: apply 1..N ──► ack] [append k+1 ...]
   after:   [commit k completion: ack] [append k+1] [apply turn] [apply turn] ...
```

An ack does not carry the applied position. Delivery is exactly-once and in log order, through
`LogApplicator.ApplyLogToConsumerAsync`. The applied cursor advances only over delivered entries, and it
completes the read-index and `ConfirmLocalApplicationAsync` waiters. The cursor can trail the commit
frontier by the entries queued for a turn. The other delivery paths (the tick's retry, promotion,
snapshot install and the apply hold) read the WAL from that cursor. The lane has its entries in memory,
which means that a compaction below the checkpoint cannot make it step over them.

A turn also delivers any backlog above eight entry caps (`FollowerApplyTurnBudget`). A consumer that is
slower than the commit rate therefore slows the follower. The system partition always delivers inline.

---

## The lazy commit marker

The per-entry `Committed` marker is written with `sync = false`.

- **RocksDB** writes it with `SetSync(false)`. A later sync write syncs the shared WAL up to that point.
- **SQLite** sets `PRAGMA synchronous` to `OFF` for that transaction, under the shard lock, and restores
  `FULL` afterward. The next `FULL` commit syncs the `-wal` file with the prior frames.

`FairWalScheduler` writes a batch sync-off **only if each log in it is a per-entry `Committed` marker**
for a row that the partition already has. Any proposed entry, `CommittedCheckpoint` or rollback forces a
**sync** write. So does a `Committed` row that is the first write of its entry (`RequiresSync`). A sync
write also flushes the markers that ride with it.

```
  batch = all Committed markers      ─► sync-off (rides the next sync)
  batch = has a Proposed / checkpoint ─► sync  (and flushes pending markers)
```

The number of `walAdapter.Write` calls is two per write in both modes.

---

## Crash and recovery

After a crash the on-disk type is not a reliable committed/uncommitted boundary, in **either** commit
mode. On the fast path a crash can lose lazy markers. With the fast path off, a follower learns commits
from an asynchronous broadcast and can crash between the entry and its marker. Restart is therefore the
same in both modes, and `RaftWriteAhead.CompleteRestoreAsync` reconstructs the frontier conservatively.

- `commitIndex = (highest contiguous committed prefix) + 1` is the **safe lower bound**. A lost marker
  stops the prefix. A durable `CommittedCheckpoint` certifies the prefix below it and jumps it.
- `proposeIndex = maxLogId + 1` preserves the durable `Proposed` tail. Its ids are not reused, which
  prevents the overwrite of an acknowledged entry. An unacknowledged entry is not promoted to committed.

The frontier above the lower bound is then re-supplied.

- A restarted **follower** reports its real commit frontier on heartbeat acks. The leader detects the
  regression, which its monotonic `matchIndex` cannot show, and re-ships the committed tail through
  backfill. The re-shipped `Committed` copies upsert the `Proposed` rows and apply them idempotently.
- `BackfillThreshold` does **not** gate this repair. No future broadcast carries the missing entries, and
  the idle-tail trigger in `SendHeartbeat` does not push committed state that came from a restore (the
  `liveCommitFloor` guard). A gap below the threshold would stay open on an idle cluster. The batch is
  anchored to the frontier the follower reported, since `nextIndex` points above the regressed range.
- A restarted node that **wins election** re-commits the durable `Proposed` prefix once a current-term
  entry commits.

```
  on disk after crash:  [Committed 1..K][Proposed K+1..N]   (markers K+1..N lost)
  recovery:             commitIndex = K+1   proposeIndex = N+1   (tail kept, not promoted)
  leader re-supply:     re-ships Committed K+1..N  ─► follower converges to N
```

---

## Configuration

These settings are in `RaftConfiguration.cs`.

| Setting | Default | What it does |
|---|---|---|
| `WalSingleFsyncCommit` | `true` | Acks `autoCommit` writes when the propose quorum is durable and writes the commit marker lazily. Off ⇒ the commit sync is on the client's critical path, and all WAL batches sync. |
| `FanOutBeforeLocalWrite` | `true` | Sends a proposal to the followers while the leader's own write is queued. The quorum waits for the leader's write. Off ⇒ the followers get the batch after the leader's write is durable (two syncs in series per round). |
| `FollowerApplyInOwnTurn` | `true` | A follower acks an append before it delivers the entries the append committed. Off ⇒ the follower delivers inside the append's completion, before its ack. |
| `FollowerApplyTurnTime` | `100 µs` | How long one follower apply turn delivers before it yields to queued appends and acks. At least one entry per turn. `Zero` ⇒ only the entry cap. |
| `FollowerApplyTurnBudget` | `1024` | The entry cap of one follower apply turn. A backlog above eight caps is delivered anyway (backpressure). `0` ⇒ no cap. |
| `WalGroupCommitLingerMs` | `0` | `> 0` lets a WAL worker linger up to this many ms to gather more ready partitions into one `fsync`. Evidence-gated: a worker waits only while another partition has queued work that must become ready soon. `0` ⇒ opportunistic batching. |
| `MaxWalGroupBatchPartitions` | `64` | Max partitions coalesced into a single `walAdapter.Write`. |
| `MaxWalBatchSize` | `256` | Max operations drained from one partition per write. |
| `WriteIOThreads` | `4` | WAL scheduler worker threads, which is how many writes can sync concurrently across partitions. |
| `IdleWorkerMaxSpinBeforePark` | `20 µs` | How long one idle WAL worker polls the ready-queue before it parks. A parked worker costs no CPU. An enqueue that makes a partition ready wakes one worker (a lingering worker first), or none while a worker is in this spin. `Zero` ⇒ no spin. Shared with the executor pool and the read scheduler. |

- **`WalSingleFsyncCommit`** changes no on-disk format, and you can turn it off at any time. In both modes
  a restarted node's applied state is the contiguous committed prefix until re-supply lands.
- **`WalGroupCommitLingerMs`** helps most when writes spread across many partitions and arrive staggered.
  Start around `2` ms and measure. The median does not drop. Idle, sequential and closed-loop load pays no
  wait.
- The two settings are complementary and can be enabled together.

---

## Full-disk (ENOSPC) recovery

On a full data volume the node stays up and heals when space returns. This section describes the RocksDB
backend, which has an error state of its own.

### Full-disk effects on the node

1. **Writes fail as a status.** `RocksDbWAL.Write` returns `Errored`. The Raft layer regresses its
   in-memory presence and commit frontiers to the durable log for the failed range
   (`RaftWriteAhead.RegressFrontiersAfterFailedWriteAsync`). The node does not advertise entries it does
   not have. An acknowledged entry is synced on a quorum and is not lost.
2. **Elections pause.** `IWAL.PersistHardState` persists the term and the vote, and returns `false` on a
   full disk. An election attempt that cannot record its term and self-vote is abandoned before any
   `RequestVote` is sent, and the node returns to `Follower`. A vote that cannot be recorded is withheld,
   which prevents a double vote after a crash. Both are retried on the normal election cadence.
3. **The WAL logs once, then at 30-second intervals** while writes fail, with the duration of the episode
   and the last error.

### RocksDB self-recovery limits

RocksDB has one background-error status per engine.

| Failure | RocksDB error text | RocksDB severity | Self-heals? |
|---|---|---|---|
| Append to the current WAL file fails | `While appending to file: …/NNNNNN.log` | Hard error | **Yes.** The engine's built-in `SstFileManager` polls free space and resumes the engine about 5 s after space returns. |
| Creation of a fresh WAL file fails (the memtable switch of a flush) | `While open a file for appending: …/NNNNNN.log` | Unrecoverable | **No.** Automatic recovery does not start, and a manual resume is refused. Each later write returns the cached status and does not touch the disk. |

A sustained full-disk episode ends in the second failure when a memtable fills, or when the shared
write-buffer manager asks for a flush.

### `RocksDbWAL` recovery

After a write-path failure the WAL makes two checks and does not parse error strings.

1. **Does the filesystem accept writes?** A 64 KiB probe file in the engine directory is written,
   synced and deleted, at most once per second.
2. **Does the engine accept writes?** A canary key is written to the metadata column family with a synced
   write.

If the filesystem accepts writes and the engine does not, the engine is closed and reopened on the same
directory with the same options. The reopen takes the exclusive side of an engine guard that all
operations take on the read side. It replays the log files on disk. Acknowledged sync writes are already
synced, and un-synced records were handed to the operating system on append, which means that a
same-process reopen loses no acknowledged write. The failed batch was already reported as `Errored`.

The entry point that saw the failure then retries its operation once. The operations are idempotent
upserts or deletes by `(partition, id)`.

A disk that is still full does not pass the first check, and the node returns `Errored` until space
returns. An engine that resumed passes the second check and is left alone. If the reopen fails, the WAL
is closed, a `Critical` line is logged, and the next write-path failure retries the reopen.

### Operator signals

- `Warning` — `RocksDB WAL at '…' rejected a write: …` on the first failure of an episode, then at
  30-second intervals with the duration and count.
- `Warning` — `RocksDB WAL at '…' was reopened to clear a latched storage error (reopen #N)`.
- `Information` — `RocksDB WAL at '…' accepts writes again after N failures over D`.
- `Critical` — `RocksDB WAL at '…' could not be reopened after a storage failure`.
- Metric `raft.wal.engine_reopens_total`, tagged `outcome=success|failed`. With `outcome=success`, a
  non-zero value means that the node healed from a full-disk episode with no restart.

The library does not stop the host process. A deployment that prefers to fail fast should watch the
metric and the `Warning` cadence, and act from outside.

---

## Storage corruption and lost writes

This section covers damage under a running or restarted node. Examples are a synced record that is gone
after a restart, a block with a bad checksum, or an application store that was wiped or restored from an
older copy under an intact WAL. The rule is from protocol-aware recovery (Alagappan et al., FAST 2018): a
replica that cannot prove it has what it promised must not vote or lead from what it has. It must also
show that state to the operator and the host's readiness probe.

### The term and the vote are fsynced

`IWAL.PersistHardState` uses the backend's sync write options (`RocksDbWAL.SetMetaDataCore`). A granted
vote that is lost in a power failure would let the node vote again in the same term, and two leaders
could commit conflicting entries.

### Built-in engine checks

RocksDB verifies a checksum on each block it reads and each log record it replays. The WAL opens with
`paranoid_checks`, `verify_sst_unique_id_in_manifest` and `track_and_verify_wals_in_manifest`. With the
last option, a missing or short **closed** log fails the open. RocksDB does not track the live log. The
sync ledger covers it.

- `TolerateCorruptedTailRecords` tolerates only a **truncated** tail, which is the residue of a torn
  write. A checksum mismatch anywhere in the file, the last record included, fails the open. RocksDB's own
  default is `kPointInTimeRecovery`.
- An iterator that reaches a damaged block does not throw. It becomes invalid and has the `Corruption`
  status. A scan that tests only `Valid()` would read a damaged SST as an empty log. All scans in
  `RocksDbWAL` end through `IteratorEnded`, which rethrows the error.

### The acknowledged-presence watermark

With `syncWrites` on, the contiguous presence frontier in each ack is a promise. The watermark is the
durable record of that promise, stored next to the rows. It catches rows that go missing under a
surviving watermark, for example in a damaged or restored shard.

Each log-carrying WAL operation carries `WALWriteOperation.AcknowledgedPresence`, the frontier that the
partition will have after that batch and the ones queued before it land
(`RaftWriteAhead.ProjectPresenceAfter`). The scheduler folds it per partition in submission order and
passes it to `IWAL.Write(logs, sync, acknowledgedPresence)`. `RocksDbWAL` stages it into the **same
`WriteBatch`** as the rows, under the key `raft_acked_presence_p{partition}`. It adds no `fsync`.

- It is **assigned** and not maximised. Within a partition's FIFO it rises, except after a failed write.
  Then `RegressFrontiersAfterFailedWriteAsync` queues a `WALWriteOperationType.AcknowledgedPresence`
  correction behind any operation that carried the optimistic frontier.
- Each path that removes rows **clamps** it in the same batch. `TruncateLogsAfter` and the proposed-only
  sweep clamp it to the cut. A truncating snapshot install clamps it to the boundary, and a retained one
  raises it to the boundary. `DeletePartitionWAL` removes it.
- The projection does not count an id buffered over a gap. A low watermark narrows what the check can
  detect. A high one would give a false loss report.

On restore, after all the frontier seeds (checkpoint, compaction floor, soft floor), `CompleteRestoreAsync`
compares the watermark with the reconstructed contiguous frontier. A watermark above it means that rows
this node promised are gone. The leader may have counted them toward a commit.

**Limit.** The watermark is in the same log file as the rows. A live log truncated **after** its `fsync`
looks like a torn write, and it loses the watermark together with the rows, the term and the vote. The
node would then report `Healthy` and could vote twice in one term. The two ledgers below are outside the
log for this reason.

### The sync ledger: a truncated live log

`RocksDbSyncLedger` is a two-slot, 1 KiB file beside the engine (`.kommander-sync-ledger`). It attests the
highest RocksDB sequence number that a synchronous write of this process completed. All synchronous
engine writes go through `RocksDbWAL.WriteAttested`. It takes the sequence from the batch's own header,
since a concurrent unsynced write may raise the engine's latest sequence past what this `fsync` covered.
The sequence is written to the other slot and synced before the write is reported successful. Concurrent
writers group-commit the ledger.

The ledger is written after the engine's `fsync`. At open, a ledger ahead of the replayed sequence proves
that synced writes are gone. The engine then records a verdict (metadata key `raft_lost_synced_writes`, a
monotonic id plus a reason with both sequences), resets the ledger and continues to run. Its files are
consistent but stale, and replication from a leader repairs them. All partitions on the engine are
suspect. `IWAL.GetLostSyncedWritesReason(partition)` returns the reason for each one until that partition
records its reconciliation (`MarkLostSyncedWritesReconciled`, metadata key
`raft_reconciled_lost_synced_writes_p{partition}`). A crash before the reconciliation quarantines again. A
later event has a higher id and quarantines partitions that reconciled the earlier one.

- The ledger carries the engine's identity (`raft_engine_identity`). A ledger copied from another
  directory is reset.
- A torn slot is skipped in favour of the other. Two unreadable slots count as no ledger, with a warning.
- A directory from before the ledger is upgraded at its next open.
- A torn **unsynced** record was not attested and reopens healthy. With `syncWrites` off no sequence is
  attested.

The cost is one extra device round trip per synchronous engine write (an in-place sector write plus
`fdatasync`, grouped across concurrent writers). The ledger avoids the full-device flush that
`FileStream.Flush(true)` issues on macOS.

### The hard-state ledger: a forgotten vote

The truncation of a live log can also remove a granted vote. `RocksDbHardStateLedger`
(`.kommander-hard-state`) has the last term and vote per partition outside the log. It is append-only,
with one record and one `fsync` per hard-state write. It is replayed at open and compacted when large. At
open, a ledger entry ahead of the engine's (a higher term, or a vote in the same term that the engine
lacks) is written back before the Raft layer reads it. The ledger is appended after the engine's put
returned. It is therefore ahead only when the engine lost the put, and the quarantine below cannot lift
onto a forgotten vote.

### Quarantine release after a lost tail

The surviving log may be a *stale prefix*. The cut can also revert a leader's truncation, and the same ids
may then have entries from an older term. The restore records the surviving frontier and its term
(`RaftRestoreStorageVerdict.LogTermBar`). The partition votes and campaigns again once one of three facts
proves that its log is a leader's prefix.

- A leader has **replicated past the surviving frontier**.
- The frontier's **term rose above the bar**. This is what a leader with a shorter log than the stale one
  appends.
- The leader's **empty heartbeat confirmed the frontier**. Each empty heartbeat carries the leader's
  contiguous frontier in the Log Matching fields (`HeartbeatDriver`, `BackfillSender.AppendLogToNode`). A
  frontier equal to it is the leader's whole log. A quiet partition, such as the system partition, needs
  this condition.

The election gates open on the live condition. The tick records the reconciliation in the backend and
then reports `Healthy`, which means that a probe that reads `Healthy` can be acted on safely.

### Partition storage states

The restore returns a `RaftRestoreStorageVerdict`. The state machine turns it into a
`RaftPartitionStorageState` value, which `IRaft.GetPartitionStorageHealth` exposes for the host's
readiness probe.

| State | What happened | What the partition withholds | How it ends |
|---|---|---|---|
| `LostAcknowledgedEntries` | Watermark above the restored frontier, **or** the engine's lost-synced-writes verdict (the health reason says which) | Votes, pre-votes, candidacy, leadership transfers. Acks continue: they are truthful. | On its own, once the leader has replicated past the watermark or a snapshot covers it. For the engine's verdict, see [Quarantine release](#quarantine-release-after-a-lost-tail). A single-voter partition is leaderless until the operator restores the data or sets `LostAcknowledgedEntriesPolicy.AcceptAndLog`. This policy logs the loss at `Critical`, rewrites the watermark (or records the reconciliation), and is audited as a disabled safety fence at startup. |
| `ApplicationFloorBelowCompaction` | `IApplicationDurabilityProvider` gives a floor with `floor + 1 < compactionFloor`: the entries the application needs were compacted | Consumer replay and live applies, candidacy, transfers. It votes: its Raft log is complete. | The tick asks the leader for a whole-partition snapshot (`RequestReseedAsync`) and repeats the request until one installs. Any installed snapshot clears it. A provider floor of `-1` is "no opinion" and does not trigger this. A provider for an empty store must return `0`. |
| `EngineFailed` | The engine fail-stopped on a `Corruption` status | Everything: a leader steps down with a step-down notice, and the partition does not vote, campaign or acknowledge | Not in this process. The operator moves the damaged directory aside and the node rebuilds from its peers. |
| `RestoreFailed` | The WAL restore threw and the executor stopped the partition | Everything (the partition is stopped) | Operator. |

The gates are `RaftPartitionCoreState.StorageWithholdsCandidacy` and `StorageDeniesVote`, read by
`ElectionCoordinator` on each campaign entry and vote decision. A real vote is denied **after** the higher
term is adopted, which means that a deposed leader's appends are fenced.

A replica that withholds its candidacy still tells its peers that it has no leader. On each election timeout
that it yields, it sends a *leaderless notice*: a pre-vote probe that asks for no grant. A quiesced leader wakes
on the notice and sends the append that can release the quarantine. A quiesced follower that expects the
quarantined replica to lead stops waiting for it. See
[The leaderless notice](partition-quiescence-developer-guide.md#the-leaderless-notice).

### Fail-stop on `Corruption`

`RocksDbWAL` classifies a failure by the status prefix the C API exposes (`"Corruption:"`, the one place
error text is parsed) and **fail-stops**. `TryRecoverEngineAfterFailure` does not probe or reopen, since a
reopen would succeed and hit the damage again. All write-path entry points fail fast through
`AcquireEngineForWrite`. A `Corruption` seen on a read latches too. Reads of intact blocks are served.

An open that hits `Corruption` logs at `Critical` and throws. The node does not start on partial data.
`IWAL.GetFailStopReason` carries the engine's message, which names the damaged file. The state machine
polls it on its tick and moves the partition to `EngineFailed`.

### Operator signals

- `Critical` — `RocksDB WAL at '…' lost synced writes: the sync ledger '…' attests sequence A as durable, but the engine replayed only through sequence R …` (the engine, once per event)
- `Critical` — `Restore found the storage engine lost synced writes: the surviving log is contiguous through P at term T …` (each partition)
- `Warning` — `… restored the hard state of partition N from the hard-state ledger (term T, vote V) …`
- `Critical` — `Restore found acknowledged entries missing: … acknowledged ids through A … contiguous only through P …`
- `Critical` — `The application's durable floor F is below the WAL compaction floor C …`
- `Critical` — `RocksDB WAL at '…' fail-stopped on a storage corruption …` / `… refused to open …`
- `Warning`, at most once per 30 s per partition — `Storage quarantine holds this replica's candidacy …` /
  `… denies vote to …`
- `Information` — `Lost synced writes repaired: a leader reconciled the log …` / `Lost acknowledged entries repaired …` / `Application floor repaired …`

| Metric | Counts |
|---|---|
| `raft.wal.lost_synced_writes_total` | Engine opens that found the ledger ahead of the replay |
| `raft.storage.lost_acknowledged_entries_total{outcome=quarantined\|accepted}` | Both verdicts |
| `raft.storage.application_floor_below_compaction_total` | Application floors below the compaction floor |
| `raft.storage.quarantine_transitions_total{state,direction}` | Quarantine transitions |
| `raft.wal.engine_fail_stops_total` | Engine fail-stops |

---

## Observability

`FairWalScheduler` exposes these counters.

| Property | Meaning |
|---|---|
| `TotalBatchesWritten` | Number of `walAdapter.Write` calls. The fast path does not change it. |
| `TotalSyncBatchesWritten` | Number of those calls that synced. Equals `TotalBatchesWritten` with the fast path off. Drops toward ~1× per committed write with it on. |
| `TotalPartitionsBatched` | Sum of partitions across all batches. Divided by `TotalBatchesWritten` it gives the mean partitions per `fsync`, the batch density. |
| `TotalLingerWaits` | Timed linger waits entered. Must be `0` under closed-loop / sequential load. |
| `TotalLingerPartitionsGathered` | Partitions added to a batch by a linger wait. Divided by `TotalLingerWaits` it gives the linger's hit rate. |

To check the fast path, compare `TotalSyncBatchesWritten ÷ committed-writes` with the flag off (~2) and on
(~1). A low linger hit rate means that the linger is probably not worth its window on that workload.

---

## Code map

```
Kommander/
├─ RaftConfiguration.cs            WalSingleFsyncCommit, WalGroupCommitLingerMs,
│                                   MaxWalGroupBatchPartitions, MaxWalBatchSize, WriteIOThreads
├─ Consensus/ReplicationAckProcessor.cs
│     └─ CompleteAppendLogsAsync        propose-quorum handling; follower re-supply on a regressed frontier
├─ Consensus/ProposalRegistry.cs
│     └─ TryReleaseTicketOnQuorumDurable release the ticket before the commit sync
├─ Consensus/WalCompletionRouter.cs
│     └─ CompleteLeaderCommit           the (still-running) commit marker + broadcast
├─ RaftWriteAhead.cs               EnqueuePropose / EnqueueCommit (the two phases);
│     └─ CompleteRestoreAsync           frontier reconstruction on restart
├─ WAL/IO/FairWalScheduler.cs      group commit, the deferred linger, sync vs sync-off decision,
│                                   TotalBatchesWritten / TotalSyncBatchesWritten / TotalPartitionsBatched
└─ WAL/
      ├─ IWAL.cs                    Write(logs)  and  Write(logs, sync)  (the sync-off primitive)
      ├─ RocksDbWAL.cs              SetSync(true/false); engine recovery after a latched storage error
│                                   (engineGuard, TryRecoverEngineAfterFailure, ReopenEngine);
│                                   WriteAttested + BindSyncLedger (the lost-synced-writes verdict),
│                                   PersistHardState + RestoreHardStateFromLedger
      ├─ RocksDbSyncLedger.cs       the two-slot attestation of the last synced sequence, outside the log
      ├─ RocksDbHardStateLedger.cs  the append-only term/vote ledger, outside the log
      ├─ SqliteWAL.cs               PRAGMA synchronous FULL / OFF toggle
      └─ InMemoryWAL.cs             no fsync — the non-durable control

Kommander.Tests/
├─ WAL/FsyncCountTests.cs                      syncs/write = 2× off, 1× on (RocksDB + SQLite)
├─ WAL/SingleFsyncCommitTests.cs              fast-path = baseline end-to-end; single-voter; concurrency
├─ WAL/RecoveryReconstructionCrashMatrixTests.cs  recovers exactly the contiguous prefix, keeps the tail
├─ WAL/RecoveryReSupplyClusterTests.cs        a regressed follower re-converges via leader re-supply
├─ WAL/TestRocksDbWalEnospcRecovery.cs        full-disk episode on a real small volume: writes recover
│                                              in-process, a cluster elects again after space returns
├─ WAL/TestRocksDbSyncLedger.cs               real file damage against the ledgers: truncated, cut, flushed,
│                                              deleted live logs; the forgotten vote; the counterweights
├─ WAL/TestStorageVerdictRestore.cs           a single voter on a truncated live log: quarantined across
│                                              restarts, accepted by policy, healthy after
├─ WAL/TestLostAcknowledgedEntriesCluster.cs  a follower's live log cut on disk, repaired by the leader
└─ Scheduler/TestFairWalScheduler.cs          group commit + linger behaviour
```

---

## Invariants you must not break

1. **Never ack before quorum durability.** The fast path acks when the propose quorum is durable, and not
   earlier.
2. **`CommittedCheckpoint` is always sync.** It is the recovery anchor. The lazy path applies to per-entry
   `Committed` markers of rows that are already present. It does not apply to checkpoints, proposed
   entries or rollbacks.
3. **Preserve the durable `Proposed` tail on restart.** `proposeIndex = maxLogId + 1`. A later propose
   must not reuse ids that an acknowledged entry with a lost marker occupies.
4. **Recover only the *contiguous* committed prefix.** Recovery must stop the committed prefix at the gap
   that a lost marker leaves. Re-supply or re-commit fills the rest.
5. **Flag-off means always sync.** With `WalSingleFsyncCommit = false` all WAL batches sync, and the client
   ack waits for the commit sync. Recovery is identical in both modes.

---

## Testing

- **Sync count (deterministic)** — `FsyncCountTests` asserts that `TotalSyncBatchesWritten` is exactly
  `2×W` with the flag off and `1×W` with it on, on RocksDB and SQLite. `TotalBatchesWritten` is `2×W` in
  both cases.
- **End-to-end equivalence** — `SingleFsyncCommitTests` asserts that an `autoCommit` write commits the same
  with the flag on and off. It includes the single-voter path and a 50-way concurrent burst (unique,
  contiguous indices under out-of-order quorum completion).
- **Recovery** — `RecoveryReconstructionCrashMatrixTests` builds the on-disk shape of a lost marker and
  asserts that recovery restores the contiguous committed prefix and puts `proposeIndex` past the durable
  tail. `RecoveryReSupplyClusterTests` crashes a follower with demoted markers and asserts that leader
  re-supply brings it to the full committed log.
- **Scheduler** — `TestFairWalScheduler` covers opportunistic and deferred (linger) group commit and the
  adaptive bail.

Do not run more than one `dotnet test` command at a time. The suite starts in-process Raft clusters, and
concurrent runs give false failures. Use a targeted `--filter` while you iterate, then run the full
project for shared scheduling/WAL/cluster changes.

```sh
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~FsyncCount
```

---

## Glossary

- **Propose-quorum-durable** — a quorum has the entry on disk, which is the Raft commit point.
- **Commit marker** — the in-place `Proposed → Committed` type update for an entry.
- **Lazy / sync-off write** — a WAL write that does not force an `fsync` and rides the next sync.
- **`CommittedCheckpoint`** — the always-durable recovery anchor. It certifies the prefix below it.
- **Commit frontier** — the highest contiguous committed id a node knows (`commitIndex`).
- **Re-supply** — the leader re-ships the committed tail to a follower whose frontier regressed.
- **Sync ledger / hard-state ledger** — the two files beside a RocksDB engine that record the last synced
  sequence and the last term and vote per partition, outside the engine's log.

---

*For the broader system, see the [Architecture Overview](architecture-overview.md). For how a lagging
follower is caught up, see the [Log Catch-Up & Backfill Developer Guide](log-backfill-developer-guide.md).*
