# WAL Write Durability & the Single-Fsync Commit Fast Path — Developer Guide

This guide explains how Kommander makes a write **durable** on the persistent WAL backends (RocksDB,
SQLite), why a committed write costs **two** disk syncs by default, and the two knobs that reduce that
cost: **group commit** (a throughput lever) and the **single-fsync commit fast path** (a latency lever).
It is written for two readers — an operator deciding whether to turn these on and how to tune them, and a
developer who wants to understand or extend the write path. No prior Raft expertise is assumed; we build
the idea up first, then walk the real flows, the configuration, and the code.

---

## Table of contents

1. [Summary](#summary)
2. [Why a committed write costs two fsyncs](#why-a-committed-write-costs-two-fsyncs)
3. [Lever 1 — Group commit (throughput)](#lever-1--group-commit-throughput)
4. [Lever 2 — The single-fsync fast path (latency)](#lever-2--the-single-fsync-fast-path-latency)
5. [Flow 1 — A committed write, by default](#flow-1--a-committed-write-by-default)
6. [Flow 2 — Group commit amortizes fsyncs across writes](#flow-2--group-commit-amortizes-fsyncs-across-writes)
7. [Flow 3 — The fast path: ack on propose-quorum-durable](#flow-3--the-fast-path-ack-on-propose-quorum-durable)
8. [Flow 4 — The lazy commit marker](#flow-4--the-lazy-commit-marker)
9. [Flow 5 — Crash and recovery](#flow-5--crash-and-recovery)
10. [Why the fast path is safe](#why-the-fast-path-is-safe)
11. [Configuration](#configuration)
12. [Full disk (ENOSPC) and how the WAL recovers](#full-disk-enospc-and-how-the-wal-recovers)
13. [Storage corruption and lost writes](#storage-corruption-and-lost-writes)
14. [Observability](#observability)
15. [Code map](#code-map)
16. [Invariants you must not break](#invariants-you-must-not-break)
17. [Testing](#testing)
18. [Glossary](#glossary)

---

## Summary

A Raft write is durable when a **quorum of nodes has it on disk**. Kommander commits in two phases —
**propose** then **commit** — and on a durable backend each phase issues its own `fsync`:

```
   client write ──► leader PROPOSE  (fsync #1)  ──► replicate, wait for quorum
                 ──► leader COMMIT   (fsync #2)  ──► ack client
```

Two `fsync`s sit on the critical path of every write, one after the other. On an SSD where a single
`fsync` is tens of milliseconds, that dominates write latency.

There are two independent ways to spend less time syncing:

- **Group commit** packs *many concurrent writes* into *one* `fsync`. It raises **throughput** but does
  not shorten any single write's path (each write still waits for its own two syncs). Always on; tunable
  via `WalGroupCommitLingerMs`.
- **The single-fsync fast path** (`WalSingleFsyncCommit`) removes the **second** `fsync` from a write's
  critical path: it acknowledges the client the moment a **quorum holds the entry durably** (the true
  Raft commit point), and writes the per-entry commit marker *lazily* so it rides a later sync. This
  lowers **latency** and roughly halves syncs-per-write. Off by default.

```
   default:    PROPOSE(fsync) ──► quorum ──► COMMIT(fsync) ──► ack     (2 fsyncs on the path)
   fast path:  PROPOSE(fsync) ──► quorum ──► ack ; commit marker rides next sync   (1)
```

---

## Why a committed write costs two fsyncs

A committed write traverses Raft's two phases, and the persistent WAL syncs on each:

- **Propose.** The leader appends the entry as `Proposed` and `fsync`s it, then replicates it. Followers
  append it durably and acknowledge. When a quorum has acknowledged, the entry is *committed* in the
  Raft sense — a quorum holds it on disk.
- **Commit.** The leader then writes a per-entry `Committed` marker (an in-place update of the same row,
  flipping its type `Proposed → Committed`) and `fsync`s *that*, before acknowledging the client.

The two syncs of one write can never coalesce with **each other**: a partition is single-owner and the
commit phase isn't even created until the propose reaches quorum, a network round-trip later. So
`propose-fsync → quorum → commit-fsync` is strictly serial. Two serial syncs ≈ the bulk of write
latency on a durable backend.

> The second `fsync` is redundant *for durability* — the entry is already quorum-durable after propose.
> The commit marker only lets the WAL describe its own committed prefix on restart without
> re-derivation. That observation is what the fast path exploits.

This only applies when the backend syncs at all: `RocksDbWAL` and `SqliteWAL` are constructed with
`syncWrites: true` by default (RocksDB `WriteOptions.SetSync(true)`, SQLite `PRAGMA synchronous=FULL`).
`InMemoryWAL` never syncs, so none of this affects it.

---

## Lever 1 — Group commit (throughput)

`FairWalScheduler` already coalesces across partitions: when several partitions have writes ready, one
worker drains up to `MaxWalGroupBatchPartitions` (default 64) of them and issues **one** `walAdapter.Write`
— which is **one `fsync`** on RocksDB regardless of how many partitions or phases it spans. A single
batch can even mix one partition's propose with another's commit into the same sync. This is why
throughput stays high under concurrency.

By default this batching is **opportunistic**: a worker syncs whatever happens to be ready the instant it
wakes. Where ready work *trickles in* rather than arriving all at once — notably the follower append
path, paced by replication RPCs — opportunistic batching leaves each arrival to force a near-solo sync.

`WalGroupCommitLingerMs` turns on **deferred** group commit: after taking the first ready partition, the
worker waits a bounded, **adaptive** interval to let more partitions become ready and share the sync. It
bails the instant a probe finds nothing newly ready, so sequential / low-overlap load pays at most one
short probe — never the full window. This raises batch density (and tightens the latency tail) at
near-zero cost, but it is still a *throughput* lever: it cannot shorten a write already blocked on two
serial syncs of its own.

---

## Lever 2 — The single-fsync fast path (latency)

`WalSingleFsyncCommit` removes one `fsync` from the **critical path**, which is what write-latency
percentiles reflect. It has two parts, applied symmetrically on the leader and followers:

1. **Acknowledge on propose-quorum-durable.** When a quorum has the entry on disk, the entry is
   committed — so for the common `autoCommit` single-round write, release the client *then*, instead of
   waiting for the leader's own second (commit) sync.
2. **Write the commit marker lazily.** Still write the per-entry `Committed` marker (it makes restart
   cheap), but **without its own `fsync`** — it rides the next durable write. This removes the second
   sync from per-write load on every node, leader and follower alike.

Scope is strictly the `autoCommit` single-round path. The explicit two-phase (`!AutoCommit`) path keeps
its separate durable commit, untouched. The flag is **off by default**; with it off the write path is
byte-for-byte the prior always-sync behaviour.

In a 3-node loopback RocksDB benchmark (mixed workload, concurrency 64), enabling the flag dropped set
p50 by roughly 18% and cut syncs-per-committed-write from ~2 to ~1, at unchanged durability. Your numbers
will vary with disk `fsync` latency and write concurrency.

---

## Flow 1 — A committed write, by default

```
  leader: append Proposed(id=N) ─ fsync ─► replicate to followers
  followers: append Proposed(id=N) ─ fsync ─► ack
  leader: quorum reached ─► write Committed(id=N) ─ fsync ─► ack client
```

Two syncs on the leader's path (propose, commit); followers also sync twice per write (the replicated
propose and the replicated commit). `RaftWriteAhead` enqueues the propose and commit as **separate**
`WALWriteOperation`s; `FairWalScheduler` carries each to the backend.

---

## Flow 2 — Group commit amortizes fsyncs across writes

```
  partitions ready:  P1(commit)  P2(propose)  P3(commit)  P4(propose)  …
                          └──────────── one walAdapter.Write ───────────┘
                                         = ONE fsync (RocksDB)
```

The worker blocks for the first ready partition, then sweeps up the rest (opportunistically, or — with
`WalGroupCommitLingerMs > 0` — after a brief evidence-gated linger to gather more), and issues a single write
spanning all of them. `MaxWalGroupBatchPartitions` caps the partitions per batch; `MaxWalBatchSize` caps
the operations drained per partition. More concurrency ⇒ denser batches ⇒ fewer syncs per write.

---

## Flow 3 — The fast path: ack on propose-quorum-durable

With `WalSingleFsyncCommit` on, when the propose quorum lands for an `autoCommit` proposal the leader
advances its in-memory commit frontier and releases the client ticket **before** enqueuing the commit
marker:

```
  propose quorum reached
     ├─► advance localCommittedIndex to this entry      (in memory)
     ├─► mark proposal Committed ─► client ticket releases   ← ack happens here
     └─► enqueue the Committed marker write (lazy — Flow 4)
```

A single-voter leader is its own quorum, so the propose sync that just completed already satisfies this;
the fast path applies there too. The client now waits for **one** sync (propose) plus the quorum
round-trip, not two.

### The leader's propose sync overlaps the followers'

With `FanOutBeforeLocalWrite` on (the default), the leader sends a proposal to its followers as soon as its
own `Proposed` write is **queued**, not once it is durable. The leader's sync and the followers' syncs run
at the same time instead of one after the other:

```
   serial:      leader PROPOSE(fsync) ──► send ──► follower APPEND(fsync) ──► ack ──► quorum
   overlapped:  leader PROPOSE(fsync) ─────────────────────────────┐
                send ──► follower APPEND(fsync) ──► ack ───────────┴──► quorum
```

The quorum still counts the leader only once its own write is durable (`RaftProposalQuorum.LocalDurable`):
two follower acks are not enough while the leader's copy is still queued. So everything that runs at quorum
(the commit marker, the leader's applies, backfill reads up to the commit index) still finds the batch on
the leader's disk. The one visible difference is a failed leader write. The followers may already hold the
batch and a later leader can commit it, so the caller gets `ProposalOutcomeUnknown`, not the storage error.
The promotion barrier keeps the serial order.

### The follower acks before it applies

With `FollowerApplyInOwnTurn` on (the default), a follower acknowledges an append before it delivers the
entries that append committed to the application (`OnReplicationReceived`). It delivers them afterwards,
in executor turns of their own (`FollowerApplyLane`), at most `FollowerApplyTurnTime` (100 µs) per turn,
in the maintenance class. The next proposal's append is in the replication class, so the executor runs it
before the next apply turn. That append used to wait for every consumer callback of the previous commit:

```
   before:  [commit k completion: apply 1..N ──► ack] [append k+1 ...]
   after:   [commit k completion: ack] [append k+1] [apply turn] [apply turn] ...
```

What an ack means does not change: it never carried the applied position. Delivery is still exactly-once
and in log order, through `LogApplicator.ApplyLogToConsumerAsync`, so the applied cursor still advances
only over delivered entries, and it completes the read-index and `ConfirmLocalApplicationAsync` waiters.
The cursor can trail the commit frontier by the entries queued for a turn. Every other delivery path
reads the WAL from that cursor: the tick's retry, promotion, snapshot install and the apply hold. The lane
holds its entries in memory, so a compaction below the checkpoint cannot make it step over them. The turn bound is in time
because what it protects is the append's wait: an entry bound cost a cheap consumer a turn per few dozen
entries. A turn also delivers any backlog above eight entry caps (`FollowerApplyTurnBudget`), so a
consumer slower than the commit rate still slows the follower. The system partition always delivers inline.

---

## Flow 4 — The lazy commit marker

The per-entry `Committed` marker is written with `sync = false` so it does not force its own `fsync`:

- **RocksDB** writes it with `SetSync(false)`. A later sync write `fsync`s the shared WAL up to that
  point, making the marker durable.
- **SQLite** flips `PRAGMA synchronous` to `OFF` for that transaction (under the shard lock, restored to
  `FULL` afterward). The next `FULL` commit `fsync`s the `-wal`, flushing the prior frames.

`FairWalScheduler` decides per batch: it writes a batch sync-off **only if every log in it is a per-entry
`Committed` marker**. Any proposed entry, any `CommittedCheckpoint` (the durable recovery anchor), any
rollback, or any mixed batch forces a **sync** write — which also durably flushes any markers riding
alongside it.

```
  batch = all Committed markers      ─► sync-off (rides the next sync)
  batch = has a Proposed / checkpoint ─► sync  (and flushes pending markers)
```

The number of `walAdapter.Write` *calls* is unchanged (still two per write); only how many of them
`fsync` drops — from ~2 to ~1 per committed write.

---

## Flow 5 — Crash and recovery

Because the marker is lazy, a crash can leave a **durable `Proposed` prefix whose commit markers were
lost**. The on-disk type is therefore no longer a reliable committed/uncommitted boundary, so
`RaftWriteAhead.CompleteRestoreAsync` reconstructs the frontier conservatively when the flag is on:

- `commitIndex = (highest contiguous committed prefix) + 1` — the **safe lower bound**. A gap from a lost
  marker stops the prefix; a durable `CommittedCheckpoint` jumps it (it certifies the whole prefix below
  it).
- `proposeIndex = maxLogId + 1` — the durable `Proposed` tail is **preserved**, never reused or
  overwritten, so an acked-but-lazily-committed entry can't be lost, and an unacknowledged entry is never
  promoted to committed.

The true frontier above that lower bound is then re-supplied:

- A restarted **follower** advertises its real commit frontier on heartbeat acks. The leader detects the
  regression (which its monotonic `matchIndex` can't see) and re-ships the still-committed tail via the
  existing backfill path. The re-shipped `Committed` copies upsert the entries already present as
  `Proposed` and apply them — idempotent.

  **`BackfillThreshold` does not gate this.** The threshold encodes "a small gap rides on normal
  replication", which is true of a follower that is merely behind and false of a regressed one: its
  missing entries are already-committed history, so no future propose broadcast carries them, and the
  idle-tail trigger in `SendHeartbeat` deliberately refuses to push merely-restored committed state
  (the `liveCommitFloor` guard). A sub-threshold regression left to the threshold would only be repaired
  when unrelated writes happened to push the gap past it — never, on an idle cluster. A commit frontier
  cannot legitimately move backwards, so the signature is unambiguous and is repaired at any gap size.
  The batch is anchored to the frontier the follower just reported, not to `nextIndex` — the latter
  tracks the monotonic `matchIndex` and still points above exactly the range that regressed.
- A restarted node that **wins election** re-commits the durable `Proposed` prefix once a current-term
  entry commits (standard Raft leader completeness).

```
  on disk after crash:  [Committed 1..K][Proposed K+1..N]   (markers K+1..N lost)
  recovery:             commitIndex = K+1   proposeIndex = N+1   (tail kept, not promoted)
  leader re-supply:     re-ships Committed K+1..N  ─► follower converges to N
```

---

## Why the fast path is safe

The load-bearing invariant is unchanged: **a write acknowledged to the client is durable on a quorum, and
crash recovery yields the same committed prefix.** The fast path preserves it because:

- **Ack timing.** A follower acknowledges a propose only *after* its own WAL write is durable. So when the
  leader observes a propose quorum, a quorum already holds the entry on disk — exactly Raft's commit
  condition. Acking there does not ack before durability; it stops waiting for the *leader's second*
  sync, not for quorum durability.
- **Lazy marker.** Losing a marker on a crash only demotes an entry to `Proposed` on disk. That entry was
  quorum-durable from its propose sync, so it is reconstructible (Flow 5). No *acknowledged* data is lost.
- **The recovery anchor stays durable.** `CommittedCheckpoint` is always written sync, on both the
  checkpoint-commit and snapshot-install paths. It is never made lazy.
- **The proposed tail is preserved.** `proposeIndex = maxLogId + 1` guarantees a later propose cannot
  overwrite an acked-but-lazily-committed entry.
- **The overlapped fan-out waits for the leader's own sync.** With `FanOutBeforeLocalWrite`, followers may
  write a batch before the leader does, but the proposal reaches quorum only once the leader's write is
  durable too, so an ack still means a quorum that includes the leader holds the entry.

---

## Configuration

In `RaftConfiguration.cs`:

| Setting | Default | What it does |
|---|---|---|
| `WalSingleFsyncCommit` | `true` | The latency lever. Acks `autoCommit` writes on propose-quorum-durable and writes the commit marker lazily, removing one `fsync` from the critical path. Off ⇒ byte-for-byte the prior two-sync behaviour. |
| `FanOutBeforeLocalWrite` | `true` | Sends a proposal to the followers while the leader's own write is queued, so the leader's and followers' syncs overlap. The quorum still waits for the leader's write. Off ⇒ the followers get the batch only after the leader's write is durable (two syncs in series per round). |
| `FollowerApplyInOwnTurn` | `true` | A follower acks an append before it delivers the entries the append committed, and delivers them in executor turns of their own, so the next proposal's append does not wait for the consumer's callbacks. Off ⇒ the follower delivers inside the append's completion, before its ack. |
| `FollowerApplyTurnTime` | `100 µs` | How long one follower apply turn delivers before it yields to queued appends and acks: the most an append waits behind the application's callbacks. At least one entry per turn. `Zero` ⇒ only the entry cap. |
| `FollowerApplyTurnBudget` | `1024` | The entry cap of one follower apply turn. A backlog above eight caps is delivered anyway (backpressure). `0` ⇒ no cap. |
| `WalGroupCommitLingerMs` | `0` | The throughput/tail lever. `> 0` lets a WAL worker linger up to this many ms to gather more ready partitions into one `fsync`. Evidence-gated: a worker waits only while another partition has queued work that must become ready soon, so low-overlap and closed-loop load pay **zero** wait. `0` keeps purely opportunistic batching. |
| `MaxWalGroupBatchPartitions` | `64` | Max partitions coalesced into a single `walAdapter.Write` (one `fsync` on RocksDB). |
| `MaxWalBatchSize` | `256` | Max operations drained from one partition per write. |
| `WriteIOThreads` | `4` | WAL scheduler worker threads — how many writes can sync concurrently across partitions. |

Tuning notes:

- **`WalSingleFsyncCommit`** is **on by default**. It is what makes write latency acceptable on a durable
  backend (`syncWrites: true`), and it changes no on-disk format, so it can be turned off again at any
  time. Two operational notes:
  - **Set it uniformly across the cluster.** The regressed-frontier re-supply is a follower-reports /
    leader-reacts pair; with the flag off a follower's heartbeat ack reports `-1` instead of its real
    frontier, so a mixed cluster leaves that channel half-wired.
  - **Turn it off** if you need the disk alone to answer "what was committed" without help from a leader
    — e.g. forensic single-node recovery, or an environment where a restarted node may serve reads before
    it reconnects. On the fast path a restarted node's applied state is the *contiguous* committed prefix
    until re-supply lands (Flow 5).
- **`WalGroupCommitLingerMs`** helps most when writes spread across **many partitions** and arrive
  staggered (the follower append path). Start around `2` ms and measure; the win is denser batches and a
  tighter tail, not a lower median. A larger value bounds the linger; the evidence gate keeps idle,
  sequential and closed-loop load from paying any of it (a worker waits only while another partition has
  queued work that must become ready soon — check `TotalLingerWaits` and
  `TotalLingerPartitionsGathered` to see the gate and its hit rate in action).
- The two levers are **complementary**: group commit reduces total syncs under load; the fast path
  removes a sync from each write's path. They can be enabled together.

---

## Full disk (ENOSPC) and how the WAL recovers

A data volume that fills is an operational event, not a crash. Kommander keeps the node alive and
makes it heal on its own when space returns. This section describes what happens on the RocksDB
backend, because RocksDB keeps error state of its own.

### What a full disk does to the node

1. **Writes fail as a status.** `RocksDbWAL.Write` returns `Errored`. The Raft layer regresses its
   in-memory presence and commit frontiers to the durable log for the failed range
   (`RaftWriteAhead.RegressFrontiersAfterFailedWriteAsync`), so the node never advertises entries it
   does not hold. Nothing acknowledged is lost: an acknowledged entry is fsynced on a quorum.
2. **Elections pause.** The term and the vote are persisted through `IWAL.PersistHardState`, which
   returns `false` on a full disk instead of throwing. An election attempt that cannot record its
   term and self-vote is abandoned before any `RequestVote` is sent, and the node drops back to
   `Follower`. A vote that cannot be recorded is withheld. Both are retried on the normal election
   cadence. A vote the node could forget across a crash is the double-vote hazard hard state exists
   to prevent, so "no durable vote, no vote" is the safe choice.
3. **The WAL logs once, then every 30 seconds** while writes keep failing, with the duration of the
   episode and the last error.

### Why RocksDB does not always heal on its own

RocksDB keeps one background-error status per engine. Two shapes matter:

| Failure | RocksDB error text | RocksDB severity | Self-heals? |
|---|---|---|---|
| Append to the current WAL file fails | `While appending to file: …/NNNNNN.log` | Hard error | **Yes.** The engine's built-in `SstFileManager` polls free space and resumes the engine about 5 s after space returns. |
| Creation of a fresh WAL file fails (the memtable switch every flush performs) | `While open a file for appending: …/NNNNNN.log` | Unrecoverable | **No.** Automatic recovery never runs; a manual resume is refused. Every later write returns the cached status without touching the disk. |

The second shape is the end state of any sustained full-disk episode: a memtable fills under ordinary
load, or the shared write-buffer manager asks for a flush, and the switch to a new log file fails.
This is what the field incident of 2026-09-01 showed: identical failures naming the same log file for
many minutes after 24 GiB had been freed, until a process restart.

### How `RocksDbWAL` recovers

After a write-path failure the WAL asks two questions, without parsing error strings:

1. **Does the filesystem accept writes now?** A probe file inside the engine directory is created,
   written (64 KiB), fsynced, and deleted. Rate-limited to one probe per second.
2. **Does the engine accept writes now?** A canary key is written to the metadata column family
   with a synced write.

"Filesystem yes, engine no" means the engine is latched. Only then is it closed and reopened on the
same directory with the same options. The reopen holds the exclusive side of an engine guard that
every operation takes on its read side, so no iterator or write can touch the handle being replaced.
The reopen replays the log files on disk: every acknowledged sync write is already fsynced, and
un-synced records were handed to the operating system on append, so a same-process reopen loses
nothing that was acknowledged. The failed batch itself was reported as `Errored` before, and the Raft
layer already regressed its frontiers for it.

The write-path entry point that observed the failure then retries its operation once. All of them are
idempotent upserts or deletes keyed by `(partition, id)`, so the retry is safe.

A still-full disk fails question 1, so nothing is reopened and the node keeps reporting `Errored`
until space returns. An engine that resumed on its own passes question 2 and is left alone. If the
reopen itself fails (the open needs space too), the WAL stays closed, logs at `Critical`, and the next
write-path failure retries the reopen.

### What an operator sees

- `Warning` — `RocksDB WAL at '…' rejected a write: …` on the first failure of an episode, then every
  30 seconds with the duration and count.
- `Warning` — `RocksDB WAL at '…' was reopened to clear a latched storage error (reopen #N)`.
- `Information` — `RocksDB WAL at '…' accepts writes again after N failures over D`.
- `Critical` — `RocksDB WAL at '…' could not be reopened after a storage failure`.
- Metric `raft.wal.engine_reopens_total`, tagged `outcome=success|failed`. Any non-zero value means
  the node went through a full-disk episode and healed without a restart.

The library never stops the host process on its own. A deployment that prefers to fail fast after a
bounded window should watch the metric and the `Warning` cadence and act from outside.

---

## Storage corruption and lost writes

A full disk is transient; damage is not. This section covers what the RocksDB backend and the Raft
layer do when the storage under them lies: a synced record that is gone after a restart, a block
whose checksum no longer matches, an application store that was wiped or restored from an older copy
under an intact WAL. The rule throughout is the one from protocol-aware recovery (Alagappan et al.,
FAST 2018): a replica that cannot prove it still holds what it promised must not vote or lead from
what it has, and must say so where an operator and the host's readiness probe can see it.

### The term and the vote are fsynced

`IWAL.PersistHardState` is written with the backend's sync write options (`RocksDbWAL.SetMetaDataCore`).
It used to ride the next log fsync, so a granted vote could reach the candidate and not the disk: after
a power loss the node would grant a second candidate the same term, and two leaders could commit
conflicting entries in it. Elections are rare, so one fsync per vote costs nothing measurable.

### What the engine already refuses

RocksDB verifies a checksum on every block it reads and every log record it replays, and the WAL opens
with `paranoid_checks`, `verify_sst_unique_id_in_manifest` and now `track_and_verify_wals_in_manifest`.
That last option records each **closed** write-ahead log's number and synced size in the MANIFEST: a
closed log that is missing or shorter than recorded fails the open instead of replaying short. The live
log is deliberately not tracked by RocksDB, which is the gap the sync ledger below closes.

Two corrections to common assumptions:

- `TolerateCorruptedTailRecords` tolerates a **truncated** tail only — a record header or body cut off
  by the end of the file, the residue of a torn write. A checksum mismatch anywhere in the file, the last
  record included, is reported as corruption and fails the open in this mode too. RocksDB's own default
  is `kPointInTimeRecovery`; the mode here is the one whose tolerance matches exactly what a crash can
  leave.
- An iterator that reaches a damaged block does **not** throw: it becomes invalid and parks the
  `Corruption` status on itself. A scan that only tested `Valid()` returned a short result as if the log
  ended there, so a restore could read an empty log from a damaged SST and treat it as a fresh
  partition. Every scan in `RocksDbWAL` now ends through `IteratorEnded`, which rethrows a parked error.

### The acknowledged-presence watermark

With `syncWrites` an acknowledged entry is fsynced, so the Raft layer knows exactly what it promised: the
contiguous presence frontier it reports in every ack. The watermark is the durable statement of that
promise, kept next to the rows, and it catches the shapes where rows go missing under a surviving
watermark: a damaged or restored shard, and the API-level fault injections the tests use.

Every log-carrying WAL operation therefore carries `WALWriteOperation.AcknowledgedPresence`, the
frontier the partition will have once that batch and everything queued before it have landed
(`RaftWriteAhead.ProjectPresenceAfter`, a conservative dry run of the live frontier advance). The
scheduler folds it per partition in submission order and hands it to
`IWAL.Write(logs, sync, acknowledgedPresence)`; `RocksDbWAL` stages it into the **same `WriteBatch`**
as the rows, under the key `raft_acked_presence_p{partition}`. It costs no fsync of its own, and "the
rows are durable" and "this node claimed them" become true in one atomic step.

Three rules keep it honest:

- It is **assigned**, never maximised. Within a partition's FIFO it only rises — except after a failed
  write, when `RegressFrontiersAfterFailedWriteAsync` queues a `WALWriteOperationType.AcknowledgedPresence`
  correction that lands behind any operation that still carried the optimistic frontier.
- Every path that removes rows **clamps** it in the same batch: `TruncateLogsAfter` and the proposed-only
  sweep to the cut, a truncating snapshot install to the boundary (a retained one raises it to the
  boundary), `DeletePartitionWAL` removes it.
- The projection never counts an id buffered over a gap, so the watermark never exceeds what the node
  really holds once the batch lands. A low watermark narrows what the check can detect; a high one would
  report a loss that never happened, and that is the error the design rules out.

On restore, after every frontier seed (checkpoint, compaction floor, soft floor), `CompleteRestoreAsync`
compares the watermark with the reconstructed contiguous frontier. A watermark above it means rows this
node promised — and the leader may have counted toward a commit — are gone.

**What the watermark cannot see.** Every column family of a RocksDB engine shares one log file, and the
watermark lands in the same batch as the rows. A live log truncated **after** its fsync — the one shape
`TolerateCorruptedTailRecords` cannot tell from a torn write — therefore removes the watermark together
with the rows it covers, and the surviving watermark always matches the surviving rows. The storage
review of 2026-11-04 proved this at the WAL level (every truncation of the live log reopened "clean")
and in a five-node cluster: the victim reported `Healthy` on four partitions that had each lost 24 to 44
acknowledged entries, the same cut removed its term and vote, it granted a second vote in a term it had
already led, and two leaders committed different values at 146 indices. The two ledgers below are the
answer: a proof that must survive the loss of the log cannot live inside the log.

### The sync ledger: a truncated live log

`RocksDbSyncLedger` is a two-slot, 1 KiB file beside the engine (`.kommander-sync-ledger`) that
attests, **outside the log**, the highest RocksDB sequence number a synchronous write of this process
completed. After every synchronous engine write — rows, watermark, hard state, frontier, floor, every
one goes through `RocksDbWAL.WriteAttested` — the sequence of that batch is read from the batch's own
header (RocksDB fills it in during the write; the engine's *latest* sequence is never used, because a
concurrent unsynced write may have raised it past what this fsync covered), written to the other slot,
and fsynced before the write is reported successful. Concurrent writers group-commit the ledger: the
first one through the gate writes the highest pending sequence for everybody.

At open, the ledger is compared with the sequence the engine replayed. The ledger can never be ahead
of what the engine fsynced (it is written after), so a ledger ahead of the replay is proof that synced
writes are gone: a log file truncated or lost after its fsync. The engine then **records a verdict**
(metadata key `raft_lost_synced_writes`, a monotonic id plus the reason naming both sequences), resets
the ledger to the engine, and **keeps running**: its files are consistent, only stale, and the repair is
replication from a leader — which is why this is a quarantine and not the `Corruption` fail-stop. Every
partition on the engine is suspect (the lost tail may have held any partition's rows, markers or vote),
so `IWAL.GetLostSyncedWritesReason(partition)` answers the reason for each one until that partition
records its reconciliation (`MarkLostSyncedWritesReconciled`, metadata key
`raft_reconciled_lost_synced_writes_p{partition}`). A crash before the reconciliation quarantines again;
a later event carries a higher id and quarantines partitions that reconciled the earlier one.

The ledger carries the engine's identity (`raft_engine_identity`), so a ledger copied from another
directory is reset, not believed; a torn slot is skipped in favour of the other; both slots unreadable
(not a crash shape) is treated as no ledger, with a warning. A directory from before the ledger is
upgraded at its next open. A torn **unsynced** record — the residue of a real kill — was never attested
and reopens healthy. With `syncWrites` off nothing is attested, because nothing is promised.

Cost: one extra device round trip per synchronous engine write (an in-place sector write plus
`fdatasync`, grouped across concurrent writers), on top of the engine's own fsync. The ledger uses the
same primitive as the engine's log on each platform, not the full-device flush `FileStream.Flush(true)`
issues on macOS.

### The hard-state ledger: a forgotten vote

`IWAL.PersistHardState` is a synchronous metadata put, so a granted vote is on disk before the reply
leaves — in the same log as the rows. The same truncation removes it, and the node restarts with an
older term and no vote. `RocksDbHardStateLedger` (`.kommander-hard-state`, append-only, one record and
one fsync per hard-state write, replayed at open, compacted when large) keeps the last term and vote per
partition outside the log. At open, a partition whose ledger entry is ahead of the engine's — a higher
term, or a vote in the same term the engine lacks — gets it written back before the Raft layer reads it.
The ledger is appended only after the engine's put returned, so it is ahead only when the engine lost
the put; the quarantine below can therefore never lift onto a forgotten vote.

### How the quarantine lifts after a lost tail

The surviving log may be a *stale prefix*: the cut can revert a leader's truncation as well as remove
appended rows, so the same ids may hold entries from an older term, and there is no watermark to reach.
The restore records the surviving frontier and its term (`RaftRestoreStorageVerdict.LogTermBar`), and
the partition votes and campaigns again once any of three facts proves its log is a leader's prefix:

- a leader has **replicated past the surviving frontier** (the consistency check at the frontier passed,
  or the stale tail was truncated and replaced on the way);
- the frontier's **term rose above the bar** — a leader's own entry at a newer term, which is what a
  leader with a shorter log than the stale one appends;
- the leader's **empty heartbeat confirmed the frontier as its own**: every empty heartbeat carries the
  leader's contiguous frontier in the Log Matching fields (`HeartbeatDriver` reads its tracked frontier,
  no WAL access; `BackfillSender.AppendLogToNode` puts it on the entry-less request), and a frontier
  equal to it is, by the Log Matching Property, the leader's whole log. This is the condition a quiet partition needs —
  the system partition, most of the time — which would otherwise stay quarantined until its next entry.

The election gates open on the live condition; the tick records the reconciliation in the backend and
only then reports `Healthy`, so a probe that reads `Healthy` can be acted on safely.

### Quarantine, not silence

The restore reports a `RaftRestoreStorageVerdict`; the state machine turns it into one of the
`RaftPartitionStorageState` values, which `IRaft.GetPartitionStorageHealth` exposes for the host's
readiness probe:

| State | What happened | What the partition withholds | How it ends |
|---|---|---|---|
| `LostAcknowledgedEntries` | Watermark above the restored frontier, **or** the engine's lost-synced-writes verdict (the health reason says which) | Votes, pre-votes, candidacy, leadership transfers. Acks continue: they are truthful. | On its own, once the leader has replicated past the watermark (or a snapshot covers it); for the engine's verdict, once a leader has replicated past the surviving frontier, appended an entry from a newer term, or confirmed the frontier through an empty heartbeat, after which the partition records the reconciliation. A single-voter partition stays leaderless until the operator restores the data or sets `LostAcknowledgedEntriesPolicy.AcceptAndLog`, which logs the loss at `Critical`, rewrites the watermark (or records the reconciliation), and is audited as a disabled safety fence at startup. |
| `ApplicationFloorBelowCompaction` | `IApplicationDurabilityProvider` reports a floor with `floor + 1 < compactionFloor`: the entries the application still needs were compacted (its store was wiped, restored, or rolled back) | Consumer replay and live applies (a suffix applied onto a store without its prefix is a corrupt store), candidacy, transfers. It keeps voting: its Raft log is complete. | The tick asks the leader for a whole-partition snapshot (`RequestReseedAsync`) and repeats the request until one installs; any installed snapshot clears it. A provider floor of `-1` is "no opinion" by contract and does not trigger this; a provider for an empty store must report `0`. |
| `EngineFailed` | The engine fail-stopped on a `Corruption` status | Everything: a leader steps down with a step-down notice, and the partition neither votes, campaigns nor acknowledges | Never, for this process. The operator moves the damaged directory aside and the node rebuilds from its peers. |
| `RestoreFailed` | The WAL restore threw and the executor stopped the partition | Everything (the partition is not running) | Operator. |

The gates live in `RaftPartitionCoreState.StorageWithholdsCandidacy` and `StorageDeniesVote`, read by
`ElectionCoordinator` on every campaign entry and vote decision; a real vote is denied **after** the
higher term is adopted, so a deposed leader's appends stay fenced.

### Fail-stop on `Corruption`

The ENOSPC recovery used to reopen the engine on any `RocksDbException`. A `Corruption` status — a
compaction that read a damaged SST, a probe that hit a bad block — latched the same way, the reopen
succeeded (the open verifies sizes and identities, not every block), the next compaction hit the damage
again, and the WAL looped every probe interval with only `Warning` lines.

`RocksDbWAL` now classifies the failure by the status prefix the C API exposes (`"Corruption:"`, the
one place error text is parsed) and **fail-stops**: `TryRecoverEngineAfterFailure` neither probes nor
reopens, every write-path entry point fails fast through `AcquireEngineForWrite`, a `Corruption` seen on
a read latches too, and reads of intact blocks keep serving (every block RocksDB returns is verified).
An open that fails on `Corruption` logs at `Critical` and throws: the node does not start on partial
data. `IWAL.GetFailStopReason` carries the engine's message, which names the damaged file; the state
machine polls it on its tick and moves the partition to `EngineFailed`.

### What an operator sees

- `Critical` — `RocksDB WAL at '…' lost synced writes: the sync ledger '…' attests sequence A as durable, but the engine replayed only through sequence R …` (the engine, once per event)
- `Critical` — `Restore found the storage engine lost synced writes: the surviving log is contiguous through P at term T …` (each partition)
- `Warning` — `… restored the hard state of partition N from the hard-state ledger (term T, vote V) …`
- `Critical` — `Restore found acknowledged entries missing: … acknowledged ids through A … contiguous only through P …`
- `Critical` — `The application's durable floor F is below the WAL compaction floor C …`
- `Critical` — `RocksDB WAL at '…' fail-stopped on a storage corruption …` / `… refused to open …`
- `Warning`, at most every 30 s per partition — `Storage quarantine holds this replica's candidacy …` /
  `… denies vote to …`
- `Information` — `Lost synced writes repaired: a leader reconciled the log …` / `Lost acknowledged entries repaired …` / `Application floor repaired …`
- Metrics: `raft.wal.lost_synced_writes_total` (engine opens that found the ledger ahead of the replay),
  `raft.storage.lost_acknowledged_entries_total{outcome=quarantined|accepted}` (both verdicts),
  `raft.storage.application_floor_below_compaction_total`,
  `raft.storage.quarantine_transitions_total{state,direction}`, `raft.wal.engine_fail_stops_total`.

---

## Observability

`FairWalScheduler` exposes counters that make the levers measurable:

| Property | Meaning |
|---|---|
| `TotalBatchesWritten` | Number of `walAdapter.Write` calls — the batching/Write-call count. **Unchanged** by the fast path. |
| `TotalSyncBatchesWritten` | Number of those calls that actually `fsync`ed — the **true sync count**. Equals `TotalBatchesWritten` with the fast path off; drops toward ~1× per committed write with it on. |
| `TotalPartitionsBatched` | Sum of partitions across all batches; divided by `TotalBatchesWritten` gives **mean partitions per `fsync`** — the batch density the linger raises. |
| `TotalLingerWaits` | Timed linger waits entered (the evidence gate admitted a wait). Must stay `0` under closed-loop / sequential load. |
| `TotalLingerPartitionsGathered` | Partitions added to a batch by a linger wait; divided by `TotalLingerWaits` gives the linger's **hit rate**. |

To verify the fast path is doing its job, compare `TotalSyncBatchesWritten ÷ committed-writes` with the
flag off (~2) and on (~1). To verify the linger, compare `TotalPartitionsBatched ÷ TotalBatchesWritten`,
and check `TotalLingerPartitionsGathered ÷ TotalLingerWaits` — a low hit rate means the gate is admitting
waits that rarely pay, and the linger is probably not worth its window on that workload.

---

## Code map

```
Kommander/
├─ RaftConfiguration.cs            WalSingleFsyncCommit, WalGroupCommitLingerMs,
│                                   MaxWalGroupBatchPartitions, MaxWalBatchSize, WriteIOThreads
├─ RaftPartitionStateMachine.cs    the commit path:
│     ├─ CompleteAppendLogsAsync        propose-quorum handling + fast-path ticket release;
│     │                                  fast-path follower re-supply on a regressed frontier
│     ├─ TryReleaseTicketOnQuorumDurable advance frontier + release ticket before the commit sync
│     └─ CompleteLeaderCommit           the (still-running) commit marker + broadcast
├─ RaftWriteAhead.cs               EnqueuePropose / EnqueueCommit (the two phases);
│     └─ CompleteRestoreAsync           frontier reconstruction on restart (Flow 5)
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
└─ WAL/TestLostAcknowledgedEntriesCluster.cs  a follower's live log cut on disk, repaired by the leader
└─ Scheduler/TestFairWalScheduler.cs          group commit + linger behaviour
```

---

## Invariants you must not break

1. **Never ack before quorum durability.** The fast path acks on propose-quorum-durable — a quorum
   already holds the entry on disk. It must never ack earlier.
2. **`CommittedCheckpoint` is always sync.** It is the recovery anchor. The lazy-marker path applies to
   per-entry `Committed` markers only — never to checkpoints, proposed entries, or rollbacks.
3. **Preserve the durable `Proposed` tail on restart.** `proposeIndex = maxLogId + 1` on the fast path;
   a later propose must not reuse ids that an acked-but-lazily-committed entry occupies.
4. **Recover the *contiguous* committed prefix, not the last on-disk marker.** A lost marker leaves a gap;
   recovery must stop the committed prefix at that gap and let re-supply/re-commit fill the rest.
5. **Flag-off is sacred.** With `WalSingleFsyncCommit = false`, the write and recovery paths must be
   byte-for-byte the prior always-sync behaviour. The fast-path branches are gated on the flag.

---

## Testing

The durability behavior is covered at several layers:

- **Sync count (deterministic)** — `FsyncCountTests` drives a fixed committed-write count through a
  single-worker scheduler and asserts `TotalSyncBatchesWritten` is exactly `2×W` with the flag off and
  `1×W` with it on, across RocksDB and SQLite, while `TotalBatchesWritten` stays `2×W` either way.
- **End-to-end equivalence** — `SingleFsyncCommitTests` asserts that with the flag on an `autoCommit`
  write commits to a quorum identically to the flag-off path, including the single-voter path and a
  50-way concurrent burst (unique, contiguous indices under out-of-order quorum completion).
- **Recovery** — `RecoveryReconstructionCrashMatrixTests` materializes the exact on-disk shape a lost
  marker leaves and asserts recovery restores exactly the contiguous committed prefix, retains the
  durable tail, and keeps `proposeIndex` past it; `RecoveryReSupplyClusterTests` crashes a follower with
  demoted markers and asserts it re-converges to the full committed log via leader re-supply.
- **Scheduler** — `TestFairWalScheduler` covers opportunistic and deferred (linger) group commit and the
  adaptive bail.

Reminder: **never run multiple `dotnet test` commands at once.** The suite spins up
in-process Raft clusters; concurrent runs produce false failures. Use a targeted `--filter` while
iterating, then the full project for shared scheduling/WAL/cluster changes.

```sh
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~FsyncCount
```

---

## Glossary

- **Propose / commit phases** — Raft's two steps: replicate an entry (`Proposed`), then mark it
  `Committed` once a quorum holds it.
- **fsync** — the disk durability barrier; on a durable WAL each synced write pays one.
- **Group commit** — coalescing concurrent writes into one `fsync`; opportunistic by default, deferred
  with `WalGroupCommitLingerMs`.
- **Single-fsync fast path** — `WalSingleFsyncCommit`: ack on propose-quorum-durable + lazy commit marker.
- **Propose-quorum-durable** — a quorum holds the entry on disk; the true Raft commit point.
- **Commit marker** — the in-place `Proposed → Committed` type update for an entry.
- **Lazy / sync-off write** — a WAL write that does not force its own `fsync` and rides the next sync.
- **`CommittedCheckpoint`** — the always-durable recovery anchor; certifies the prefix below it.
- **Commit frontier** — the highest contiguous committed id a node knows (`commitIndex`).
- **Re-supply** — the leader re-shipping the still-committed tail to a follower whose frontier regressed.
- **Sync ledger / hard-state ledger** — the two files beside a RocksDB engine that keep, outside its log,
  the last synced sequence and the last term and vote per partition; see "Storage corruption and lost
  writes".
- **`autoCommit` path** — the single-round propose-and-commit path the fast path applies to.

---

*For the broader system, see the [Architecture Overview](architecture-overview.md); for how a lagging
follower is caught up, see the [Log Catch-Up & Backfill Developer Guide](log-backfill-developer-guide.md).*
