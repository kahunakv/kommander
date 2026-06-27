# WAL Write Durability & the Single-Fsync Commit Fast Path — Developer Guide

This guide explains how Kommander makes a write **durable** on the persistent WAL backends (RocksDB,
SQLite), why a committed write costs **two** disk syncs by default, and the two knobs that reduce that
cost: **group commit** (a throughput lever) and the **single-fsync commit fast path** (a latency lever).
It is written for two readers — an operator deciding whether to turn these on and how to tune them, and a
developer who wants to understand or extend the write path. No prior Raft expertise is assumed; we build
the idea up first, then walk the real flows, the configuration, and the code.

---

## Table of contents

1. [The 60-second mental model](#the-60-second-mental-model)
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
12. [Observability](#observability)
13. [Code map](#code-map)
14. [Invariants you must not break](#invariants-you-must-not-break)
15. [Testing](#testing)
16. [Glossary](#glossary)

---

## The 60-second mental model

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
`WalGroupCommitLingerMs > 0` — after a brief adaptive linger to gather more), and issues a single write
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

---

## Configuration

In `RaftConfiguration.cs`:

| Setting | Default | What it does |
|---|---|---|
| `WalSingleFsyncCommit` | `false` | The latency lever. Acks `autoCommit` writes on propose-quorum-durable and writes the commit marker lazily, removing one `fsync` from the critical path. Off ⇒ byte-for-byte the prior two-sync behaviour. |
| `WalGroupCommitLingerMs` | `0` | The throughput/tail lever. `> 0` lets a WAL worker linger up to this many ms to gather more ready partitions into one `fsync`. Adaptive: low-overlap load pays at most one short probe. `0` keeps purely opportunistic batching. |
| `MaxWalGroupBatchPartitions` | `64` | Max partitions coalesced into a single `walAdapter.Write` (one `fsync` on RocksDB). |
| `MaxWalBatchSize` | `256` | Max operations drained from one partition per write. |
| `WriteIOThreads` | `4` | WAL scheduler worker threads — how many writes can sync concurrently across partitions. |

Tuning notes:

- **`WalSingleFsyncCommit`** is the knob to reach for when **write latency** (not throughput) is the
  problem and the backend is durable (`syncWrites: true`). Enable it, then confirm syncs-per-write drops
  (see [Observability](#observability)). It is safe to leave off; it changes no on-disk format.
- **`WalGroupCommitLingerMs`** helps most when writes spread across **many partitions** and arrive
  staggered (the follower append path). Start around `2` ms and measure; the win is denser batches and a
  tighter tail, not a lower median. A larger value bounds the linger; the adaptive bail keeps idle load
  from paying it.
- The two levers are **complementary**: group commit reduces total syncs under load; the fast path
  removes a sync from each write's path. They can be enabled together.

---

## Observability

`FairWalScheduler` exposes counters that make the levers measurable:

| Property | Meaning |
|---|---|
| `TotalBatchesWritten` | Number of `walAdapter.Write` calls — the batching/Write-call count. **Unchanged** by the fast path. |
| `TotalSyncBatchesWritten` | Number of those calls that actually `fsync`ed — the **true sync count**. Equals `TotalBatchesWritten` with the fast path off; drops toward ~1× per committed write with it on. |
| `TotalPartitionsBatched` | Sum of partitions across all batches; divided by `TotalBatchesWritten` gives **mean partitions per `fsync`** — the batch density the linger raises. |

To verify the fast path is doing its job, compare `TotalSyncBatchesWritten ÷ committed-writes` with the
flag off (~2) and on (~1). To verify the linger, compare `TotalPartitionsBatched ÷ TotalBatchesWritten`.

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
      ├─ RocksDbWAL.cs              SetSync(true/false)
      ├─ SqliteWAL.cs               PRAGMA synchronous FULL / OFF toggle
      └─ InMemoryWAL.cs             no fsync — the non-durable control

Kommander.Tests/
├─ WAL/FsyncCountTests.cs                      syncs/write = 2× off, 1× on (RocksDB + SQLite)
├─ WAL/SingleFsyncCommitTests.cs              fast-path = baseline end-to-end; single-voter; concurrency
├─ WAL/RecoveryReconstructionCrashMatrixTests.cs  recovers exactly the contiguous prefix, keeps the tail
├─ WAL/RecoveryReSupplyClusterTests.cs        a regressed follower re-converges via leader re-supply
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
- **`autoCommit` path** — the single-round propose-and-commit path the fast path applies to.

---

*For the broader system, see the [Architecture Overview](architecture-overview.md); for how a lagging
follower is caught up, see the [Log Catch-Up & Backfill Developer Guide](log-backfill-developer-guide.md).*
