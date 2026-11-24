# Log Catch-Up & Backfill — Developer Guide

This guide explains **backfill**, which brings a lagging follower back in sync with its leader, and the
snapshot path that takes over when the leader no longer has the missing entries.

---

## Table of contents

1. [Summary](#summary)
2. [Live and backfill replication paths](#live-and-backfill-replication-paths)
3. [Core concepts](#core-concepts)
4. [Flow 1 — Detecting a lagging follower](#flow-1--detecting-a-lagging-follower)
5. [Flow 2 — Sending a bounded backfill round](#flow-2--sending-a-bounded-backfill-round)
6. [Flow 3 — The follower's Log Matching check](#flow-3--the-followers-log-matching-check)
7. [Flow 4 — Multi-round convergence](#flow-4--multi-round-convergence)
8. [Flow 5 — Snapshots below the compaction floor](#flow-5--snapshots-below-the-compaction-floor)
9. [Flow 6 — Requested re-seed and withheld candidacy](#flow-6--requested-re-seed-and-withheld-candidacy)
10. [The unanchored live path](#the-unanchored-live-path)
11. [Configuration](#configuration)
12. [Code map](#code-map)
13. [Invariants you must not break](#invariants-you-must-not-break)
14. [Testing](#testing)

---

## Summary

A follower falls behind when it is slow, paused, partitioned or new to the cluster. A broadcast of the
newest entry does not help it, since the entries it missed would be a **gap** in its log.

The leader detects the lag, reads the missing range of its log and ships it in **bounded, anchored
chunks**. The anchor states which entry the follower must already have. A follower that does not have it
rejects the chunk, and the leader retries from a lower index. Rounds repeat until the follower is caught
up. If the leader already **compacted** the missing entries, the backfill read comes back empty and
catch-up switches to a snapshot install.

```
   LEADER log:   1 2 3 4 5 6 7 8 9 10
   follower:     1 2 3                       ← 7 entries behind
                       └──────── backfill ships 4..10 in bounded rounds ──────►
   follower:     1 2 3 4 5 6 7 8 9 10        ← caught up, contiguous
```

---

## Live and backfill replication paths

| | Live path | Backfill path |
|---|---|---|
| When | The follower is up to date | The follower lags > `BackfillThreshold`, or another trigger in [Flow 1](#flow-1--detecting-a-lagging-follower) applies |
| Trigger | A new propose/commit/rollback, broadcast to all followers | The leader's lag detection |
| Anchored? | **No** (`PrevLogIndex = 0`) | **Yes** `(PrevLogIndex, PrevLogTerm)` |
| Bounded? | One entry/op at a time | `MaxBackfillEntriesPerRound` per round |
| Enforces Log Matching? | No | Yes |

The live path is unanchored for liveness. For more information, see
[The unanchored live path](#the-unanchored-live-path).

---

## Core concepts

| Term | Meaning |
|---|---|
| **Log index** | The position of an entry in the log (1, 2, 3, …). |
| **Term** | The leadership epoch in which an entry was created. |
| **Lag** | How many committed entries a follower is behind the leader. |
| **Anchor** | The pair `(PrevLogIndex, PrevLogTerm)` on a backfill batch: the entry the follower must already have. |
| **Log Matching Property (LMP)** | A follower takes entries only if it has the entry at `PrevLogIndex` with `PrevLogTerm`. |
| **`matchIndex`** | Per-follower: the highest index the leader knows the follower has stored. |
| **`nextIndex`** | Per-follower: the next index the leader will try to send. |
| **Backtrack** | After a `LogMismatch`, the leader lowers `nextIndex` and retries from an earlier index. |
| **Divergent tail** | Uncommitted follower entries that do not match the leader's. They are truncated and replaced during backfill. |
| **Compaction floor** | The earliest index the leader retains after compaction. Below it, only a snapshot can help. |
| **`SnapshotRequired`** | Reserved enum value that is not produced. The handoff to a snapshot is internal to the leader. To observe it, use `IRaft.GetSnapshotStatuses`. |

---

## Flow 1 — Detecting a lagging follower

The leader compares each follower's `matchIndex` with its own committed index. There are **three**
independent triggers, and `BackfillThreshold` gates only the first.

```
   LEADER, for each follower:
        gap = leaderCommittedIndex - matchIndex[follower]
        │
        ▼
   BackfillEnabled ? ──── no ──▶ never backfill this follower
        │ yes
        ▼
   gap > BackfillThreshold ?          ─── yes ──▶ actively behind: stream it forward
        │ no
        ▼
   gap >= 1 and writes have paused ?  ─── yes ──▶ idle tail: re-ship the missed entries
        │ no
        ▼
   follower reported a frontier below ─── yes ──▶ crash-restart: re-supply from its
   its recorded matchIndex ?                       reported frontier
        │ no
        ▼
   leave it to the live path
```

A follower that is one or two entries behind is usually mid-broadcast, and the threshold leaves it to the
live path. The other two triggers ignore the threshold, since the live path cannot heal those gaps. Once
writes stop, empty heartbeats carry no entries and one missed tail entry would strand the follower. A
follower that restarted and lost its lazy commit markers must be re-supplied from the frontier it reports.

The leader also ships a batch directly from an ack that shows the follower more than `BackfillThreshold`
behind (the ack fast path). A multi-batch catch-up then does not wait a heartbeat per batch. The fast path
skips a follower whose gap is already in flight to it as commit or rollback broadcasts, counted from the
moment the commit marker is queued (`ReplicationTracker.AreResolutionsInFlight`). This prevents a third
`AppendLogs` and a third WAL write per proposal under steady writes. A broadcast that is not answered within
`HeartbeatInterval` stops counting. The gate does not apply to the heartbeat triggers.

**`BackfillThreshold` is not a disable switch.** With a high threshold, the idle-tail trigger fires when the
partition goes idle. To turn backfill off for a node's partitions, set `BackfillEnabled = false`. It
disables all three triggers **and** the snapshot fallback. Do that only if another mechanism catches up
those peers, since the library will not re-ship entries they missed.

The startup safety audit logs `BackfillEnabled=false` at Warning. A host that turns backfill off on
purpose, such as a standalone node whose other voters are in-process witnesses, should add
`BackfillEnabled` to `RaftConfiguration.IntendedFenceDeviations` with a one-sentence reason. The audit then
logs the line at Information with that reason. `Validate` refuses a key that the audit does not report and a
blank reason.

`IRaft` exposes a follower's lag. `GetFollowerLagAsync` is the protocol commit frontier and needs an
executor round-trip. `GetFollowerProgress` is a lock-free read of the leader's acknowledgement snapshot for
the follower: its *durable* frontier, its pending-write age and whether that counts as a stall. It is cheap
enough to call per operation.

---

## Flow 2 — Sending a bounded backfill round

```
   pick the start index:  from = nextIndex[follower]  (where we believe it needs entries)
        │
        ▼
   read entries [from .. from + MaxBackfillEntriesPerRound)  from the leader's WAL
        │
        ▼
   compute the anchor:
        PrevLogIndex = from - 1
        PrevLogTerm  = term of the leader's entry at (from - 1)
        │
        ▼
   send AppendLogs(entries, PrevLogIndex, PrevLogTerm)  → follower
```

- **`MaxBackfillEntriesPerRound`** caps the entries in one round. Backfill therefore does not starve the
  live heartbeat traffic.
- **One round in flight per follower.** The leader waits for the reply before it decides the next round.

---

## Flow 3 — The follower's Log Matching check

A follower checks an **anchored** append before it changes its log.

```
   follower receives AppendLogs(entries, PrevLogIndex, PrevLogTerm)
        │
        ▼
   PrevLogIndex > 0 ?  (i.e. is this an anchored / backfill append?)
        │                       │
        no (live path)          yes
        ▼                       ▼
   append directly         do I hold an entry at PrevLogIndex
   (see live-path             with term == PrevLogTerm ?
    section)                    │                 │
                                yes               no
                                ▼                 ▼
                        append the batch,    REJECT with LogMismatch
                        truncating any       (leader will backtrack)
                        divergent tail first
                                ▼
                        ack with new matchIndex
```

- **Match → append.** The batch slots in contiguously. A divergent uncommitted tail past the anchor is
  **truncated and replaced**.
- **Mismatch → reject.** The follower replies `LogMismatch` and the leader backtracks to the last index
  they agree on.

---

## Flow 4 — Multi-round convergence

```
   ┌─────────────────────────────────────────────────────┐
   │  send round (Flow 2)                                 │
   │        │                                             │
   │        ▼                                             │
   │  reply?                                              │
   │    ├── Success → advance matchIndex/nextIndex        │
   │    │            still lagging > threshold?           │
   │    │              yes → send the next round ─────────┤  loop
   │    │              no  → done, back to live path      │
   │    │                                                 │
   │    └── LogMismatch → backtrack nextIndex lower ──────┤  loop
   └─────────────────────────────────────────────────────┘
```

On **Success**, the leader advances `matchIndex`/`nextIndex`. If the follower is beyond the threshold, it
sends the next round at once. On **LogMismatch**, it backtracks and retries lower. The loop ends when the
follower is within the threshold and the live path takes over.

---

## Flow 5 — Snapshots below the compaction floor

The entries below the last checkpoint are removable. A follower that needs entries below the resulting
**compaction floor** needs a snapshot, and the empty backfill read is the handoff signal. No status crosses
the wire for it. A new node that joins a long-running cluster typically takes this path first, then
switches to backfill.

```
   LEADER log after compaction:        [floor=50] 51 52 ... 100
   follower needs:                     20, 21, ...   ← below the floor, GONE
        │
        ▼
   leader can't read them → backfill read comes back EMPTY
        │
        ▼
   catch-up switches to SNAPSHOT INSTALL:
        ship a compact state snapshot → follower replaces its state
        → resume normal replication from the snapshot point
```

### Retry and install wait

- A follower whose snapshot **fails** (the export throws, a chunk is rejected) is retried with exponential
  backoff from the heartbeat interval, capped at 30 s. A **saturated** follower, which pauses entry batches
  to drain its WAL queue, is not escalated to a snapshot.
- The **install has no timeout**. The receiver acknowledges the last chunk once the snapshot is staged and
  verified (`InstallPending`). The leader then asks for the outcome in short calls until the follower
  returns `Installed`, `SkippedAlreadyCovered` or a failure. `IRaft.GetSnapshotStatuses` shows such a
  transfer as `InFlight` with `AwaitingInstall` set.
- A retry **resumes**. Before it exports, each attempt asks the follower if an install of the partition is
  already queued or running, from its own earlier attempt or a previous leader. If so, it waits for that
  install. A follower has one install per partition at a time and stages no other snapshot for that
  partition meanwhile.
- When **no snapshot transfer is registered**, a follower that needs a snapshot cannot catch up. The
  condition is recorded once per episode. `IRaft.GetSnapshotStatuses(partitionId)` returns it with the
  in-flight transfers, attempt counts and last errors.

### Retention on all replicas

The leader sends its live-replica retention floor with each AppendLogs. The floor is the slowest replica's
durable position, or the leader's own when its disk is behind. Each follower limits its own compaction to
that floor within `CompactionLiveReplicaLagBudget`. After a leader change, the successor's log therefore
reaches back to a replica that was catching up. A new leader retains the budget's full depth for a peer
until the first ack from that peer gives its position.

### When a snapshot is offered

- **A read that starts above the anchor** is refused. The run at the follower's next index is uncommitted
  or was compacted away, and an anchored batch that skips the gap would repeat forever. The refusal is
  logged **once per episode** (repeats drop to Debug, and a line is logged when it clears).
  `IRaft.GetBackfillStatuses(partitionId)` shows the anchor, the first index the leader could read and the
  last checkpoint. A checkpoint at or above the first readable index means that the anchor was compacted
  away and only a snapshot can seed that follower.
- **No progress.** When batches anchored at a follower's frontier are acknowledged but its reported
  frontiers **do not advance**, the no-progress probe paces and re-anchors the batches. After four
  fruitless ships (the warning threshold) the leader also offers a snapshot from its last checkpoint. This
  lets a **new** leader resume a seed that the old leader left unfinished, when its own WAL can serve the
  anchor and no refusal occurs. No snapshot is shipped if there is no checkpoint.
- **Slow is not stuck.** A ship is fruitless only when *none* of the follower's reported frontiers moved.
  These are its commit frontier, its durable frontier and its contiguous presence frontier. The presence
  frontier is credited only for a ship whose batch reached it and so could have extended it. A follower
  that closes a log hole therefore counts as progress although its commit frontier is pinned under the
  hole. In the marker-loss wedge the presence frontier climbs with the live stream while no entry below it
  is repaired, and it gets no credit.
- **A freshly seeded follower gets a grace.** After a confirmed install the probe defers its escalation
  (pacing and the Warning continue). The grace is `BackfillNoProgressPauseCap` plus the time to close the
  install's lag at the leader's sustained commit rate, bounded by `BackfillSeededCatchUpGraceCap`
  (3 minutes). A refused backfill escalates at once.
- **A follower that reaches the checkpoint is not seeded.** All escalations pass one gate before the export.
  When the follower's gap-aware commit frontier reaches the leader's last checkpoint, no transfer starts.
  The hold is counted (`raft.snapshot.transfers_withheld_peer_reaches_checkpoint_total`) and the probe logs
  it once per episode. The *durable* frontier is not considered, since a snapshot cannot move it. The
  *presence* frontier does not stand in for the commit frontier: a follower that has a range but not its
  commit markers needs the snapshot.

Each snapshot transfer starts with a Warning that names its trigger (`SnapshotTransferTrigger`: a
non-contiguous or empty backfill read, the no-progress probe, a compacted anchor, the follower's own
re-seed request). The line also has the anchor and first readable entry, the fruitless streak, the
follower's three frontiers, and the leader's checkpoint and retention floor. The trigger is also on
`RaftSnapshotStatus.Trigger` and tags `raft.snapshot.transfers_started_total`.

### While a follower is being seeded

A snapshot transfer occupies the follower's partition executor for the length of the install, and
entry-carrying appends cannot be applied there before the installed state. These rules prevent a loop of
installs.

- **The follower sheds entry-carrying appends.** `RaftPartition.AppendLogs` answers them at the receive
  path with `FollowerInstallingSnapshot` and gives no position. Heartbeats are queued. The leader treats
  the status as a saturated WAL: entry-carrying sends to the peer pause for `FollowerSaturationBackoff`,
  renewed by each refusal. The install's confirmation advances the peer's cursors to the installed
  boundary. `raft.follower.appends_refused_while_installing_total` counts the refusals, and one Information
  line per install sums them.
- **The leader sends the peer no entries while its transfer is in flight.** `BackfillSender.AppendLogToNode`
  and the batch path consult `SnapshotSender.TryGetSeedingIndex` and skip the send
  (`raft.backfill.seeding_peer_sends_skipped_total`). The entries ship contiguously from the installed
  boundary afterwards.
- **Retention starts at the install's boundary and is exempt from the budget while the install is
  unresolved.** The floor publisher retains from the boundary + 1 while a transfer is in flight *or* an
  install is remembered as active (`SnapshotSender.TryGetRetentionHoldIndex`, the lower of the two). It
  raises the published budget to cover a just-seeded peer's need (`commit − need + 1`, under
  `CompactionLiveReplicaLagCap`). The hold outlives the transfer attempt and ends when the peer gives a
  position at or past the boundary. Otherwise the floor could pass the boundary and the peer would be
  installed again. The raised budget travels on each AppendLogs. A node that wins an election inherits the
  last budget it received (`HeartbeatDriver.InheritRetentionBudget`) until its own commit history fills the
  window. `raft.compaction.seeded_peer_retention_holds_total` counts the rounds that raised the budget.
- **The follower names its install in each ack, for a successor leader.**
  `CompleteAppendLogsRequest.SeedIndex` carries the boundary of the install that occupies the follower's
  executor (on `FollowerInstallingSnapshot` refusals), or of the install it landed within the last
  `BackfillSeededCatchUpGraceCap` (on positional acks). A leader that did not send the install treats an
  active one as unresolved: retention starts from it, and its first transfer waits for it. A landed install
  becomes the peer's snapshot seed, with the same budget exemption. A boundary arms the seed once. Peers
  that predate the field send 0 and get the previous behaviour.
- **A stalled read figure does not end the wait.** The progress figure is the bytes read from the staged
  snapshot, and it stops while the import's later phases (drain, purge, store decode) run. The sender logs
  a Warning once per `SnapshotTransferStepTimeout` of no progress
  (`raft.snapshot.install_waits_past_step_timeout_total`) and continues to wait. The wait ends when the
  follower reports the install done, failed or lost, goes silent for `SnapshotChunkAckTimeout`, or this node
  stops leading.

---

## Flow 6 — Requested re-seed and withheld candidacy

A replica can have a complete WAL but incomplete **application state**. Examples are an earlier install that
marked entries applied and did not deliver them, or a consumer that rejected an entry. The apply cursor and
the checkpoint boundary both say "covered", so the ordinary install path *skips* such a replica and it
can be elected. Two application-driven calls handle it (Kommander 1.8.1).

```
   APPLICATION on replica F finds F's projection incomplete
        │
        ├─ IRaft.SetCandidacyWithheld(p, true)        F never campaigns, ignores TransferLeadership
        │
        └─ IRaft.RequestReseedAsync(p)
                │  F holds committed applies at its cursor C
                │  F ──ReseedRequest──▶ LEADER
                │                          proposes a checkpoint; once it commits at N (> C)
                │                          ships a snapshot at N with Forced = true
                │  F ◀──InstallSnapshot(N, Forced)
                │       imports even though F's boundary/log already cover N
                │       (an install BELOW F's cursor is still refused: C ≤ N by construction)
                │       cursor := N, applies resume above N
                ▼
   APPLICATION sees ImportPartitionState land → SetCandidacyWithheld(p, false)
```

- **`RequestReseedAsync`** answers `NodeIsNotLeader` on a leader, which must relinquish first, and
  `Errored` when no leader is known. A repeat while pending re-sends the request. The hold is bounded by
  `RaftConfiguration.ReseedRequestTimeout` (default 3 min). If no install lands by then, the follower
  resumes delivery where it stopped and logs `Re-seed request expired`. The leader drops a request whose
  checkpoint does not commit in the same bound. A replica that becomes leader while pending resumes at once.
- **The leader's side** takes a checkpoint *newer than the request*. The snapshot's index is then above the
  entries the follower applied before the hold. The forced transfer bypasses the convergence breaker, the
  failure backoff and the post-success pause. It does not bypass the in-flight guard or the
  durable-write-stall deferral.
- **`SetCandidacyWithheld`** is a per-node in-memory flag. Each campaign entry and the transfer receive path
  consult it, and it has no time limit. The replica continues to vote, replicate and acknowledge.

The nodes log the cycle at Warning (`Re-seed requested …`,
`Re-seed of … checkpoint … committed, starting the forced snapshot transfer`, `InstallSnapshot … answers
a re-seed request`, `Re-seed complete`, `Deferring candidacy: the application withheld …`).

---

## The unanchored live path

An anchor on the live path causes a **liveness failure**. A live proposal would be anchored at the latest
index. A follower that is *transiently* behind does not have that anchor yet and rejects the proposal with
`LogMismatch`. The live proposal path has **no recovery loop**. The proposal misses quorum and times out
(`ProposalTimeout`), and under load this becomes a livelock.

- **Live path**: unanchored (`PrevLogIndex = 0`). Lag detection hands a follower that falls behind to
  backfill.
- **Backfill path**: anchored, with full Log Matching enforcement. This is the *only* place where gaps and
  divergent tails are prevented. It is sufficient, since the entries a follower is missing arrive through
  backfill.

In production, the **`LogMismatch → backtrack` branch is reachable only from the backfill path**. Unit tests
exercise it directly.

---

## Configuration

These settings are in `RaftConfiguration.cs`.

| Setting | Default | What it does |
|---|---|---|
| `BackfillEnabled` | `true` | Master switch. `false` means this node does not backfill and does not fall back to a snapshot transfer for a lagging follower. Peers converge only through the live path, and a follower that misses entries remains behind. |
| `BackfillThreshold` | `10` | A follower must lag by more than this many entries before the *actively-behind* trigger engages. Does not gate the idle-tail or crash-restart triggers (Flow 1). |
| `MaxBackfillEntriesPerRound` | `128` | Maximum entries shipped in a single backfill round. |
| `BackfillNoProgressPauseCap` | `30 s` | Cap on the exponential pause between batches to a follower none of whose reported frontiers is advancing. |
| `BackfillSeededCatchUpGraceCap` | `3 min` | Upper bound on the grace a follower gets after a snapshot install before the no-progress probe may offer it another one (Flow 5). Zero disables the grace. |

- A lower `BackfillThreshold` gives faster recovery and more work on small lags. `int.MaxValue` does **not**
  disable backfill. Use `BackfillEnabled` for that.
- A lower `MaxBackfillEntriesPerRound` gives smaller messages and more rounds to converge.
- `FollowerSaturationBackoff` (default 1 s) also paces the leader after a `FollowerInstallingSnapshot`
  refusal.
- The seeding retention hold has no setting. It is bounded by the transfer's lifetime, the seeded-peer
  install record (`BackfillSeededCatchUpGraceCap`) and `CompactionLiveReplicaLagCap`.
- Aggressive compaction (`CompactEveryOperations`, `CompactNumberEntries`, `MaxEntriesPerCompaction`) raises
  the compaction floor faster. Far-behind followers are then more likely to need a snapshot.

---

## Code map

```
Kommander/
├─ Consensus/
│     ├─ ReplicationTracker.cs           the per-follower nextIndex/matchIndex state
│     ├─ BackfillSender.cs
│     │     └─ TrySendBackfillBatchAsync assemble & send one anchored round (Flow 2)
│     ├─ ReplicationAckProcessor.cs
│     │     └─ CompleteAppendLogsAsync   handle the reply: Success advances,
│     │                                   LogMismatch backtracks (Flow 4)
│     └─ FollowerAppendHandler.cs
│           └─ AppendLogsCoreAsync       follower-side Log Matching check (Flow 3)
├─ RaftWriteAhead.cs
│     └─ GetAnyTermAtAsync               read the term at an index (for the anchor)
├─ Data/
│     ├─ AppendLogsRequest.cs            carries PrevLogIndex / PrevLogTerm
│     └─ RaftOperationStatus.cs          LogMismatch (SnapshotRequired is reserved, never produced)
├─ WAL/IO/FairWalScheduler.cs           applies divergent-tail truncation
└─ RaftConfiguration.cs                 BackfillEnabled, BackfillThreshold,
                                        MaxBackfillEntriesPerRound

Kommander.Tests/
├─ TestLogMatchingCheck.cs              direct-injection LMP tests (reject/accept/backtrack)
├─ TestThreeNodeCluster.cs             BackfilledFollower_HasContiguousLog_MatchingLeader,
│                                       DivergentSuffix_Backfill_OverwritesStaleUncommittedEntries,
│                                       PausedStaleFollower_MultiRoundBackfill_ConvergesAcrossChunks,
│                                       AssertContiguousLog (helper)
└─ WAL/WalConformanceTests.cs          TruncateLogsAfter_* across all WAL backends
```

`IRaft.GetFollowerLagAsync(partitionId, followerEndpoint)` returns the lag by the protocol commit frontier.
`IRaft.GetFollowerProgress(partitionId, followerEndpoint)` returns the leader's `RaftFollowerProgress`
snapshot (durable frontier, protocol frontier, pending-write age, stall flag, the leader's commit index when
the ack was folded). The ack path publishes it (`ReplicationAckProcessor` →
`IRaftPartitionHost.PublishFollowerProgress`) into a concurrent map on `RaftPartition`. It is cleared with
the tracker on step-down, and is `null` on a non-leader or for a peer the leadership has not heard from.

---

## Invariants you must not break

1. **No gaps.** A follower's log must be contiguous. Do not append an entry past a hole.
2. **No divergent committed tail.** Uncommitted entries that disagree with the leader are truncated and
   replaced during an anchored append. Committed entries are never overwritten.
3. **Log Matching lives on the backfill path.** Do not anchor the live broadcast path. It brings back the
   `ProposalTimeout` livelock (see [The unanchored live path](#the-unanchored-live-path)).
4. **Backfill is bounded.** Honour `MaxBackfillEntriesPerRound` and one round in flight. Backfill must not
   starve live replication.
5. **Below the floor, hand off to the snapshot path.** Do not synthesize missing entries.

---

## Testing

- **Unit (direct injection)** — `TestLogMatchingCheck` exercises the follower's Log Matching check and the
  leader's backtrack logic, with the mismatch branch that the live path cannot reach.
- **Cluster integration** — `TestThreeNodeCluster`.
  - `BackfilledFollower_HasContiguousLog_MatchingLeader`: a lagging follower ends with a contiguous,
    term/type-identical log (checked entry by entry via `AssertContiguousLog`).
  - `DivergentSuffix_Backfill_OverwritesStaleUncommittedEntries`: a divergent uncommitted tail is truncated
    and replaced.
  - `PausedStaleFollower_MultiRoundBackfill_ConvergesAcrossChunks`: a far-behind follower converges across
    several bounded rounds.
- **WAL conformance** — `TruncateLogsAfter_*` verifies divergent-tail truncation across InMemory, SQLite
  and RocksDB.

Do not run more than one `dotnet test` command at a time. The suite starts in-process Raft clusters, and
concurrent runs give false failures. Use a targeted `--filter` while you iterate, then run the full project
for shared scheduling/WAL/cluster changes.

```sh
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~Backfill
```

---

*For the broader system, see the [Architecture Overview](architecture-overview.md). For how a new node joins
and catches up, see the [Dynamic Membership Developer Guide](dynamic-membership-developer-guide.md).*
