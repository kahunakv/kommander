# Log Catch-Up & Backfill — Developer Guide

This guide explains how Kommander keeps a **lagging follower** in sync with its leader: the
**backfill** feature. It is written for two readers — someone who wants to *understand* how catch-up
works, and someone who wants to *extend or debug* it. No prior Raft expertise is assumed; we build the
idea up first, then walk the real flows and the code.

---

## Table of contents

1. [The 60-second mental model](#the-60-second-mental-model)
2. [Why backfill exists (the problem)](#why-backfill-exists-the-problem)
3. [Two replication paths: live vs. backfill](#two-replication-paths-live-vs-backfill)
4. [Core concepts](#core-concepts)
5. [Flow 1 — Detecting a lagging follower](#flow-1--detecting-a-lagging-follower)
6. [Flow 2 — Sending a bounded backfill round](#flow-2--sending-a-bounded-backfill-round)
7. [Flow 3 — The follower's Log Matching check](#flow-3--the-followers-log-matching-check)
8. [Flow 4 — Multi-round convergence](#flow-4--multi-round-convergence)
9. [Flow 5 — When backfill can't help: the compaction floor](#flow-5--when-backfill-cant-help-the-compaction-floor)
10. [Why the live path is *not* anchored](#why-the-live-path-is-not-anchored)
11. [Configuration](#configuration)
12. [Code map](#code-map)
13. [Invariants you must not break](#invariants-you-must-not-break)
14. [Testing](#testing)
15. [Glossary](#glossary)

---

## The 60-second mental model

A Raft follower is supposed to hold the **same log** as its leader — the same entries, in the same
order, with no gaps. But followers fall behind: one is slow, one was briefly paused, one was
partitioned for a few seconds, one just joined the cluster.

**Backfill** is how the leader catches a behind follower back up:

> The leader notices a follower is lagging by more than a threshold, reads the missing slice of its own
> log, and ships it in **bounded chunks**. Each chunk is **anchored**: it tells the follower "you must
> already hold entry *F* at term *T* before I give you *F+1*." The follower applies the chunk only if the
> anchor matches; otherwise it rejects, and the leader backs up and retries lower. Rounds repeat until
> the follower is caught up.

If the follower has fallen so far behind that the leader has already **compacted** the entries it needs,
backfill can't help — the data is gone. The leader then signals `SnapshotRequired` and catch-up hands
off to snapshot install.

```
   LEADER log:   1 2 3 4 5 6 7 8 9 10
   follower:     1 2 3                       ← 7 entries behind
                       └──────── backfill ships 4..10 in bounded rounds ──────►
   follower:     1 2 3 4 5 6 7 8 9 10        ← caught up, contiguous
```

---

## Why backfill exists (the problem)

Normal replication is a **broadcast**: when the leader commits a new entry, it pushes that entry to all
followers right away. That works great for a follower that is *keeping up* — it gets entry N, then N+1,
then N+2, in order.

But it does nothing for a follower that **missed entries while it was away**. If a follower last saw
entry 3 and the leader is now broadcasting entry 10, naively appending entry 10 would leave a **gap**
(4–9 missing). A log with a gap is corrupt: replaying it would skip changes and the follower's state
would silently diverge from everyone else's.

So we need a second, dedicated mechanism whose whole job is "find the missing range and fill it in
correctly, without ever creating a gap or a divergent tail." That mechanism is backfill.

---

## Two replication paths: live vs. backfill

This is the single most important thing to internalize. Kommander has **two** ways entries reach a
follower, and they behave differently **on purpose**.

```
   ┌───────────────────────── LIVE PATH (broadcast) ─────────────────────────┐
   │  Leader proposes / commits / rolls back an entry → push to all followers │
   │  Used when followers are keeping up.                                     │
   │  NOT anchored — entries flow without a Log Matching precondition.        │
   └─────────────────────────────────────────────────────────────────────────┘

   ┌──────────────────────────── BACKFILL PATH ─────────────────────────────┐
   │  Leader detects a follower lagging > threshold → ships the missing      │
   │  range in bounded chunks.                                               │
   │  ANCHORED — each chunk carries (PrevLogIndex, PrevLogTerm); the          │
   │  follower enforces Log Matching and rejects a mismatch.                  │
   └─────────────────────────────────────────────────────────────────────────┘
```

| | Live path | Backfill path |
|---|---|---|
| When | follower is keeping up | follower lags > `BackfillThreshold` |
| Trigger | a new propose/commit/rollback | leader's lag detection |
| Anchored? | **No** (`PrevLogIndex = 0`) | **Yes** `(PrevLogIndex, PrevLogTerm)` |
| Bounded? | one entry/op at a time | `MaxBackfillEntriesPerRound` per round |
| Enforces Log Matching? | no | yes |

Why the live path is deliberately *not* anchored is covered in [its own section](#why-the-live-path-is-not-anchored) — it's a subtle but important correctness/liveness decision.

---

## Core concepts

| Term | Meaning |
|---|---|
| **Log index** | The position of an entry in the log (1, 2, 3, …). |
| **Term** | The leadership epoch in which an entry was created. Index + term uniquely pins an entry. |
| **Lag** | How many committed entries a follower is behind the leader. |
| **Anchor** | The pair `(PrevLogIndex, PrevLogTerm)` attached to a backfill batch — the entry the follower must already hold. |
| **Log Matching Property (LMP)** | A follower accepts entries only if it holds the entry at `PrevLogIndex` with `PrevLogTerm`. This is what guarantees contiguity and consistency. |
| **`matchIndex`** | Per-follower: the highest index the leader knows the follower has stored. |
| **`nextIndex`** | Per-follower: the next index the leader will try to send. Backtracks on a mismatch. |
| **Backtrack** | On a Log Matching rejection, the leader lowers `nextIndex` and retries from an earlier point. |
| **Divergent tail** | Uncommitted entries on a follower that don't match the leader's — truncated and replaced during backfill. |
| **Compaction floor** | The earliest index the leader still retains after compaction. Below it, only a snapshot can help. |
| **`SnapshotRequired`** | The status the leader returns when a follower needs entries below the compaction floor. |

---

## Flow 1 — Detecting a lagging follower

The leader tracks, per follower, how far behind it is (via `matchIndex` vs. its own committed index).
Backfill only kicks in once the gap crosses `BackfillThreshold` — small, transient lag is left to the
live path to settle.

```
   LEADER, for each follower:
        gap = leaderCommittedIndex - matchIndex[follower]
        │
        ▼
   gap > BackfillThreshold ?
        │              │
        no             yes
        ▼              ▼
   leave it to     start a backfill round (Flow 2)
   the live path
```

Why a threshold and not "any lag"? Because a follower that is one or two entries behind is almost
certainly mid-broadcast and about to catch up on its own. Backfilling it would be wasted work and could
race with the live path. The threshold draws the line between "transiently behind" and "genuinely
lagging."

> Note (`GetFollowerLagAsync`): the public `IRaft` API exposes a follower's lag so applications and
> operators can observe catch-up progress.

---

## Flow 2 — Sending a bounded backfill round

Once a follower is flagged, the leader assembles one round:

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

Two bounds keep this safe and cheap:

- **`MaxBackfillEntriesPerRound`** caps how many entries ride in one round, so backfill never tries to
  ship the whole log in a single message and never starves the live heartbeat traffic it rides
  alongside.
- **One round in flight per follower.** The leader sends a round, waits for the reply, then decides the
  next round. It does not fire-and-forget overlapping rounds at the same follower.

---

## Flow 3 — The follower's Log Matching check

This is where contiguity is enforced. When a follower receives an **anchored** append, it checks the
anchor before touching its log:

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

Two outcomes matter:

- **Match → append.** The follower holds the anchor, so the incoming batch slots in contiguously. If the
  follower had *uncommitted* entries past the anchor that disagree with the leader (a divergent tail),
  those are **truncated and replaced** — the leader's log always wins.
- **Mismatch → reject.** The follower does **not** hold the anchored entry at the expected term. It
  refuses and replies `LogMismatch`. The leader lowers `nextIndex` and retries from earlier (backtrack),
  converging on the last index they agree on.

The check is gated on `PrevLogIndex > 0`. A live (unanchored) append carries `PrevLogIndex = 0`, so the
follower skips the check — which is exactly the live-vs-backfill split from earlier.

---

## Flow 4 — Multi-round convergence

A single round rarely catches a far-behind follower up. The leader drives a loop, reacting to each
reply:

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

On **Success**, the leader advances its `matchIndex`/`nextIndex` for that follower and, if the follower
is still beyond the threshold, immediately sends the next round so catch-up doesn't wait a full
heartbeat each time. On **LogMismatch**, it backtracks and retries lower. Either way the loop is
bounded per round, so it shares the partition cleanly with normal traffic. It ends when the follower is
within the threshold — at which point the live path takes over again.

---

## Flow 5 — When backfill can't help: the compaction floor

Kommander **compacts** old log entries to bound disk usage (everything below the last checkpoint is
removable). That creates a **compaction floor**: the earliest index the leader still has.

If a follower needs entries *below* that floor, the leader literally cannot read them — they've been
trimmed. Backfill is out of options:

```
   LEADER log after compaction:        [floor=50] 51 52 ... 100
   follower needs:                     20, 21, ...   ← below the floor, GONE
        │
        ▼
   leader can't read them → returns SnapshotRequired
        │
        ▼
   catch-up switches to SNAPSHOT INSTALL:
        ship a compact state snapshot → follower replaces its state
        → resume normal replication from the snapshot point
```

`SnapshotRequired` is the handoff signal. Snapshot install is a separate mechanism (its own RPC and
follower-side state replacement); backfill's job is just to *detect* the floor and hand off cleanly.
Brand-new nodes joining a long-running cluster typically hit this path first, then switch to backfill
once they're above the floor.

---

## Why the live path is *not* anchored

This is the design decision people most often question, so it's worth stating plainly.

It is tempting to anchor **every** append — live broadcasts too — so the Log Matching check runs
everywhere. Kommander deliberately does **not** do this, because anchoring the live path causes a
**liveness failure**:

- A live proposal is anchored at the latest index.
- A follower that is *transiently* behind (mid-broadcast, slightly slow) won't yet hold that anchor.
- It rejects the live proposal with `LogMismatch` — but the **live proposal path has no recovery loop**
  (recovery lives in the backfill loop). The proposal can't make quorum, so it times out
  (`ProposalTimeout`), and under load this turns into a livelock.

This was observed directly: anchoring the live path made stress tests fail with `ProposalTimeout`;
reverting to an unanchored live path fixed them. So:

- **Live path**: unanchored (`PrevLogIndex = 0`). Followers that are keeping up apply broadcasts
  directly; followers that fall behind are caught by lag detection and handed to backfill.
- **Backfill path**: anchored, with full Log Matching enforcement. This is the *only* place gaps and
  divergent tails are prevented — and it's sufficient, because every entry a follower is missing
  ultimately arrives through backfill, where it *is* checked.

A practical consequence for contributors: the **`LogMismatch → backtrack` branch is not reachable from
the live path** in production — only the backfill path can trigger it. It's exercised directly by unit
tests rather than through a live proposal, and is retained as defensive scaffolding. If you ever make
the live path anchored "to be safe," re-read this section first.

---

## Configuration

In `RaftConfiguration.cs`:

| Setting | Default | What it does |
|---|---|---|
| `BackfillThreshold` | `10` | A follower must lag by more than this many entries before backfill engages. Below it, the live path handles catch-up. |
| `MaxBackfillEntriesPerRound` | `128` | Maximum entries shipped in a single backfill round. Caps message size and keeps backfill sharing the partition fairly. |

Tuning notes:

- **Lower `BackfillThreshold`** → backfill engages sooner (faster recovery, slightly more eager work on
  small lags). **Higher** → more lag tolerated before the dedicated path kicks in.
- **Lower `MaxBackfillEntriesPerRound`** → smaller, more frequent rounds (smoother, more rounds to
  converge). **Higher** → fewer, larger rounds (faster bulk catch-up, bigger messages).

Compaction settings (`CompactEveryOperations`, `CompactNumberEntries`, `MaxEntriesPerCompaction`)
indirectly affect backfill: aggressive compaction lowers the compaction floor faster, making the
snapshot path (Flow 5) more likely for far-behind followers.

---

## Code map

```
Kommander/
├─ RaftPartitionStateMachine.cs   the backfill engine:
│     ├─ lag detection + the per-follower nextIndex/matchIndex loop
│     ├─ TrySendBackfillBatchAsync       assemble & send one anchored round (Flow 2)
│     ├─ CompleteAppendLogsAsync         handle the reply: Success advances,
│     │                                   LogMismatch backtracks (Flow 4)
│     └─ AppendLogsCoreAsync             follower-side Log Matching check (Flow 3)
├─ RaftWriteAhead.cs
│     └─ GetAnyTermAtAsync               read the term at an index (for the anchor)
├─ Data/
│     ├─ AppendLogsRequest.cs            carries PrevLogIndex / PrevLogTerm
│     └─ RaftOperationStatus.cs          LogMismatch, SnapshotRequired
├─ WAL/IO/FairWalScheduler.cs           applies divergent-tail truncation
└─ RaftConfiguration.cs                 BackfillThreshold, MaxBackfillEntriesPerRound

Kommander.Tests/
├─ TestLogMatchingCheck.cs              direct-injection LMP tests (reject/accept/backtrack)
├─ TestThreeNodeCluster.cs             BackfilledFollower_HasContiguousLog_MatchingLeader,
│                                       DivergentSuffix_Backfill_OverwritesStaleUncommittedEntries,
│                                       PausedStaleFollower_MultiRoundBackfill_ConvergesAcrossChunks,
│                                       AssertContiguousLog (helper)
└─ WAL/WalConformanceTests.cs          TruncateLogsAfter_* across all WAL backends
```

`IRaft.GetFollowerLagAsync(partitionId, followerEndpoint)` is the observability entry point for
how far a follower is behind.

---

## Invariants you must not break

1. **No gaps.** A follower's log must stay contiguous. Never append an entry past a hole. The backfill
   anchor exists precisely to prevent this.
2. **No divergent committed tail.** Uncommitted entries that disagree with the leader are truncated and
   replaced during an anchored append; committed entries are never overwritten.
3. **Log Matching lives on the backfill path.** Don't move the anchor onto the live broadcast path — it
   reintroduces the `ProposalTimeout` livelock (see [its section](#why-the-live-path-is-not-anchored)).
4. **Backfill is bounded.** Honour `MaxBackfillEntriesPerRound` and one-round-in-flight; backfill must
   never starve live replication.
5. **Below the floor, hand off — don't fake it.** If entries are below the compaction floor, return
   `SnapshotRequired`. Never try to synthesize missing entries.

---

## Testing

The backfill behavior is covered at two layers:

- **Unit (direct injection)** — `TestLogMatchingCheck` exercises the follower's Log Matching check and
  the leader's backtrack logic directly, including the mismatch branch that the live path can't reach.
- **Cluster integration** — in `TestThreeNodeCluster`:
  - `BackfilledFollower_HasContiguousLog_MatchingLeader` — a lagging follower ends with a contiguous,
    term/type-identical log (via `AssertContiguousLog`, not just matching `GetMaxLog`).
  - `DivergentSuffix_Backfill_OverwritesStaleUncommittedEntries` — a divergent uncommitted tail is
    truncated and replaced.
  - `PausedStaleFollower_MultiRoundBackfill_ConvergesAcrossChunks` — a far-behind follower converges
    across several bounded rounds.
- **WAL conformance** — `TruncateLogsAfter_*` verifies divergent-tail truncation across InMemory,
  SQLite, and RocksDB.

Reminder: **never run multiple `dotnet test` commands at once.** The suite spins up
in-process Raft clusters; concurrent runs produce false failures. Use a targeted `--filter` while
iterating, then the full project for shared scheduling/WAL/cluster changes.

```sh
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~Backfill
```

---

## Glossary

- **Anchor** — the `(PrevLogIndex, PrevLogTerm)` a backfill batch asserts the follower already holds.
- **Backfill** — leader-driven, bounded catch-up of a lagging follower.
- **Backtrack** — lowering `nextIndex` after a `LogMismatch` to retry from an earlier index.
- **Compaction floor** — the earliest log index still retained after compaction.
- **Divergent tail** — uncommitted follower entries that disagree with the leader; truncated on append.
- **Lag** — how many committed entries a follower is behind the leader.
- **Live path** — the normal, unanchored broadcast of new entries to keeping-up followers.
- **Log Matching Property (LMP)** — accept entries only if the entry at `PrevLogIndex` has `PrevLogTerm`.
- **`matchIndex` / `nextIndex`** — per-follower progress markers the leader maintains.
- **`SnapshotRequired`** — the signal that a follower needs entries below the compaction floor.

---

*For the broader system, see the [Architecture Overview](architecture-overview.md); for how a brand-new
node joins and catches up, see the [Dynamic Membership Developer Guide](dynamic-membership-developer-guide.md).*
