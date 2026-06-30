# Leader Balancer — Developer Guide

Welcome! This guide is for the next developer who wants to **use, understand, or maintain**
Kommander's leader balancer. It assumes you know roughly what Kommander is (a Raft-based distributed
system) but **no prior knowledge of leader balancing**. We build every concept from the ground up,
walk through how a rebalance actually happens, and finish with configuration, observability, and the
invariants you must not break.

If you are brand new to Kommander, skim the [Architecture Overview](architecture-overview.md) first —
it explains partitions, leaders, and the system partition, which this guide builds on.

---

## Table of contents

1. [Summary](#summary)
2. [The problem: why leaders pile up](#the-problem-why-leaders-pile-up)
3. [Core concepts](#core-concepts)
4. [How a rebalance happens, step by step](#how-a-rebalance-happens-step-by-step)
5. [The two-tier balancing policy](#the-two-tier-balancing-policy)
6. [The suggestion mechanism (and why it exists)](#the-suggestion-mechanism-and-why-it-exists)
7. [Tracking moves without waiting](#tracking-moves-without-waiting)
8. [Using it: configuration](#using-it-configuration)
9. [Observability](#observability)
10. [Safety: what can and cannot go wrong](#safety-what-can-and-cannot-go-wrong)
11. [Code map — where everything lives](#code-map--where-everything-lives)
12. [Maintaining & extending](#maintaining--extending)
13. [FAQ](#faq)

---

## Summary

Kommander splits your data into many **partitions**. Each partition runs its own little Raft group,
and each group elects one **leader** that does all the real work for that partition (handling writes,
sending heartbeats, driving replication). Followers mostly just keep up.

Leaders are elected independently and somewhat randomly. Nothing coordinates *where* they land. So
over time you can end up with a lopsided cluster: one node holding most of the leaders while its peers
sit nearly idle. That node becomes a bottleneck even though you have spare machines.

The **leader balancer** fixes this. It watches how leaders are spread across the cluster, notices when
one node is carrying too much, and politely asks busy nodes to hand some leaderships to idle ones —
using Kommander's existing, safe leadership-handoff machinery. It is **off by default**, fully
advisory, and can never corrupt data; the worst it can do is make a pointless move.

```
Before:                          After balancing:

 node A: ████████████ (12)        node A: ████ (4)
 node B: ██ (2)                    node B: ████ (4)
 node C: ██ (2)                    node C: ████ (4)
        ^ overloaded                      ^ evenly spread
```

---

## The problem: why leaders pile up

Two distinct kinds of imbalance show up in practice:

1. **Count skew.** Pure bad luck in election timing puts a disproportionate *number* of leaderships on
   one node. With 64 partitions and 3 nodes you'd like ~21 each, but you might get 40 / 12 / 12. The
   busy node does the heartbeat, replication, and commit work for all 40.

2. **Load skew.** Even when the *counts* are equal, the **hot** partitions (high write rate, deep
   queues) can cluster on one node while another holds only cold, idle partitions. Counting leaders
   alone won't catch this — you have to look at how much work each partition actually does.

The balancer handles both: it equalizes counts first, then spreads load.

---

## Core concepts

Take these one at a time; the rest of the guide just combines them.

### Partition leader
The single node currently responsible for a partition. Only the leader can hand off its own
leadership — this matters a lot later.

### The system partition (P0)
A special partition (id `0`) that every node participates in. Its leader is the cluster's natural
coordination point: it already drives cluster-wide decisions, so there is exactly **one** of it and no
risk of two nodes making conflicting decisions. **The leader balancer runs only on the P0 leader.** We
call it "the controller."

### Load report
A small, periodic message each node gossips out describing the partitions *it currently leads* and how
busy each one is. Think of it as every node raising its hand and saying "here's what I'm carrying right
now." Reports are **advisory** — they ride the gossip path and are never written to the Raft log,
because they change too fast and don't need to be durable.

### Load score
A single number summarizing how busy one partition is. It blends two signals Kommander already
measures:

```
load = (ops weight) × (recent operations/sec)  +  (queue weight) × (pending work in queues)
```

The "operations/sec" part uses an **EWMA** (exponentially weighted moving average) — a smoothed
average that reacts to sustained traffic but ignores brief spikes, and naturally decays toward zero
when a partition goes idle. Only real *workload* operations count (client writes and replication);
background chatter like heartbeats is excluded, because it happens equally on every leader and would
just add noise.

### Global view
On the P0 leader, all the incoming load reports are reduced into one snapshot: who leads what, how many
leaders each node has, and how loaded each node is. This is the controller's picture of the world. Old
reports (past a time-to-live) are dropped so a node that went silent doesn't appear to still hold
leaderships.

### Transfer suggestion
The controller never *forces* anything. When it decides partition P should move from node A to node B,
it sends node A a **suggestion**: "please consider handing partition P to B." Node A — the only node
that actually knows whether it still leads P — validates and decides. This keeps the whole system safe
and advisory.

---

## How a rebalance happens, step by step

Every `LeaderBalancerInterval` (default 30s), the P0 leader runs one **balancing pass**:

1. **Gather.** Collect the latest load report from each node into the global view. Drop expired ones.
2. **Reconcile in-flight moves.** For any suggestion sent on a previous pass, check the view: did the
   target actually become the leader? If yes, mark it done and put that partition on a short cooldown.
   If too much time passed with no change, give up on it (it may have been dropped) and retry later.
3. **Check completeness.** If the view is missing a fresh report from any live node, **skip this pass**
   entirely. Acting on a half-complete picture could make bad moves, so the balancer would rather wait.
4. **Plan.** Run the two-tier policy (next section) to produce a short list of moves.
5. **Dispatch.** Send each planned move as a suggestion to the partition's current leader, and remember
   it as "outstanding" so we can confirm it later.

That's the whole loop. Notice it never blocks waiting for a transfer to finish — it fires suggestions
and re-checks reality next time. This is what makes it robust: every pass re-derives the truth from
fresh reports, so a failed or lost suggestion simply gets re-planned later.

---

## The two-tier balancing policy

The planner is a **pure function**: given the global view and the configuration, it returns a list of
moves and has no side effects. (That makes it easy to test and reason about.) It works in two tiers:

### Tier 1 — count balance (primary)
Compute the ideal: `total leaders ÷ number of live nodes`. A node holding more than that (beyond a
small tolerance, `CountDeadband`) is **over-loaded**; one holding fewer is **under-loaded**. The
planner moves leaderships from the most over-loaded node to the most under-loaded node, and when it
has a choice, it moves the **hottest** partition onto the **coolest** node — improving count *and*
load in one move.

### Tier 2 — load balance (secondary)
If counts are already even but one node is still much hotter than another (load skew above
`LoadImbalanceThreshold`), the planner emits a **count-neutral swap**: move a hot partition off the
hot node and a cold partition back onto it. Counts stay the same; load evens out. It only does a swap
if the swap actually *reduces* the imbalance — never churn for nothing.

### Guard rails on every move
Before a move is allowed, it must pass all of these:

- The partition is in the normal **Active** state (never move one that's splitting, draining, etc.).
- Its leader has been stable for at least `MinLeaderStabilityMs` (don't move a leadership that just
  formed — it's probably still settling).
- The target is a live, voting member that actually belongs to that partition's group.
- The partition isn't in **cooldown** from a recent move (prevents ping-ponging).
- The partition doesn't already have an **outstanding** suggestion in flight (prevents duplicates).

Plus two throttles: at most `MaxMovesPerPass` moves are planned per pass, and at most
`MaxConcurrentTransfers` moves may be in flight cluster-wide at once. Balance is reached gradually over
several passes, never in one disruptive burst.

---

## The suggestion mechanism (and why it exists)

Here's a subtle but crucial rule of Raft: **only the current leader of a partition can hand off that
partition's leadership.** A node cannot transfer leadership of a partition it doesn't lead.

But the *decider* is the P0 leader, which usually does **not** lead the partition it wants to move. So
the controller can't just do the transfer itself. Instead it sends a suggestion to whoever currently
leads the partition, and that node does the actual handoff:

```
   P0 controller                     Node that leads partition P
   (the decider)                     (the only one that can act)
        │                                       │
        │  "suggest: move P to node C"          │
        ├──────────────────────────────────────►
        │                                       │  validate:
        │                                       │   • do I still lead P?
        │                                       │   • is P Active?
        │                                       │   • is C a live voter?
        │                                       │
        │                                       │  if all yes → hand off P to C
        │                                       │  if any no  → silently ignore
```

The recipient is **authoritative**: it re-checks everything at the moment of action. If the
controller's view was stale (e.g. this node already lost leadership of P), the suggestion is simply
dropped. The actual handoff uses Kommander's normal, Raft-correct leadership-transfer path, which
itself re-validates terms and leadership. So a wrong suggestion can never cause an unsafe transfer —
only a wasted one.

> **One easy-to-miss case:** the P0 controller often *does* lead some of the overloaded partitions
> itself. When the "from" node is the controller's own node, the suggestion is delivered **in-process**
> rather than over the network — a node is not its own network peer, so a self-addressed message would
> simply be dropped. If you ever touch the dispatch code, keep this self-delivery path intact; without
> it the balancer silently ignores every partition led by the P0 node.

---

## Tracking moves without waiting

Because the controller fires suggestions and moves on, it needs a way to know later whether each one
worked. It keeps a small in-memory table of **outstanding moves** (partition → intended target +
deadline). On each pass it reconciles that table against the fresh global view:

- **Confirmed** — the view now shows the target leading the partition. Mark success, start a
  `MoveCooldown` on that partition so it isn't immediately moved again.
- **Timed out** — `SuggestionTimeout` elapsed and ownership never changed. Assume it was dropped or
  failed; clear it and let a future pass retry (still respecting cooldown).

This "fire-and-confirm-by-observation" approach means there's **no durable balancer state**. If P0
leadership moves to another node, the new controller just starts fresh from the next round of reports —
at worst one extra pass before it converges again.

---

## Using it: configuration

The balancer is **off by default**. Turn it on by setting `EnableLeaderBalancer = true` in
`RaftConfiguration` on your nodes. Everything else has sensible defaults:

| Setting | Default | What it controls |
|---|---|---|
| `EnableLeaderBalancer` | `false` | Master on/off switch. When off: no reports, no passes, zero overhead. |
| `LeaderBalancerInterval` | `30s` | How often the controller runs a balancing pass. |
| `LeaderBalancerReportInterval` | `5s` | How often each node emits its load report. |
| `LeaderBalancerReportTtl` | `20s` | A report older than this is ignored (the node is treated as silent). |
| `CountDeadband` | `1` | How far above/below the average a node may drift before it's acted on. |
| `LoadImbalanceThreshold` | `0.25` | How skewed load must be (when counts are even) to trigger a swap. |
| `MinLeaderStabilityMs` | `5000` | A leadership younger than this is not eligible to move. |
| `MoveCooldown` | `60s` | After a partition moves, how long before it can move again. |
| `MaxMovesPerPass` | `4` | Most moves planned in a single pass. |
| `MaxConcurrentTransfers` | `2` | Most moves in flight cluster-wide at once. |
| `SuggestionTimeout` | `15s` | How long to wait for a suggested move to show up before giving up on it. |
| `LeaderBalancerOpsWeight` | `1.0` | Weight of throughput in the load score. |
| `LeaderBalancerQueueWeight` | `0.5` | Weight of queue depth in the load score. |

**Tuning tips for beginners:**

- Start with defaults. They're deliberately conservative — slow and gentle.
- If rebalancing feels too sluggish, lower `LeaderBalancerInterval` or raise `MaxMovesPerPass` /
  `MaxConcurrentTransfers` — but expect more churn.
- Keep `SuggestionTimeout` comfortably larger than `LeaderBalancerReportInterval` **plus** the time it
  takes gossip to spread across your cluster. If it's too small, successful moves get mis-counted as
  failures (because their "I'm the new leader" report hasn't reached the controller yet) and get
  needlessly retried. On bigger clusters, raise it.
- `CountDeadband` and `LoadImbalanceThreshold` are your anti-oscillation knobs. Raise them if you see
  the balancer fidgeting; lower them for tighter balance at the cost of more moves.

---

## Observability

The balancer publishes metrics under the `Kommander` meter (consumable via OpenTelemetry,
`dotnet-counters`, etc.). Watch these to confirm it's working and not thrashing:

| Metric | Type | Meaning |
|---|---|---|
| `raft.balancer.moves_total` | counter | Moves tagged by outcome: `planned`, `succeeded`, `timed_out`. |
| `raft.balancer.skipped_passes_total` | counter | Passes skipped because the view was incomplete. |
| `raft.balancer.count_imbalance` | gauge | How far the most-loaded node is above the ideal count. Trends to ~0 as it converges. |
| `raft.balancer.load_imbalance` | gauge | Fractional load skew across nodes. Also trends down. |

**Reading them:**

- Healthy: a burst of `planned` then `succeeded`, and both imbalance gauges drifting toward zero.
- A high `timed_out` rate means suggestions aren't landing — recipients are dropping them, or
  `SuggestionTimeout` is too tight for your gossip latency.
- A high `skipped_passes_total` means the view is often incomplete — a node may be silent, or report
  TTL/interval are mismatched.

> The two imbalance gauges are process-global and only meaningful on the P0 leader (other nodes report
> `0`). Don't rely on them in test harnesses that run several nodes inside one process.

---

## Safety: what can and cannot go wrong

The single most important property: **the balancer is advisory and can never violate correctness.**

- Every actual leadership change goes through Kommander's normal leadership-transfer path, which
  re-validates term and leadership at the moment of the move. The balancer only *suggests*.
- A stale, dropped, or out-of-date report can only cause a **wasted or skipped** move — never an unsafe
  one. The node that owns the partition is the final authority and rejects anything that no longer
  applies.
- There's a single decider (the P0 leader), so two nodes can't issue conflicting plans.
- An incomplete picture makes the controller **wait**, not guess.
- All state is in memory; losing the controller just means the next P0 leader starts fresh.

In short: the failure modes are "didn't rebalance" or "rebalanced something pointlessly," and both
self-correct on the next pass. There is no path to data loss or split-brain through this feature.

---

## Code map — where everything lives

| Concern | Where |
|---|---|
| Per-partition load measurement (the EWMA accumulator) | `Kommander/Diagnostics/PartitionLoadAccumulator.cs` |
| Building a node's own report | `RaftManager.BuildLocalLoadReport()` |
| The report types | `Kommander/System/NodeLoadReport.cs`, `PartitionLoad.cs` |
| Reducing reports into the snapshot | `Kommander/System/GlobalLeadershipView.cs` |
| The two-tier planner (pure function) | `Kommander/System/LeaderBalancePlanner.cs`, `LeaderMove.cs` |
| The controller / balancing pass | `Kommander/System/RaftSystemCoordinator.cs` (`RunBalancerPassAsync`) |
| The suggestion message + send/receive | `Kommander/Data/TransferLeadershipSuggestionRequest.cs`, `RaftManager.SendTransferLeadershipSuggestion` / `ReceiveTransferLeadershipSuggestion` |
| The actual handoff primitive | `RaftManager.TransferLeadershipAsync` |
| Configuration knobs | `Kommander/RaftConfiguration.cs` |
| Metrics | `Kommander/Diagnostics/KommanderMetrics.cs` |

A good reading order if you're learning the code: `PartitionLoadAccumulator` → `NodeLoadReport` →
`GlobalLeadershipView` → `LeaderBalancePlanner` → `RunBalancerPassAsync`. That follows the data from
"how busy is one partition" all the way up to "what should we move."

---

## Maintaining & extending

A few principles to keep things correct as you work on this:

- **The planner must stay pure.** Keep all mutable state (cooldowns, outstanding moves) in the
  controller and pass it *in*. This is what makes the policy testable without a live cluster.
- **There is exactly one way to read ownership/load from reports** — the global view. Both planning and
  the confirm-completed-moves step must use it. Never hand-roll a second, ad-hoc owner map; it will
  drift from the real reduction (e.g. forget the TTL) and confirm moves incorrectly.
- **Preserve in-process self-delivery** of suggestions when the controller leads the partition (see the
  callout above). It's the common case, not an edge case.
- **A move's eligibility filters are load-bearing.** If you add a new partition lifecycle state or a new
  reason a move should be blocked, add it to the planner's filters *and* the recipient's validation —
  both sides check independently on purpose.
- **Keep it advisory.** Resist the temptation to make any of this durable or to have the controller
  drive transfers directly. The "suggest, then observe reality" loop is what makes the feature safe and
  self-healing.

Ideas that fit naturally on top of this design (not yet built): blending host-level CPU/memory into the
load score, operator-supplied anti-affinity hints (keep certain partitions apart), and evicting a
node's report immediately when it leaves the cluster instead of waiting for it to age out.

> **Related:** the same per-node load report also carries a per-partition **log-replication rate** and
> **WAL queue depth** signal, exposed through `IRaft` for load-based split decisions. See the
> [Per-Partition Log-Replication Rate Signal guide](partition-log-rate-signal-developer-guide.md).

---

## FAQ

**Does enabling this risk my data?**
No. It only moves *leadership*, never data, and every move goes through the same safe handoff Raft uses
internally. The balancer can only ever cause a wasted move, which self-corrects.

**Will it constantly move things around?**
No. Cooldowns, deadbands, stability gates, and per-pass caps make it gentle and convergent. Once the
cluster is balanced it goes quiet until something changes.

**What happens during a node failure or election?**
A partition with no current leader simply doesn't appear in any report, so the balancer ignores it that
pass. It never starts or speeds up elections — it only relocates leaderships that already exist.

**Why suggestions instead of the controller just doing the transfer?**
Because only a partition's own leader can hand off its leadership. The controller usually isn't that
leader, so it has to ask. This also keeps the owning node as the final authority, which is what makes
stale decisions harmless.

**It's enabled but nothing is moving — why?**
Check `raft.balancer.skipped_passes_total` (incomplete view — a node may be silent) and
`raft.balancer.moves_total{outcome=timed_out}` (suggestions not landing — possibly `SuggestionTimeout`
too small for your gossip latency). Also confirm the imbalance is actually beyond `CountDeadband`; if
the cluster is already close to even, there's nothing to do.

**Where does it run?**
Only on the leader of the system partition (P0). Other nodes just emit reports and act on suggestions
they receive.
