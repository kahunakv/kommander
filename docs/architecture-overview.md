# Kommander Architecture Overview

Welcome! This guide is the front door to Kommander. It is written for two kinds of reader:

- **Developers who want to understand the system** — what Kommander does, the vocabulary it uses, and
  how a request travels through it.
- **Developers who want to improve or contribute** — where the moving parts live, why they are shaped
  the way they are, and which invariants you must not break.

It assumes **no prior knowledge of Raft or distributed consensus**. We build the concepts from the
ground up, then walk the main runtime flows end to end. Deeper feature guides
([Dynamic Membership](dynamic-membership-developer-guide.md),
[Elastic Partitions](elastic-partitions-developer-guide.md)) pick up where this one leaves off.

---

## Table of contents

1. [The 60-second mental model](#the-60-second-mental-model)
2. [The problem Kommander solves](#the-problem-kommander-solves)
3. [Core concepts (the vocabulary)](#core-concepts-the-vocabulary)
4. [The component map](#the-component-map)
5. [Architecture flows](#architecture-flows)
   - [Flow 1 — Starting up and joining a cluster](#flow-1--starting-up-and-joining-a-cluster)
   - [Flow 2 — Electing a leader](#flow-2--electing-a-leader)
   - [Flow 3 — Writing data (the propose → replicate → commit path)](#flow-3--writing-data-the-propose--replicate--commit-path)
   - [Flow 4 — Keeping a slow follower in sync (backfill)](#flow-4--keeping-a-slow-follower-in-sync-backfill)
   - [Flow 5 — Restarting and replaying the log](#flow-5--restarting-and-replaying-the-log)
   - [Flow 6 — Compaction and snapshots](#flow-6--compaction-and-snapshots)
6. [The pluggable layers](#the-pluggable-layers)
7. [Partitions, membership, and the system partition](#partitions-membership-and-the-system-partition)
8. [Configuration you will actually touch](#configuration-you-will-actually-touch)
9. [Code map — where everything lives](#code-map--where-everything-lives)
10. [Invariants you must not break](#invariants-you-must-not-break)
11. [How to extend Kommander](#how-to-extend-kommander)
12. [Glossary](#glossary)

---

## The 60-second mental model

Kommander keeps **the same data, in the same order, on several machines**, so that the system keeps
working even when some machines fail.

It does this with the **Raft consensus protocol**. The idea in one breath:

> One node is elected **leader**. Every change is first written to an ordered, on-disk **log**. The
> leader copies new log entries to the **followers**. Once a **majority** of nodes have stored an
> entry, it is **committed** — guaranteed to survive, and applied to the application's state.

Kommander adds one twist on top of textbook Raft: it is **partitioned**. The data is sliced into
independent **partitions**, and *each partition runs its own Raft group with its own leader*. A single
node can be the leader for partition 3 and a follower for partition 7. This spreads work across the
cluster instead of funnelling everything through one leader.

```
                        A Kommander cluster of 3 nodes
   ┌────────────── Node A ──────────────┐  ┌─ Node B ─┐  ┌─ Node C ─┐
   │ Partition 1:  LEADER   ───────────────► follower ──► follower   │
   │ Partition 2:  follower ◄─────────────── LEADER   ──► follower   │
   │ Partition 3:  follower ◄─────────────── follower ◄── LEADER     │
   └─────────────────────────────────────┘  └──────────┘  └──────────┘
        each partition = an independent Raft group with its own leader & log
```

Everything else in this document is detail hanging off that sentence.

---

## The problem Kommander solves

Imagine you have a service that holds important state — account balances, a key/value store, a job
queue. If it runs on one machine and that machine dies, you lose availability and maybe data.

The obvious fix — "just run copies on several machines" — immediately raises a hard question: **when
two copies disagree, who is right?** Naively replicating writes leads to split brain (two machines
both think they are in charge), lost updates, and inconsistent reads.

**Consensus** is the formal answer to "how does a group of machines agree on a single sequence of
events despite failures and network delays." **Raft** is a consensus protocol designed to be
understandable, and Kommander is a C#/.NET implementation of it.

What Kommander gives an application:

- **Leader election** — the cluster always converges on exactly one leader per partition, and elects a
  new one automatically when the leader fails.
- **Replicated, ordered log** — every change is recorded in the same order on every node.
- **Durability** — changes are written to a **Write-Ahead Log (WAL)** on disk *before* they are
  acknowledged, so a crash does not lose committed data.
- **Strong consistency** — a committed entry is on a majority of nodes and will never be lost or
  reordered, even across leader changes.

What Kommander deliberately keeps **out** of the core and pluggable: *where* the log is stored (the WAL
backend), *how* nodes talk (the transport), and *how* nodes find each other (discovery). You pick those
per deployment.

---

## Core concepts (the vocabulary)

You will see these words everywhere in the code and the rest of this guide. Learn them once here.

| Term | What it means in Kommander |
|---|---|
| **Node** | One participant in the cluster — usually one process. Identified by an endpoint like `host:port`. |
| **Partition** | An independent Raft group. The data space is split into partitions so different leaders can make progress in parallel. Partition `0` is reserved (see below); application partitions start at `1`. |
| **Log** | The ordered, append-only sequence of entries for a partition. Each entry has an **index** (its position) and a **term**. This is the source of truth. |
| **Log entry (`RaftLog`)** | A single record in the log: an index, a term, a type, and the payload bytes. |
| **Term** | A logical clock for leadership. Every election bumps the term. Terms let nodes detect a stale leader: a message from an older term is rejected. |
| **Role** | Each node is, per partition, a **Follower**, a **Candidate** (currently running for election), or the **Leader**. See `RaftNodeState`. |
| **Quorum** | A majority of voting nodes (e.g. 2 of 3, 3 of 5). An entry is committed once a quorum has stored it. Majorities guarantee any two quorums overlap, which is what makes the protocol safe. |
| **Propose** | The leader appends a new entry and starts replicating it. Not yet durable across the cluster. |
| **Commit** | A quorum has acknowledged the entry. It is now permanent and may be applied to application state. |
| **Rollback** | An uncommitted proposal is abandoned (e.g. quorum was not reached, or the app chose to). |
| **Checkpoint** | A marker in the log saying "everything up to here is settled." Used as the floor for compaction. |
| **WAL** | Write-Ahead Log: the durable, on-disk store of log entries. Written *before* callbacks run. |
| **Learner** | A non-voting node that is catching up. It receives the log but does not count toward quorum until promoted to **Voter**. |
| **HLC** | Hybrid Logical Clock — a timestamp combining wall-clock time with a logical counter, used to order proposals causally. See `HybridLogicalClock`. |

A useful way to hold it together:

```
   index:   1     2     3     4     5     6
            ├─────┼─────┼─────┼─────┼─────┤
   term:    1     1     1     2     2     2
            └──── committed ───┘  └ proposed ┘
                                  (not yet on a quorum)
```

---

## The component map

Kommander is layered. From the application down to the disk:

```
   ┌──────────────────────────────────────────────────────────────┐
   │  Your application                                              │
   │    calls ReplicateLogs / CommitLogs, handles OnReplication*    │
   └───────────────▲───────────────────────────────┬──────────────┘
                   │ events/callbacks               │ API calls
   ┌───────────────┴───────────────────────────────▼──────────────┐
   │  RaftManager  (IRaft)  — the entry point                       │
   │    owns all partitions, the WAL, communication, discovery      │
   └───────┬───────────────────────────┬──────────────────────────┘
           │ delegates per partition    │ system coordination
   ┌───────▼─────────┐         ┌────────▼───────────────┐
   │  RaftPartition  │  ...    │  RaftSystemCoordinator  │
   │  (one per       │         │  partition 0: maps,     │
   │   partition)    │         │  membership, split/merge│
   └───────┬─────────┘         └─────────────────────────┘
           │ serializes operations
   ┌───────▼──────────────────┐   ┌────────────────┐   ┌────────────┐
   │  RaftPartitionExecutor   │   │  Communication │   │ Discovery  │
   │  (Scheduling/) one queue │   │  gRPC / REST / │   │ static /   │
   │  per partition, the      │   │  in-memory     │   │ dynamic /  │
   │  state machine logic     │   └────────────────┘   │ multicast  │
   └───────┬──────────────────┘
           │ durable reads/writes via fair I/O schedulers
   ┌───────▼──────────────────┐
   │  IWAL                     │
   │  InMemory / SQLite /      │
   │  RocksDB                  │
   └───────────────────────────┘
```

The pieces, in plain terms:

- **`RaftManager`** (implements `IRaft`, in `RaftManager.cs`) — the public entry point. You construct
  one per node. It owns the set of partitions and the pluggable WAL, communication, and discovery
  components, and it exposes the API (`ReplicateLogs`, `CommitLogs`, `AmILeader`, membership/partition
  operations, and the application events `OnReplicationReceived`, `OnLogRestored`, `OnLeaderChanged`,
  etc.).

- **`RaftPartition`** (`RaftPartition.cs`) — one per partition. It runs that partition's slice of the
  protocol: election timers, leadership state, and replication.

- **`RaftPartitionExecutor`** (`Scheduling/RaftPartitionExecutor.cs`) — the heart of a partition. Every
  operation for a partition (a client proposal, an incoming append, an election timer tick, a commit)
  is turned into an operation and pushed onto a **single bounded queue**, then processed **one at a
  time**. This "actorless" serial executor is *why the state machine is easy to reason about*: there is
  no lock soup — within a partition, exactly one thing happens at a time, in order. The actual protocol
  logic lives in `RaftPartitionStateMachine.cs`.

- **`RaftSystemCoordinator`** (`System/`) — manages **partition 0**, the special "system" partition
  that holds cluster-wide configuration: the partition map (which partition owns which key range) and
  the membership roster. It is itself a Raft group, so cluster metadata is replicated with the same
  guarantees as application data.

- **`IWAL`** (`WAL/`) — the durable log. Three backends ship: `InMemoryWAL` (tests/ephemeral),
  `SqliteWAL`, and `RocksDbWAL`. They all implement the same interface (`ReadLogs`, `Write`,
  `GetMaxLog`, `GetLastCheckpoint`, `CompactLogsOlderThan`, …) and must be thread-safe.

- **`ICommunication`** (`Communication/`) — node-to-node transport. gRPC is the primary networked
  transport; REST/JSON is a fallback and a debugging aid; in-memory is for tests.

- **`IDiscovery`** (`Discovery/`) — how a node finds peers: static lists, application-managed dynamic
  lists, or multicast for a local network.

- **Fair I/O schedulers** (`WAL/IO/`, exposed as `ReadScheduler` / `WalScheduler`) — synchronous disk
  reads and writes are funnelled through configurable fair schedulers so that slow storage work does
  not stall the partition's state transitions.

---

## Architecture flows

This is the part most worth reading carefully. Each flow follows one real scenario end to end.

### Flow 1 — Starting up and joining a cluster

```
   construct RaftManager
        │
        ▼
   JoinCluster(seeds)
        │   discovery resolves peers; transport is wired up
        ▼
   for each partition → create RaftPartition → create executor
        │
        ▼
   WAL is opened and replayed (see Flow 5) → local state restored
        │
        ▼
   election timers start → cluster converges on a leader per partition
```

1. The application builds a `RaftManager` with a configuration (node id, host/port, partition count,
   chosen WAL/transport/discovery).
2. `JoinCluster(...)` brings the node online: discovery resolves the peer set, the transport starts
   listening, and partitions are created.
3. Each partition opens its WAL and **replays** any existing log so the node restarts where it left off.
4. Election timers begin. With no leader, a node eventually times out and starts an election (Flow 2).

A brand-new node added to a *running* cluster joins as a non-voting **Learner**, catches up via
backfill/snapshot, and is auto-promoted once it is close enough and stable — that lifecycle is the
subject of the [Dynamic Membership guide](dynamic-membership-developer-guide.md).

### Flow 2 — Electing a leader

Leadership is per partition. A follower that has not heard from a leader within a randomized election
timeout (between `StartElectionTimeout` and `EndElectionTimeout`) suspects the leader is gone and acts:

```
   Follower's election timer fires
        │
        ▼
   becomes Candidate, increments term, votes for itself
        │
        ▼
   RequestVote → all peers
        │
        ├── peers grant a vote if: candidate's term ≥ theirs
        │   AND candidate's log is at least as up to date
        │   AND they have not already voted this term
        ▼
   received votes from a QUORUM?
        │                 │
        yes               no / split vote
        ▼                 ▼
   becomes LEADER     timeout again with a new random delay, retry
        │
        ▼
   sends heartbeats (empty appends) to assert leadership
```

Two details that matter:

- **Randomized timeouts** make simultaneous candidacies (split votes) rare, so an election usually
  settles in one round.
- **The "log up to date" check** is a safety rule: a node will not vote for a candidate whose log is
  behind its own. This guarantees a new leader already has every committed entry — the protocol never
  has to "un-commit" anything. Kommander also uses a **PreVote** step to stop a flapping node from
  disrupting a healthy leader.

### Flow 3 — Writing data (the propose → replicate → commit path)

This is the core write path and the one to understand best.

```
   App calls ReplicateLogs(partition, type, data, autoCommit: true)
        │
        ▼  (must be on the leader for that partition)
   LEADER: append entry to its own WAL  ── index N, current term
        │
        ▼
   LEADER → followers:  AppendLogs(entry N)
        │
        ├──► Follower 1: write to WAL → reply CompleteAppendLogs(ack)
        ├──► Follower 2: write to WAL → reply CompleteAppendLogs(ack)
        │
        ▼
   LEADER counts acks. Quorum reached (incl. itself)?
        │
        yes
        ▼
   COMMIT index N  → run OnReplicationReceived on every node
        │            (the application applies the change to its state)
        ▼
   ReplicateLogs returns success to the caller
```

Step by step:

1. The application calls `ReplicateLogs` on the **leader** (proposing on a follower fails — leadership
   is checked). It passes a `type` string and the payload `byte[]` (or a batch of payloads).
2. The leader assigns the entry the next index and the current term, and **writes it to its own WAL
   first** (write-ahead — durability before acknowledgement).
3. The leader sends `AppendLogs` to the followers. Each follower writes the entry to its WAL and
   replies with `CompleteAppendLogs`.
4. The leader tallies acknowledgements. The quorum-tracking lives in
   `RaftProposalQuorum`/`RaftSyncProposalQuorum`. Once a **majority** (counting the leader itself) has
   the entry, it is **committed**.
5. On commit, every node fires `OnReplicationReceived` so the application can apply the change to its
   own state machine. `ReplicateLogs` returns a `RaftReplicationResult`.

**Auto-commit vs. manual commit.** With `autoCommit: true` (the default) the leader commits as soon as
quorum is reached. With `autoCommit: false`, the proposal stays pending and the application explicitly
calls `CommitLogs(partition, ticketId)` or abandons it with `RollbackLogs(...)`. The `ticketId` is an
HLC timestamp identifying the proposal — that is how HLCs give you causal ordering of proposals.

Everything inside a partition is serialized by its executor (the bounded queue), so even though
proposals, appends, and commits arrive concurrently from the network, the state machine processes them
one at a time, in a well-defined order.

### Flow 4 — Keeping a slow follower in sync (backfill)

A follower can fall behind — it was slow, paused, briefly partitioned, or just joined. The leader
detects this and **backfills** the missing entries, separately from the live broadcast path.

```
   LEADER tracks, per follower, how far behind it is
        │
        ▼
   follower's gap  >  BackfillThreshold  ?
        │
        yes
        ▼
   LEADER reads the missing range from its WAL (bounded by
   MaxBackfillEntriesPerRound) and sends it as an anchored append:
        "here are entries from index F+1, and I assert that you already
         hold entry F with term T"   ← the Log Matching anchor
        │
        ▼
   FOLLOWER checks the anchor:
        ├── I hold (F, T)?  → append the batch, advance, ack
        └── mismatch?       → reject; leader backs up and retries lower
        │
        ▼
   repeat in bounded rounds until the follower is caught up
```

The key design choices, so you do not get surprised when reading the code:

- **Backfill is leader-driven and bounded.** The leader catches a follower up in chunks of at most
  `MaxBackfillEntriesPerRound`, riding alongside normal heartbeat traffic, instead of dumping the whole
  log at once.
- **The Log Matching anchor is enforced on the backfill path only.** A backfill batch says "you should
  already have entry F at term T before I give you F+1," and the follower refuses if that does not hold
  — this is what prevents a follower from ever growing a *gap* or keeping a *divergent* tail. The live
  broadcast path (ordinary proposals/commits) is intentionally not anchored, because anchoring the live
  path can reject transiently-behind followers with no recovery and stall progress.
- **If a follower is so far behind that the leader has already compacted the entries it needs**, backfill
  cannot help (the data is gone). The leader signals `SnapshotRequired`, and catch-up switches to
  installing a snapshot instead (Flow 6).

For a full walkthrough, see the [Log Catch-Up & Backfill Developer Guide](log-backfill-developer-guide.md).

### Flow 5 — Restarting and replaying the log

Because the WAL is durable, a restart is not a blank slate.

```
   process starts → RaftManager → open WAL
        │
        ▼
   OnRestoreStarted(partition)
        │
        ▼
   read persisted entries  →  for each committed entry:
        fire OnLogRestored(partition, entry)
        (application rebuilds its state machine from the log)
        │
        ▼
   OnRestoreFinished(partition) → node rejoins, participates in elections
```

The application's job is to make `OnLogRestored` rebuild its in-memory state from each entry, so that
after replay the node is exactly where it was before the crash. Only then does it start participating
again. This is why the WAL must record entries *before* callbacks run: the log, not memory, is the
authority that survives a crash.

### Flow 6 — Compaction and snapshots

A log that only ever grows would eventually fill the disk. Two mechanisms keep it bounded:

- **Compaction.** Once an entry is below the last **checkpoint** (everything before it is settled), its
  history is removable. Counters trigger bounded, per-partition compaction: roughly every
  `CompactEveryOperations` committed operations the partition trims old entries in capped batches
  (`CompactNumberEntries`, `MaxEntriesPerCompaction`) via `IWAL.CompactLogsOlderThan`. "Bounded" is
  deliberate — a compaction pass never blocks the partition for an unbounded time.

- **Snapshots.** Compaction creates a **compaction floor**: the earliest index the leader still has. If
  a follower needs entries *below* that floor, the leader can no longer replay them. Instead it installs
  a **snapshot** — a compact representation of the state up to a point — and the follower resumes normal
  replication from there. This is the escape hatch backfill (Flow 4) hands off to.

```
   log grows ──► checkpoint marks settled prefix ──► compaction trims below it
                                                          │
                          creates a "compaction floor" ───┘
                                                          │
   follower needs entries below the floor? ──► they're gone ──► install SNAPSHOT
```

---

## The pluggable layers

A defining property of Kommander is that the consensus core does not care *how* it stores, talks, or
discovers. Three seams let you swap implementations:

**Storage — `IWAL`**

| Backend | Use it for |
|---|---|
| `InMemoryWAL` | tests and ephemeral nodes; nothing survives a restart |
| `SqliteWAL` | a simple embedded durable store |
| `RocksDbWAL` | higher-throughput durable storage |

All three honour the same conformance contract (the tests in `Kommander.Tests/WAL/`). If you write a
new backend, that contract is your spec.

**Transport — `ICommunication`**

| Transport | Use it for |
|---|---|
| gRPC | primary networked clusters (Protobuf definitions in `Communication/Grpc/Protos/`) |
| REST/JSON | HTTP integration and human-readable debugging |
| in-memory | fast deterministic tests, no sockets |

Node-to-node traffic can be authenticated with a shared-secret HMAC and optional server certificate
pinning (see `RaftTransportAuthenticator`).

**Discovery — `IDiscovery`**

Static (fixed list), dynamic (application-managed), or multicast (local-network). Note: discovery feeds
*peer connectivity*, but in a dynamically-membered cluster it is **not** the source of who may vote —
that comes from the committed roster (next section).

---

## Partitions, membership, and the system partition

Two cluster-wide concerns are themselves stored as replicated Raft state on **partition 0**, the
system partition managed by `RaftSystemCoordinator`:

- **The partition map** — which partition owns which key range. Partitions can be **created, split,
  merged, and removed at runtime** without restarting any node. Because the map is replicated through
  P0, every node converges on every change, and two-phase split/merge ensures no key range is ever left
  uncovered. Full details: [Elastic Partitions guide](elastic-partitions-developer-guide.md).

- **The membership roster** — the authoritative, versioned list of nodes and their roles (Learner vs.
  Voter). It is a *committed Raft record*, never derived from gossip or discovery, so quorum math is
  always consistent. A SWIM-style gossip layer spreads the roster faster and flags dead nodes, but it
  is strictly advisory — only a committed membership change actually alters who can vote. Full details:
  [Dynamic Membership guide](dynamic-membership-developer-guide.md).

The principle shared by both: **truth by consensus, dissemination by gossip.** Anything that affects
safety (who votes, who owns a key range) is a committed log entry; gossip only makes that truth travel
faster.

To route a key to its partition, use `GetPartitionKey(key)` (or `GetPrefixPartitionKey`) and then call
`ReplicateLogs` against that partition.

---

## Configuration you will actually touch

Configuration lives in `RaftConfiguration.cs`. You do not need most of it; these are the knobs that
come up in practice. Defaults shown.

**Identity & topology**

| Setting | Default | What it does |
|---|---|---|
| `NodeId` / `NodeName` / `Host` / `Port` | — | how this node identifies and exposes itself |
| `InitialPartitions` | `1` | how many partitions to start with |

**Election & heartbeats** (timing — adjust together for your network's latency)

| Setting | Default | What it does |
|---|---|---|
| `HeartbeatInterval` | 500 ms | how often a leader asserts leadership |
| `StartElectionTimeout` / `EndElectionTimeout` | 2000 / 4000 ms | randomized window before a follower starts an election |
| `VotingTimeout` | 1500 ms | how long a candidate waits for votes |

**Throughput & I/O**

| Setting | Default | What it does |
|---|---|---|
| `ReadIOThreads` / `WriteIOThreads` | 8 / 4 | fair-scheduler threads for WAL reads/writes |
| `MaxWalBatchSize` | 256 | max entries coalesced into one WAL write |
| `MaxQueuedClientProposalsPerPartition` | 2048 | backpressure on client proposals |

**Catch-up & compaction**

| Setting | Default | What it does |
|---|---|---|
| `BackfillThreshold` | 10 | how far behind a follower must be before backfill kicks in |
| `MaxBackfillEntriesPerRound` | 128 | cap on entries sent per backfill round |
| `CompactEveryOperations` | 10000 | committed-op interval that triggers compaction |
| `CompactNumberEntries` / `MaxEntriesPerCompaction` | 100 / 5000 | per-pass compaction batch sizing |

**Membership & gossip** (only relevant with dynamic membership)

| Setting | Default | What it does |
|---|---|---|
| `LearnerPromotionLag` / `LearnerPromotionStableWindow` | 10 / 3 s | how caught-up and stable a learner must be before promotion |
| `GossipInterval` / `GossipFanout` | 5 s / 2 | how often and to how many peers gossip spreads |
| `SuspicionTimeout` / `DeadMemberEvictionGrace` | 5 s / 30 s | SWIM failure-detection timing |

---

## Code map — where everything lives

```
Kommander/
├─ RaftManager.cs              entry point (IRaft); owns partitions + plugins
├─ IRaft.cs                    the public API surface
├─ RaftPartition.cs            per-partition state machine driver
├─ RaftPartitionStateMachine.cs   the actual protocol logic (election, replicate, commit)
├─ RaftConfiguration.cs        every tunable knob
├─ RaftNodeState.cs            Follower / Candidate / Leader
├─ RaftProposalQuorum*.cs      quorum accounting for a proposal
├─ RaftWriteAhead.cs           the WAL-facing replication helpers
├─ Scheduling/                 the serial per-partition executor + operation types
│   └─ RaftPartitionExecutor.cs   one bounded queue per partition
├─ WAL/                        durable log
│   ├─ IWAL.cs   InMemoryWAL.cs   SqliteWAL.cs   RocksDbWAL.cs
│   └─ IO/                     fair read/write schedulers
├─ Communication/             transport
│   ├─ Grpc/   Rest/   Memory/
├─ Discovery/                 static / dynamic / multicast peer discovery
├─ Gossip/                    SWIM gossip + failure detection (advisory)
└─ System/                    RaftSystemCoordinator: partition map, membership, split/merge

Kommander.Server/   ASP.NET Core host exposing Kommander as a gRPC/REST service
Kommander.Tests/    NUnit suite: clusters, WAL conformance, scheduler, safety
docs/               this guide + feature deep-dives
```

Reading order if you are new: `IRaft.cs` (what it does) → this guide's flows →
`RaftPartitionStateMachine.cs` (how a partition behaves) → `Scheduling/RaftPartitionExecutor.cs` (how
operations are serialized) → a WAL backend.

---

## Invariants you must not break

These are the load-bearing rules. Violating one is how you introduce data loss or split brain.

1. **Write-ahead, always.** An entry is written to the WAL *before* it is acknowledged or applied. The
   log, not memory, is the authority.
2. **Commit means quorum.** Never apply or report an entry as committed before a majority of voters
   holds it. Two quorums always overlap; that overlap is the entire safety argument.
3. **No gaps, no divergent tails.** A follower's log must stay contiguous and consistent with the
   leader's. The Log Matching anchor on the backfill path enforces this; do not "fill over" a gap.
4. **One thing at a time per partition.** Partition state is mutated only through its executor's serial
   queue. Do not reach around it with ad-hoc locks or background mutation.
5. **Truth by consensus.** Anything affecting safety — the voter set, the partition map — is a committed
   record on the system partition, never gossip- or discovery-derived.
6. **WAL backends are thread-safe and conformant.** A new backend must pass the WAL conformance tests
   unchanged.

---

## How to extend Kommander

A few common contribution shapes and where to start:

- **A new WAL backend** → implement `IWAL`, keep it thread-safe, and make it pass
  `Kommander.Tests/WAL/`. Use `SqliteWAL` as the reference for shape.
- **A new transport** → implement `ICommunication`; mirror the gRPC/REST request/response types. The
  in-memory transport is the simplest reference.
- **Protocol changes** → live in `RaftPartitionStateMachine.cs` and the `Scheduling/` operation types.
  Because the executor serializes everything, reason about your change as "what new operation kind is
  enqueued, and how is it handled in order."
- **Tests** → NUnit 3. Mark anything touching shared in-process clusters or timing-sensitive behavior
  `[NonParallelizable]`. **Never run multiple `dotnet test` commands at once** — the suite spins up
  in-process Raft clusters and concurrent runs produce false failures.

Build and test:

```sh
dotnet restore Kommander.sln
dotnet build  Kommander.sln -c Debug
dotnet test   Kommander.Tests/Kommander.Tests.csproj
# targeted, while iterating:
dotnet test   Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~YourTest
```

When you change WAL formats, partition state shape, or the scheduling protocol, update the affected
tests *in the same change*, and add or update the XML `<summary>` on any key class/method whose
invariant you touched. (See `CLAUDE.md` for the full contributor checklist.)

---

## Glossary

- **Backfill** — leader-driven, bounded catch-up of a follower that has fallen behind.
- **Candidate** — a node currently standing for election in a term.
- **Checkpoint** — a log marker indicating the prefix before it is settled; the floor for compaction.
- **Commit** — the point at which a quorum holds an entry, making it permanent.
- **Compaction** — trimming removable history below the last checkpoint to bound the log's size.
- **Compaction floor** — the earliest index still retained after compaction.
- **Consensus** — agreement by a group of nodes on a single ordered sequence of events despite failures.
- **Follower** — a node that replicates the leader's log and does not propose.
- **HLC (Hybrid Logical Clock)** — a timestamp blending wall-clock time and a logical counter to order proposals.
- **Leader** — the single node per partition that accepts proposals and drives replication.
- **Learner** — a non-voting node catching up before promotion to Voter.
- **Log / log entry** — the ordered, append-only record of changes; each entry has an index and a term.
- **Log Matching** — the rule that a follower accepts an entry only if it holds the immediately preceding entry at the expected term.
- **Partition** — an independent Raft group; data is split across partitions for parallelism.
- **Quorum** — a majority of voting nodes.
- **Roster / membership** — the committed, versioned list of cluster members and their roles.
- **Snapshot** — a compact state representation used to catch up a follower past the compaction floor.
- **System partition (P0)** — the reserved partition holding the partition map and membership roster.
- **Term** — a monotonically increasing logical clock for leadership.
- **Voter** — a member that counts toward quorum.
- **WAL (Write-Ahead Log)** — the durable on-disk store of log entries, written before callbacks run.

---

*This document is an overview. For the membership lifecycle see the
[Dynamic Membership Developer Guide](dynamic-membership-developer-guide.md); for runtime partition
changes see the [Elastic Partitions Developer Guide](elastic-partitions-developer-guide.md).*
