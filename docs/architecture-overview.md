# Kommander Architecture Overview

This guide introduces Kommander to developers who use it or contribute to it. It assumes **no prior
knowledge of Raft or distributed consensus**. It defines the concepts and then follows the main runtime
flows from start to end.

---

## Table of contents

1. [Summary](#summary)
2. [Purpose and scope](#purpose-and-scope)
3. [Core concepts](#core-concepts)
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
8. [Common configuration](#common-configuration)
9. [Code map](#code-map)
10. [Invariants you must not break](#invariants-you-must-not-break)
11. [How to extend Kommander](#how-to-extend-kommander)

---

## Summary

Kommander stores **the same data, in the same order, on several machines**. The system continues to work
when some of the machines fail.

It uses the **Raft consensus protocol**. The nodes elect one **leader**. The leader writes each change to
an ordered, on-disk **log** and copies the new entries to the **followers**. When a **majority** of nodes
has stored an entry, the entry is **committed**. A committed entry survives failures and is applied to the
application's state.

Kommander divides the data into independent **partitions**. Each partition is a separate Raft group with a
separate leader, and one node can lead partition 3 and follow partition 7. This spreads the work across
the cluster.

```
                        A Kommander cluster of 3 nodes
   ┌────────────── Node A ──────────────┐  ┌─ Node B ─┐  ┌─ Node C ─┐
   │ Partition 1:  LEADER   ───────────────► follower ──► follower   │
   │ Partition 2:  follower ◄─────────────── LEADER   ──► follower   │
   │ Partition 3:  follower ◄─────────────── follower ◄── LEADER     │
   └─────────────────────────────────────┘  └──────────┘  └──────────┘
        each partition = an independent Raft group with its own leader & log
```

---

## Purpose and scope

When several machines store copies of the same state, they need a rule for which copy is correct. Simple
replication leads to lost updates and to split brain, where two machines both act as the leader.
**Consensus** lets a group of machines agree on one sequence of events despite failures and network
delays.

Kommander gives an application these guarantees.

- **Leader election.** The cluster converges on one leader per partition and elects a new one when the
  leader fails.
- **Replicated, ordered log.** All nodes record the changes in the same order.
- **Durability.** Changes are written to a **Write-Ahead Log (WAL)** on disk *before* they are
  acknowledged. A crash does not lose committed data.
- **Strong consistency.** A committed entry is on a majority of nodes. It is not lost or reordered when
  the leader changes.

The consensus core does not include [storage, transport, or discovery](#the-pluggable-layers). You select an
implementation of each one for your deployment.

---

## Core concepts

| Term | What it means in Kommander |
|---|---|
| **Node** | One participant in the cluster, usually one process. Its identity is an endpoint such as `host:port`. |
| **Partition** | An independent Raft group. Partition `0` is reserved for the system. Application partitions start at `1`. |
| **Log** | The ordered, append-only sequence of entries of a partition. |
| **Log entry (`RaftLog`)** | A record in the log. It has an **index** (its position), a **term**, a type, and the payload bytes. |
| **Term** | A logical clock for leadership. Each election increments it. A message from an older term identifies a stale leader and is rejected. |
| **Role** | Per partition, a node is a **Follower**, a **Candidate** (in an election), or the **Leader**. See `RaftNodeState`. |
| **Quorum** | A majority of the voting nodes, for example 2 of 3 or 3 of 5. Any two quorums overlap, and the safety of the protocol depends on that overlap. |
| **Propose** | The leader appends a new entry and starts to replicate it. The entry is not yet durable across the cluster. |
| **Commit** | A quorum has acknowledged the entry. It is permanent and can be applied to the application state. |
| **Rollback** | An uncommitted proposal is abandoned, for example when the quorum was not reached. |
| **Checkpoint** | A log marker that shows that the entries before it are settled. It is the floor for compaction. |
| **WAL** | Write-Ahead Log. The durable store of log entries, written *before* callbacks run. |
| **Learner** | A non-voting node that receives the log. It does not count toward quorum until it is promoted to **Voter**. |
| **HLC** | Hybrid Logical Clock. A timestamp that combines wall-clock time with a logical counter to order proposals causally. See `HybridLogicalClock`. |

```
   index:   1     2     3     4     5     6
            ├─────┼─────┼─────┼─────┼─────┤
   term:    1     1     1     2     2     2
            └──── committed ───┘  └ proposed ┘
                                  (not yet on a quorum)
```

---

## The component map

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

- **`RaftManager`** (implements `IRaft`) is the public entry point, with one instance per node. It owns the
  partitions and the pluggable components. It exposes the API (`ReplicateLogs`,
  `CommitLogs`, `AmILeader`, membership and partition operations) and the application events
  (`OnReplicationReceived`, `OnLogRestored`, `OnLeaderChanged`).
- **`RaftPartition`** drives the protocol for one partition.
- **`RaftPartitionExecutor`** (`Scheduling/`) is the serial executor of a partition. Client proposals,
  inbound appends, timer ticks, and commits become queued operations that execute **one at a time**. This
  means that the state machine needs no locks. The protocol logic is in `RaftPartitionStateMachine.cs` and
  in the classes of `Consensus/`.
- **`RaftSystemCoordinator`** (`System/`) manages **partition 0**, which stores the partition map (which
  partition owns which key range) and the membership roster. Partition 0 is also a Raft group, and cluster
  metadata has the same guarantees as application data.
- **`IWAL`** (`WAL/`) is the durable log (`ReadLogs`, `Write`, `GetMaxLog`, `GetLastCheckpoint`,
  `CompactLogsOlderThan`, …). Implementations must be thread-safe.
- **`ICommunication`** (`Communication/`) is the node-to-node transport.
- **`IDiscovery`** (`Discovery/`) finds peers.
- **Fair I/O schedulers** (`WAL/IO/`, exposed as `ReadScheduler` and `WalScheduler`) process disk reads and
  writes on separate threads. Slow storage does not stall the state transitions of a partition.

---

## Architecture flows

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

A new node that joins a *running* cluster starts as a non-voting **Learner**. It catches up through backfill
or a snapshot, and is promoted automatically when it is close enough and stable. For more information, see
the [Dynamic Membership guide](dynamic-membership-developer-guide.md).

### Flow 2 — Electing a leader

A follower starts an election when it does not hear from a leader within a randomized timeout, between
`StartElectionTimeout` and `EndElectionTimeout`.

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

- **Randomized timeouts** make simultaneous candidacies (split votes) rare, and an election usually settles
  in one round.
- **The "log up to date" check** is a safety rule. A node does not vote for a candidate whose log is behind
  its own log. This guarantees that a new leader already has all committed entries.
- **PreVote** stops a flapping node from disrupting a healthy leader.

### Flow 3 — Writing data (the propose → replicate → commit path)

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

1. The application calls `ReplicateLogs` on the **leader** with a `type` string and a `byte[]` payload, or a
   batch of payloads. A call on a follower fails.
2. The leader assigns the next index and the current term, and **writes the entry to its own WAL first**.
3. Each follower writes the entry to its WAL and replies with `CompleteAppendLogs`.
4. The leader counts the acknowledgements in `RaftProposalQuorum`. The entry is **committed** when a
   **majority**, which includes the leader, has it.
5. On commit, each node fires `OnReplicationReceived`. `ReplicateLogs` then returns a
   `RaftReplicationResult`. A follower learns of the commit from the leader's commit broadcast. It
   acknowledges that append first and applies the entries in separate executor turns
   (`FollowerApplyInOwnTurn`). The next proposal's append does not wait for the callbacks.

With `autoCommit: true` (the default), the leader commits when the quorum is reached. With
`autoCommit: false`, the proposal is pending until the application calls
`CommitLogs(partition, ticketId)` or `RollbackLogs(...)`. The `ticketId` is the HLC timestamp of the
proposal.

### Flow 4 — Keeping a slow follower in sync (backfill)

A follower can fall behind, for example when it is slow or has just joined. The leader **backfills** the
missing entries on a path that is separate from the live broadcast.

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

- **Backfill is leader-driven and bounded.** Each chunk has at most `MaxBackfillEntriesPerRound` entries and
  is sent with normal heartbeat traffic.
- **The Log Matching anchor is enforced on the backfill path only.** It prevents a *gap* or a *divergent*
  tail in a follower's log. The live broadcast path is intentionally not anchored. An anchor there can
  reject a transiently-behind follower that has no way to recover, and progress stalls.
- **A follower below the compaction floor needs a snapshot (Flow 6).** The leader detects the empty backfill
  read at the floor and switches to a snapshot on its heartbeat path. There is no wire status for this
  handoff. A snapshot that cannot be produced or delivered is retried with exponential backoff and is
  visible in `IRaft.GetSnapshotStatuses`.

For more information, see the [Log Catch-Up & Backfill Developer Guide](log-backfill-developer-guide.md).

### Flow 5 — Restarting and replaying the log

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

Your `OnLogRestored` handler must rebuild the in-memory state from each entry. The node participates in
elections only after the replay ends. The WAL must record entries *before* callbacks run, as only the log
survives a crash.

### Flow 6 — Compaction and snapshots

- **Compaction.** Entries below the last **checkpoint** are removable. After about
  `CompactEveryOperations` committed operations, the partition trims old entries in capped batches
  (`CompactNumberEntries`, `MaxEntriesPerCompaction`) through `IWAL.CompactLogsOlderThan`. The caps limit
  how long a pass can block the partition.
- **Snapshots.** Compaction creates a **compaction floor**, the earliest index that the leader has. A
  follower that needs entries *below* the floor gets a **snapshot**, a compact representation of the state
  up to a point. Normal replication resumes from there.

```
   log grows ──► checkpoint marks settled prefix ──► compaction trims below it
                                                          │
                          creates a "compaction floor" ───┘
                                                          │
   follower needs entries below the floor? ──► they're gone ──► install SNAPSHOT
```

---

## The pluggable layers

**Storage — `IWAL`**

| Backend | Use it for |
|---|---|
| `InMemoryWAL` | tests and ephemeral nodes; the log does not survive a restart |
| `SqliteWAL` | a simple embedded durable store |
| `RocksDbWAL` | higher-throughput durable storage |

All backends must pass the conformance tests in `Kommander.Tests/WAL/`.

**Transport — `ICommunication`**

| Transport | Use it for |
|---|---|
| gRPC | primary networked clusters (Protobuf definitions in `Communication/Grpc/Protos/`) |
| REST/JSON | HTTP integration and human-readable debugging |
| in-memory | fast deterministic tests, no sockets |

Node-to-node traffic can be authenticated with a shared-secret HMAC and optional server certificate
pinning (see `RaftTransportAuthenticator`).

**Discovery — `IDiscovery`**

Discovery is static (a fixed list), dynamic (application-managed), or multicast (a local network). It feeds
*peer connectivity* only. In a cluster with dynamic membership, the committed roster decides who can vote.

---

## Partitions, membership, and the system partition

`RaftSystemCoordinator` stores two cluster-wide records on **partition 0** (P0), the system partition.

- **The partition map** shows which partition owns which key range. You can create and
  remove partitions at runtime, and you can split or merge them. No node restarts. A two-phase protocol for splits and merges keeps
  all key ranges covered. See the [Elastic Partitions guide](elastic-partitions-developer-guide.md).
- **The membership roster** is the versioned list of nodes and their roles (Learner or Voter). A SWIM-style
  gossip layer spreads the roster faster and flags dead nodes, but it is advisory. Only a committed
  membership change alters who can vote. See the
  [Dynamic Membership guide](dynamic-membership-developer-guide.md).

To route a key, find its partition with `GetPartitionKey(key)` or `GetPrefixPartitionKey`. Then call
`ReplicateLogs` on that partition.

---

## Common configuration

The options are in `RaftConfiguration.cs`. The tables show the ones that deployments change most.

**Identity & topology**

| Setting | Default | What it does |
|---|---|---|
| `NodeId` / `NodeName` / `Host` / `Port` | — | how this node identifies and exposes itself |
| `InitialPartitions` | `1` | how many partitions to start with |

**Election & heartbeats** (adjust these together for the latency of your network)

| Setting | Default | What it does |
|---|---|---|
| `HeartbeatInterval` | 500 ms | how often a leader asserts leadership |
| `StartElectionTimeout` / `EndElectionTimeout` | 2000 / 4000 ms | randomized window before a follower starts an election |
| `VotingTimeout` | 1500 ms | how long a candidate waits for votes |

**Throughput & I/O**

| Setting | Default | What it does |
|---|---|---|
| `ReadIOThreads` / `WriteIOThreads` | 8 / 4 | fair-scheduler threads for WAL reads/writes |
| `IdleWorkerMaxSpinBeforePark` | 20 µs | longest spin of one idle worker per pool (executor pool, WAL write and read schedulers) before it parks |
| `MaxWalBatchSize` | 256 | max entries coalesced into one WAL write |
| `MaxQueuedClientProposalsPerPartition` | 2048 | backpressure on client proposals |

**Catch-up & compaction**

| Setting | Default | What it does |
|---|---|---|
| `BackfillThreshold` | 10 | how far behind a follower must be before backfill starts |
| `MaxBackfillEntriesPerRound` | 128 | cap on entries sent per backfill round |
| `CompactEveryOperations` | 10000 | committed-op interval that triggers compaction |
| `CompactNumberEntries` / `MaxEntriesPerCompaction` | 100 / 5000 | per-pass compaction batch sizing |

**Membership & gossip** (only relevant with dynamic membership)

| Setting | Default | What it does |
|---|---|---|
| `LearnerPromotionLag` / `LearnerPromotionStableWindow` | 10 / 3 s | how caught-up and stable a learner must be before promotion |
| `GossipInterval` / `GossipFanout` | 5 s / 2 | how often and to how many peers gossip spreads |
| `SuspicionTimeout` / `DeadMemberEvictionGrace` | 5 s / 10 min | SWIM failure-detection timing; set the grace to the longest outage that the roster must survive intact |

---

## Code map

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
│   └─ RaftPartitionExecutor.cs   one serial executor per partition
├─ WAL/                        durable log
│   ├─ IWAL.cs   InMemoryWAL.cs   SqliteWAL.cs   RocksDbWAL.cs
│   └─ IO/                     fair read/write schedulers
├─ Communication/             transport
│   ├─ Grpc/   Rest/   Memory/
├─ Discovery/                 static / dynamic / multicast peer discovery
├─ Gossip/                    SWIM gossip + failure detection (advisory)
└─ System/                    RaftSystemCoordinator: partition map, membership, split/merge

Kommander.Server/   ASP.NET Core host exposing Kommander as a gRPC/REST service
Kommander.Tests/    xUnit suite: clusters, WAL conformance, scheduler, safety
docs/               this guide + feature deep-dives
```

If you are new, start with `IRaft.cs` and the flows in this guide. Continue with
`RaftPartitionStateMachine.cs` and `Scheduling/RaftPartitionExecutor.cs`, and end with a WAL backend.

---

## Invariants you must not break

A violation of these rules can cause data loss or split brain.

1. **Write-ahead.** An entry is written to the WAL *before* it is acknowledged or applied.
2. **Commit means quorum.** Do not apply or report an entry as committed before a majority of voters has it.
3. **No gaps and no divergent tails.** A follower's log must be contiguous and consistent with the leader's
   log. Do not "fill over" a gap. The Log Matching anchor on the backfill path enforces this rule.
4. **One thing at a time per partition.** Change partition state only through the serial queue of its
   executor. Do not use ad-hoc locks or background mutation.
5. **Truth by consensus.** The voter set and the partition map are committed records on the system
   partition. Do not derive them from gossip or discovery.
6. **WAL backends are thread-safe and conformant.** A new backend must pass the WAL conformance tests
   unchanged.

---

## How to extend Kommander

- **A new WAL backend:** implement `IWAL` and pass `Kommander.Tests/WAL/`. Use `SqliteWAL` as the reference.
- **A new transport:** implement `ICommunication` and mirror the gRPC/REST request and response types. The
  in-memory transport is the simplest reference.
- **Protocol changes:** the logic is in `RaftPartitionStateMachine.cs` and `Consensus/`. The operation types are in
  `Scheduling/`. Identify the new operation kind that is enqueued and how it is handled in order.
- **Tests:** the suite uses xUnit v3. Add tests that touch shared in-process clusters or timing-sensitive
  behavior to `[Collection(ClusterIntegrationCollection.Name)]`. **Do not run two `dotnet test` commands at
  once.** The suite starts in-process Raft clusters, and concurrent runs produce false failures.

```sh
dotnet restore Kommander.sln
dotnet build  Kommander.sln -c Debug
dotnet test   Kommander.Tests/Kommander.Tests.csproj
# targeted, while iterating:
dotnet test   Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~YourTest
```

A change to WAL formats, partition state shape, or the scheduling protocol includes the affected tests. Also update the XML `<summary>` of the classes and methods whose invariant changed.
