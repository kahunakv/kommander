# Kommander (Raft Consensus)

Kommander is an open-source distributed consensus library for C#/.NET. It uses the Raft protocol to provide leader election, partitioned log replication, durable write-ahead logging, and cluster coordination for replicated services.

Kommander is designed to keep the consensus core separate from storage, discovery, and transport concerns. Applications can choose RocksDB, SQLite, or in-memory WAL implementations; static, dynamic, or multicast discovery; and gRPC, REST/JSON, or in-memory communication depending on their deployment and testing needs.

[![NuGet](https://img.shields.io/nuget/v/Kommander.svg?style=flat-square)](https://www.nuget.org/packages/Kommander)
[![NuGet](https://img.shields.io/nuget/dt/Kommander)](https://www.nuget.org/packages/Kommander)

> [!WARNING]
> Kommander is beta software. APIs and operational behavior may still change between releases.

## Features

- **Raft consensus algorithm:** Per-partition leader election, quorum-based proposal replication, commits, rollbacks, checkpoints, and leader change notifications.
- **Partitioned replication:** Nodes can lead some partitions and follow others, allowing application data to be distributed across independent Raft groups. Partition `0` is reserved for replicated system configuration; application partitions start at `1`.
- **Elastic partitions:** Create, split, merge, and remove partitions at runtime without restarting any node. The partition map is replicated through the system partition so every node converges on every change, and two-phase protocols ensure no key range is ever uncovered during a split or merge. See [Elastic Partitions Developer Guide](docs/elastic-partitions-developer-guide.md).
- **Dynamic cluster membership:** Add and remove nodes from a running cluster. The voting roster is a committed, versioned record on the system partition (never gossip- or discovery-derived), so quorum is always consistent. New nodes join as non-voting learners, catch up, and are auto-promoted to voters; a SWIM-style gossip layer disseminates the roster and detects failures advisorily. See [Dynamic Membership Developer Guide](docs/dynamic-membership-developer-guide.md).
- **Log catch-up & backfill:** A leader detects a follower that has fallen behind and backfills the missing entries in bounded, anchored chunks, enforcing the Raft Log Matching Property so a follower's log never grows a gap or a divergent tail. Followers too far behind the compaction floor are caught up with a snapshot instead. See [Log Catch-Up & Backfill Developer Guide](docs/log-backfill-developer-guide.md).
- **Partition quiescence:** An idle leader stops sending per-partition heartbeats after a configurable idle window and sends followers a one-time quiesce marker; followers then gate elections on the SWIM failure detector instead of a per-partition timer. This drops steady-state heartbeat traffic from `O(nodes × partitions)` toward per-node SWIM probing, making deployments with hundreds or thousands of mostly-idle partitions practical. Any write wakes the partition instantly, and a dead leader is still detected promptly via SWIM. See [Partition Quiescence Developer Guide](docs/partition-quiescence-developer-guide.md).
- **Leader balancing:** Leaders are elected independently, so over time they can pile up on one node while peers sit idle. The leader balancer watches how leaderships are distributed and how loaded each one is, then spreads them evenly — equalizing the *count* first and the *load* (a smoothed throughput + queue-depth score) second. The system-partition leader plans moves and sends advisory *suggestions* to each partition's current leader, which performs the safe Raft handoff; it is throttled, self-healing, off by default, and can never affect correctness. See [Leader Balancer Developer Guide](docs/leader-balancer-developer-guide.md).
- **Durable write-ahead logging:** Built-in WAL adapters for RocksDB and SQLite persist proposed, committed, rolled-back, and checkpoint entries before state-machine callbacks run.
- **Automatic WAL compaction:** Committed-operation counters trigger bounded per-partition compaction so removable history below the last committed checkpoint does not grow without bound.
- **Testing-friendly in-memory components:** `InMemoryWAL`, `InMemoryCommunication`, and focused test utilities support fast local simulations without external infrastructure.
- **Cluster discovery options:** Static discovery for fixed clusters, dynamic discovery for application-managed membership lists, and multicast discovery for local-network discovery.
- **Transport choices:** gRPC for networked clusters, REST/JSON for HTTP-based integration and debugging, and in-memory communication for tests.
- **Node authentication:** Shared-secret HMAC authentication for node-to-node REST and gRPC traffic, with optional server certificate thumbprint pinning.
- **Batch replication:** Replicate multiple log entries in a single proposal to reduce coordination overhead.
- **Manual proposal control:** Use automatic commits for the common path, or disable auto-commit and explicitly call `CommitLogs` or `RollbackLogs`.
- **Hybrid logical clocks:** Proposal tickets use HLC timestamps to preserve causality across physical time and logical counters.
- **Actorless partition executors:** Each partition is driven by an explicit serial executor instead of an actor, making ownership and blocking boundaries easier to reason about.
- **Fair I/O schedulers:** Synchronous WAL reads and writes are processed through configurable fair schedulers so storage work does not block partition state transitions.
- **Application callbacks:** Restore, replication, replication-error, and leadership events let applications rebuild and advance their own state machines.
- **ASP.NET Core integration:** Route extensions expose Raft gRPC and REST endpoints from an existing web host.

## About Raft And Kommander

Raft is a consensus protocol that helps a cluster of nodes maintain a replicated state machine by synchronizing a durable log. A leader receives proposed changes, writes them locally, replicates them to followers, and commits them after a quorum acknowledges the proposal.

Kommander implements this model with partitioned Raft groups. Each partition elects its own leader, so a node can be the leader for one partition and a follower for another. This improves throughput when workloads can be routed by key while preserving ordered replication inside each partition.

The library keeps storage, discovery, and communication pluggable. That separation lets applications use the same Raft behavior with different persistence engines, network transports, and cluster discovery strategies.

## Documentation

| Guide | What it covers |
| --- | --- |
| [Architecture Overview](docs/architecture-overview.md) | A beginner-friendly tour of the whole system: concepts, the component map, and the main runtime flows (election, write path, catch-up, restart, compaction). Start here. |
| [Log Catch-Up & Backfill Developer Guide](docs/log-backfill-developer-guide.md) | How a lagging follower is brought back in sync: the live-vs-backfill split, the Log Matching anchor, multi-round convergence, and the snapshot handoff. |
| [Dynamic Membership Developer Guide](docs/dynamic-membership-developer-guide.md) | Adding/removing nodes at runtime, the committed roster, learner promotion, and the SWIM failure detector. |
| [Elastic Partitions Developer Guide](docs/elastic-partitions-developer-guide.md) | Creating, splitting, merging, and removing partitions at runtime, the two-phase protocols, and the generation fence. |
| [Partition Quiescence Developer Guide](docs/partition-quiescence-developer-guide.md) | How idle partitions stop heartbeating and lean on SWIM for liveness: the quiesce/wake flows, the failover-timing rule, and the configuration knobs. |
| [Leader Balancer Developer Guide](docs/leader-balancer-developer-guide.md) | How leaderships are spread evenly across the cluster: load reports, the global view, the two-tier (count then load) planner, the transfer-suggestion mechanism, configuration, and observability. |

## Packages And Targets

Kommander multi-targets `.NET 8.0` and `.NET 10.0`.

Current package version in this tree: `0.17.2`.

Install from NuGet:

```shell
dotnet add package Kommander
```

Or with the NuGet Package Manager Console:

```powershell
Install-Package Kommander
```

## Concepts

**Node:** A running `RaftManager` instance. Each node has a local endpoint built from `RaftConfiguration.Host` and `RaftConfiguration.Port`.

**Partition:** A separately elected Raft group. Partition `0` is the system partition: Kommander replicates its own configuration there using the reserved `_RaftSystem` log type. Application data normally uses user partitions, which start at `1`. The system partition can also **co-locate consumer data**: entries written with any non-`_RaftSystem` log type are replicated and dispatched to the consumer callbacks just like a user partition, while Kommander's own `_RaftSystem` entries continue to drive the system coordinator. Routing on partition `0` is by log type, so the two never interfere. Partition `0` itself can never be created, split, merged, or removed.

**Leader:** The node currently allowed to accept proposals for a partition.

**Follower:** A node that receives and persists append-log requests from a partition leader.

**Proposal:** A set of log entries written by the leader and replicated to followers. A proposal is complete after quorum acknowledgment.

**Commit:** The durable state transition that marks proposed logs as committed. `ReplicateLogs` auto-commits by default; callers can disable that and call `CommitLogs` or `RollbackLogs` explicitly.

**Checkpoint:** A special replicated log entry used to mark a stable point in a partition log.

**WAL:** The write-ahead log used to persist proposed, committed, rolled-back, and checkpoint entries.

## Quick Start

This example creates one node using static discovery, RocksDB storage, and gRPC communication. In a real cluster, run one `RaftManager` per node with a unique host/port and a discovery list containing the other nodes.

```csharp
using System.Text;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
ILogger<IRaft> logger = loggerFactory.CreateLogger<IRaft>();

RaftConfiguration configuration = new()
{
    NodeName = "node-1",
    NodeId = 1,
    Host = "localhost",
    Port = 8001,
    InitialPartitions = 8
};

IRaft raft = new RaftManager(
    configuration,
    new StaticDiscovery([
        new RaftNode("localhost:8002"),
        new RaftNode("localhost:8003")
    ]),
    new RocksDbWAL(path: "./data", revision: "node-1", logger),
    new GrpcCommunication(),
    new HybridLogicalClock(),
    logger
);

raft.OnReplicationReceived += (partitionId, log) =>
{
    string payload = Encoding.UTF8.GetString(log.LogData ?? []);
    Console.WriteLine($"{partitionId}: {log.Id} {log.Type} {log.LogType} {payload}");
    return Task.FromResult(true);
};

await raft.JoinCluster();

int partitionId = 1;
using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(10));

if (await raft.AmILeader(partitionId, timeout.Token))
{
    RaftReplicationResult result = await raft.ReplicateLogs(
        partitionId,
        "Greeting",
        Encoding.UTF8.GetBytes("Hello from Kommander"),
        cancellationToken: timeout.Token
    );

    Console.WriteLine(result.Success
        ? $"Committed log #{result.LogIndex}"
        : $"Replication failed: {result.Status}");
}

await raft.LeaveCluster(dispose: true);
```

## Hosting gRPC Or REST Endpoints

When using network transports, each process must expose matching Raft endpoints.

For gRPC:

```csharp
using Kommander.Communication.Grpc;

WebApplication app = builder.Build();
app.MapGrpcRaftRoutes();
app.Run();
```

For REST/JSON:

```csharp
using Kommander.Communication.Rest;

WebApplication app = builder.Build();
app.MapRestRaftRoutes();
app.Run();
```

The sample server in `Kommander.Server` maps both gRPC and REST endpoints and starts the cluster from command-line options.

## Transport Security And Node Authentication

### Threat Model

TLS encrypts traffic and proves the server's identity when the certificate is validated correctly. It does not authenticate the calling node. Without an additional check, any client that can reach a Raft port can send handshake, vote, append, or step-down messages.

Kommander separates these two concerns:

1. **Transport security** — TLS encrypts the wire and validates the remote server certificate.
2. **Node authentication** — every inbound REST and gRPC Raft request proves it belongs to the same cluster.

In-memory communication (used in tests) is excluded from these requirements.

### Authentication Modes

`RaftTransportSecurityOptions.NodeAuthenticationMode` controls which mode is active:

| Mode | Description |
| --- | --- |
| `Disabled` | No authentication. Existing unauthenticated clusters keep working. A warning is logged at startup. |
| `SharedSecret` | HMAC-SHA256 per-request signature derived from a shared cluster secret. Recommended for production. |
| `MutualTls` | mTLS client certificate validation. Not yet implemented. |

### Shared Secret Protocol

Each request carries four signed fields:

| Header | Content |
| --- | --- |
| `X-Kommander-Cluster-Auth` | Base64url HMAC-SHA256 signature |
| `X-Kommander-Cluster-Node` | Sender node endpoint |
| `X-Kommander-Cluster-Timestamp` | Unix milliseconds |
| `X-Kommander-Cluster-Nonce` | Random 128-bit nonce, base64url |

The signature covers `method + path + sender + timestamp + nonce + SHA-256(body)`. Validation rejects missing fields, malformed fields, timestamp skew beyond the configured window (default 60 s), and replayed nonces. All HMAC comparisons use constant-time equality.

### Configuring Shared Secret

```csharp
RaftConfiguration configuration = new()
{
    Host = "node-1",
    Port = 8001,
    TransportSecurity = new RaftTransportSecurityOptions
    {
        NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
        SharedSecret = Environment.GetEnvironmentVariable("CLUSTER_SECRET"),
        AllowedClockSkew = TimeSpan.FromSeconds(60)
    }
};
```

Generate at least 256 bits of random secret material and distribute it through a secret manager, Docker secret, Kubernetes Secret, or environment variable. Do not put the secret in source control.

### Certificate Validation

| Option | Default | Description |
| --- | ---: | --- |
| `RequireTls` | `true` | Reject requests that arrive over plain HTTP. Throws at startup if `HttpScheme` is `http://`. |
| `AllowInsecureCertificateValidation` | `false` | Skip remote certificate validation. Development only. A warning is logged at startup. |
| `TrustedServerCertificateThumbprints` | empty | SHA-256 hex thumbprints of accepted server certificates. When set, only pinned certificates are accepted regardless of chain trust. |
| `TrustedClientCertificateThumbprints` | empty | Reserved for future mTLS client certificate pinning. |

When neither `AllowInsecureCertificateValidation` nor thumbprints are set, the platform default certificate chain and hostname validation applies.

### Kommander.Server CLI Options

| Option | Description |
| --- | --- |
| `--node-auth-mode` | `Disabled`, `SharedSecret`, or `MutualTls` |
| `--node-shared-secret` | Shared secret value. Required when mode is `SharedSecret`. |
| `--node-auth-header` | Override the default `X-Kommander-Cluster-Auth` header name. |
| `--allow-insecure-certificate-validation` | Skip TLS certificate validation. Development only. |
| `--trusted-server-cert-thumbprint` | One or more trusted server certificate SHA-256 thumbprints (hex). |
| `--trusted-client-cert-thumbprint` | Reserved for future mTLS use. |

### Secret Rotation

To rotate the shared secret without downtime:

1. Sign outbound requests with the new secret.
2. Accept both old and new secrets on inbound requests during a transition window (requires a secondary secret field, not yet built in — use a rolling restart in the interim).
3. Remove the old secret after all nodes are updated.

## Creating A Node

`RaftManager` is the main implementation of `IRaft`:

```csharp
IRaft raft = new RaftManager(
    configuration,
    discovery,
    walAdapter,
    communication,
    new HybridLogicalClock(),
    logger
);
```

Use a unique `NodeId` when you can. If `NodeId` is `0`, Kommander derives one from `NodeName`.

## IRaft API Surface

| Area | Members |
| --- | --- |
| Lifecycle | `JoinCluster` (no-arg and seed-based overloads), `LeaveCluster`, `UpdateNodes` |
| Membership | `GetMembership`, `LocalRole`, `OnMembershipChanged` |
| Cluster state | `Joined`, `IsInitialized`, `GetNodes`, `GetLocalEndpoint`, `GetLocalNodeId`, `GetLocalNodeName` |
| Leadership | `AmILeaderQuick`, `AmILeader`, `WaitForLeader`, `WaitForLeaderStableAsync` |
| Leadership control | `StepDownAsync`, `TransferLeadershipAsync`, `SuspendHeartbeatsAsync`, `ResumeHeartbeatsAsync` |
| Replication | `ReplicateLogs`, `ReplicateCheckpoint`, `CommitLogs`, `RollbackLogs` |
| Catch-up observability | `GetFollowerLagAsync`, `GetActiveNodes`, `GetLastNodeActivity` |
| Partition routing | `GetPartitionKey`, `GetPrefixPartitionKey` |
| Elastic partitions | `CreatePartitionAsync`, `RemovePartitionAsync`, `SplitPartitionAsync`, `MergePartitionsAsync`, `GetPartitionMap`, `GetPartitionGeneration`, `RegisterStateMachineTransfer` |
| Partition events | `OnPartitionMapChanged` |
| Transport entry points | `Handshake`, `RequestVote`, `Vote`, `AppendLogs`, `CompleteAppendLogs` |
| Components | `WalAdapter`, `Communication`, `Discovery`, `Configuration`, `HybridLogicalClock`, `ReadScheduler`, `WalScheduler` |
| Events | `OnRestoreStarted`, `OnRestoreFinished`, `OnReplicationError`, `OnLogRestored`, `OnReplicationReceived`, `OnLeaderChanged` |

The transport entry points are intended for communication adapters and HTTP/gRPC endpoint handlers, not normal application writes.

## Configuration

| Property | Default | Description |
| --- | ---: | --- |
| `NodeName` | machine name | Stable node name used when deriving a node id. |
| `NodeId` | `0` | Integer node id. `0` means derive from `NodeName`. |
| `Host` | `null` | Host advertised as part of the node endpoint. |
| `Port` | `0` | Port advertised as part of the node endpoint. |
| `InitialPartitions` | `1` | Number of initial user partitions. Partition `0` is reserved. |
| `TransportSecurity` | see below | Node authentication and TLS validation settings. See [Transport Security And Node Authentication](#transport-security-and-node-authentication). |
| `HttpScheme` | `https://` | Scheme used by `RestCommunication`. |
| `GrpcScheme` | `https://` | URL scheme prepended to peer endpoints when opening gRPC channels. Set to `http://` (plus `Http2UnencryptedSupport`) for cleartext test environments. |
| `GrpcChannelsPerNode` | `4` | Number of pooled gRPC channels (and matching streaming calls) created per peer URL. Each channel owns its own `SocketsHttpHandler` and TCP/HTTP2 connection, so effective per-peer concurrency is approximately `GrpcChannelsPerNode × MaxConcurrentStreams`. Clamped to [1, 64] at runtime. Set to `1` to restore single-connection behaviour. |
| `GrpcEnableMultipleHttp2Connections` | `false` | When `true`, each pooled channel's handler may open additional TCP/HTTP2 connections on demand, raising the per-channel stream ceiling further on saturated links. |
| `HttpAuthBearerToken` | empty | **Deprecated.** Legacy bearer token. Use `TransportSecurity.SharedSecret` instead. |
| `HttpTimeout` | `5` | REST request timeout in seconds. |
| `HttpVersion` | `2.0` | REST HTTP version. |
| `HeartbeatInterval` | `500 ms` | Leader heartbeat interval. |
| `RecentHeartbeat` | `100 ms` | Cross-partition recent-heartbeat window. |
| `VotingTimeout` | `1500 ms` | Candidate vote wait timeout. |
| `CheckLeaderInterval` | `250 ms` | Leader election supervision interval. |
| `TimerInitialDelay` | `2500 ms` | Initial delay before periodic Raft timers start. Tests can lower this to speed up elections. |
| `UpdateNodesInterval` | `5000 ms` | Discovery refresh interval. |
| `StartElectionTimeout` | `2000 ms` | Lower election timeout bound. |
| `EndElectionTimeout` | `4000 ms` | Upper election timeout bound. |
| `StartElectionTimeoutIncrement` | `100 ms` | Lower timeout backoff increment. |
| `EndElectionTimeoutIncrement` | `200 ms` | Upper timeout backoff increment. |
| `EnableQuiescence` | `true` | When `true`, idle partitions stop sending per-partition heartbeats and followers gate elections on SWIM node liveness. Set to `false` to restore per-partition heartbeating on every interval. Requires `PingInterval > 0` and `PingInterval < StartElectionTimeout` (validated at construction). See [Partition Quiescence](#partition-quiescence). |
| `QuiesceAfter` | `1500 ms` | How long a partition must be idle (no proposals, no in-flight replication) before its leader quiesces it. |
| `PingInterval` | `1000 ms` | SWIM failure-detector probe cadence. With quiescence on, a quiesced follower detects a dead leader roughly one `PingInterval` after the crash; must be `> 0` and `< StartElectionTimeout`. |
| `SlowRaftStateMachineLog` | `50 ms` | Slow partition state-machine operation warning threshold. |
| `SlowRaftWALMachineLog` | `25 ms` | Slow WAL warning threshold. |
| `ReadIOThreads` | `8` | Fair scheduler workers for synchronous WAL reads. |
| `WriteIOThreads` | `4` | Fair scheduler workers for synchronous WAL writes. |
| `BackfillThreshold` | `10` | How many entries a follower may lag before the leader engages backfill. Below this, the live replication path handles catch-up. |
| `MaxBackfillEntriesPerRound` | `128` | Maximum entries shipped in a single backfill round, bounding message size and keeping backfill fair to live traffic. |
| `CompactEveryOperations` | `10000` | Successfully persisted commit/follower-append operations between automatic WAL compaction triggers per partition. Set to `0` or below to disable automatic compaction. |
| `CompactNumberEntries` | `100` | Max entries removed per adapter compaction batch. Values below `1` are treated as `1`. |
| `MaxEntriesPerCompaction` | `5000` | Upper bound on entries removed by one triggered compaction pass before yielding. Values below `CompactNumberEntries` are clamped up. |

## Replicating Logs

Replicate a single entry:

```csharp
RaftReplicationResult result = await raft.ReplicateLogs(
    partitionId: 1,
    type: "OrderCreated",
    data: payload,
    cancellationToken: cancellationToken
);
```

Replicate multiple entries in one proposal:

```csharp
RaftReplicationResult result = await raft.ReplicateLogs(
    partitionId: 1,
    type: "OrderEvent",
    logs: new[] { createdPayload, paidPayload, shippedPayload },
    cancellationToken: cancellationToken
);
```

`RaftReplicationResult` contains:

| Property | Description |
| --- | --- |
| `Success` | `true` when the operation completed successfully. |
| `Status` | Detailed `RaftOperationStatus`. |
| `TicketId` | Hybrid logical clock timestamp that identifies the proposal. |
| `LogIndex` | Last log index assigned to the proposal. |

You may also replicate to the system partition (`0`) to co-locate consumer state with the partition map — useful for coordination data that must share a leader with partition lifecycle operations. The only restriction is the log `type`: `_RaftSystem` is reserved for Kommander, so `ReplicateLogs(0, "_RaftSystem", …)` throws a `RaftException`. Any other type is accepted and routed to the consumer callbacks.

## Manual Commit And Rollback

`ReplicateLogs` auto-commits by default. Set `autoCommit: false` to stop after quorum proposal completion, then commit or roll back explicitly:

```csharp
RaftReplicationResult proposal = await raft.ReplicateLogs(
    partitionId: 1,
    type: "PaymentReserved",
    data: payload,
    autoCommit: false,
    cancellationToken: cancellationToken
);

if (proposal.Success)
{
    (bool committed, RaftOperationStatus status, long commitLogId) =
        await raft.CommitLogs(1, proposal.TicketId);
}
```

Rollback uses the same ticket:

```csharp
(bool rolledBack, RaftOperationStatus status, long rollbackLogId) =
    await raft.RollbackLogs(1, proposal.TicketId);
```

## Checkpoints

Replicate a checkpoint for a user partition:

```csharp
RaftReplicationResult checkpoint = await raft.ReplicateCheckpoint(1, cancellationToken);
```

Checkpoint entries use `RaftLogType.ProposedCheckpoint`, `CommittedCheckpoint`, or `RolledBackCheckpoint` internally.

## Leadership APIs

```csharp
bool quick = await raft.AmILeaderQuick(1);
bool leader = await raft.AmILeader(1, cancellationToken);
string endpoint = await raft.WaitForLeader(1, cancellationToken);
```

`AmILeaderQuick` checks cached partition state. `AmILeader` waits up to the internal leadership timeout. `WaitForLeader` returns the elected leader endpoint or throws `RaftException`.

## Operation Status Values

`RaftOperationStatus` describes why an operation succeeded, failed, or is still in progress:

| Status | Meaning |
| --- | --- |
| `Success` | Operation completed successfully. |
| `Errored` | Operation failed with an internal error. |
| `NodeIsNotLeader` | The local node is not leader for the requested partition. |
| `LeaderInOldTerm` | A request came from a leader with an old term. |
| `LeaderAlreadyElected` | A leader was already known for the term. |
| `LogsFromAnotherLeader` | A follower received logs from a node other than the expected leader. |
| `ActiveProposal` | Another proposal is still active. |
| `ProposalNotFound` | The supplied proposal ticket was not found. |
| `ProposalTimeout` | The proposal did not complete in time. |
| `ReplicationFailed` | Replication failed before commit. |
| `Pending` | Internal state used while asynchronous work is in progress. |
| `LogMismatch` | A follower rejected a backfill batch because it does not hold the anchored preceding entry; the leader backtracks and retries lower. |
| `SnapshotRequired` | A follower needs entries below the leader's compaction floor; catch-up switches from backfill to snapshot install. |

## Partition Routing

Use partition helpers to map application keys to user partitions:

```csharp
int partition = raft.GetPartitionKey("tenant-42/order-1001");
int prefixPartition = raft.GetPrefixPartitionKey("tenant-42");
```

`GetPartitionKey` uses the prefix before the last `/` separator. `GetPrefixPartitionKey` hashes the complete string provided.

The partition helpers map keys onto user partitions only and never return `0`. You *can* replicate to partition `0` explicitly to co-locate consumer state with the system partition (see [Replicating Logs](#replicating-logs)); the only rejected write is the reserved `_RaftSystem` log type.

## Events

Subscribe before `JoinCluster` if you need restore callbacks.

```csharp
raft.OnRestoreStarted += partitionId => { };
raft.OnRestoreFinished += partitionId => { };

raft.OnLogRestored += (partitionId, log) =>
{
    // Rebuild application state from committed WAL entries.
    return Task.FromResult(true);
};

raft.OnReplicationReceived += (partitionId, log) =>
{
    // Apply committed replicated entries to the application state machine.
    return Task.FromResult(true);
};

raft.OnReplicationError += (partitionId, log) => { };

raft.OnLeaderChanged += (partitionId, leaderEndpoint) =>
{
    return Task.FromResult(true);
};
```

System partition events also exist on `RaftManager` for internal configuration replication, but they are not part of `IRaft`.

## WAL Adapters

Kommander persists Raft logs through `IWAL`.

| Adapter | Use case |
| --- | --- |
| `RocksDbWAL` | Durable production-oriented storage backed by RocksDB. |
| `SqliteWAL` | Durable embedded storage backed by SQLite. |
| `InMemoryWAL` | Tests and simulations only. Data is lost when the process exits. |

### WAL Durability And Compaction

RocksDB WAL keys include both partition id and log id, so partitions that share an internal RocksDB shard can safely store overlapping log indexes. This uses RocksDB WAL format `2.0.0`; existing RocksDB WAL directories from the old id-only key format must be recreated or migrated before opening them with this release.

Acknowledged RocksDB append writes use synchronous write options in both the single-entry and batched paths by default. SQLite uses `PRAGMA synchronous=FULL` for partition WAL databases by default. For CI or benchmark scenarios where crash durability is not being tested, both durable adapters can be constructed with `syncWrites: false`; this improves write throughput but acknowledged writes may be lost on process or machine crash.

Automatic compaction runs per partition after `CompactEveryOperations` successfully persisted commit/follower-append operations. A pass reads the last committed checkpoint and removes entries with ids below that checkpoint in batches of `CompactNumberEntries`, capped by `MaxEntriesPerCompaction`. Compaction runs through the partition-aware read scheduler; adapter-level locks still serialize SQLite deletes with writes.

Compaction only removes history older than the last committed checkpoint. If an application never replicates checkpoints, there is little or nothing eligible to compact.

RocksDB:

```csharp
IWAL wal = new RocksDbWAL("./data", "node-1", logger);
```

RocksDB with fsync disabled for tests:

```csharp
IWAL wal = new RocksDbWAL("./data", "node-1", logger, syncWrites: false);
```

SQLite:

```csharp
IWAL wal = new SqliteWAL("./data", "node-1", logger);
```

SQLite with synchronous writes disabled for tests:

```csharp
IWAL wal = new SqliteWAL("./data", "node-1", logger, syncWrites: false);
```

In-memory:

```csharp
IWAL wal = new InMemoryWAL(logger);
```

Custom adapters implement:

```csharp
public interface IWAL
{
    List<RaftLog> ReadLogs(int partitionId);
    List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex);
    RaftOperationStatus Write(List<(int partitionId, List<RaftLog> logs)> logs);
    long GetMaxLog(int partitionId);
    long GetCurrentTerm(int partitionId);
    long GetLastCheckpoint(int partitionId);
    string? GetMetaData(string key);
    bool SetMetaData(string key, string value);
    (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries);
    void Dispose();
}
```

## Communication Adapters

| Adapter | Use case |
| --- | --- |
| `GrpcCommunication` | Networked clusters using gRPC streaming. |
| `RestCommunication` | Networked clusters using REST/JSON endpoints. |
| `InMemoryCommunication` | Unit tests and in-process simulations. |

For `RestCommunication`, configure `HttpScheme`, `HttpTimeout`, and `HttpVersion` on `RaftConfiguration`, and `TransportSecurity` for node authentication and certificate validation.

Custom transports implement `ICommunication`.

## Discovery Adapters

| Adapter | Use case |
| --- | --- |
| `StaticDiscovery` | Fixed cluster membership. |
| `DynamicDiscovery` | Mutable in-memory node list controlled by the application. |
| `MulticastDiscovery` | UDP multicast discovery on local networks. |

`RedisDiscovery` is present in source as a placeholder and returns no nodes in this release. Do not use it for cluster formation.

Custom discovery providers implement:

```csharp
public interface IDiscovery
{
    Task Register(RaftConfiguration configuration);
    List<RaftNode> GetNodes();
}
```

## Elastic Partitions

Partitions can be created, split, merged, and removed at runtime without restarting any node. All lifecycle operations are serialized through the system partition and replicated to every node, so the partition map is always consistent and durable.

```csharp
// Create a new unrouted partition addressed by id directly.
RaftPartitionLifecycleResult created = await raft.CreatePartitionAsync(
    partitionId: 10,
    mode: RaftRoutingMode.Unrouted);

// Split partition 1 into two hash-range halves.
RaftPartitionLifecycleResult split = await raft.SplitPartitionAsync(
    sourcePartitionId: 1);

// Merge partition 3 (source) into partition 2 (survivor).
RaftPartitionLifecycleResult merged = await raft.MergePartitionsAsync(
    survivorPartitionId: 2,
    sourcePartitionId:   3);

// Remove a partition (tombstone persists; WAL is reclaimed).
RaftPartitionLifecycleResult removed = await raft.RemovePartitionAsync(partitionId: 10);
```

All operations require the caller to be the leader of the relevant partition(s). Splits and merges use a two-phase commit so the hash ring is never incomplete during the transition. The system partition (`0`) is never a valid target — `CreatePartitionAsync`, `SplitPartitionAsync`, `MergePartitionsAsync`, and `RemovePartitionAsync` all reject it with a `RaftException`.

The **generation fence** protects writes during topology changes. Pass `expectedGeneration` to `ReplicateLogs` to detect when the partition a client cached has been split or merged:

```csharp
long gen = raft.GetPartitionGeneration(partitionId);

RaftReplicationResult result = await raft.ReplicateLogs(
    partitionId,
    type:              "MyEvent",
    data:              payload,
    expectedGeneration: gen);

if (result.Status == RaftOperationStatus.PartitionMoved)
{
    // Partition topology changed; re-read the map and retry on the new partition.
}
```

Subscribe to `OnPartitionMapChanged` to react to map updates without polling:

```csharp
raft.OnPartitionMapChanged += snapshot =>
{
    foreach (RaftPartitionRange range in snapshot)
        Console.WriteLine($"  P{range.PartitionId} {range.State} [{range.StartRange},{range.EndRange}] gen={range.Generation}");
};
```

For large datasets, register a snapshot transfer implementation to copy state from source to target in one step instead of shipping individual log entries:

```csharp
raft.RegisterStateMachineTransfer(new MySnapshotTransfer());
```

See [Elastic Partitions Developer Guide](docs/elastic-partitions-developer-guide.md) for a full walkthrough of the two-phase protocols, crash recovery, routing, the generation fence, snapshot transfer, and how to extend the system.

## Dynamic Cluster Membership

Nodes can join and leave a running cluster without a restart or an operator editing discovery. The authoritative voting roster is a committed, versioned record on the system partition (partition `0`) — it is changed only through Raft, never by a gossip message or a raw discovery snapshot — so every node agrees on exactly who can vote. A separate gossip + SWIM layer spreads the roster faster and detects dead nodes, but it is strictly advisory: only the system-partition leader, through a committed change, ever alters who counts toward quorum.

A new node joins as a non-voting **learner**, receives replication to catch up, and is automatically promoted to **voter** once it is within `LearnerPromotionLag` entries of the committed log for `LearnerPromotionStableWindow`. Because learners never count toward quorum, adding a node can never stall commits.

```csharp
// New node joins an existing cluster by contacting seed endpoints.
// Blocks until this node is a committed voter (or the deadline trips).
await raft.JoinCluster(
    seeds: ["host1:7000", "host2:7000", "host3:7000"],
    cancellationToken: cts.Token);

// Or use the IDiscovery implementation registered at construction.
await raft.JoinCluster(cancellationToken: cts.Token);

// Read the current roster.
ClusterMembership roster = raft.GetMembership();
foreach (ClusterMember m in roster.Members)
    Console.WriteLine($"  {m.Endpoint}  {m.Role}");   // Learner | Voter | Leaving

// This node's role.
ClusterMemberRole role = raft.LocalRole;

// Leave gracefully: commits a RemoveMember and shuts down.
await raft.LeaveCluster(dispose: true);
```

Subscribe to `OnMembershipChanged` to react to roster updates without polling. It fires in commit order on join, promotion, leave, and failure-driven eviction; use `MembershipVersion` as a monotonic sequence number:

```csharp
raft.OnMembershipChanged += membership =>
{
    string members = string.Join(", ", membership.Members.Select(m => $"{m.Endpoint}({m.Role})"));
    Console.WriteLine($"[v{membership.MembershipVersion}] {members}");
};
```

Membership changes are single-server (one node added, promoted, or removed per committed step), so any two consecutive configurations always share a majority. A removal that would leave zero voters is refused with `InsufficientVoters`.

> **Transport support:** the committed roster, joins, graceful leave, and cross-partition learner promotion all work on every transport (gRPC, REST, and in-memory). Gossip dissemination and the SWIM failure detector are currently wired only on the in-process (`InMemoryCommunication`) transport; on gRPC/REST those RPCs are not yet implemented, so the failure detector is **disabled by default** (`PingInterval` defaults to `TimeSpan.Zero`) to avoid evicting healthy peers. Do not enable it on a transport without working Ping support. Without gossip, roster convergence still happens via Raft replication (just not epidemically). See the developer guide's transport matrix for the current state.

See [Dynamic Membership Developer Guide](docs/dynamic-membership-developer-guide.md) for the design motivations, the full lifecycle walkthroughs, the SWIM failure detector, configuration, the code map, an operations runbook, and the invariants to preserve when extending the system.

## Log Catch-Up & Backfill

A follower can fall behind its leader — it was slow, briefly partitioned, paused, or just joined. Kommander keeps it in sync through two distinct paths:

- The **live path** broadcasts each new proposal/commit to followers that are keeping up. It is intentionally unanchored, so a transiently-behind follower never stalls a live proposal.
- The **backfill path** engages once a follower lags by more than `BackfillThreshold`. The leader reads the missing range from its own log and ships it in bounded chunks (`MaxBackfillEntriesPerRound`), each **anchored** with the preceding entry's index and term. The follower enforces the Log Matching Property: it applies a chunk only if it already holds that anchor, otherwise it rejects with `LogMismatch` and the leader backtracks. This is what guarantees a follower's log never grows a gap or keeps a divergent tail.

If a follower needs entries the leader has already compacted away (below the **compaction floor**), backfill cannot help; the leader returns `SnapshotRequired` and catch-up hands off to snapshot install.

Backfill is automatic and needs no application calls. You can observe how far a follower is behind:

```csharp
long? lag = await raft.GetFollowerLagAsync(partitionId: 1, followerEndpoint: "host2:8002");
```

See [Log Catch-Up & Backfill Developer Guide](docs/log-backfill-developer-guide.md) for the full flows, the live-vs-backfill rationale, configuration tuning, the code map, and the invariants to preserve when extending it.

## Partition Quiescence

Heartbeats in Kommander are per-partition: every leader periodically tells each follower "I'm still your leader of this partition." With many mostly-idle partitions, that steady-state cost grows as `O(nodes × partitions)` even when nothing is being written. **Quiescence** removes that waste.

When a partition has had no writes for `QuiesceAfter` (default 1500 ms), its leader sends followers a one-time **quiesce marker** and then stops heartbeating that partition. Quiesced followers stop watching the per-partition election timer and instead rely on the cluster-wide **SWIM failure detector** — which already tracks "is that node alive?" once per node, independent of how many partitions it leads. The result is that idle partitions cost essentially nothing on the wire, while a genuinely dead leader is still detected promptly (followers fail over on SWIM `Suspect`, after roughly one `PingInterval`).

Quiescence is transparent: any write instantly un-quiesces the partition and resumes normal replication and heartbeating. It is enabled by default and is a pure local optimization — it never changes elections, quorum, or the commit path.

```csharp
RaftConfiguration config = new()
{
    EnableQuiescence = true,                         // default
    QuiesceAfter = TimeSpan.FromMilliseconds(1500),  // idle window before a leader quiesces
    PingInterval = TimeSpan.FromSeconds(1),          // SWIM probe cadence; must be > 0 and < StartElectionTimeout
};
```

Because quiesced followers depend on SWIM for failover, `RaftConfiguration.Validate()` (run at construction) requires `PingInterval > 0` and `PingInterval < StartElectionTimeout` whenever quiescence is enabled; set `EnableQuiescence = false` to restore classic per-partition heartbeating.

See [Partition Quiescence Developer Guide](docs/partition-quiescence-developer-guide.md) for the quiesce/wake flows, the failover-timing rule, the code map, and the invariants to preserve when extending it.

## Leader Balancing

Each partition elects its leader independently, with nothing coordinating *where* leaders land. Bad luck in election timing can leave one node holding most of the leaders — doing the heartbeat, replication, and commit work for all of them — while its peers sit nearly idle. Even when the *counts* are even, the **hot** partitions can cluster on one node. The **leader balancer** corrects both.

The leader of the system partition acts as the controller. Every node periodically gossips an advisory **load report** of the partitions it leads and how busy each is (a smoothed throughput + queue-depth score). The controller reduces these into a global view and runs a two-tier plan: **count balance** first (drive each node toward the cluster average), then **load balance** (count-neutral hot/cold swaps when load is still skewed). Because only a partition's own leader can hand off its leadership, the controller does not transfer directly — it sends a **suggestion** to the current leader, which validates and performs the standard Raft handoff. Moves are throttled, gated by cooldowns and stability checks, and confirmed by observing the next round of reports, so the balancer is convergent and self-healing.

It is **off by default** and purely advisory: a stale or dropped suggestion can only cause a wasted or skipped move, never an unsafe one, since every actual transfer is re-validated by the partition leader.

```csharp
RaftConfiguration config = new()
{
    EnableLeaderBalancer = true,                          // default false
    LeaderBalancerInterval = TimeSpan.FromSeconds(30),    // how often the controller plans a pass
    LeaderBalancerReportInterval = TimeSpan.FromSeconds(5), // how often each node emits a load report
    MaxMovesPerPass = 4,                                  // cap on moves planned per pass
    MaxConcurrentTransfers = 2,                           // cap on moves in flight cluster-wide
    MoveCooldown = TimeSpan.FromSeconds(60),              // per-partition cooldown after a move
};
```

Progress is observable via the `Kommander` meter: `raft.balancer.moves_total` (by outcome), `raft.balancer.skipped_passes_total`, and the `raft.balancer.count_imbalance` / `raft.balancer.load_imbalance` gauges (which trend toward zero as the cluster converges).

See [Leader Balancer Developer Guide](docs/leader-balancer-developer-guide.md) for the load score, the global view, the two-tier planner, the suggestion mechanism, the full configuration and metrics reference, the code map, and the invariants to preserve when extending it.

## Utilities

Kommander also exposes a small set of utility APIs used by the library and available to callers.

### Hashing

`HashUtils` provides xxHash-based helpers and jump consistent hashing:

```csharp
int nodeId = HashUtils.SmallSimpleHash("node-1");
ulong hash = HashUtils.SimpleHash("tenant-42");
ulong bucket = HashUtils.StaticHash("tenant-42", buckets: 128);
long prefixed = HashUtils.PrefixedHash("tenant-42/order-1", '/', buckets: 128);
long inversePrefixed = HashUtils.InversePrefixedHash("tenant-42/order-1", '/', buckets: 128);
int consistent = HashUtils.ConsistentHash("tenant-42", numBuckets: 128);
```

### Hybrid Logical Clocks

`HybridLogicalClock` and `HLCTimestamp` are used by proposals and exposed in replication results:

```csharp
HybridLogicalClock clock = new();
HLCTimestamp timestamp = clock.SendOrLocalEvent(nodeId: 1);
```

### Parallelization Extensions

`Kommander.Support.Parallelization` contains `ForEachAsync` extensions for `IEnumerable<T>`, `IAsyncEnumerable<T>`, `List<T>`, arrays, and `HashSet<T>`:

```csharp
using Kommander.Support.Parallelization;

await items.ForEachAsync(maxDegreeOfParallelism: 8, async item =>
{
    await Process(item);
});
```

### SmallDictionary

`SmallDictionary<TKey, TValue>` is a fixed-capacity, non-thread-safe dictionary optimized for very small maps.

## ASP.NET Core Sample Server

`Kommander.Server` is a runnable ASP.NET Core host that:

- creates a `RaftManager`;
- uses `StaticDiscovery`;
- uses `RocksDbWAL`;
- uses `GrpcCommunication`;
- maps REST and gRPC Raft routes;
- starts a background replication service.

Important command-line options:

| Option | Description |
| --- | --- |
| `--initial-cluster` | Other node endpoints for static discovery. |
| `--initial-cluster-partitions` | Initial user partition count. |
| `--raft-nodename` | Stable node name. |
| `--raft-nodeid` | Integer node id. |
| `--raft-host` | Host advertised for Raft traffic. |
| `--raft-port` | Port advertised for Raft traffic. |
| `--http-ports` | HTTP ports to bind. |
| `--https-ports` | HTTPS ports to bind. |
| `--https-certificate` | HTTPS certificate path. |
| `--https-certificate-password` | HTTPS certificate password. |
| `--wal-adapter` | Parsed option with default `rocksdb`; the current server construction path always creates `RocksDbWAL`. |
| `--rocksdb-wal-path` | Parsed RocksDB WAL path option. Not used by the current server construction path. |
| `--rocksdb-wal-revision` | Parsed RocksDB WAL revision option. Not used by the current server construction path. |
| `--sqlite-wal-path` | WAL path currently passed to `RocksDbWAL`. |
| `--sqlite-wal-revision` | WAL revision currently passed to `RocksDbWAL`. |

The current server construction path uses `RocksDbWAL` with the configured SQLite path/revision option names. Prefer constructing your own host if you need exact storage-option naming.

## Testing

Build:

```shell
dotnet build Kommander.sln
```

Run tests:

```shell
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category!=Stress"
```

Cluster stress tests are kept available but are excluded from the normal test lane because they intentionally run heavier race scenarios:

```shell
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category=Stress"
```

Useful focused slices:

```shell
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~TestSmallDictionary
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~Kommander.Tests.WAL
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category!=Stress&FullyQualifiedName~TestTwoNodeCluster"
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category!=Stress&FullyQualifiedName~TestThreeNodeCluster"
```

## Current Limitations

- Dynamic cluster membership is supported via `IRaft` (`JoinCluster`, `LeaveCluster`, `GetMembership`, `LocalRole`, `OnMembershipChanged`) on every transport, including graceful leave and cross-partition learner promotion over gRPC/REST. Gossip dissemination and the SWIM failure detector are wired only on the in-memory transport today; on gRPC/REST they are not yet implemented and the failure detector is disabled by default (`PingInterval = TimeSpan.Zero`). See the [Dynamic Membership Developer Guide](docs/dynamic-membership-developer-guide.md).
- Partition `0` is the system partition: it cannot be created, split, merged, or removed, and the `_RaftSystem` log type is reserved. Application data may still be co-located there using any other log type (see Concepts).
- RocksDB WAL format `2.0.0` is not compatible with pre-`0.10.7` id-only RocksDB WAL keys. Start with a fresh data directory or migrate existing keys.
- WAL compaction is checkpoint-driven. Historical entries below the last committed checkpoint are removable; uncheckpointed history is retained.
- The Nixie actor runtime has been removed from the production library. Existing code that passed an `ActorSystem` to `RaftManager` must use the new constructor shown above.
- `RedisDiscovery` is not implemented in this release.
- The sample server is a demonstration host, not a complete production deployment template.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
