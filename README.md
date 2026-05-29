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
- **Durable write-ahead logging:** Built-in WAL adapters for RocksDB and SQLite persist proposed, committed, rolled-back, and checkpoint entries before state-machine callbacks run.
- **Testing-friendly in-memory components:** `InMemoryWAL`, `InMemoryCommunication`, and focused test utilities support fast local simulations without external infrastructure.
- **Cluster discovery options:** Static discovery for fixed clusters, dynamic discovery for application-managed membership lists, and multicast discovery for local-network discovery.
- **Transport choices:** gRPC for networked clusters, REST/JSON for HTTP-based integration and debugging, and in-memory communication for tests.
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

## Packages And Targets

Kommander targets `.NET 8.0`.

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

**Partition:** A separately elected Raft group. Partition `0` is reserved for Kommander system configuration. Application data should use user partitions, which start at `1`.

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
| Lifecycle | `JoinCluster`, `LeaveCluster`, `UpdateNodes` |
| Cluster state | `Joined`, `IsInitialized`, `GetNodes`, `GetLocalEndpoint`, `GetLocalNodeId`, `GetLocalNodeName` |
| Leadership | `AmILeaderQuick`, `AmILeader`, `WaitForLeader` |
| Replication | `ReplicateLogs`, `ReplicateCheckpoint`, `CommitLogs`, `RollbackLogs` |
| Partition routing | `GetPartitionKey`, `GetPrefixPartitionKey` |
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
| `HttpScheme` | `https://` | Scheme used by `RestCommunication`. |
| `HttpAuthBearerToken` | empty | Bearer token attached by `RestCommunication`. |
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
| `SlowRaftStateMachineLog` | `50 ms` | Slow partition state-machine operation warning threshold. |
| `SlowRaftWALMachineLog` | `25 ms` | Slow WAL warning threshold. |
| `ReadIOThreads` | `8` | Fair scheduler workers for synchronous WAL reads. |
| `WriteIOThreads` | `4` | Fair scheduler workers for synchronous WAL writes. |
| `CompactEveryOperations` | `10000` | Configured compaction interval value. WAL adapters expose compaction, but callers should verify scheduling behavior before relying on automatic compaction. |
| `CompactNumberEntries` | `100` | Max entries to remove when compaction is invoked. |

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

## Partition Routing

Use partition helpers to map application keys to user partitions:

```csharp
int partition = raft.GetPartitionKey("tenant-42/order-1001");
int prefixPartition = raft.GetPrefixPartitionKey("tenant-42");
```

`GetPartitionKey` uses the prefix before the last `/` separator. `GetPrefixPartitionKey` hashes the complete string provided.

Partition `0` is reserved for system configuration. Do not call public replication APIs with partition `0`; `RaftManager` rejects userland writes to the system partition.

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

RocksDB:

```csharp
IWAL wal = new RocksDbWAL("./data", "node-1", logger);
```

SQLite:

```csharp
IWAL wal = new SqliteWAL("./data", "node-1", logger);
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
    RaftOperationStatus CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries);
    void Dispose();
}
```

## Communication Adapters

| Adapter | Use case |
| --- | --- |
| `GrpcCommunication` | Networked clusters using gRPC streaming. |
| `RestCommunication` | Networked clusters using REST/JSON endpoints. |
| `InMemoryCommunication` | Unit tests and in-process simulations. |

For `RestCommunication`, configure `HttpScheme`, `HttpAuthBearerToken`, `HttpTimeout`, and `HttpVersion` on `RaftConfiguration`.

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

## Dynamic Partitions

Initial user partitions are replicated through the reserved system partition and then started on every node. If you keep a concrete `RaftManager` reference, you can request a partition split:

```csharp
RaftManager manager = /* created node */;
await manager.SplitPartition(partitionId);
```

The caller must be initialized, the target partition cannot be partition `0`, and the local node must be leader for the target partition.

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
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category!=Stress&FullyQualifiedName~TestTwoNodeCluster"
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category!=Stress&FullyQualifiedName~TestThreeNodeCluster"
```

## Current Limitations

- Cluster membership is discovery-driven; there is no public membership-change API on `IRaft`.
- Partition `0` is reserved and not available for application replication.
- The Nixie actor runtime has been removed from the production library. Existing code that passed an `ActorSystem` to `RaftManager` must use the new constructor shown above.
- `RedisDiscovery` is not implemented in this release.
- The sample server is a demonstration host, not a complete production deployment template.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
