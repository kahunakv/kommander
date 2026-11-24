# Kommander (Raft Consensus)

Kommander is an open-source distributed consensus library for C#/.NET. It implements the Raft protocol with partitioned log replication on a durable write-ahead log (WAL). You can select or replace the storage engine, the discovery provider and the network transport.

[![NuGet](https://img.shields.io/nuget/v/Kommander.svg?style=flat-square)](https://www.nuget.org/packages/Kommander)
[![NuGet](https://img.shields.io/nuget/dt/Kommander)](https://www.nuget.org/packages/Kommander)

## Used In Production

Kommander is the consensus layer of two production products.

- **[CamusDB](https://camusdb.github.io/):** a distributed SQL database for .NET. Its transactional key-value storage layer is replicated with Kommander.
- **[Kahuna](https://kahunakv.github.io/):** distributed locking with fencing tokens and leases, and a consistent key/value store with transactions and watchers.

## Features

- **Raft consensus:** each partition has a separate leader election and a separate replicated log, with checkpoints.
- **Partitioned replication:** application data is distributed across independent Raft groups. A node can lead some partitions and follow others.
- **[Elastic partitions](docs/elastic-partitions-developer-guide.md):** create, split, merge, and remove partitions at runtime. The hash ring is fully covered during a split or a merge.
- **[Dynamic cluster membership](docs/dynamic-membership-developer-guide.md):** add and remove nodes in a running cluster. The voting roster is a committed, versioned record on the system partition.
- **[Log catch-up and backfill](docs/log-backfill-developer-guide.md):** a lagging follower receives the missing entries in bounded and anchored chunks. If the leader has compacted them, the follower receives a snapshot.
- **[Partition quiescence](docs/partition-quiescence-developer-guide.md):** idle partitions stop their heartbeats. This makes thousands of mostly idle partitions practical.
- **[Leader balancing](docs/leader-balancer-developer-guide.md):** spreads partition leaders across nodes by count and then by load. Off by default.
- **[Replica placement](docs/replica-placement-developer-guide.md):** stores a fixed number of copies of each range on a subset of the nodes, with quorum computed over that range's replicas. Off by default, which means full replication.
- **Durable WAL with automatic compaction:** RocksDB and SQLite adapters, plus an in-memory adapter for tests.
- **Pluggable transport:** gRPC or REST/JSON, with route extensions for an existing ASP.NET Core host. Tests can use the in-memory transport.
- **Pluggable discovery:** static and dynamic node lists, and UDP multicast.
- **[Node authentication](docs/node-transport-authentication-developer-guide.md):** shared-secret HMAC or mutual TLS, with optional certificate pinning.
- **Batch and manual proposals:** replicate several entries in one proposal, and commit or roll back explicitly when you turn off auto-commit.
- **Hybrid logical clocks:** proposal tickets use timestamps that preserve causality.
- **[Thread-free embedded mode](docs/thread-free-embedded-mode-developer-guide.md):** a node can run on single-threaded WebAssembly.
- **[Correctness testing](#correctness-testing):** Jepsen and deterministic simulation test the code, and TLA+ models check the commit path.

## About Raft And Kommander

Raft synchronizes a durable log across a cluster so that the nodes maintain the same state machine. A leader writes each proposed change to its log and replicates it to the followers. The change is committed after a quorum acknowledges it.

In Kommander, each partition is a separate Raft group with a separate leader. Order is preserved within a partition, and throughput grows when the workload can be routed by key.

## Documentation

Start with the [Architecture Overview](docs/architecture-overview.md). The other guides cover one topic each.

| Topic | Guides |
| --- | --- |
| Cluster | [Dynamic Membership](docs/dynamic-membership-developer-guide.md) · [Node Transport Authentication](docs/node-transport-authentication-developer-guide.md) |
| Partitions | [Elastic Partitions](docs/elastic-partitions-developer-guide.md) · [Partition Quiescence](docs/partition-quiescence-developer-guide.md) · [Partition Scaling](docs/partition-scaling-developer-guide.md) · [System-Partition State Snapshots](docs/system-partition-state-snapshots-developer-guide.md) |
| Placement and balance | [Replica Placement](docs/replica-placement-developer-guide.md) · [Leader Balancer](docs/leader-balancer-developer-guide.md) · [Partition Log Rate Signal](docs/partition-log-rate-signal-developer-guide.md) |
| Replication | [Log Catch-Up & Backfill](docs/log-backfill-developer-guide.md) · [Heterogeneous Write Coalescing](docs/heterogeneous-write-coalescing-developer-guide.md) |
| Storage | [WAL Commit Durability](docs/wal-commit-durability-developer-guide.md) · [Shared RocksDB Memory](docs/shared-rocksdb-memory-developer-guide.md) |
| WebAssembly | [Thread-Free Embedded Mode](docs/thread-free-embedded-mode-developer-guide.md) |

## Packages And Targets

Kommander multi-targets `.NET 8.0`, `.NET 10.0` and `.NET 11.0`. Install it from NuGet.

```shell
dotnet add package Kommander
```

You can also use the NuGet Package Manager Console.

```powershell
Install-Package Kommander
```

## Concepts

| Concept | Meaning |
| --- | --- |
| **Node** | A running `RaftManager` instance. Its endpoint is built from `RaftConfiguration.Host` and `RaftConfiguration.Port`. |
| **Partition** | A separately elected Raft group. Partition `0` is the system partition. User partitions start at `1`. |
| **Leader** | The node currently allowed to accept proposals for a partition. |
| **Follower** | A node that receives and persists log entries from a partition leader. |
| **Proposal** | A set of log entries written by the leader and replicated to followers. It is complete after quorum acknowledgment. |
| **Commit** | The durable state transition that marks proposed logs as committed. |
| **Checkpoint** | A replicated log entry that marks a stable point in a partition log. |
| **WAL** | The write-ahead log, which persists all entry types. |

Kommander replicates its configuration on partition `0` with the reserved `_RaftSystem` log type. Entries on partition `0` with any other log type are dispatched to the consumer callbacks, as on a user partition. The lifecycle operations do not accept partition `0`.

## Quick Start

This example creates one node that uses static discovery and gRPC, with RocksDB storage. In a real cluster, run one `RaftManager` per node. Give each node a unique host and port, and a discovery list of the other nodes.

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

With a network transport, each process must expose the matching Raft endpoints. This host exposes gRPC.

```csharp
using Kommander.Communication.Grpc;

WebApplication app = builder.Build();
app.MapGrpcRaftRoutes();
app.Run();
```

This host exposes REST/JSON.

```csharp
using Kommander.Communication.Rest;

WebApplication app = builder.Build();
app.MapRestRaftRoutes();
app.Run();
```

The sample server in `Kommander.Server` maps both.

## Transport Security And Node Authentication

### Threat Model

TLS encrypts traffic and proves the server's identity when the certificate is validated correctly. It does not authenticate the calling node. Unless you enable node authentication, any client that can reach a Raft port can send handshake, vote, append, or step-down messages.

In-memory communication is excluded from these requirements.

### Authentication Modes

Set the mode in `RaftTransportSecurityOptions.NodeAuthenticationMode`.

| Mode | Description |
| --- | --- |
| `Disabled` | No authentication. A warning is logged at startup. |
| `SharedSecret` | HMAC-SHA256 signature per request, derived from a shared cluster secret. Recommended for production. |
| `MutualTls` | Both sides authenticate during the TLS handshake. No per-request signature is sent. See [Mutual TLS](#mutual-tls). |

### Shared Secret Protocol

| Header | Content |
| --- | --- |
| `X-Kommander-Cluster-Auth` | Base64url HMAC-SHA256 signature |
| `X-Kommander-Cluster-Node` | Sender node endpoint |
| `X-Kommander-Cluster-Timestamp` | Unix milliseconds |
| `X-Kommander-Cluster-Nonce` | Random 128-bit nonce, base64url |

The signature covers `method + path + sender + timestamp + nonce + SHA-256(body)`. A request is refused when a field is missing or malformed, when its timestamp is outside the configured window (default 60 s), or when its nonce was already used. HMAC comparisons use constant-time equality.

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

Generate at least 256 bits of random secret material. Distribute it through a secret manager, a Kubernetes or Docker secret, or an environment variable. Do not put the secret in source control.

### Mutual TLS

In `MutualTls` mode the client validates the server certificate, and the server requires and validates a client certificate. The HMAC headers are not used.

```csharp
RaftConfiguration configuration = new()
{
    Host = "node-1",
    Port = 8001,
    TransportSecurity = new RaftTransportSecurityOptions
    {
        NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
        ClientCertificatePath = "/etc/kommander/node-1.pfx",
        ClientCertificatePassword = Environment.GetEnvironmentVariable("NODE_CERT_PASSWORD"),
        TrustedClientCertificateThumbprints =
        [
            "5E9B...", // node-1
            "A31C...", // node-2
            "7F02..."  // node-3
        ]
    }
};
```

The `Kommander.Server` host has equivalent flags. The published container passes the same flags to `dotnet /app/Kommander.Server.dll`.

```shell
dotnet run --project Kommander.Server -- \
  --node-auth-mode MutualTls \
  --https-certificate /etc/kommander/node-1.pfx \
  --https-certificate-password "$NODE_CERT_PASSWORD" \
  --client-certificate /etc/kommander/node-1.pfx \
  --client-certificate-password "$NODE_CERT_PASSWORD" \
  --trusted-client-cert-thumbprint 5E9B... A31C... 7F02...
```

If you omit `--client-certificate`, the node presents its `--https-certificate`.

#### Computing thumbprints

A thumbprint is the **uppercase hex of the SHA-256 hash over the DER-encoded certificate**. Note that `X509Certificate2.Thumbprint` in .NET is SHA-1 and does not match. Colons and lower-case letters are normalized on input, which means that you can paste the output of `openssl x509 -fingerprint -sha256`. This command prints the canonical form.

```sh
openssl pkcs12 -in node-1.pfx -clcerts -nokeys -passin pass: \
  | openssl x509 -outform DER \
  | shasum -a 256 | cut -d' ' -f1 | tr 'a-f' 'A-F'
```

#### Shared vs. per-node certificates

| Approach | Identity proven | Cost |
| --- | --- | --- |
| **Shared:** all nodes present the same `.pfx` | Membership in the cluster only. Any holder of the file can impersonate any node. | One certificate to distribute. |
| **Per-node:** each node has a separate certificate and lists the thumbprints of the others | Per-node identity. You can revoke a compromised node by removing one thumbprint. | N thumbprints to distribute and update on membership changes. |

If `TrustedClientCertificateThumbprints` is empty, any client certificate that completes the handshake is accepted, and a warning is logged at startup.

#### Rotating certificates

If you skip the first step, the cluster is partitioned as soon as the first node rotates.

1. Add the thumbprint of the new certificate to `TrustedClientCertificateThumbprints` on all nodes, next to the old one. Restart each node.
2. Switch `ClientCertificatePath` to the new certificate, one node at a time.
3. Remove the old thumbprint from all nodes.

#### Operational consequences

- **The HTTPS port becomes cluster-internal.** Kestrel requires a client certificate from all callers on that listener, including callers of routes outside `/v1/raft/*`. Bind a separate listener for any public HTTPS surface.
- **HTTP/3 is disabled** on HTTPS listeners in this mode. This is logged once at startup.

The combination of `MutualTls` and `AllowInsecureCertificateValidation` throws at startup.

### Certificate Validation

| Option | Default | Description |
| --- | ---: | --- |
| `RequireTls` | `true` | Reject requests that arrive over unencrypted HTTP. Throws at startup if `HttpScheme` is `http://`. |
| `AllowInsecureCertificateValidation` | `false` | Skip remote certificate validation. Development only. A warning is logged at startup. |
| `TrustedServerCertificateThumbprints` | empty | SHA-256 hex thumbprints of accepted server certificates. When set, only pinned certificates are accepted regardless of chain trust. |
| `TrustedClientCertificateThumbprints` | empty | SHA-256 hex thumbprints of accepted **client** certificates, used in `MutualTls` mode. When empty, any client certificate that completes the handshake is accepted. |

When neither `AllowInsecureCertificateValidation` nor thumbprints are set, the platform default certificate chain and hostname validation applies.

### Kommander.Server CLI Options

| Option | Description |
| --- | --- |
| `--node-auth-mode` | `Disabled`, `SharedSecret`, or `MutualTls` |
| `--node-shared-secret` | Shared secret value. Required when mode is `SharedSecret`. |
| `--node-auth-header` | Override the default `X-Kommander-Cluster-Auth` header name. |
| `--allow-insecure-certificate-validation` | Skip TLS certificate validation. Development only. |
| `--trusted-server-cert-thumbprint` | One or more trusted server certificate SHA-256 thumbprints (hex). |
| `--trusted-client-cert-thumbprint` | One or more trusted client certificate SHA-256 thumbprints (hex). Used in `MutualTls` mode. |
| `--client-certificate` | Path to the PKCS#12 client certificate presented in `MutualTls` mode. Defaults to `--https-certificate`. |
| `--client-certificate-password` | Password for the client certificate. |

### Secret Rotation

A node has one secret at a time, and nodes with different secrets cannot authenticate each other. Plan a maintenance window to change the secret, or use `MutualTls`, where you can roll credentials one node at a time.

## Creating A Node

`RaftManager` is the main implementation of `IRaft`.

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
| Replication | `ReplicateLogs`, `ReplicateEntries`, `ReplicateCheckpoint`, `CommitLogs`, `RollbackLogs` |
| Log retention | `SetMinRetainIndex`, `AcquireRetentionHold` |
| Catch-up observability | `GetFollowerLagAsync`, `GetFollowerProgress`, `GetActiveNodes`, `GetLastNodeActivity` |
| Load observability | `GetPartitionLogOpsPerSecond`, `GetPartitionWalQueueDepth`, `GetPartitionCommitWaitMs` |
| Partition routing | `GetPartitionKey`, `GetPrefixPartitionKey` |
| Elastic partitions | `CreatePartitionAsync`, `RemovePartitionAsync`, `SplitPartitionAsync`, `MergePartitionsAsync`, `GetPartitionMap`, `GetPartitionGeneration`, `RegisterStateMachineTransfer`, `RegisterSystemStateTransfer` |
| Partition events | `OnPartitionMapChanged` |
| Transport entry points | `Handshake`, `RequestVote`, `Vote`, `AppendLogs`, `CompleteAppendLogs` |
| Components | `WalAdapter`, `Communication`, `Discovery`, `Configuration`, `HybridLogicalClock`, `ReadScheduler`, `WalScheduler` |
| Events | `OnRestoreStarted`, `OnRestoreFinished`, `OnReplicationError`, `OnLogRestored`, `OnReplicationReceived`, `OnLeaderChanged` |

The transport entry points are for communication adapters and endpoint handlers. Application code should use the replication members.

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
| `GrpcChannelsPerNode` | `4` | Pooled gRPC channels (and matching streaming calls) per peer URL. Clamped to [1, 64] at runtime. See [gRPC Channels](#grpc-channels). |
| `GrpcEnableMultipleHttp2Connections` | `false` | When `true`, each pooled channel may open additional TCP/HTTP2 connections on demand, which raises the stream ceiling on saturated links. |
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
| `StartElectionTimeoutIncrement` | `100 ms` | Obsolete, no effect. A failed election draws a fresh random timeout. |
| `EndElectionTimeoutIncrement` | `200 ms` | Obsolete, no effect. A failed election draws a fresh random timeout. |
| `EnableQuiescence` | `true` | Idle partitions stop per-partition heartbeats, and followers gate elections on SWIM node liveness. Requires `PingInterval > 0` and `PingInterval < StartElectionTimeout` (validated at construction). See [Partition Quiescence](#partition-quiescence). |
| `QuiesceAfter` | `1500 ms` | How long a partition must be idle (no proposals, no in-flight replication) before its leader quiesces it. |
| `PingInterval` | `1000 ms` | SWIM probe cadence. With quiescence on, a follower detects a dead leader roughly one `PingInterval` after the crash. |
| `SlowRaftStateMachineLog` | `50 ms` | Slow partition state-machine operation warning threshold. |
| `SlowRaftWALMachineLog` | `25 ms` | Obsolete, no effect. Use `WalStallWarnThreshold`. |
| `WalStallStepDownTimeout` | `3 s` | How long a leader's WAL write can be unanswered by the storage engine before the leader steps down. `0` disables the watchdog and the candidacy gate. See [WAL Stall Handling](#wal-stall-handling). |
| `CompactionLiveReplicaLagBudget` | `1000000` | Entries that the leader retains below the compaction checkpoint for a live follower that has not replicated them. A follower behind by less than this is served by log backfill. The default covers a 30–60 s device pause under load. The leader sends the floor to its followers, and the hold survives a leader change. A silent replica holds for `CompactionSilentPeerRetentionWindow`. `0` disables the hold. |
| `WalStallWarnThreshold` | `500 ms` | Age past which a pending WAL write on any node is logged as a stall. `0` disables the log lines only. The peer rules then use 500 ms. See [WAL Stall Handling](#wal-stall-handling). |
| `ReadIOThreads` | `8` | Fair scheduler workers for synchronous WAL reads. |
| `WriteIOThreads` | `4` | Fair scheduler workers for synchronous WAL writes. |
| `IdleWorkerMaxSpinBeforePark` | `20 µs` | Upper bound of how long one idle worker of the executor pool, the WAL write scheduler or the WAL read scheduler polls for work before it parks. The spin costs a little CPU and avoids a kernel wake when work arrives in a fast stream. `Zero` turns it off. |
| `BackfillThreshold` | `10` | How many entries a follower may lag before the leader starts backfill. |
| `MaxBackfillEntriesPerRound` | `128` | Maximum entries shipped in a single backfill round. |
| `CompactEveryOperations` | `10000` | Persisted commit/follower-append operations between automatic WAL compaction triggers per partition. `0` or below disables automatic compaction. |
| `CompactNumberEntries` | `100` | Max entries removed per adapter compaction batch. Values below `1` are treated as `1`. |
| `MaxEntriesPerCompaction` | `5000` | Upper bound on entries removed by one triggered compaction pass before yielding. Values below `CompactNumberEntries` are clamped up. |

### WAL Stall Handling

Election timing measures the network liveness of a leader only, and a leader on a paused disk can send heartbeats. `WalStallStepDownTimeout` is the bound that moves leadership off such a node.

- A leader whose oldest WAL write is unanswered past the timeout steps down in the same term. Its waiting proposals fail at once, which lets callers re-route.
- It notifies the most caught-up peer, and that peer's election starts within a round trip.
- It does not campaign again until the write completes.

Each append acknowledgement carries the follower's pending-write age and its durable commit frontier. When a peer's pending write reaches `WalStallWarnThreshold`, these rules apply.

| Rule | Metric |
| --- | --- |
| The leader sends no entry-carrying backfill to the peer. | `raft.backfill.wal_stall_pauses_total` |
| The leader starts no snapshot transfer to the peer. | `raft.snapshot.transfers_deferred_wal_stall_total` |
| A node does not open a snapshot session while its local disk is stalled. | `raft.snapshot.installs_refused_wal_stall_total` |

The leader also retains the WAL from the peer's durable frontier, within `CompactionLiveReplicaLagBudget`. The catch-up is then served from the log when the disk answers.

A stall is logged when it crosses the threshold, every 10 s after that, and when it clears. The `raft.wal.oldest_pending_write_age_ms` gauge and the `raft.wal.write_duration_ms` histogram give the continuous signals.

### gRPC Channels

Each pooled channel has a separate TCP/HTTP2 connection. The effective concurrency per peer is approximately `GrpcChannelsPerNode × MaxConcurrentStreams`. Replication appends to a peer always use the first streaming call, which preserves their order. Other traffic is spread round-robin.

A channel that spends 30 s in `TransientFailure` with no connect attempt (grpc-dotnet issue #2734) is disposed and rebuilt, and its streaming call re-opens on the new channel. `SharedChannels.GetChannelHealth(url)` shows this per peer. The metrics are the `raft.transport.channel_rebuilds_total` counter and the `raft.transport.channels_in_transient_failure` gauge.

## Replicating Logs

Call `ReplicateLogs` on the partition leader.

```csharp
RaftReplicationResult result = await raft.ReplicateLogs(
    partitionId: 1,
    type: "OrderCreated",
    data: payload,
    cancellationToken: cancellationToken
);
```

You can replicate several entries in one proposal.

```csharp
RaftReplicationResult result = await raft.ReplicateLogs(
    partitionId: 1,
    type: "OrderEvent",
    logs: new[] { createdPayload, paidPayload, shippedPayload },
    cancellationToken: cancellationToken
);
```

`ReplicateLogs` returns a `RaftReplicationResult`.

| Property | Description |
| --- | --- |
| `Success` | `true` when the operation completed successfully. |
| `Status` | Detailed `RaftOperationStatus`. |
| `TicketId` | Hybrid logical clock timestamp that identifies the proposal. |
| `LogIndex` | Last log index assigned to the proposal. |

You can also replicate to the system partition (`0`), for example for coordination data that must share a leader with partition lifecycle operations. The `_RaftSystem` log type is reserved: `ReplicateLogs(0, "_RaftSystem", …)` throws a `RaftException`.

## Manual Commit And Rollback

`ReplicateLogs` auto-commits by default. Set `autoCommit: false` to stop after the proposal reaches quorum, and then commit it with its ticket.

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

A rollback uses the same ticket.

```csharp
(bool rolledBack, RaftOperationStatus status, long rollbackLogId) =
    await raft.RollbackLogs(1, proposal.TicketId);
```

## Checkpoints

A checkpoint marks the history that compaction can remove.

```csharp
RaftReplicationResult checkpoint = await raft.ReplicateCheckpoint(1, cancellationToken);
```

## Leadership APIs

```csharp
bool quick = await raft.AmILeaderQuick(1);
bool leader = await raft.AmILeader(1, cancellationToken);
string endpoint = await raft.WaitForLeader(1, cancellationToken);
```

`AmILeaderQuick` checks cached partition state. `AmILeader` waits up to the internal leadership timeout. `WaitForLeader` returns the elected leader endpoint or throws `RaftException`.

## Operation Status Values

| Status | Meaning |
| --- | --- |
| `Success` | Operation completed successfully. |
| `Errored` | Operation failed with an internal error. |
| `NodeIsNotLeader` | The local node is not leader for the requested partition. |
| `LeaderInOldTerm` | A request came from a leader with an old term. |
| `LeaderAlreadyElected` | A leader was already known for the term. |
| `LogsFromAnotherLeader` | A follower received logs from a node other than the expected leader. |
| `ActiveProposal` | Another proposal is active. |
| `ProposalNotFound` | The supplied proposal ticket was not found. |
| `ProposalTimeout` | The proposal did not complete in time. |
| `ReplicationFailed` | Replication failed before commit. |
| `Pending` | Internal state used while asynchronous work is in progress. |
| `LogMismatch` | A follower rejected a backfill batch because it does not hold the anchored preceding entry; the leader backtracks and retries lower. |
| `SnapshotRequired` | Reserved for wire compatibility and not produced. To observe the snapshot handoff, use `GetSnapshotStatuses`. |

## Partition Routing

The partition helpers map application keys to user partitions. They do not return `0`.

```csharp
int partition = raft.GetPartitionKey("tenant-42/order-1001");
int prefixPartition = raft.GetPrefixPartitionKey("tenant-42");
```

`GetPartitionKey` uses the prefix before the last `/` separator. `GetPrefixPartitionKey` hashes the complete string.

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

The system partition events on `RaftManager` are for internal configuration replication and are not part of `IRaft`.

## WAL Adapters

| Adapter | Use case |
| --- | --- |
| `RocksDbWAL` | Durable production-oriented storage backed by RocksDB. |
| `SqliteWAL` | Durable embedded storage backed by SQLite. |
| `InMemoryWAL` | Tests and simulations only. Data is lost when the process exits. |

### WAL Durability And Compaction

Acknowledged RocksDB writes use synchronous write options by default, and SQLite uses `PRAGMA synchronous=FULL`. You can construct both adapters with `syncWrites: false` for CI or benchmarks. Throughput improves, but a process or machine crash can lose acknowledged writes.

Automatic compaction starts for a partition after `CompactEveryOperations` persisted commit or follower-append operations. A pass removes entries below the last committed checkpoint in batches of `CompactNumberEntries`, up to `MaxEntriesPerCompaction`. If your application does not replicate checkpoints, compaction has little to remove.

RocksDB WAL format `2.0.0` keys entries by partition id and log id. Directories from the older id-only format must be recreated or migrated.

```csharp
IWAL rocksDb = new RocksDbWAL("./data", "node-1", logger);
IWAL rocksDbNoSync = new RocksDbWAL("./data", "node-1", logger, syncWrites: false);

IWAL sqlite = new SqliteWAL("./data", "node-1", logger);
IWAL sqliteNoSync = new SqliteWAL("./data", "node-1", logger, syncWrites: false);

IWAL memory = new InMemoryWAL(logger);
```

A custom adapter implements `IWAL`.

```csharp
public interface IWAL : IDisposable
{
    List<RaftLog> ReadLogs(int partitionId);
    List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue);
    RaftOperationStatus Write(List<(int partitionId, List<RaftLog> logs)> logs);
    long GetMaxLog(int partitionId);
    long GetCurrentTerm(int partitionId);
    long GetLastCheckpoint(int partitionId);
    int CountPersistedLogs(int partitionId);
    int CountRemovableLogs(int partitionId);
    string? GetMetaData(string key);
    bool SetMetaData(string key, string value);
    (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(
        int partitionId,
        long lastCheckpoint,
        int compactNumberEntries);
    RaftOperationStatus DeletePartitionWAL(int partitionId);
    RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId);
    (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId);
}
```

`IWAL` also declares `RaftOperationStatus Write(List<(int, List<RaftLog>)> logs, bool sync)`, with a default implementation that forwards to the single-argument `Write`. Override it if your adapter can distinguish a durable (fsync'd) write from a buffered one.

## Communication Adapters

| Adapter | Use case |
| --- | --- |
| `GrpcCommunication` | Networked clusters using gRPC streaming. |
| `RestCommunication` | Networked clusters using REST/JSON endpoints. |
| `InMemoryCommunication` | Unit tests and in-process simulations. |

For `RestCommunication`, set `HttpScheme`, `HttpTimeout`, and `HttpVersion` on `RaftConfiguration`. Custom transports implement `ICommunication`.

## Discovery Adapters

| Adapter | Use case |
| --- | --- |
| `StaticDiscovery` | Fixed cluster membership. |
| `DynamicDiscovery` | Mutable in-memory node list controlled by the application. |
| `MulticastDiscovery` | UDP multicast discovery on local networks. |

`RedisDiscovery` is a placeholder in this release and returns no nodes. Do not use it. Custom discovery providers implement `IDiscovery`.

```csharp
public interface IDiscovery
{
    Task Register(RaftConfiguration configuration);
    List<RaftNode> GetNodes();
}
```

## Elastic Partitions

Partition lifecycle operations do not need a restart. They are serialized through the system partition and replicated to all nodes.

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

The caller must be the leader of the relevant partitions. Splits and merges use a two-phase commit, and the hash ring is complete during the transition. All four methods throw a `RaftException` for the system partition (`0`).

The **generation fence** protects writes during topology changes. Pass `expectedGeneration` to detect that the partition a client cached was split or merged.

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

The **term fence** protects writes whose preconditions were computed under a leadership that may have ended. Read the term with `GetPartitionTerm` when that work starts, for example at a staged intent or a lock check, and stamp the write with `expectedTerm`. If the node no longer serves in that term, the write returns `TermMismatch` **before anything is appended**. The result is a definite "did not take effect".

```csharp
long term = raft.GetPartitionTerm(partitionId); // -1 when this node does not host the partition

RaftReplicationResult result = await raft.ReplicateLogs(
    partitionId,
    type:         "MyEvent",
    data:         payload,
    expectedTerm: term);

if (result.Status == RaftOperationStatus.TermMismatch)
{
    // Leadership changed since the term was read; drop the belief-only state and re-decide.
}
```

`RaftProposalEntry.ExpectedTerm` applies the same fence to a `ReplicateEntries` batch. One mismatch refuses the entire batch. Subscribe to `OnLeadershipLost` to drop such state as soon as this node stops leading a partition. The event carries the term that the node led in.

Subscribe to `OnPartitionMapChanged` to receive map updates.

```csharp
raft.OnPartitionMapChanged += snapshot =>
{
    foreach (RaftPartitionRange range in snapshot)
        Console.WriteLine($"  P{range.PartitionId} {range.State} [{range.StartRange},{range.EndRange}] gen={range.Generation}");
};
```

A split moves no application data. You can register a snapshot transfer that copies the state from source to target once, on the coordinator node only. The application must bring the state to the other replicas.

```csharp
raft.RegisterStateMachineTransfer(new MySnapshotTransfer());

// Separately, whole-partition snapshots seed a follower that has fallen below the WAL
// compaction floor (e.g. a freshly placed replica) — see the replica placement guide.
raft.RegisterPartitionStateTransfer(new MyPartitionStateTransfer());
```

For more information, see the [Elastic Partitions Developer Guide](docs/elastic-partitions-developer-guide.md).

## Dynamic Cluster Membership

Nodes can join and leave a running cluster. The voting roster is a committed, versioned record on the system partition, and it changes only through Raft. A gossip and SWIM layer spreads the roster faster and detects dead nodes, but it is advisory.

A new node joins as a non-voting **learner**. It becomes a **voter** when it is within `LearnerPromotionLag` entries of the committed log for `LearnerPromotionStableWindow`. Learners do not count toward quorum, and adding a node cannot stall commits.

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

`OnMembershipChanged` fires in commit order on each roster change, including a failure-driven eviction. Use `MembershipVersion` as a monotonic sequence number.

```csharp
raft.OnMembershipChanged += membership =>
{
    string members = string.Join(", ", membership.Members.Select(m => $"{m.Endpoint}({m.Role})"));
    Console.WriteLine($"[v{membership.MembershipVersion}] {members}");
};
```

Each committed step changes one node. A removal that would leave zero voters is refused with `InsufficientVoters`.

Membership and the failure detector work on all transports. The failure detector is enabled by default (`PingInterval` is `1 s`). To disable it, set `PingInterval` to `TimeSpan.Zero`, which is valid only if `EnableQuiescence` is `false`.

For the lifecycle and an operations runbook, see the [Dynamic Membership Developer Guide](docs/dynamic-membership-developer-guide.md).

## Log Catch-Up & Backfill

Catch-up is automatic and uses two paths.

- The **live path** broadcasts each new proposal and commit. It is unanchored, and a follower that is briefly behind does not stall a live proposal.
- The **backfill path** starts when a follower lags by more than `BackfillThreshold`. The leader sends the missing range in chunks of `MaxBackfillEntriesPerRound`, each **anchored** with the index and term of the preceding entry. A follower that does not hold the anchor answers `LogMismatch`, and the leader backtracks. A follower's log can therefore contain no gap and no divergent tail.

If the leader has compacted the entries that a follower needs, it installs a snapshot. Failed transfers are retried with exponential backoff. `raft.GetSnapshotStatuses(partitionId)` lists the followers whose snapshot fails repeatedly, including the permanent case where no snapshot transfer implementation is registered.

```csharp
long? lag = await raft.GetFollowerLagAsync(partitionId: 1, followerEndpoint: "host2:8002");

// Lock-free, per-operation cheap: what the leader last heard about the follower's disk.
RaftFollowerProgress? progress = raft.GetFollowerProgress(partitionId: 1, followerEndpoint: "host2:8002");
if (progress is { WalStalled: false } && progress.EntriesBehind(raft.GetCommitIndex(1)) < 1_000)
    // the follower can plausibly apply what was just committed
```

`GetFollowerLagAsync` uses the follower's protocol commit frontier, which includes entries that are only queued for its disk. `GetFollowerProgress` uses what the disk has written, plus the pending-write age and the stall flag. Use `GetFollowerProgress` before you wait for a follower to apply something. It is `null` on a node that does not lead the partition.

For more information, see the [Log Catch-Up & Backfill Developer Guide](docs/log-backfill-developer-guide.md).

## Partition Quiescence

Heartbeats are per partition, and their cost grows as `O(nodes × partitions)` even when the cluster is idle. After an idle time of `QuiesceAfter`, the leader of a partition sends its followers one **quiesce marker** and stops the heartbeats. The followers then depend on the **SWIM failure detector**, which probes each node once for all of its partitions. They fail over on SWIM `Suspect`, after roughly one `PingInterval`.

Any write wakes the partition. Quiescence is enabled by default and is a local optimization: it does not change quorum or the commit path.

```csharp
RaftConfiguration config = new()
{
    EnableQuiescence = true,                         // default
    QuiesceAfter = TimeSpan.FromMilliseconds(1500),  // idle window before a leader quiesces
    PingInterval = TimeSpan.FromSeconds(1),          // SWIM probe cadence; must be > 0 and < StartElectionTimeout
};
```

When quiescence is enabled, `RaftConfiguration.Validate()` requires `PingInterval > 0` and `PingInterval < StartElectionTimeout`.

For more information, see the [Partition Quiescence Developer Guide](docs/partition-quiescence-developer-guide.md).

## Leader Balancing

Each partition elects its leader independently. Over time, one node can lead most partitions, or most of the busy ones. The **leader balancer** corrects both cases.

The leader of the system partition is the controller. Nodes gossip a **load report** of the partitions they lead and a smoothed throughput and queue-depth score for each. The controller plans moves in two tiers: **count balance** toward the cluster average, then count-neutral **load balance** swaps. It sends a **suggestion** to the current leader, which validates it and performs the standard Raft handoff.

The balancer is **off by default**. A stale or dropped suggestion can waste or skip a move and cannot affect safety.

```csharp
RaftConfiguration config = new()
{
    EnableLeaderBalancer = true,                          // default false
    LeaderBalancerInterval = TimeSpan.FromSeconds(30),    // how often the controller plans a pass
    MaxMovesPerPass = 4,                                  // cap on moves planned per pass
    MaxConcurrentTransfers = 2,                           // cap on moves in flight cluster-wide
    MoveCooldown = TimeSpan.FromSeconds(60),              // per-partition cooldown after a move
};
```

The `Kommander` meter exposes `raft.balancer.moves_total` (by outcome), `raft.balancer.skipped_passes_total`, and the `raft.balancer.count_imbalance` and `raft.balancer.load_imbalance` gauges. The gauges trend toward zero as the cluster converges.

For more information, see the [Leader Balancer Developer Guide](docs/leader-balancer-developer-guide.md).

## Utilities

### Hashing

`HashUtils` provides xxHash-based helpers and jump consistent hashing.

```csharp
int nodeId = HashUtils.SmallSimpleHash("node-1");
ulong hash = HashUtils.SimpleHash("tenant-42");
ulong bucket = HashUtils.StaticHash("tenant-42", buckets: 128);
long prefixed = HashUtils.PrefixedHash("tenant-42/order-1", '/', buckets: 128);
long inversePrefixed = HashUtils.InversePrefixedHash("tenant-42/order-1", '/', buckets: 128);
int consistent = HashUtils.ConsistentHash("tenant-42", numBuckets: 128);
```

### Hybrid Logical Clocks

Proposals use `HybridLogicalClock` and `HLCTimestamp`, and replication results expose them.

```csharp
HybridLogicalClock clock = new();
HLCTimestamp timestamp = clock.SendOrLocalEvent(nodeId: 1);
```

### Parallelization Extensions

`Kommander.Support.Parallelization` contains `ForEachAsync` extensions for `IEnumerable<T>`, `IAsyncEnumerable<T>`, `List<T>`, arrays, and `HashSet<T>`.

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

`Kommander.Server` is a runnable ASP.NET Core host with `StaticDiscovery` and `GrpcCommunication`. It starts a background replication service. It maps the REST and gRPC Raft routes. It is a demonstration host and is not a complete production deployment template.

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
| `--wal-adapter` | `rocksdb` (default) or `sqlite`. |
| `--rocksdb-wal-path`, `--rocksdb-wal-revision` | RocksDB WAL location. |
| `--sqlite-wal-path`, `--sqlite-wal-revision` | SQLite WAL location. |

If the path of the selected adapter is empty, the server uses the path and revision options of the other adapter and logs a warning.

## Testing

```shell
dotnet build Kommander.sln
```

The stress tests run heavier race scenarios and are excluded from the normal test lane.

```shell
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category!=Stress"
```

Run the stress tests with the category filter.

```shell
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category=Stress"
```

You can also run a focused slice.

```shell
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~TestSmallDictionary
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter FullyQualifiedName~Kommander.Tests.WAL
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category!=Stress&FullyQualifiedName~TestTwoNodeCluster"
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category!=Stress&FullyQualifiedName~TestThreeNodeCluster"
```

## Correctness Testing

Three methods search for defects that unit and cluster tests do not expect.

| Method | What it tests | Where |
| --- | --- | --- |
| [Jepsen](#jepsen) | A real five-node cluster in Docker, from the outside, under injected faults. | Separate repository, on demand. |
| [Deterministic Simulation Testing (DST)](#deterministic-simulation-testing-dst) | Real three-node clusters in one process, in a simulated environment. | A smoke run on each push and pull request. The full search is nightly. |
| [TLA+](#tla) | A formal model of the commit and durability path. | `formal/run-tlc.sh`. |

### Jepsen

[Jepsen](https://jepsen.io) sends client operations to a five-node cluster while a nemesis injects network partitions, process kills and pauses, membership changes, and clock skew. A checker then verifies the recorded history. The suite has two workloads.

- **`register`:** linearizability of a compare-and-set register over a replicated key/value map, checked with Knossos.
- **`log-append`:** the Raft Log Matching Property. Replicas must agree, and each acknowledged entry must be applied exactly once.

The suite embeds `IRaft` in a small ASP.NET host that references Kommander by project, and a run tests the code in your working tree. It can test the gRPC and the REST transports. The suite and its `FINDINGS.md` are in the **[kommander-jepsen](https://github.com/kahunakv/kommander-jepsen)** repository.

### Deterministic Simulation Testing (DST)

DST runs the real Kommander code in a simulated environment, with virtual time, a simulated transport, and a simulated WAL decorator. A seeded generator builds a plan of actions such as client writes and crashes. The harness checks the cluster after each step.

- **Safety invariants:** for example, one leader per term and agreement of committed entries.
- **Liveness rules:** a connected majority must elect a reachable leader within a bound, and the cluster must converge after the faults heal.
- **Client history:** each acknowledged write survives, and reads are correct.

A saved plan replays the same actions in the same order, and a shrinker reduces a failing plan to the few actions that cause the failure. Plans with a known cause go into a regression folder that is replayed on each run.

The nightly job (`.github/workflows/nightly-dst.yml`) runs the `DSTRandom` search and uploads the shrunk plans as artifacts.

```shell
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category=DSTSmoke"
dotnet test Kommander.Tests/Kommander.Tests.csproj --filter "Category=DSTRandom"
```

For replay and how to read a failure, see [Kommander.Tests/Simulation/README.md](Kommander.Tests/Simulation/README.md).

### TLA+

The `formal/` folder contains [TLA+](https://lamport.azurewebsites.net/tla/tla.html) models of the commit and durability path of one partition. They include the single-fsync rule, crash and restore, and anchored backfill over an unreliable network. The TLC model checker explores all reachable states of a small configuration and prints the first state that breaks a property.

**Trace validation** connects the model to the code. TLC replays the write-ahead events that a DST run recorded through the same model operators, and each frontier value must match.

Each fixed defect has a **control** configuration that turns the defect back on. The control must fail, which shows that the model can see the defect.

```shell
formal/run-tlc.sh                    # every config and every stored trace
formal/run-tlc.sh --trace <dir|file> # validate traces recorded by a DST run
```

TLC needs Java 11 or later. The script downloads a pinned `tla2tools.jar`. For the properties and the findings, see [formal/README.md](formal/README.md).

## Current Limitations

- Partition `0` cannot be created, split, merged, or removed, and the `_RaftSystem` log type is reserved.
- RocksDB WAL format `2.0.0` is not compatible with the id-only keys of releases before `0.10.7`. Start with a fresh data directory or migrate existing keys.
- WAL compaction is checkpoint-driven. Uncheckpointed history is retained.
- The Nixie actor runtime has been removed. Code that passed an `ActorSystem` to `RaftManager` must use the constructor shown above.
- `RedisDiscovery` is not implemented in this release.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
