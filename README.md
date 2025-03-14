# 🔱 Kommander (Raft Consensus)

Kommander is an open-source, distributed consensus library implemented in C# for the .NET platform. It leverages the Raft algorithm to provide a robust and reliable mechanism for leader election, log replication, and data consistency across clusters. Kommander is designed to be flexible and resilient, supporting multiple discovery mechanisms and communication protocols to suit various distributed system architectures.

[![NuGet](https://img.shields.io/nuget/v/Kommander.svg?style=flat-square)](https://www.nuget.org/packages/Kommander)
[![Nuget](https://img.shields.io/nuget/dt/Kommander)](https://www.nuget.org/packages/Kommander)

**This is an alpha project please don't use it in production.**

---

## Features

- **Raft Consensus Algorithm:**
  Implemented using the Raft protocol, which enables a cluster of nodes to maintain a replicated state machine by keeping a synchronized log across all nodes. For an in-depth explanation of Raft, see [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) by Diego Ongaro and John Ousterhout.
- **Distributed Cluster Discovery:** Discover other nodes in the cluster using either:
  - **Registries:** Centralized or decentralized service registries.
  - **Multicast Discovery:** Automatic node discovery via multicast messages.
  - **Static Discovery:** Manually configured list of known nodes.
- **Flexible Role Management:** Nodes can serve as leaders and followers simultaneously across different partitions, enabling granular control over cluster responsibilities.
- **Persistent Log Replication:** Each node persists its log to disk to ensure data durability. Kommander utilizes a Write-Ahead Log (WAL) internally to safeguard against data loss:
  - **RocksDB:** A high-performance, embedded key-value store optimized for fast storage and retrieval. (Default)
  - **SQLite:** Lightweight, embedded database engine that provides transactional support and crash recovery.
- **Multiple Communication Protocols:** Achieve consensus and data replication over:
  - **gRPC:** For low-latency and high-throughput scenarios (Default)
  - **HTTP/2:** For RESTful interactions and easier debugging. 

---

## About Raft and Kommander

Raft is a consensus protocol that helps a cluster of nodes maintain a
replicated state machine by synchronizing a replicated log. This log
ensures that each node's state remains consistent across the cluster.
Kommander implements the core Raft algorithm, providing a minimalistic
design that focuses solely on the essential components of the protocol.
By separating storage, messaging serialization, and network transport
from the consensus logic, Kommander offers flexibility, determinism,
and improved performance.

---

## Getting Started

### Prerequisites

- [.NET 8.0 SDK](https://dotnet.microsoft.com/download/dotnet/9.0) or higher

### Installation

To install Kommander into your C#/.NET project, you can use the .NET CLI or the NuGet Package Manager.

#### Using .NET CLI

```shell
dotnet add package Kommander --version 0.4.4
```

### Using NuGet Package Manager

Search for Kommander and install it from the NuGet package manager UI, or use the Package Manager Console:

```shell
Install-Package Kommander -Version 0.4.4
```

Or, using the NuGet Package Manager in Visual Studio, search for **Kommander** and install it.

---

## Usage

Below is a basic example demonstrating how to set up a simple Kommander node, join a cluster, and start the consensus process.

```csharp

// Identify the node configuration, including the host, port, and the maximum number of partitions.
RaftConfiguration config = new()
{
    // Node will announce itself as localhost:8001
    NodeId = "node1",
    Host = "localhost",
    Port = 8001,
    // Partitions allow nodes to be leaders/followers for different sets of data
    MaxPartitions = 3
};

// Create a Raft node with the specified configuration.
IRaft node = new RaftManager(
    new ActorSystem(),
    config,

    // Node will use a static discovery mechanism to find other nodes in the cluster.
    new StaticDiscovery([new("localhost:8002"), new("localhost:8003")]),

    // Node will use a RocksDb Write-Ahead Log (WAL) per partition for log persistence.
    new RocksDbWAL(path: "./data", version: "v1"),

    // Node will use gRPC endpoints for communication with other nodes.
    new GrpcCommunication()

    // Node will use a new HybridLogicalClock for timestamping log entries.
    new HybridLogicalClock(),
    logger
);

// Subscribe to the OnReplicationReceived event to receive log entries from other nodes
// if the node is a follower
node.OnReplicationReceived += (RaftLog log) =>
{
    Console.WriteLine("Replication received: {0} {1} {2}", log.Id, log.LogType, Encoding.UTF8.GetString(log.LogData));

    return Task.FromResult(true);
};

// Start the node and join the cluster.
await node.JoinCluster();

// Check if the node is the leader of partition 0 and replicate a log entry.
if (await node.AmILeader(0))
{
    (bool success, RaftOperationStatus status, long commitLogId) = await node.ReplicateLogs(0, "Kommander is awesome!");
    
    if (success)
        Console.WriteLine("Replicated log with id: {0}", commitLogId);
    else
        Console.WriteLine("Replication failed {0}", status);
}

```

### Advanced Configuration

Kommander supports advanced configurations including:

- **Custom Log Storage:** Implement your own storage engine by extending the log replication modules.
- **Dynamic Partitioning:** Configure nodes to handle multiple partitions with distinct leader election processes.
- **Security:** Integrate with your existing security framework to secure HTTP/TCP communications.

For detailed configuration options, please refer to the [Documentation](https://github.com/andresgutierrez/kommander/wiki).

---

### **Basic Concepts**

- **Partitions**: A Raft cluster can have multiple partitions within its dataset.
Each partition elects its own leader and followers, allowing each node in the
cluster to act as both a leader and a follower across different partitions.
Having multiple partitions can improve the cluster's throughput by reducing
contention and enabling more operations to run concurrently. However, it
also increases network traffic and can lead to bottlenecks depending on the
available hardware. Proper tuning of this parameter is essential to maintain
a healthy cluster.

- **Quorum**: A quorum is the minimum number of nodes required to reach a consensus in a Raft cluster. 
For a cluster with N nodes, a quorum is defined as (N/2) + 1. This ensures that a majority of nodes 
must agree on a decision before it is considered committed. Quorums are essential for maintaining 
consistency and fault tolerance in distributed systems.

- **Elections**: The Raft algorithm selects a leader for each partition.
The remaining nodes become followers. If the leader node fails or becomes
unreachable, a new election process is triggered to select a replacement.

- **Leader**: Each partition designates a leader. The leader is responsible
for handling requests assigned to its partition and replicating them to
followers until a quorum is reached. It maintains a local
copy of the logs.

- **Followers**: Followers receive replication logs from
the leader and store local copies on their respective nodes. They
continuously monitor the leader, and if it fails, they may nominate
themselves as candidates in a new election for leadership.

- **Logs**: Logs store any information that developers need to persist
and replicate within certain partition in the cluster.

- **Checkpoint**: Developers can determine when stored logs are secure
and create a **log checkpoint**, which speeds up future recovery
processes and simplifies the addition of new nodes to the cluster.

- **Write-Ahead Log (WAL)**: Logs are immediately written to disk
in the most durable and consistent manner possible before being
replicated to other nodes or notifying the application of a newly
arrived log. This ensures data recovery in the event of node
failures or crashes.

- **Heartbeat**: The leader continuously sends heartbeat signals
to followers to confirm its availability and health.

- **Communication**: Nodes communicate with each other via RPCs to
handle leader elections, send heartbeats, and replicate logs.
**Kommander** supports communication using either **gRPC** or **Rest/Json**,
both of which offer distinct advantages and are widely familiar to developers.

- **Log Id**: Kommmander maintains a per partition 64-bit cluster-wide counter, 
known as the log id, which increments each time a new proposal is added. 
This revision functions as a global logical clock, ensuring a sequential order for all 
updates to the partition log. Each new log id represents an incremental change. 

---

## Consensus Process

Kommander relies on the following consensus algorithm to ensure that changes are applied consistently across all nodes. The commit phase is a critical part of this process. Here’s how it works:

### 1. Log Entry Proposal and Replication
- **Leader Receives a Client Request:** When a client sends a replication request, the leader node first creates a log entry for this operation.
- **Appending to the Leader's Log:** The leader appends this log entry to its own log.
- **Replicating the Log Entry:** The leader then sends this new log entry to all the follower nodes. Each follower 
appends the entry to its own log but it isn’t applied to the state machine yet (OnReplicationReceived event).

### 2. Achieving a Quorum
- **Acknowledgment from Followers:** Each follower sends an acknowledgment back to the leader once they’ve safely stored the log entry.
- **Quorum Requirement:** In Raft (and thus in Kommander), a log entry is not considered committed until a majority (a quorum) 
of the nodes have acknowledged the entry. This majority rule ensures that the system can tolerate node failures while 
still maintaining consistency.

### 3. Committing the Log Entry
- **Advancing the Commit Index:** Once the leader receives acknowledgments from a quorum of nodes, it marks the log entry 
as committed by advancing its commit index. This commit index is a pointer indicating up to which log entries are safe to apply.
- **Notifying the Followers:** The leader then informs the followers about the new commit index. Each follower, upon learning 
that the entry is committed, will also update its commit index accordingly.

### 4. Applying to the State Machine
- **Execution of the Command:** With the log entry committed, every node (both the leader and the followers) 
applies the corresponding operation to its local state machine. This step makes the change visible to clients.
- **Ensuring Consistency:** By applying the same committed log entry on every node, Kommander guarantees that all 
nodes remain consistent, even in the presence of failures.

### Why the Commit Phase Is Important
- **Data Consistency:** The commit phase ensures that a change isn’t applied until it has been safely 
replicated to a majority of nodes, preventing inconsistencies.
- **Fault Tolerance:** Even if some nodes fail, the fact that the entry is committed on a quorum means 
that the system can recover without data loss.
- **Linearizability:** This process guarantees that once a client is notified of a successful operation, 
any subsequent read will reflect that change, maintaining strong consistency.

## Contributing

We welcome contributions to Kommander! If you’d like to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Write tests and ensure all tests pass.
4. Submit a pull request with a clear description of your changes.

For more details, see our [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

Kommander is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Community & Support

- **GitHub Issues:** Report bugs or request features via our [GitHub Issues](https://github.com/your-repo/Kommander/issues) page.

---
