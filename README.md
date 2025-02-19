# 🔱 Kommander (Raft Consensus)

Kommander is an open-source, distributed consensus library implemented in C# for the .NET platform. It leverages the Raft algorithm to provide a robust and reliable mechanism for leader election, log replication, and data consistency across clusters. Kommander is designed to be flexible and resilient, supporting multiple discovery mechanisms and communication protocols to suit various distributed system architectures.

---

## Features

- **Raft Consensus Algorithm:**
  Implemented using the Raft protocol, which enables a cluster of nodes to maintain a replicated state machine by keeping a synchronized log across all nodes. For an in-depth explanation of Raft, see [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) by Diego Ongaro and John Ousterhout.

- **Distributed Cluster Discovery:**
  Discover other nodes in the cluster using either:
  - **Registries:** Centralized or decentralized service registries.
  - **Multicast Discovery:** Automatic node discovery via multicast messages.

- **Persistent Log Replication:**
  Each node persists its log to disk to ensure data durability. Kommander utilizes a Write-Ahead Log (WAL) internally to safeguard against data loss.

- **Flexible Role Management:**
  Nodes can serve as leaders and followers simultaneously across different partitions, enabling granular control over cluster responsibilities.

- **Multiple Communication Protocols:**
  Achieve consensus and data replication over:
  - **HTTP:** For RESTful interactions and easier integration with web services.
  - **Plain TCP:** For low-latency and high-throughput scenarios.

---

## About Raft and Kommander

Raft is a consensus protocol that helps a cluster of nodes maintain a replicated state machine by synchronizing a replicated log. This log ensures that each node's state remains consistent across the cluster. Kommander implements the core Raft algorithm, providing a minimalistic design that focuses solely on the essential components of the protocol. By separating storage, messaging serialization, and network transport from the consensus logic, Kommander offers flexibility, determinism, and improved performance.

---

## Getting Started

### Prerequisites

- [.NET 9.0 SDK](https://dotnet.microsoft.com/download/dotnet/9.0) or higher
- A C# development environment (e.g., Visual Studio, VS Code)

### Installation

To install Kommander into your C#/.NET project, you can use the .NET CLI or the NuGet Package Manager.

#### Using .NET CLI

```shell
dotnet add package Kommander --version 0.0.1
```

### Using NuGet Package Manager

Search for Kommander and install it from the NuGet package manager UI, or use the Package Manager Console:

```shell
Install-Package Kommander -Version 0.0.1
```

Or, using the NuGet Package Manager in Visual Studio, search for **Kommander** and install it.

---

## Usage

Below is a basic example demonstrating how to set up a simple Kommander node, join a cluster, and start the consensus process.

```csharp
using Kommander;
using Kommander.Configuration;
using Kommander.Discovery;
using Kommander.LogReplication;

class Program
{
    static async Task Main(string[] args)
    {
        // Configure node settings
        var nodeConfig = new NodeConfiguration
        {
            NodeId = Guid.NewGuid().ToString(),
            DataDirectory = "path/to/data",
            CommunicationProtocol = CommunicationProtocol.Http, // or CommunicationProtocol.Tcp
            Port = 5000
        };

        // Initialize discovery mechanism (Registry-based or Multicast)
        IDiscovery discovery = new MulticastDiscovery(nodeConfig); // or new RegistryDiscovery(nodeConfig, registryUrl);

        // Configure Raft consensus settings
        var raftConfig = new RaftConfiguration
        {
            ElectionTimeout = TimeSpan.FromSeconds(5),
            HeartbeatInterval = TimeSpan.FromSeconds(1)
        };

        // Initialize the Kommander node
        var KommanderNode = new KommanderNode(nodeConfig, raftConfig, discovery);

        // Start the node
        await KommanderNode.StartAsync();

        // Example: Append a log entry (this operation will be replicated across the cluster)
        var logEntry = new LogEntry { Command = "SetValue", Data = "example_data" };
        await KommanderNode.ReplicateAsync(logEntry);

        Console.WriteLine("Kommander node started. Press any key to exit.");
        Console.ReadKey();

        await KommanderNode.StopAsync();
    }
}
```

### Advanced Configuration

Kommander supports advanced configurations including:

- **Custom Log Storage:** Implement your own storage engine by extending the log replication modules.
- **Dynamic Partitioning:** Configure nodes to handle multiple partitions with distinct leader election processes.
- **Security:** Integrate with your existing security framework to secure HTTP/TCP communications.

For detailed configuration options, please refer to the [Documentation](docs/CONFIGURATION.md).

---

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

Harness the power of distributed consensus with Kommander and build resilient, high-availability systems on the .NET platform. Happy coding!