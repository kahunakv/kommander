# Lux (Raft Consensus)

Lux is an open-source, distributed consensus library implemented in C# for the .NET platform. It leverages the Raft algorithm to provide a robust and reliable mechanism for leader election, log replication, and data consistency across clusters. Lux is designed to be flexible and resilient, supporting multiple discovery mechanisms and communication protocols to suit various distributed system architectures.

---

## Features

- **Raft Consensus Algorithm:**  
  Implemented using the Raft algorithm to handle leader election, log replication, and fault tolerance.

- **Distributed Cluster Discovery:**  
  Discover other nodes in the cluster using either:
  - **Registries:** Centralized or decentralized service registries.
  - **Multicast Discovery:** Automatic node discovery via multicast messages.

- **Persistent Log Replication:**  
  Each node persists its log to disk to ensure data durability. Lux utilizes a Write-Ahead Log (WAL) internally to safeguard against data loss.

- **Flexible Role Management:**  
  Nodes can serve as leaders and followers simultaneously across different partitions, enabling granular control over cluster responsibilities.

- **Multiple Communication Protocols:**  
  Achieve consensus and data replication over:
  - **HTTP:** For RESTful interactions and easier integration with web services.
  - **Plain TCP:** For low-latency and high-throughput scenarios.

---

## Getting Started

### Prerequisites

- [.NET 9.0 SDK](https://dotnet.microsoft.com/download/dotnet/9.0) or higher
- A C# development environment (e.g., Visual Studio, VS Code)

### Installation

You can install Lux via NuGet. In your terminal, run:

```bash
dotnet add package Lux
```

Or, using the NuGet Package Manager in Visual Studio, search for **Lux** and install it.

---

## Usage

Below is a basic example demonstrating how to set up a simple Lux node, join a cluster, and start the consensus process.

```csharp
using Lux;
using Lux.Configuration;
using Lux.Discovery;
using Lux.LogReplication;

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

        // Initialize the Lux node
        var luxNode = new LuxNode(nodeConfig, raftConfig, discovery);

        // Start the node
        await luxNode.StartAsync();

        // Example: Append a log entry (this operation will be replicated across the cluster)
        var logEntry = new LogEntry { Command = "SetValue", Data = "example_data" };
        await luxNode.ReplicateAsync(logEntry);

        Console.WriteLine("Lux node started. Press any key to exit.");
        Console.ReadKey();

        await luxNode.StopAsync();
    }
}
```

### Advanced Configuration

Lux supports advanced configurations including:

- **Custom Log Storage:** Implement your own storage engine by extending the log replication modules.
- **Dynamic Partitioning:** Configure nodes to handle multiple partitions with distinct leader election processes.
- **Security:** Integrate with your existing security framework to secure HTTP/TCP communications.

For detailed configuration options, please refer to the [Documentation](docs/CONFIGURATION.md).

---

## Contributing

We welcome contributions to Lux! If you’d like to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Write tests and ensure all tests pass.
4. Submit a pull request with a clear description of your changes.

For more details, see our [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

Lux is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Community & Support

- **GitHub Issues:** Report bugs or request features via our [GitHub Issues](https://github.com/your-repo/lux/issues) page.
- **Discussion Forum:** Join our community discussions on [Discourse](https://discourse.example.com).
- **Chat:** Connect with us on [Gitter](https://gitter.im/lux-project/community) for real-time support and collaboration.

---

Harness the power of distributed consensus with Lux and build resilient, high-availability systems on the .NET platform. Happy coding!