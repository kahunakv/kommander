
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Lux.Discovery;

public sealed class MulticastDiscoveryPayload
{
    public string? Host { get; set; }
    
    public int Port { get; set; }
}

public class Multicast : IDiscovery
{
    private static readonly IPAddress MulticastAddress = IPAddress.Parse("239.0.0.222");
    
    private const int Port = 1900;

    private readonly Dictionary<string, bool> nodes = new();

    private async Task StartBroadcasting(RaftConfiguration configuration)
    {
        using UdpClient udpClient = new();
        
        // Allow multicast loopback so the sender can receive its own messages if needed.
        udpClient.MulticastLoopback = true;

        IPEndPoint multicastEndpoint = new(MulticastAddress, Port);

        Console.WriteLine("Starting broadcaster... {0} {1}", configuration.Host, configuration.Port);

        MulticastDiscoveryPayload payload = new()
        {
            Host = configuration.Host,
            Port = configuration.Port
        };
        
        // Construct your discovery message.
        string message = JsonSerializer.Serialize(payload); //$"Hello from node {Environment.MachineName} at {DateTime.Now}";
        byte[] data = Encoding.UTF8.GetBytes(message);

        while (true)
        {
            // Send the multicast message.
            await udpClient.SendAsync(data, data.Length, multicastEndpoint);
            Console.WriteLine($"Sent: {message}");

            // Wait for a specified interval before sending the next announcement.
            await Task.Delay(5000); // 5 seconds delay
        }
    }

    private async Task StartListening(RaftConfiguration configuration)
    {
        using UdpClient udpClient = new();
        
        // Allow multiple sockets to use the same port.
        udpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

        // Bind to the multicast port on any IP address.
        IPEndPoint localEp = new(IPAddress.Any, Port);
        udpClient.Client.Bind(localEp);

        // Join the multicast group.
        udpClient.JoinMulticastGroup(MulticastAddress);

        Console.WriteLine($"Listening for multicast messages on {MulticastAddress}:{Port}...");

        while (true)
        {
            UdpReceiveResult result = await udpClient.ReceiveAsync();
            string payloadMessage = Encoding.UTF8.GetString(result.Buffer);

            MulticastDiscoveryPayload? payload = JsonSerializer.Deserialize<MulticastDiscoveryPayload>(payloadMessage);
            if (payload == null)
                continue;

            string host = result.RemoteEndPoint.Address.ToString();
            
            Console.WriteLine($"Received from {host}: {payloadMessage}");

            if (configuration.Host == payload.Host && configuration.Port == payload.Port)
                continue;
            
            nodes[payload.Host + ":" + payload.Port] = true;
        }
    }

    public async Task Register(RaftConfiguration configuration)
    {
        _ = Task.WhenAll(
            StartBroadcasting(configuration),
            StartListening(configuration)
        );

        await Task.CompletedTask;
    }
    
    public async Task UpdateNodes()
    {
        await Task.CompletedTask;
    }

    public List<RaftNode> GetNodes()
    {
        List<string> rn = nodes.Keys.ToList();
        
        return rn.Select(x => new RaftNode(x)).ToList();
    }
}