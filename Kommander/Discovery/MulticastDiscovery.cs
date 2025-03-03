
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Kommander.Discovery.Data;

namespace Kommander.Discovery;

/// <summary>
/// Allow discovery of other Raft nodes using multicast UDP messages
/// </summary>
public class MulticastDiscovery : IDiscovery
{
    private static readonly IPAddress MulticastAddress = IPAddress.Parse("239.0.0.222");
    
    private const int Port = 1900;

    private readonly Dictionary<string, bool> nodes = new();

    private async Task StartBroadcasting(RaftConfiguration configuration)
    {
        try
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
                await udpClient.SendAsync(data, data.Length, multicastEndpoint).ConfigureAwait(false);
                Console.WriteLine($"Sent: {message}");

                // Wait for a specified interval before sending the next announcement.
                await Task.Delay(5000); // 5 seconds delay
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("StartListening: {0}", ex.Message);
        }
    }

    private async Task StartListening(RaftConfiguration configuration)
    {
        try
        {
            using UdpClient udpClient = new();

            // Allow multiple sockets to use the same port.
            udpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

            IPAddress x = IPAddress.Parse(configuration.Host ?? "");

            // Bind to the multicast port on any IP address.
            IPEndPoint localEp = new(IPAddress.Any, Port);
            udpClient.Client.Bind(localEp);

            // Join the multicast group.
            udpClient.JoinMulticastGroup(MulticastAddress, x);

            Console.WriteLine($"Listening for multicast messages on {MulticastAddress}:{Port}...");

            while (true)
            {
                UdpReceiveResult result = await udpClient.ReceiveAsync().ConfigureAwait(false);
                string payloadMessage = Encoding.UTF8.GetString(result.Buffer);
                
                Console.WriteLine($"Received packet {result.Buffer.Length}");

                string host = result.RemoteEndPoint.Address.ToString();

                MulticastDiscoveryPayload? payload = JsonSerializer.Deserialize<MulticastDiscoveryPayload>(payloadMessage);
                if (payload == null)
                {
                    Console.WriteLine($"Received invalid message from {host}");
                    continue;
                }

                Console.WriteLine($"Received from {host}: {payloadMessage}");

                if (configuration.Host == payload.Host && configuration.Port == payload.Port)
                {
                    Console.WriteLine($"Received message from self: {host}");
                    continue;
                }

                nodes[string.Concat(payload.Host, ":", payload.Port)] = true;

                Console.WriteLine($"Updated node {payload.Host}:{payload.Port}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("StartListening: {0}", ex.Message);
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