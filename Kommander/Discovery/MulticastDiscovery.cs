using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Kommander.Discovery.Data;

namespace Kommander.Discovery;

/// <summary>
/// Allow discovery of other Raft nodes using multicast UDP messages
/// </summary>
public class MulticastDiscovery : IDiscovery, IDisposable
{
    private static readonly IPAddress MulticastAddress = IPAddress.Parse("239.0.0.222");

    private const int Port = 1900;

    private readonly Dictionary<string, bool> nodes = new();
    private readonly CancellationTokenSource cancellation = new();
    private readonly object startLock = new();
    private Task? broadcastTask;
    private Task? listenTask;
    private int disposed;

    private async Task StartBroadcasting(RaftConfiguration configuration, CancellationToken cancellationToken)
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

            while (!cancellationToken.IsCancellationRequested)
            {
                // Send the multicast message.
                await udpClient.SendAsync(data, data.Length, multicastEndpoint)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                Console.WriteLine($"Sent: {message}");

                // Wait for a specified interval before sending the next announcement.
                await Task.Delay(5000, cancellationToken); // 5 seconds delay
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Console.WriteLine("StartBroadcasting: {0}", ex.Message);
        }
    }

    private async Task StartListening(RaftConfiguration configuration, CancellationToken cancellationToken)
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

            while (!cancellationToken.IsCancellationRequested)
            {
                UdpReceiveResult result = await udpClient.ReceiveAsync(cancellationToken).ConfigureAwait(false);
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

                lock (nodes)
                    nodes[string.Concat(payload.Host, ":", payload.Port)] = true;

                Console.WriteLine($"Updated node {payload.Host}:{payload.Port}");
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Console.WriteLine("StartListening: {0}", ex.Message);
        }
    }

    public Task Register(RaftConfiguration configuration)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);

        lock (startLock)
        {
            if (broadcastTask is null)
                broadcastTask = Task.Run(() => StartBroadcasting(configuration, cancellation.Token));

            if (listenTask is null)
                listenTask = Task.Run(() => StartListening(configuration, cancellation.Token));
        }

        return Task.CompletedTask;
    }

    public Task UpdateNodes()
    {
        return Task.CompletedTask;
    }

    public List<RaftNode> GetNodes()
    {
        List<string> rn;
        lock (nodes)
            rn = nodes.Keys.ToList();

        return rn.Select(x => new RaftNode(x)).ToList();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        cancellation.Cancel();

        List<Task> tasks = [];
        lock (startLock)
        {
            if (broadcastTask is not null)
                tasks.Add(broadcastTask);
            if (listenTask is not null)
                tasks.Add(listenTask);
        }

        try
        {
            Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(5));
        }
        catch
        {
        }

        cancellation.Dispose();
    }
}
