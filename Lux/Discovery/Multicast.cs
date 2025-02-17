using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Lux.Discovery;

public static class Multicast
{
    private static readonly IPAddress MulticastAddress = IPAddress.Parse("239.0.0.222");
    
    private const int Port = 1900;
    
    public static async Task StartBroadcasting()
    {
        using (UdpClient udpClient = new UdpClient())
        {
            // Allow multicast loopback so the sender can receive its own messages if needed.
            udpClient.MulticastLoopback = true;

            IPEndPoint multicastEndpoint = new IPEndPoint(MulticastAddress, Port);

            Console.WriteLine("Starting broadcaster...");

            while (true)
            {
                // Construct your discovery message.
                string message = $"Hello from node {Environment.MachineName} at {DateTime.Now}";
                byte[] data = Encoding.UTF8.GetBytes(message);

                // Send the multicast message.
                await udpClient.SendAsync(data, data.Length, multicastEndpoint);
                Console.WriteLine($"Sent: {message}");

                // Wait for a specified interval before sending the next announcement.
                await Task.Delay(5000); // 5 seconds delay
            }
        }
    }
    
    public static async Task StartListening()
    {
        using UdpClient udpClient = new UdpClient();
        
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
            string receivedMessage = Encoding.UTF8.GetString(result.Buffer);
            Console.WriteLine($"Received from {result.RemoteEndPoint}: {receivedMessage}");
        }
    }
}