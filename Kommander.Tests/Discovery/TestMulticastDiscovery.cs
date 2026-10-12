using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Kommander.Discovery;

namespace Kommander.Tests.Discovery;

/// <summary>
/// Multicast discovery over a real multicast group on this host: two instances must discover
/// each other from their periodic announcements, a node must never discover itself, a packet
/// that parses to a null payload must be skipped without stopping the listener, and the
/// register/dispose lifecycle must be safe. Announcements repeat on a fixed cadence, so every
/// expectation polls with a bounded timeout instead of sleeping a fixed amount.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public sealed class TestMulticastDiscoveryRoundTrip
{
    private static readonly IPAddress Group = IPAddress.Parse("239.0.0.222");

    private const int MulticastPort = 1900;

    /// <summary>
    /// The interface address the OS would actually route multicast through, discovered without
    /// sending any packet: a connected UDP socket's local endpoint is picked by the routing
    /// table. Falls back to the first up, multicast-capable, non-loopback IPv4 interface.
    /// </summary>
    private static IPAddress? TryGetLocalMulticastAddress()
    {
        try
        {
            using Socket probe = new(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            // TEST-NET-1 is routable (so the routing table answers) but never delivers anywhere.
            probe.Connect("192.0.2.1", 65530);
            if (probe.LocalEndPoint is IPEndPoint ep && ep.Address.AddressFamily == AddressFamily.InterNetwork
                && !IPAddress.IsLoopback(ep.Address))
                return ep.Address;
        }
        catch (SocketException)
        {
        }

        try
        {
            return NetworkInterface.GetAllNetworkInterfaces()
                .Where(n => n.OperationalStatus == OperationalStatus.Up && n.SupportsMulticast)
                .SelectMany(n => n.GetIPProperties().UnicastAddresses)
                .Select(a => a.Address)
                .FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(a));
        }
        catch (NetworkInformationException)
        {
            return null;
        }
    }

    /// <summary>
    /// Probes exactly what the listener needs — bind the fixed multicast port with address reuse
    /// and join the group on the chosen interface — so an environment that cannot host the
    /// exchange skips rather than fails after the full poll timeout.
    /// </summary>
    private static bool CanJoinMulticastGroup(IPAddress localAddress)
    {
        try
        {
            using UdpClient probe = new();
            probe.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            probe.Client.Bind(new IPEndPoint(IPAddress.Any, MulticastPort));
            probe.JoinMulticastGroup(Group, localAddress);
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    private static RaftConfiguration DiscoveryConfig(IPAddress localAddress, int announcedPort) => new()
    {
        Host = localAddress.ToString(),
        Port = announcedPort
    };

    private static async Task<List<RaftNode>> PollUntilNodes(
        MulticastDiscovery discovery, string expectedEndpoint, TimeSpan bound)
    {
        DateTime deadline = DateTime.UtcNow + bound;
        List<RaftNode> nodes = [];

        while (DateTime.UtcNow < deadline)
        {
            nodes = discovery.GetNodes();
            if (nodes.Any(n => n.Endpoint == expectedEndpoint))
                return nodes;

            await Task.Delay(200, TestContext.Current.CancellationToken);
        }

        return nodes;
    }

    [Fact]
    public async Task TwoInstances_OnTheLocalMulticastGroup_DiscoverEachOtherAndNotThemselves()
    {
        IPAddress? localAddress = TryGetLocalMulticastAddress();
        Assert.SkipWhen(localAddress is null, "no up non-loopback IPv4 multicast interface on this host");
        Assert.SkipWhen(!CanJoinMulticastGroup(localAddress!), "cannot bind/join the multicast port on this host");

        // Local multicast delivery can lag when another member of the same group just left (a
        // preceding test's dispose): group membership churn delays the first delivery by more
        // than a beacon cycle. Retry once with fresh instances before declaring failure.
        for (int attempt = 0; ; attempt++)
        {
            List<RaftNode> seenByFirst;
            List<RaftNode> seenBySecond;

            MulticastDiscovery first = new();
            MulticastDiscovery second = new();
            try
            {
                await first.Register(DiscoveryConfig(localAddress!, 57001));
                await second.Register(DiscoveryConfig(localAddress!, 57002));

                // The cadence is several seconds between announcements and the two listeners
                // start at slightly different times, so the first cross-delivery can miss the
                // other's initial beacon; the bound must cover a full re-announcement cycle.
                seenByFirst = await PollUntilNodes(first, $"{localAddress}:57002", TimeSpan.FromSeconds(30));
                seenBySecond = await PollUntilNodes(second, $"{localAddress}:57001", TimeSpan.FromSeconds(30));
            }
            finally
            {
                first.Dispose();
                second.Dispose();
            }

            bool discovered =
                seenByFirst.Any(n => n.Endpoint == $"{localAddress}:57002")
                && seenBySecond.Any(n => n.Endpoint == $"{localAddress}:57001");

            if (discovered || attempt > 0)
            {
                Assert.Contains(seenByFirst, n => n.Endpoint == $"{localAddress}:57002");
                Assert.Contains(seenBySecond, n => n.Endpoint == $"{localAddress}:57001");

                // A node's own announcement arrives at its listener too; it must be filtered out.
                Assert.DoesNotContain(seenByFirst, n => n.Endpoint == $"{localAddress}:57001");
                Assert.DoesNotContain(seenBySecond, n => n.Endpoint == $"{localAddress}:57002");
                return;
            }
        }
    }

    [Fact]
    public async Task NullPayloadPacket_IsSkippedAndTheListenerSurvivesIt()
    {
        IPAddress? localAddress = TryGetLocalMulticastAddress();
        Assert.SkipWhen(localAddress is null, "no up non-loopback IPv4 multicast interface on this host");
        Assert.SkipWhen(!CanJoinMulticastGroup(localAddress!), "cannot bind/join the multicast port on this host");

        MulticastDiscovery discovery = new();

        try
        {
            await discovery.Register(DiscoveryConfig(localAddress!, 57003));

            // "null" is valid JSON for the payload type and deserializes to a null reference:
            // the documented skip-without-registration path for an unusable message.
            byte[] nullPayload = Encoding.UTF8.GetBytes("null");
            byte[] validPayload = Encoding.UTF8.GetBytes(
                JsonSerializer.Serialize(
                    new Kommander.Discovery.Data.MulticastDiscoveryPayload { Host = localAddress!.ToString(), Port = 57009 },
                    Kommander.System.SystemJsonContext.Default.MulticastDiscoveryPayload));

            IPEndPoint groupEndpoint = new(Group, MulticastPort);
            using UdpClient sender = new();
            sender.MulticastLoopback = true;
            // Send out of the interface the listener joined on, not whatever the default route picks.
            sender.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastInterface, localAddress!.GetAddressBytes());

            // Register only starts the listener; it binds and joins the group on a background task,
            // so a single send right after it can land before anyone is listening and be lost. Keep
            // re-sending the pair until the valid announcement is observed. Every round puts the
            // null payload first: if it had killed the listener, the valid announcement sent right
            // after it would never be observed.
            string expected = $"{localAddress}:57009";
            List<RaftNode> nodes = [];
            DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);

            while (DateTime.UtcNow < deadline)
            {
                await sender.SendAsync(nullPayload, nullPayload.Length, groupEndpoint);
                await sender.SendAsync(validPayload, validPayload.Length, groupEndpoint);

                nodes = await PollUntilNodes(discovery, expected, TimeSpan.FromMilliseconds(500));
                if (nodes.Any(n => n.Endpoint == expected))
                    break;
            }

            Assert.Contains(nodes, n => n.Endpoint == $"{localAddress}:57009");
            Assert.DoesNotContain(nodes, n => n.Endpoint == $"{localAddress}:57003");
        }
        finally
        {
            discovery.Dispose();
        }
    }

    [Fact]
    public void GetNodes_BeforeAnyRegistration_IsEmpty()
    {
        using MulticastDiscovery discovery = new();

        Assert.Empty(discovery.GetNodes());
    }

    [Fact]
    public async Task Dispose_IsSafeToCallTwiceAndRegisterAfterDisposeThrows()
    {
        MulticastDiscovery discovery = new();

        discovery.Dispose();
        discovery.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => discovery.Register(new RaftConfiguration()));
    }
}

/// <summary>
/// The discovery wire payload in isolation: the exact JSON shape the broadcaster emits must
/// round-trip through the source-generated context the listener decodes with, and a JSON null
/// must deserialize to a null payload (the input shape the listener's skip path handles).
/// </summary>
public sealed class TestMulticastDiscoveryPayloadSerialization
{
    [Fact]
    public void Payload_RoundTripsThroughTheGeneratedContext()
    {
        Kommander.Discovery.Data.MulticastDiscoveryPayload payload = new() { Host = "10.1.2.3", Port = 57010 };

        byte[] json = JsonSerializer.SerializeToUtf8Bytes(
            payload, Kommander.System.SystemJsonContext.Default.MulticastDiscoveryPayload);

        Kommander.Discovery.Data.MulticastDiscoveryPayload? decoded = JsonSerializer.Deserialize(
            json, Kommander.System.SystemJsonContext.Default.MulticastDiscoveryPayload);

        Assert.NotNull(decoded);
        Assert.Equal("10.1.2.3", decoded.Host);
        Assert.Equal(57010, decoded.Port);
    }

    [Fact]
    public void JsonNull_DeserializesToANullPayload()
    {
        Kommander.Discovery.Data.MulticastDiscoveryPayload? decoded = JsonSerializer.Deserialize(
            "null"u8, Kommander.System.SystemJsonContext.Default.MulticastDiscoveryPayload);

        Assert.Null(decoded);
    }
}
