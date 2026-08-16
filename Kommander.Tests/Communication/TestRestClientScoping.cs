
using Flurl.Http;
using Kommander;
using Kommander.Communication.Rest;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Communication;

/// <summary>
/// Covers the REST client scoping change: cluster requests are issued through a per-manager client
/// configured from that manager's transport security, instead of through Flurl's process-wide
/// defaults.
/// </summary>
/// <remarks>
/// <para>
/// The security reason is that <c>--allow-insecure-certificate-validation</c> used to be installed on
/// <c>FlurlHttp.Clients</c>, which disabled certificate validation for every Flurl call in the
/// process — including calls made by an application that merely hosts Kommander — rather than only
/// for cluster traffic.
/// </para>
/// <para>
/// The tests here pin the mechanical consequences of that move: the composed URL must not change,
/// and per-manager clients must be distinct instances so two managers in one process (as in these
/// tests) cannot inherit each other's certificate policy.
/// </para>
/// </remarks>
public sealed class TestRestClientScoping
{
    [Fact]
    public void CreateRaftRequest_ComposesTheSameAbsoluteUrlAsBefore()
    {
        using RaftManager manager = CreateManager("node-a", 5000);

        IFlurlRequest request = RestCommunication.CreateRaftRequest(
            manager,
            new RaftNode("node-b:5001"),
            "/v1/raft/append-logs",
            "{}");

        Assert.Equal("https://node-b:5001/v1/raft/append-logs", request.Url.ToString());
    }

    /// <summary>
    /// A leading and trailing slash on the path must not produce empty segments or a doubled slash —
    /// the old code trimmed and split the path for exactly this reason, and that handling had to
    /// survive the move to a based client.
    /// </summary>
    [Theory]
    [InlineData("/v1/raft/vote")]
    [InlineData("v1/raft/vote")]
    [InlineData("/v1/raft/vote/")]
    public void CreateRaftRequest_NormalizesPathSegments(string path)
    {
        using RaftManager manager = CreateManager("node-a", 5000);

        IFlurlRequest request = RestCommunication.CreateRaftRequest(
            manager,
            new RaftNode("node-b:5001"),
            path,
            "{}");

        Assert.Equal("https://node-b:5001/v1/raft/vote", request.Url.ToString());
    }

    /// <summary>
    /// Each peer gets its own client, so a request is never issued against a previously cached peer's
    /// base URL.
    /// </summary>
    [Fact]
    public void CreateRaftRequest_TargetsThePeerItWasGiven()
    {
        using RaftManager manager = CreateManager("node-a", 5000);

        IFlurlRequest toB = RestCommunication.CreateRaftRequest(
            manager, new RaftNode("node-b:5001"), "/v1/raft/ping", "{}");

        IFlurlRequest toC = RestCommunication.CreateRaftRequest(
            manager, new RaftNode("node-c:5002"), "/v1/raft/ping", "{}");

        Assert.Equal("https://node-b:5001/v1/raft/ping", toB.Url.ToString());
        Assert.Equal("https://node-c:5002/v1/raft/ping", toC.Url.ToString());
    }

    /// <summary>
    /// Repeated requests to one peer reuse a single client — the cache is what keeps connection
    /// pooling intact after the move off Flurl's shared defaults.
    /// </summary>
    [Fact]
    public void CreateRaftRequest_ReusesOneClientPerPeer()
    {
        using RaftManager manager = CreateManager("node-a", 5000);
        RaftNode peer = new("node-b:5001");

        IFlurlRequest first = RestCommunication.CreateRaftRequest(manager, peer, "/v1/raft/ping", "{}");
        IFlurlRequest second = RestCommunication.CreateRaftRequest(manager, peer, "/v1/raft/vote", "{}");

        Assert.Same(first.Client, second.Client);
    }

    /// <summary>
    /// Two managers in one process get separate clients even for the same peer address. This is the
    /// isolation the whole change exists for: certificate policy is per-cluster, and a shared client
    /// would let one manager's settings (including the insecure bypass) apply to another's traffic.
    /// </summary>
    [Fact]
    public void CreateRaftRequest_DoesNotShareClientsAcrossManagers()
    {
        using RaftManager first = CreateManager("node-a", 5000);
        using RaftManager second = CreateManager("node-d", 5003);
        RaftNode peer = new("node-b:5001");

        IFlurlRequest fromFirst = RestCommunication.CreateRaftRequest(first, peer, "/v1/raft/ping", "{}");
        IFlurlRequest fromSecond = RestCommunication.CreateRaftRequest(second, peer, "/v1/raft/ping", "{}");

        Assert.NotSame(fromFirst.Client, fromSecond.Client);
    }

    private static RaftManager CreateManager(string host, int port)
    {
        RaftConfiguration configuration = new()
        {
            NodeName = host,
            NodeId = port,
            Host = host,
            Port = port,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EndElectionTimeout = 250,
            EnableQuiescence = false,
        };

        return new RaftManager(
            configuration,
            new Kommander.Discovery.StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new RestCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);
    }
}
