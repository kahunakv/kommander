
using Kommander.Communication;
using Kommander.Gossip;
using GossipPingRequest = Kommander.Gossip.PingRequest;
using GossipPingResponse = Kommander.Gossip.PingResponse;
using GossipPingReqRequest = Kommander.Gossip.PingReqRequest;
using GossipPingReqResponse = Kommander.Gossip.PingReqResponse;
using Kommander.Logging;
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Owns the <see cref="LivenessTable"/> (SWIM failure-detector state) and implements the
/// gossip and probing protocol on behalf of <see cref="RaftManager"/>:
/// <see cref="ReceiveGossip"/> (inbound gossip message), <see cref="GossipAsync"/> (periodic
/// gossip round), <see cref="ReceivePing"/> / <see cref="ReceivePingReq"/> (SWIM probes),
/// and <see cref="PingAsync"/> (outbound probe round).
/// <para>
/// Callers access liveness state through the <see cref="Liveness"/> property;
/// <see cref="RaftManager.Liveness"/> is a forwarding property pointing here.
/// </para>
/// </summary>
internal sealed class GossipService
{
    internal readonly LivenessTable Liveness = new();

    private readonly ICommunication communication;
    private readonly Func<IReadOnlyList<RaftNode>> getNodes;
    // Lazily resolved — GossipService is constructed before RaftSystemCoordinator
    // to break the circular init dependency (coordinator ctor reads manager.Liveness).
    private readonly Func<RaftSystemCoordinator> getCoordinator;
    private readonly Func<NodeLoadReport?> buildLocalLoadReport;
    private readonly Action<string> wakePartitionsForLeader;
    private readonly RaftConfiguration configuration;
    private readonly string localEndpoint;
    private readonly ILogger<IRaft> logger;

    internal GossipService(
        ICommunication communication,
        Func<IReadOnlyList<RaftNode>> getNodes,
        Func<RaftSystemCoordinator> getCoordinator,
        Func<NodeLoadReport?> buildLocalLoadReport,
        Action<string> wakePartitionsForLeader,
        RaftConfiguration configuration,
        string localEndpoint,
        ILogger<IRaft> logger)
    {
        this.communication = communication;
        this.getNodes = getNodes;
        this.getCoordinator = getCoordinator;
        this.buildLocalLoadReport = buildLocalLoadReport;
        this.wakePartitionsForLeader = wakePartitionsForLeader;
        this.configuration = configuration;
        this.localEndpoint = localEndpoint;
        this.logger = logger;
    }

    internal GossipAck ReceiveGossip(RaftManager manager, GossipMessage digest)
    {
        ClusterMembership local = getCoordinator().GetMembership();

        if (digest.Roster is not null && digest.MembershipVersion > local.MembershipVersion)
            getCoordinator().Send(new RaftSystemRequest(digest.Roster));

        (bool selfSuspected, IReadOnlyList<string> newlySuspectOrDead) =
            Liveness.ApplyUpdates(localEndpoint, digest.LivenessUpdates);

        if (selfSuspected)
        {
            long refutedInc = Liveness.RefuteSuspicion(localEndpoint);
            logger.LogInfoReceiveGossipRefuting(refutedInc);
        }

        foreach (string suspectEndpoint in newlySuspectOrDead)
            wakePartitionsForLeader(suspectEndpoint);

        if (configuration.EnableLeaderBalancer && digest.LoadReport is { } report)
            getCoordinator().Send(new RaftSystemRequest(report));

        bool includeRoster = local.MembershipVersion > digest.MembershipVersion;
        return new GossipAck(local.MembershipVersion, includeRoster ? local : null);
    }

    internal async Task GossipAsync(RaftManager manager, CancellationToken cancellationToken = default)
    {
        ClusterMembership membership = getCoordinator().GetMembership();

        NodeLoadReport? localReport = null;
        if (configuration.EnableLeaderBalancer)
        {
            localReport = buildLocalLoadReport();
            if (localReport is not null)
                getCoordinator().Send(new RaftSystemRequest(localReport));
        }

        if (membership.MembershipVersion == 0 || configuration.GossipFanout <= 0)
            return;

        List<RaftNode> peers = [..getNodes()];
        if (peers.Count == 0)
            return;

        IReadOnlyList<MemberLivenessEntry> livenessUpdates = Liveness.GetAll();
        GossipMessage digest = new(localEndpoint, membership.MembershipVersion, membership)
        {
            LivenessUpdates = livenessUpdates.Count > 0 ? livenessUpdates : null,
            LoadReport = localReport,
        };

        int fanout = Math.Min(configuration.GossipFanout, peers.Count);

        for (int i = 0; i < fanout; i++)
        {
            int j = Random.Shared.Next(i, peers.Count);
            (peers[i], peers[j]) = (peers[j], peers[i]);
        }

        for (int i = 0; i < fanout; i++)
        {
            try
            {
                GossipAck ack = await communication.SendGossip(manager, peers[i], digest, cancellationToken).ConfigureAwait(false);

                if (ack.Roster is not null && ack.MembershipVersion > getCoordinator().GetMembership().MembershipVersion)
                    getCoordinator().Send(new RaftSystemRequest(ack.Roster));
            }
            catch (Exception ex)
            {
                logger.LogDebugGossipAsyncFailed(peers[i].Endpoint, ex.Message);
            }
        }
    }

    internal GossipPingResponse ReceivePing(GossipPingRequest request)
    {
        long incarnation = Liveness.GetSelfIncarnation();
        if (Liveness.GetState(localEndpoint) >= MemberLivenessState.Suspect)
            incarnation = Liveness.RefuteSuspicion(localEndpoint);
        return new GossipPingResponse(true, incarnation);
    }

    internal async Task<GossipPingReqResponse> ReceivePingReq(
        RaftManager manager,
        GossipPingReqRequest request,
        CancellationToken cancellationToken = default)
    {
        RaftNode? targetNode = getNodes().FirstOrDefault(n => n.Endpoint == request.TargetEndpoint);
        if (targetNode is null)
            return new GossipPingReqResponse(false);

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(configuration.PingTimeout);

        try
        {
            GossipPingResponse resp = await communication.SendPing(
                manager, targetNode, new GossipPingRequest(localEndpoint), cts.Token).ConfigureAwait(false);
            return new GossipPingReqResponse(resp.Alive);
        }
        catch
        {
            return new GossipPingReqResponse(false);
        }
    }

    internal async Task PingAsync(RaftManager manager, CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> newlyDead = Liveness.AdvanceExpiry(DateTimeOffset.UtcNow, configuration.SuspicionTimeout);
        foreach (string deadEndpoint in newlyDead)
            wakePartitionsForLeader(deadEndpoint);

        ClusterMembership membership = getCoordinator().GetMembership();
        if (membership.MembershipVersion == 0)
            return;

        List<RaftNode> peers = [..getNodes()];
        if (peers.Count == 0)
            return;

        int idx = Random.Shared.Next(peers.Count);
        RaftNode target = peers[idx];

        bool alive = false;
        long incarnation = 0;

        using (CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
        {
            cts.CancelAfter(configuration.PingTimeout);
            try
            {
                GossipPingResponse resp = await communication.SendPing(
                    manager, target, new GossipPingRequest(localEndpoint), cts.Token).ConfigureAwait(false);
                alive = resp.Alive;
                incarnation = resp.Incarnation;
            }
            catch
            {
                alive = false;
            }
        }

        if (alive)
        {
            Liveness.MarkAlive(target.Endpoint, incarnation);
            return;
        }

        List<RaftNode> relays = peers.Where(p => p.Endpoint != target.Endpoint).ToList();
        int fanout = Math.Min(configuration.IndirectPingFanout, relays.Count);

        for (int i = 0; i < fanout; i++)
        {
            int j = Random.Shared.Next(i, relays.Count);
            (relays[i], relays[j]) = (relays[j], relays[i]);
        }

        bool reachedViaRelay = false;
        using (CancellationTokenSource relayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
        {
            relayCts.CancelAfter(configuration.PingTimeout);

            for (int i = 0; i < fanout && !reachedViaRelay; i++)
            {
                try
                {
                    GossipPingReqResponse relayResp = await communication.SendPingReq(
                        manager, relays[i],
                        new GossipPingReqRequest(localEndpoint, target.Endpoint),
                        relayCts.Token).ConfigureAwait(false);

                    if (relayResp.Reached)
                        reachedViaRelay = true;
                }
                catch
                {
                    // relay also unreachable — continue to next
                }
            }
        }

        if (reachedViaRelay)
        {
            Liveness.ClearSuspicion(target.Endpoint);
        }
        else
        {
            MemberLivenessState prev = Liveness.GetState(target.Endpoint);
            Liveness.MarkSuspect(target.Endpoint);
            if (prev == MemberLivenessState.Alive)
            {
                logger.LogWarning("PingAsync: {Endpoint} failed direct and indirect probe; marked Suspect", target.Endpoint);
                wakePartitionsForLeader(target.Endpoint);
            }
        }
    }
}
