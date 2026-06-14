using System.Text;
using System.Text.Json;
using Flurl.Http;
using Kommander.Data;
using Kommander.Gossip;
using Microsoft.Extensions.Logging;

namespace Kommander.Communication.Rest;

/// <summary>
/// Allows for communication between Raft nodes using REST endpoints.
/// </summary>
public class RestCommunication : ICommunication
{
    public async Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.HandshakeRequest);
        
        try
        {
            return await CreateRaftRequest(manager, node, "/v1/raft/handshake", payload)
                .PostStringAsync(payload)
                .ReceiveJson<HandshakeResponse>().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] Handshake: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }
        
        return new();
    }
    
    public async Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
    {
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.RequestVotesRequest);
        
        try
        {
            return await CreateRaftRequest(manager, node, "/v1/raft/request-vote", payload)
                .PostStringAsync(payload)
                .ReceiveJson<RequestVotesResponse>().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] RequestVotes: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }
        
        return new();
    }

    public async Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
    {
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.VoteRequest);
        
        try
        {
            return await CreateRaftRequest(manager, node, "/v1/raft/vote", payload)
                .PostStringAsync(payload)
                .ReceiveJson<VoteResponse>().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] Vote: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }

        return new();
    }

    public async Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.CompleteAppendLogsRequest);
        
        try
        {
            CompleteAppendLogsResponse? response = await CreateRaftRequest(
                    manager,
                    node,
                    "/v1/raft/complete-append-logs",
                    payload)
                .PostStringAsync(payload)
                .ReceiveJson<CompleteAppendLogsResponse>().ConfigureAwait(false);

            return response;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] CompleteAppendLogs: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }

        return new();
    }

    public async Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.AppendLogsRequest);
        
        try
        {
            AppendLogsResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/append-logs", payload)
                .PostStringAsync(payload)
                .ReceiveJson<AppendLogsResponse>().ConfigureAwait(false);
            
            if (request.Logs is not null && request.Logs.Count > 0)
                manager.Logger.LogDebug("[{Endpoint}/{Partition}] Logs replicated to {RemoteEndpoint}", manager.LocalEndpoint, request.Partition, node.Endpoint);

            return response;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] AppendLogs: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }

        return new();
    }

    public async Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.BatchRequestsRequest);
        
        try
        {
            BatchRequestsResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/batch-requests", payload)
                .PostStringAsync(payload)
                .ReceiveJson<BatchRequestsResponse>().ConfigureAwait(false);

            return response;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] BatchRequestsResponse: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new();
    }

    /// <summary>
    /// Sends a <see cref="LeaveRequest"/> to <paramref name="node"/> via the
    /// <c>POST /v1/raft/leave</c> REST endpoint.  If the target is not the P0 leader it
    /// returns <see cref="LeaveResponse.LeaderHint"/> so the caller can retry against the
    /// current leader.  Returns failure on any transport or HTTP error.
    /// </summary>
    public async Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default)
    {
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.LeaveRequest);

        try
        {
            LeaveResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/leave", payload)
                .PostStringAsync(payload)
                .ReceiveJson<LeaveResponse>()
                .ConfigureAwait(false);

            return response ?? new LeaveResponse(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] SendLeave: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new LeaveResponse(false);
    }

    /// <summary>
    /// The Gossip endpoint is not yet implemented on the REST transport.
    /// Returns an empty ACK (version 0, no roster) so the gossip loop silently skips
    /// this peer.  Gossip convergence falls back to the in-process path; deployed
    /// clusters that use this transport will rely on Raft replication alone until a
    /// dedicated REST endpoint is added.
    /// </summary>
    public Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default)
        => Task.FromResult(new GossipAck(0, null));

    /// <summary>
    /// The Ping endpoint is not yet implemented on the REST transport.
    /// Returns <c>PingResponse(false, 0)</c> — i.e. the peer is reported unreachable.
    /// <para>
    /// <b>Warning:</b> if <c>PingInterval</c> is positive on a REST cluster these stubs will
    /// cause every healthy peer to be suspected, declared Dead, and eventually evicted.
    /// <c>PingInterval</c> defaults to <see cref="TimeSpan.Zero"/> precisely to prevent this;
    /// do not enable the ping timer until the wire RPC is implemented.
    /// </para>
    /// </summary>
    public Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new Gossip.PingResponse(false, 0));

    /// <summary>
    /// The PingReq endpoint is not yet implemented on the REST transport.
    /// Returns <c>PingReqResponse(false)</c> — the target is reported unreachable.
    /// See the warning on <see cref="SendPing"/>.
    /// </summary>
    public Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default)
        => Task.FromResult(new Gossip.PingReqResponse(false));

    /// <summary>
    /// Queries the remote node for the last committed log index it has recorded for
    /// <paramref name="followerEndpoint"/> on <paramref name="partitionId"/> via the
    /// <c>POST /v1/raft/get-follower-lag</c> REST endpoint.  Returns <see langword="null"/>
    /// when the remote node reports no record for the follower on that partition, or when
    /// the request fails.
    /// </summary>
    public async Task<long?> GetRemoteFollowerLag(RaftManager manager, RaftNode node, int partitionId, string followerEndpoint)
    {
        GetFollowerLagRequest request = new(partitionId, followerEndpoint);
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.GetFollowerLagRequest);

        try
        {
            GetFollowerLagResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/get-follower-lag", payload)
                .PostStringAsync(payload)
                .ReceiveJson<GetFollowerLagResponse>()
                .ConfigureAwait(false);

            return response is { HasValue: true } ? response.Value : null;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] GetRemoteFollowerLag partition {PartitionId}: {Message}",
                manager.LocalEndpoint, partitionId, e.Message);
        }

        return null;
    }

    public async Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request)
    {
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.JoinRequest);

        try
        {
            JoinResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/join", payload)
                .PostStringAsync(payload)
                .ReceiveJson<JoinResponse>().ConfigureAwait(false);

            return response ?? new JoinResponse(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] SendJoin: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new JoinResponse(false);
    }

    internal static IReadOnlyDictionary<string, string> BuildAuthenticationHeaders(
        RaftConfiguration configuration,
        string senderNode,
        string method,
        string path,
        string payload)
    {
        RaftTransportSecurityOptions transportSecurity = configuration.GetEffectiveTransportSecurity();

        if (transportSecurity.NodeAuthenticationMode != RaftNodeAuthenticationMode.SharedSecret)
            return new Dictionary<string, string>();

        RaftTransportAuthenticator authenticator = new(transportSecurity);
        RaftTransportAuthenticationHeaders authHeaders = authenticator.Sign(
            method,
            path,
            senderNode,
            Encoding.UTF8.GetBytes(payload));

        return new Dictionary<string, string>
        {
            [authHeaders.SignatureHeaderName] = authHeaders.Signature,
            [RaftTransportAuthenticationHeaders.SenderNodeHeaderName] = authHeaders.SenderNode,
            [RaftTransportAuthenticationHeaders.TimestampHeaderName] =
                authHeaders.TimestampUnixMilliseconds.ToString(),
            [RaftTransportAuthenticationHeaders.NonceHeaderName] = authHeaders.Nonce
        };
    }

    private static IFlurlRequest CreateRaftRequest(
        RaftManager manager,
        RaftNode node,
        string path,
        string payload)
    {
        RaftConfiguration configuration = manager.Configuration;
        IFlurlRequest request = (configuration.HttpScheme + node.Endpoint)
            .WithHeader("Accept", "application/json")
            .WithHeader("Content-Type", "application/json")
            .WithTimeout(configuration.HttpTimeout)
            .WithSettings(o => o.HttpVersion = configuration.HttpVersion)
            .AppendPathSegments(path.Trim('/').Split('/'));

#pragma warning disable CS0618
        if (!string.IsNullOrWhiteSpace(configuration.HttpAuthBearerToken))
            request = request.WithOAuthBearerToken(configuration.HttpAuthBearerToken);
#pragma warning restore CS0618

        foreach ((string headerName, string headerValue) in BuildAuthenticationHeaders(
                     configuration,
                     manager.LocalEndpoint,
                     "POST",
                     path,
                     payload))
        {
            request = request.WithHeader(headerName, headerValue);
        }

        return request;
    }
}
