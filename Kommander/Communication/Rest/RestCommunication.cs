using System.Text;
using System.Text.Json;
using Flurl.Http;
using Kommander.Data;
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
