
using System.Text.Json;
using Flurl.Http;
using Kommander.Data;

namespace Kommander.Communication.Rest;

/// <summary>
/// Allows for communication between Raft nodes using REST endpoints.
/// </summary>
public class RestCommunication : ICommunication
{
    public async Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        RaftConfiguration configuration = manager.Configuration;
        
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.HandshakeRequest);
        
        try
        {
            return await (configuration.HttpScheme + node.Endpoint)
                .WithOAuthBearerToken(configuration.HttpAuthBearerToken)
                .AppendPathSegments("v1/raft/handshake")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(configuration.HttpTimeout)
                .WithSettings(o => o.HttpVersion = configuration.HttpVersion)
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
        RaftConfiguration configuration = manager.Configuration;
        
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.RequestVotesRequest);
        
        try
        {
            return await (configuration.HttpScheme + node.Endpoint)
                .WithOAuthBearerToken(configuration.HttpAuthBearerToken)
                .AppendPathSegments("v1/raft/request-vote")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(configuration.HttpTimeout)
                .WithSettings(o => o.HttpVersion = configuration.HttpVersion)
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
        RaftConfiguration configuration = manager.Configuration;
        
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.VoteRequest);
        
        try
        {
            return await (configuration.HttpScheme + node.Endpoint)
                .WithOAuthBearerToken(configuration.HttpAuthBearerToken)
                .AppendPathSegments("v1/raft/vote")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(configuration.HttpTimeout)
                .WithSettings(o => o.HttpVersion = configuration.HttpVersion)
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
        RaftConfiguration configuration = manager.Configuration;
        
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.CompleteAppendLogsRequest);
        
        try
        {
            CompleteAppendLogsResponse? response = await (configuration.HttpScheme + node.Endpoint)
                .WithOAuthBearerToken(configuration.HttpAuthBearerToken)
                .AppendPathSegments("v1/raft/complete-append-logs")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(configuration.HttpTimeout)
                .WithSettings(o => o.HttpVersion = configuration.HttpVersion)
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
        RaftConfiguration configuration = manager.Configuration;
        
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.AppendLogsRequest);
        
        try
        {
            AppendLogsResponse? response = await (configuration.HttpScheme + node.Endpoint)
                .WithOAuthBearerToken(configuration.HttpAuthBearerToken)
                .AppendPathSegments("v1/raft/append-logs")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(configuration.HttpTimeout)
                .WithSettings(o => o.HttpVersion = configuration.HttpVersion)
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
    
    public async Task<AppendLogsBatchResponse> AppendLogsBatch(RaftManager manager, RaftNode node, AppendLogsBatchRequest request)
    {
        RaftConfiguration configuration = manager.Configuration;
        
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.AppendLogsBatchRequest);
        
        try
        {
            AppendLogsBatchResponse? response = await (configuration.HttpScheme + node.Endpoint)
                .WithOAuthBearerToken(configuration.HttpAuthBearerToken)
                .AppendPathSegments("v1/raft/append-logs-batch")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(configuration.HttpTimeout)
                .WithSettings(o => o.HttpVersion = configuration.HttpVersion)
                .PostStringAsync(payload)
                .ReceiveJson<AppendLogsBatchResponse>().ConfigureAwait(false);

            return response;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] AppendLogsBatch: {Message}", manager.LocalEndpoint, request.AppendLogs?[0].Partition, e.Message);
        }

        return new();
    }
    
    public async Task<CompleteAppendLogsBatchResponse> CompleteAppendLogsBatch(RaftManager manager, RaftNode node, CompleteAppendLogsBatchRequest request)
    {
        RaftConfiguration configuration = manager.Configuration;
        
        string payload = JsonSerializer.Serialize(request, RestJsonContext.Default.CompleteAppendLogsBatchRequest);
        
        try
        {
            CompleteAppendLogsBatchResponse? response = await (configuration.HttpScheme + node.Endpoint)
                .WithOAuthBearerToken(configuration.HttpAuthBearerToken)
                .AppendPathSegments("v1/raft/complete-append-logs-batch")
                .WithHeader("Accept", "application/json")
                .WithHeader("Content-Type", "application/json")
                .WithTimeout(configuration.HttpTimeout)
                .WithSettings(o => o.HttpVersion = configuration.HttpVersion)
                .PostStringAsync(payload)
                .ReceiveJson<CompleteAppendLogsBatchResponse>().ConfigureAwait(false);

            return response;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] CompleteAppendLogsBatch: {Message}", manager.LocalEndpoint, request.CompleteLogs?[0].Partition, e.Message);
        }

        return new();
    }

    public Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        throw new NotImplementedException();
    }
}