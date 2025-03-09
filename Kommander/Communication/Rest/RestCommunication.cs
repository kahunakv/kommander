
using System.Text.Json;
using Flurl.Http;
using Kommander.Data;

namespace Kommander.Communication.Rest;

/// <summary>
/// Allows for communication between Raft nodes using REST endpoints.
/// </summary>
public class RestCommunication : ICommunication
{
    public async Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftPartition partition, RaftNode node, RequestVotesRequest request)
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
            manager.Logger.LogError("[{Endpoint}/{Partition}] RequestVotes: {Message}", manager.LocalEndpoint, partition.PartitionId, e.Message);
        }
        
        return new();
    }

    public async Task<VoteResponse> Vote(RaftManager manager, RaftPartition partition, RaftNode node, VoteRequest request)
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
            manager.Logger.LogError("[{Endpoint}/{Partition}] Vote: {Message}", manager.LocalEndpoint, partition.PartitionId, e.Message);
        }

        return new();
    }

    public async Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftPartition partition, RaftNode node, CompleteAppendLogsRequest request)
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
            manager.Logger.LogError("[{Endpoint}/{Partition}] CompleteAppendLogs: {Message}", manager.LocalEndpoint, partition.PartitionId, e.Message);
        }

        return new();
    }

    public async Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftPartition partition, RaftNode node, AppendLogsRequest request)
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
                manager.Logger.LogDebug("[{Endpoint}/{Partition}] Logs replicated to {RemoteEndpoint}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint);

            return response;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] AppendLogs: {Message}", manager.LocalEndpoint, partition.PartitionId, e.Message);
        }

        return new();
    }
}