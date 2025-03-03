
using System.Collections.Concurrent;
using System.Text;
using Google.Protobuf.Collections;
using Grpc.Net.Client;
using Kommander.Data;
using Kommander.Time;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Allows for communication between Raft nodes using gRPC messages
/// </summary>
public class GrpcCommunication : ICommunication
{
    private static readonly HttpClientHandler httpHandler = GetHandler();
    
    private readonly ConcurrentDictionary<string, GrpcChannel> channels = new();
    
    private static HttpClientHandler GetHandler()
    {
        HttpClientHandler handler = new();
        
        handler.ServerCertificateCustomValidationCallback = (httpRequestMessage, cert, cetChain, policyErrors) =>
        {
            // Optionally, check for other policyErrors
            if (policyErrors == System.Net.Security.SslPolicyErrors.None)
                return true;

            // Compare the certificate's thumbprint to our trusted thumbprint.
            //if (cert is X509Certificate2 certificate && certificate.Thumbprint.Equals(trustedThumbprint, StringComparison.OrdinalIgnoreCase))
            //{
            return true;
            //}

            //return false;
        };
        
        return handler;
    }
    
    public async Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftPartition partition, RaftNode node, RequestVotesRequest request)
    {
        //manager.Logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Sent RequestVotes message to {Endpoint} on Term={Term}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, request.Term);

        if (!channels.TryGetValue(node.Endpoint, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress($"https://{node.Endpoint}", new() { HttpHandler = httpHandler });
            channels.TryAdd(node.Endpoint, channel);
        }

        Rafter.RafterClient client = new(channel);

        GrpcRequestVotesRequest requestVotes = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            MaxLogId = request.MaxLogId,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint
        };
        
        await client.RequestVotesAsync(requestVotes).ConfigureAwait(false);
        
        //manager.Logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got RequestVotes reply from {Endpoint} on Term={Term}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, request.Term);

        return new();
    }

    public async Task<VoteResponse> Vote(RaftManager manager, RaftPartition partition, RaftNode node, VoteRequest request)
    {
        //manager.Logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Send Vote to {Node} message on Term={Term}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, request.Term);
        
        if (!channels.TryGetValue(node.Endpoint, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress($"https://{node.Endpoint}", new() { HttpHandler = httpHandler });
            channels.TryAdd(node.Endpoint, channel);
        }
        
        Rafter.RafterClient client = new(channel);

        GrpcVoteRequest voteRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint
        };
        
        await client.VoteAsync(voteRequest).ConfigureAwait(false);
        
        //manager.Logger.LogDebug("[{LocalEndpoint}/{PartitionId}] Got Vote reply from {Endpoint} on Term={Term}", manager.LocalEndpoint, partition.PartitionId, node.Endpoint, request.Term);
        
        return new();
    }

    public async Task<AppendLogsResponse> AppendLogToNode(RaftManager manager, RaftPartition partition, RaftNode node, AppendLogsRequest request)
    {
        if (!channels.TryGetValue(node.Endpoint, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress($"https://{node.Endpoint}", new() { HttpHandler = httpHandler });
            channels.TryAdd(node.Endpoint, channel);
        }
        
        Rafter.RafterClient client = new(channel);

        GrpcAppendLogsRequest grpcRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            TimePhysical = request.Time.L,
            TimeCounter = request.Time.C,
            Endpoint = request.Endpoint
        };

        grpcRequest.Logs.AddRange(GetLogs(request.Logs ?? []));
        
        GrpcAppendLogsResponse? response = await client.AppendLogsAsync(grpcRequest).ConfigureAwait(false);
        return new((RaftOperationStatus)response.Status, response.CommitedIndex);
    }
    
    private static RepeatedField<GrpcRaftLog> GetLogs(List<RaftLog> requestLogs)
    {
        RepeatedField<GrpcRaftLog> logs = new();

        foreach (RaftLog? requestLog in requestLogs)
        {
            logs.Add(new GrpcRaftLog
            {
                Id = requestLog.Id,
                Term = requestLog.Term,
                Type = requestLog.Type == RaftLogType.Regular ? GrpRaftLogType.Regular : GrpRaftLogType.Checkpoint,
                LogType = requestLog.LogType,
                TimePhysical = requestLog.Time.L,
                TimeCounter = requestLog.Time.C,
                Data = Google.Protobuf.ByteString.CopyFrom(requestLog.LogData)
            });
        }

        return logs;
    }
}