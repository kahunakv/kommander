
using Google.Protobuf.Collections;
using Grpc.Net.Client;
using Kommander.Data;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Allows for communication between Raft nodes using gRPC messages
/// </summary>
public class GrpcCommunication : ICommunication
{
    private static readonly HttpClientHandler httpHandler = GetHandler();
    
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
        GrpcChannel channel = GrpcChannel.ForAddress($"https://{node.Endpoint}", new() { HttpHandler = httpHandler });
        
        Rafter.RafterClient client = new(channel);

        GrpcRequestVotesRequest requestVotes = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            MaxLogId = request.MaxLogId,
            Endpoint = request.Endpoint
        };
        
        await client.RequestVotesAsync(requestVotes);

        return new();
    }

    public async Task<VoteResponse> Vote(RaftManager manager, RaftPartition partition, RaftNode node, VoteRequest request)
    {
        GrpcChannel channel = GrpcChannel.ForAddress($"https://{node.Endpoint}", new() { HttpHandler = httpHandler });
        
        Rafter.RafterClient client = new(channel);

        GrpcVoteRequest voteRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            Endpoint = request.Endpoint
        };
        
        await client.VoteAsync(voteRequest);
        
        return new();
    }

    public async Task<AppendLogsResponse> AppendLogToNode(RaftManager manager, RaftPartition partition, RaftNode node, AppendLogsRequest request)
    {
        GrpcChannel channel = GrpcChannel.ForAddress($"https://{node.Endpoint}", new() { HttpHandler = httpHandler });
        
        Rafter.RafterClient client = new(channel);

        GrpcAppendLogsRequest grpcRequest = new()
        {
            Partition = request.Partition,
            Term = request.Term,
            Endpoint = request.Endpoint
        };

        grpcRequest.Logs.AddRange(GetLogs(request.Logs ?? []));
        
        GrpcAppendLogsResponse? response = await client.AppendLogsAsync(grpcRequest);
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
                Data = Google.Protobuf.ByteString.CopyFrom(requestLog.LogData)
            });
        }

        return logs;
    }
}