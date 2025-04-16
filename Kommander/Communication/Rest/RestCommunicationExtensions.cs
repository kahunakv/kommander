
using Kommander.Data;

namespace Kommander.Communication.Rest;

public static class RestCommunicationExtensions
{
    public static void MapRestRaftRoutes(this WebApplication app)
    {
        app.MapPost("/v1/raft/handshake", async (HandshakeRequest request, IRaft raft) =>
        {
            await raft.Handshake(request);
            return new HandshakeResponse();
        });
        
        app.MapPost("/v1/raft/append-logs", (AppendLogsRequest request, IRaft raft) =>
        {
            raft.AppendLogs(request);
            return new AppendLogsResponse();
        });
        
        app.MapPost("/v1/raft/append-logs-batch", (AppendLogsBatchRequest request, IRaft raft) =>
        {
            if (request.AppendLogs is not null)
            {
                foreach (AppendLogsRequest req in request.AppendLogs)
                    raft.AppendLogs(req);
            }

            return new AppendLogsBatchResponse();
        });
        
        app.MapPost("/v1/raft/complete-append-logs", (CompleteAppendLogsRequest request, IRaft raft) =>
        {
            raft.CompleteAppendLogs(request);

            return new AppendLogsResponse();
        });
        
        app.MapPost("/v1/raft/complete-append-logs-batch", (CompleteAppendLogsBatchRequest request, IRaft raft) =>
        {
            if (request.CompleteLogs is not null)
            {
                foreach (CompleteAppendLogsRequest req in request.CompleteLogs)
                    raft.CompleteAppendLogs(req);
            }

            return new CompleteAppendLogsBatchResponse();
        });
        
        app.MapPost("/v1/raft/batch-requests", async (BatchRequestsRequest request, IRaft raft) =>
        {
            if (request.Requests is not null)
            {
                foreach (BatchRequestsRequestItem item in request.Requests)
                {
                    switch (item.Type)
                    {
                        case BatchRequestsRequestType.Handshake:
                            await raft.Handshake(item.Handshake!);
                            break;
                        
                        case BatchRequestsRequestType.Vote:
                            raft.Vote(item.Vote!);
                            break;
                        
                        case BatchRequestsRequestType.RequestVote:
                            raft.RequestVote(item.RequestVotes!);
                            break;
                        
                        case BatchRequestsRequestType.AppendLogs:
                            raft.AppendLogs(item.AppendLogs!);
                            break;
                        
                        case BatchRequestsRequestType.CompleteAppendLogs:
                            raft.CompleteAppendLogs(item.CompleteAppendLogs!);
                            break;
                        
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            return new CompleteAppendLogsBatchResponse();
        });

        app.MapPost("/v1/raft/request-vote", (RequestVotesRequest request, IRaft raft) =>
        {
            raft.RequestVote(request);
            
            return new RequestVotesResponse();
        });

        app.MapPost("/v1/raft/vote", (VoteRequest request, IRaft raft) =>
        {
            raft.Vote(request);
            
            return new VoteResponse();
        });
        
        app.MapGet("/v1/raft/get-leader/{partitionId}", async (int partitionId, IRaft raft) =>
        {
            return await raft.WaitForLeader(partitionId, CancellationToken.None).ConfigureAwait(false);
        });
    }
}