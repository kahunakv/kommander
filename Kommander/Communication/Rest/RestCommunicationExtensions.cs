
using Kommander.Data;

namespace Kommander.Communication.Rest;

public static class RestCommunicationExtensions
{
    public static void MapRestRaftRoutes(this WebApplication app)
    {
        app.MapPost("/v1/raft/handshake", (HandshakeRequest request, IRaft raft) =>
        {
            raft.Handshake(request);
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