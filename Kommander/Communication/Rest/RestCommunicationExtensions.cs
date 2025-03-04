
using Kommander.Data;

namespace Kommander.Communication.Rest;

public static class RestCommunicationExtensions
{
    public static void MapRestRaftRoutes(this WebApplication app)
    {
        app.MapPost("/v1/raft/append-logs", async (AppendLogsRequest request, IRaft raft) =>
        {
            (RaftOperationStatus status, long commitLogIndex) = await raft.AppendLogs(request).ConfigureAwait(false);
            return new AppendLogsResponse(status, commitLogIndex);
        });

        app.MapPost("/v1/raft/request-vote", async (RequestVotesRequest request, IRaft raft) =>
        {
            await raft.RequestVote(request).ConfigureAwait(false);
            
            return new RequestVotesResponse();
        });

        app.MapPost("/v1/raft/vote", async (VoteRequest request, IRaft raft) =>
        {
            await raft.Vote(request).ConfigureAwait(false);
            
            return new VoteResponse();
        });
        
        app.MapGet("/v1/raft/get-leader/{partitionId}", async (int partitionId, IRaft raft) =>
        {
            return await raft.WaitForLeader(partitionId, CancellationToken.None).ConfigureAwait(false);
        });
    }
}