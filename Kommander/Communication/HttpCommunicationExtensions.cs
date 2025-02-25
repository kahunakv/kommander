
using Kommander.Data;

namespace Kommander.Communication;

public static class HttpCommunicationExtensions
{
    public static void MapRaftRoutes(this WebApplication app)
    {
        app.MapPost("/v1/raft/append-logs", async (AppendLogsRequest request, IRaft raft) =>
        {
            (RaftOperationStatus status, long commitLogIndex) = await raft.AppendLogs(request);
            return new AppendLogsResponse(status, commitLogIndex);
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
            string leader = await raft.WaitForLeader(partitionId);
            return leader;
        });
    }
}