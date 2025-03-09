
using Kommander.Data;

namespace Kommander.Communication.Rest;

public static class RestCommunicationExtensions
{
    public static void MapRestRaftRoutes(this WebApplication app)
    {
        app.MapPost("/v1/raft/append-logs", (AppendLogsRequest request, IRaft raft) =>
        {
            raft.AppendLogs(request);
            return new AppendLogsResponse();
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