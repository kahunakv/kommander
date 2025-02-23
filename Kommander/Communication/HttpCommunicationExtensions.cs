
using Kommander.Data;

namespace Kommander.Communication;

public static class HttpCommunicationExtensions
{
    public static void MapRaftRoutes(this WebApplication app)
    {
        app.MapPost("/v1/raft/append-logs", async (AppendLogsRequest request, IRaft raft) 
            => new AppendLogsResponse(await raft.AppendLogs(request)));

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
    }
}