
using Kommander.Data;

namespace Kommander.Communication;

public static class HttpCommunicationExtensions
{
    public static void MapRaftRoutes(this WebApplication app)
    {
        app.MapPost("/v1/raft/append-logs", async (AppendLogsRequest request, RaftManager raft) =>
        {
            await Task.CompletedTask;
            
            raft.AppendLogs(request);

            return new AppendLogsResponse();
        });

        app.MapPost("/v1/raft/request-vote", async (RequestVotesRequest request, RaftManager raft) =>
        {
            await Task.CompletedTask;
            
            raft.RequestVote(request);

            return new RequestVotesResponse();
        });

        app.MapPost("/v1/raft/vote", async (VoteRequest request, RaftManager raft) =>
        {
            await Task.CompletedTask;
            
            raft.Vote(request);

            return new VoteResponse();
        });
    }
}