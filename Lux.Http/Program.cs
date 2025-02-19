
using Lux;
using Lux.Data;
using Lux.Discovery;
using Lux.Services;
using Lux.WAL;

using Nixie;

string[] arguments = Environment.GetCommandLineArgs();

RaftConfiguration config = new()
{
    Host = arguments[1],
    Port = int.Parse(arguments[2])
};

Console.WriteLine("LUX! {0} {1}", config.Host, config.Port);

ActorSystem aas = new();

RaftManager p = new(
    aas, 
    config, 
    new StaticDiscovery(arguments[3].Split(",").Select(x => new RaftNode(x)).ToList()),
    new SqliteWAL()
);

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<RaftManager>(p);
builder.Services.AddHostedService<InstrumentationService>();

WebApplication app = builder.Build();

app.MapPost("/v1/raft/append-logs", async (AppendLogsRequest request, HttpRequest httpRequest, RaftManager raft) =>
{
    await Task.CompletedTask;
    
    raft.AppendLogs(request);

    return new AppendLogsResponse();
});

app.MapPost("/v1/raft/request-vote", async (RequestVotesRequest request, HttpRequest httpRequest, RaftManager raft) =>
{
    await Task.CompletedTask;
    
    raft.RequestVote(request);

    return new RequestVotesResponse();
});

app.MapPost("/v1/raft/vote", async (VoteRequest request, HttpRequest httpRequest, RaftManager raft) =>
{
    await Task.CompletedTask;
    
    raft.Vote(request);
    
    return new VoteResponse();
});

app.MapGet("/v1/raft/get-leader/{partitionId}", async (int partitionId, HttpRequest httpRequest, RaftManager raft) =>
{
    //StringValues header = request.Headers["X-Idempotent-Key"];    
    //return Results.Ok(header.ToString());

    return Results.Ok(await raft.WaitForLeader(partitionId));
});
  
app.MapGet("/", () => "Lux Raft Node");

app.Run("http://*:" + config.Port);