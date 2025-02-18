// See https://aka.ms/new-console-template for more information

using Lux;
using Lux.Data;
using Lux.Services;
using Microsoft.Extensions.Primitives;
using Nixie;

string[] arguments = Environment.GetCommandLineArgs();

RaftConfiguration config = new()
{
    Host = arguments[1],
    Port = int.Parse(arguments[2])
};

Console.WriteLine("Hello, World! {0} {1}", config.Host, config.Port);

ActorSystem aas = new();
var p = new RaftManager(aas, config);

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<RaftManager>(p);
builder.Services.AddHostedService<InstrumentationService>();

WebApplication app = builder.Build();

app.MapPost("/v1/raft/append-logs", async (HttpRequest request) =>
{
    await Task.CompletedTask;
    
    StringValues header = request.Headers["X-Idempotent-Key"];    
    return Results.Ok(header.ToString());
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
    
    //StringValues header = request.Headers["X-Idempotent-Key"];    
    //return Results.Ok(header.ToString());

    return new VoteResponse();
});

app.MapGet("/v1/raft/get-leader/{partitionId}", async (int partitionId, HttpRequest request) =>
{
    await Task.CompletedTask;
    
    StringValues header = request.Headers["X-Idempotent-Key"];    
    return Results.Ok(header.ToString());
});
  
app.MapGet("/", () => "Lux Raft Node");

app.Run("http://*:" + config.Port);