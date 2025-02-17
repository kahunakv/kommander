// See https://aka.ms/new-console-template for more information

using Lux;
using Lux.Services;
using Microsoft.Extensions.Primitives;
using Nixie;

Console.WriteLine("Hello, World!");

ActorSystem aas = new();
var p = new RaftManager(aas);

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

app.MapPost("/v1/raft/request-vote", async (HttpRequest request) =>
{
    await Task.CompletedTask;
    
    StringValues header = request.Headers["X-Idempotent-Key"];    
    return Results.Ok(header.ToString());
});

app.MapPost("/v1/raft/vote", async (HttpRequest request) =>
{
    await Task.CompletedTask;
    
    StringValues header = request.Headers["X-Idempotent-Key"];    
    return Results.Ok(header.ToString());
});

app.MapGet("/v1/raft/get-leader/{partitionId}", async (int partitionId, HttpRequest request) =>
{
    await Task.CompletedTask;
    
    StringValues header = request.Headers["X-Idempotent-Key"];    
    return Results.Ok(header.ToString());
});
  
app.MapGet("/", () => "Lux Raft Node");

app.Run("http://*:8004");