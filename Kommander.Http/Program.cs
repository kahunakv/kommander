
using Kommander;
using Kommander.Communication;
using Kommander.Discovery;
using Kommander.Services;
using Kommander.WAL;

using Nixie;

string[] arguments = Environment.GetCommandLineArgs();

RaftConfiguration config = new()
{
    Host = arguments[1],
    Port = int.Parse(arguments[2]),
    MaxPartitions = 1
};

Console.WriteLine("Kommander! {0} {1}", config.Host, config.Port);

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<ActorSystem>(services => new(services, services.GetRequiredService<ILogger<IRaft>>()));

builder.Services.AddSingleton<IRaft>(services => new RaftManager(
    services.GetRequiredService<ActorSystem>(), 
    config, 
    new StaticDiscovery(arguments[3].Split(",").Select(x => new RaftNode(x)).ToList()),
    new SqliteWAL(),
    new HttpCommunication(),
    services.GetRequiredService<ILogger<IRaft>>()
));

builder.Services.AddHostedService<InstrumentationService>();

WebApplication app = builder.Build();

app.MapRaftRoutes();
  
app.MapGet("/", () => "Kommander Raft Node");

app.Run("http://*:" + config.Port);