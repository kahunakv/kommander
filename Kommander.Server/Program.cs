
using Nixie;

using System.Net;
using System.Text;
using CommandLine;
using Flurl.Http;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Communication.Rest;
using Kommander.Discovery;
using Kommander.Server;
using Kommander.Services;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Server.Kestrel.Core;

ParserResult<KommanderCommandLineOptions> optsResult = Parser.Default.ParseArguments<KommanderCommandLineOptions>(args);

KommanderCommandLineOptions? opts = optsResult.Value;
if (opts is null)
    return;

try
{
    RaftConfiguration configuration = new()
    {
        NodeId = string.IsNullOrEmpty(opts.RaftNodeId) ? Environment.MachineName : opts.RaftNodeId,
        Host = opts.RaftHost,
        Port = opts.RaftPort,
        InitialPartitions = opts.InitialClusterPartitions
    };

    List<RaftNode> nodes = [];

    if (opts.InitialCluster is not null)
        nodes = [.. opts.InitialCluster.Select(k => new RaftNode(k))];

    if (nodes.Count < 2)
        throw new RaftException("Invalid number of nodes. Must be at least 2");

    Console.WriteLine("Kommander! {0} {1}", configuration.Host, configuration.Port);

    WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

    builder.Services.AddSingleton<ActorSystem>(services =>
        new(services, services.GetRequiredService<ILogger<IRaft>>()));

    builder.Services.AddSingleton<IRaft>(services =>
    {
        RaftManager node = new(
            services.GetRequiredService<ActorSystem>(),
            configuration,
            new StaticDiscovery(nodes),
            new RocksDbWAL(path: opts.SqliteWalPath, revision: opts.SqliteWalRevision),
            new RestCommunication(),
            new HybridLogicalClock(),
            services.GetRequiredService<ILogger<IRaft>>()
        );

        node.OnReplicationError += (partitionId, log) =>
        {
            Console.WriteLine("{0}: Replication error: {0} #{1}", partitionId, log.LogType, log.Id);
        };
        
        node.OnReplicationRestored += (partitionId, log) =>
        {
            Console.WriteLine("{0}: Replication restored: {0} {1} {2} {3} {4}", partitionId, log.Id, log.Type, log.LogType, Encoding.UTF8.GetString(log.LogData ?? []));
            
            return Task.FromResult(true);
        };

        node.OnReplicationReceived += (partitionId, log) =>
        {
            Console.WriteLine("{0}: Replication received: {1} {2} {3} {4}", partitionId, log.Id, log.Type, log.LogType, Encoding.UTF8.GetString(log.LogData ?? []));
            
            return Task.FromResult(true);
        };

        return node;
    });

    builder.Services.AddHostedService<ReplicationService>();
    builder.Services.AddGrpc();
    builder.Services.AddGrpcReflection();
    
    builder.WebHost.ConfigureKestrel(options =>
    {
        if (opts.HttpPorts is null || !opts.HttpPorts.Any())
            options.Listen(IPAddress.Any, 8004, listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
            });
        else
            foreach (string port in opts.HttpPorts)
                options.Listen(IPAddress.Any, int.Parse(port), listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
                });

        if (opts.HttpsPorts is null || !opts.HttpsPorts.Any())
            options.Listen(IPAddress.Any, 8005, listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3; 
                listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword);
            });
        else
        {
            foreach (string port in opts.HttpsPorts)
            {
                options.Listen(IPAddress.Any, int.Parse(port), listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3; 
                    listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword);
                });
            }
        }
    });
    
    ThreadPool.SetMinThreads(1024, 512);
    
    FlurlHttp.Clients.WithDefaults(x => x.ConfigureInnerHandler(ih => ih.ServerCertificateCustomValidationCallback = (a, b, c, d) => true));

    WebApplication app = builder.Build();

    app.MapRestRaftRoutes();
    app.MapGrpcRaftRoutes();

    app.MapGet("/", () => "Kommander Raft Node");

    app.Run();
}
catch (Exception ex)
{
    Console.WriteLine("{0}\n{1}", ex.Message, ex.StackTrace);
}