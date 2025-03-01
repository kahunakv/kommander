
using Nixie;

using System.Net;
using System.Text;
using CommandLine;

using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Communication.Rest;
using Kommander.Discovery;
using Kommander.Server;
using Kommander.Services;
using Kommander.Time;
using Kommander.WAL;

ParserResult<KommanderCommandLineOptions> optsResult = Parser.Default.ParseArguments<KommanderCommandLineOptions>(args);

KommanderCommandLineOptions? opts = optsResult.Value;
if (opts is null)
    return;

try
{
    RaftConfiguration configuration = new()
    {
        Host = opts.RaftHost,
        Port = opts.RaftPort,
        MaxPartitions = opts.InitialClusterPartitions
    };

    List<RaftNode> nodes = [];

    if (opts.InitialCluster is not null)
        nodes = [.. opts.InitialCluster.Select(k => new RaftNode(k))];

    if (nodes.Count != 2)
        throw new RaftException("Invalid number of nodes");

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
            new SqliteWAL(path: opts.SqliteWalPath, version: opts.SqliteWalRevision),
            new GrpcCommunication(),
            new HybridLogicalClock(),
            services.GetRequiredService<ILogger<IRaft>>()
        );

        node.OnReplicationError += log =>
        {
            Console.WriteLine("Replication error: {0} #{1}", log.LogType, log.Id);
        };

        node.OnReplicationReceived += (string logType, byte[] logData) =>
        {
            Console.WriteLine("Replication received: {0} {1}", logType, Encoding.UTF8.GetString(logData));
            
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
            options.Listen(IPAddress.Any, 8004, _ => { });
        else
            foreach (string port in opts.HttpPorts)
                options.Listen(IPAddress.Any, int.Parse(port), _ => { });

        if (opts.HttpsPorts is null || !opts.HttpsPorts.Any())
            options.Listen(IPAddress.Any, 8005, listenOptions =>
            {
                listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword);
            });
        else
        {
            foreach (string port in opts.HttpsPorts)
            {
                options.Listen(IPAddress.Any, int.Parse(port), listenOptions =>
                {
                    listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword);
                });
            }
        }
    });

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