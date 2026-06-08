
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
    if (!Enum.TryParse(opts.NodeAuthMode, ignoreCase: true, out RaftNodeAuthenticationMode authMode))
        throw new RaftException($"Unknown --node-auth-mode value: {opts.NodeAuthMode}");

    if (authMode == RaftNodeAuthenticationMode.SharedSecret && string.IsNullOrWhiteSpace(opts.NodeSharedSecret))
        throw new RaftException("--node-shared-secret must be set when --node-auth-mode is SharedSecret");

    RaftTransportSecurityOptions transportSecurity = new()
    {
        NodeAuthenticationMode = authMode,
        SharedSecret = string.IsNullOrWhiteSpace(opts.NodeSharedSecret) ? null : opts.NodeSharedSecret,
        AllowInsecureCertificateValidation = opts.AllowInsecureCertificateValidation,
        TrustedServerCertificateThumbprints = (IReadOnlyCollection<string>?)opts.TrustedServerCertThumbprints?.ToList()
            ?? Array.Empty<string>(),
        TrustedClientCertificateThumbprints = (IReadOnlyCollection<string>?)opts.TrustedClientCertThumbprints?.ToList()
            ?? Array.Empty<string>()
    };

    if (!string.IsNullOrWhiteSpace(opts.NodeAuthHeader))
        transportSecurity.HeaderName = opts.NodeAuthHeader;

    RaftConfiguration configuration = new()
    {
        NodeName = string.IsNullOrEmpty(opts.RaftNodeName) ? Environment.MachineName : opts.RaftNodeName,
        NodeId = opts.RaftNodeId,
        Host = opts.RaftHost,
        Port = opts.RaftPort,
        InitialPartitions = opts.InitialClusterPartitions,
        TransportSecurity = transportSecurity
    };

    List<RaftNode> nodes = [];

    if (opts.InitialCluster is not null)
        nodes = [.. opts.InitialCluster.Select(k => new RaftNode(k))];

    if (nodes.Count < 2)
        throw new RaftException("Invalid number of nodes. Must be at least 2");

    Console.WriteLine("Kommander! {0} {1}", configuration.Host, configuration.Port);

    WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

    builder.Services.AddSingleton<IRaft>(services =>
    {
        ILogger<IRaft> logger = services.GetRequiredService<ILogger<IRaft>>();
        
        RaftManager node = new(
            configuration,
            new StaticDiscovery(nodes),
            new RocksDbWAL(path: opts.SqliteWalPath, revision: opts.SqliteWalRevision, logger),
            new GrpcCommunication(),
            new HybridLogicalClock(),
            logger
        );

        node.OnReplicationError += (partitionId, log) =>
        {
            Console.Error.WriteLine("{0}: Replication error: {1} #{2}", partitionId, log.LogType, log.Id);
        };
        
        node.OnLogRestored += (partitionId, log) =>
        {
            // Console.WriteLine("{0}: Log restored: {0} {1} {2} {3} {4}", partitionId, log.Id, log.Type, log.LogType, Encoding.UTF8.GetString(log.LogData ?? []));
            
            return Task.FromResult(true);
        };

        node.OnReplicationReceived += (partitionId, log) =>
        {
            Console.WriteLine("{0}: Log received: {1} {2} {3} {4}", partitionId, log.Id, log.Type, log.LogType, Encoding.UTF8.GetString(log.LogData ?? []));
            
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
    
    ThreadPool.SetMinThreads(128, 128);
    
    if (opts.AllowInsecureCertificateValidation)
    {
        FlurlHttp.Clients.WithDefaults(x =>
            x.ConfigureInnerHandler(ih =>
                ih.ServerCertificateCustomValidationCallback = (_, _, _, _) => true));
    }
    else if (transportSecurity.TrustedServerCertificateThumbprints.Count > 0)
    {
        IReadOnlyCollection<string> thumbprints = transportSecurity.TrustedServerCertificateThumbprints;
        FlurlHttp.Clients.WithDefaults(x =>
            x.ConfigureInnerHandler(ih =>
                ih.ServerCertificateCustomValidationCallback = (_, certificate, _, _) =>
                {
                    if (certificate is null)
                        return false;
                    byte[] hash = System.Security.Cryptography.SHA256.HashData(certificate.GetRawCertData());
                    string thumbprint = Convert.ToHexString(hash);
                    return thumbprints.Any(t => string.Equals(t, thumbprint, StringComparison.OrdinalIgnoreCase));
                }));
    }

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