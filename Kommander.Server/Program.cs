
using System.Net;
using CommandLine;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Communication.Rest;
using Kommander.Discovery;
using Kommander.Server;
using Kommander.Services;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;

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

    // A node that cannot serve TLS cannot participate in mTLS at all: peers would have no server
    // certificate to validate and no handshake in which to present their own.
    if (authMode == RaftNodeAuthenticationMode.MutualTls && string.IsNullOrWhiteSpace(opts.HttpsCertificate))
        throw new RaftException("--https-certificate must be set when --node-auth-mode is MutualTls");

    // Disabling peer validation in the one mode whose entire security rests on peer validation is
    // always a misconfiguration — and a plausible copy-paste from a shared-secret development setup,
    // where the flag is harmless.
    if (authMode == RaftNodeAuthenticationMode.MutualTls && opts.AllowInsecureCertificateValidation)
    {
        throw new RaftException(
            "--allow-insecure-certificate-validation cannot be combined with --node-auth-mode MutualTls: "
            + "it would disable the peer validation that mTLS depends on");
    }

    // Resolved before anything else that depends on exposure: the fail-closed checks below are
    // statements about which addresses answer Raft requests, so they cannot run until the bind
    // address is known.
    IPAddress bindAddress = KommanderServerBindingPolicy.ResolveBindAddress(opts.Host);

    bool tlsConfigured = !string.IsNullOrWhiteSpace(opts.HttpsCertificate);

    KommanderServerBindingPolicy.ValidateAuthenticationExposure(
        authMode,
        bindAddress,
        opts.AllowUnauthenticatedCluster);

    KommanderServerBindingPolicy.ValidatePlaintextListener(authMode, opts.AllowPlaintextListener);

    if (KommanderServerBindingPolicy.IsUnauthenticatedNetworkExposure(authMode, bindAddress))
    {
        Console.WriteLine(
            "[Kommander] WARNING: --allow-unauthenticated-cluster is set. Every Raft endpoint on {0} "
            + "accepts unauthenticated requests, including log append, snapshot install, and membership "
            + "changes. Do not use this outside development.",
            bindAddress);
    }

    // Shared-certificate deployments (Docker Compose, small private clusters) present the same .pfx
    // Kestrel serves with, so falling back avoids making every such operator pass the same path twice.
    string clientCertificatePath = string.IsNullOrWhiteSpace(opts.ClientCertificate)
        ? opts.HttpsCertificate
        : opts.ClientCertificate;

    string clientCertificatePassword = string.IsNullOrWhiteSpace(opts.ClientCertificate)
        ? opts.HttpsCertificatePassword
        : opts.ClientCertificatePassword;

    if (authMode == RaftNodeAuthenticationMode.MutualTls && string.IsNullOrWhiteSpace(opts.ClientCertificate))
        Console.WriteLine("[Kommander] MutualTls: --client-certificate not set, presenting the --https-certificate instead.");

    RaftTransportSecurityOptions transportSecurity = new()
    {
        NodeAuthenticationMode = authMode,
        SharedSecret = string.IsNullOrWhiteSpace(opts.NodeSharedSecret) ? null : opts.NodeSharedSecret,
        AllowInsecureCertificateValidation = opts.AllowInsecureCertificateValidation,
        TrustedServerCertificateThumbprints = (IReadOnlyCollection<string>?)opts.TrustedServerCertThumbprints?.ToList()
            ?? Array.Empty<string>(),
        TrustedClientCertificateThumbprints = (IReadOnlyCollection<string>?)opts.TrustedClientCertThumbprints?.ToList()
            ?? Array.Empty<string>(),
        ClientCertificatePath = authMode == RaftNodeAuthenticationMode.MutualTls ? clientCertificatePath : null,
        ClientCertificatePassword = authMode == RaftNodeAuthenticationMode.MutualTls ? clientCertificatePassword : null
    };

    if (authMode == RaftNodeAuthenticationMode.MutualTls)
    {
        // Force the load now so a bad path or password fails at startup with a RaftException naming
        // the file, rather than surfacing later as a peer that mysteriously cannot connect.
        transportSecurity.GetClientCertificate();

        // Fatal, not a warning: the HTTPS listener below deliberately accepts every certificate at
        // the TLS layer so self-signed per-node certificates reach this allow-list. With no
        // allow-list to reach, both layers accept everything and mTLS is bypassed entirely.
        KommanderServerBindingPolicy.ValidateMutualTlsTrustAnchors(
            authMode,
            transportSecurity.TrustedClientCertificateThumbprints);

        Console.WriteLine("[Kommander] MutualTls: HTTP/3 is disabled on HTTPS listeners (client certificates require HTTP/1.1 or HTTP/2).");
    }

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

    // Resolved here, before the host is built, so an unknown adapter name fails at startup with a
    // message naming the flag rather than inside the DI factory on first resolution.
    KommanderWalSelection walSelection = KommanderWalConfiguration.Resolve(
        opts.WalAdapter,
        opts.RocksDbWalPath,
        opts.RocksDbWalRevision,
        opts.SqliteWalPath,
        opts.SqliteWalRevision);

    if (KommanderWalConfiguration.DescribeMismatch(walSelection) is { } walWarning)
        Console.WriteLine(walWarning);

    WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

    builder.Services.AddSingleton<IRaft>(services =>
    {
        ILogger<IRaft> logger = services.GetRequiredService<ILogger<IRaft>>();

        // --wal-adapter is now honoured. It used to be parsed and ignored: RocksDB was constructed
        // unconditionally, and from the sqlite-named options, so --rocksdb-wal-path did nothing.
        IWAL wal = walSelection.Adapter switch
        {
            KommanderWalAdapter.Sqlite => new SqliteWAL(walSelection.Path, walSelection.Revision, logger),
            _ => new RocksDbWAL(walSelection.Path, walSelection.Revision, logger),
        };

        RaftManager node = new(
            configuration,
            new StaticDiscovery(nodes),
            wal,
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
            // Metadata only. The payload is the application's replicated data — potentially
            // credentials or PII committed through Raft — and stdout is routinely shipped to a log
            // aggregator with a broader access model than the datastore, and retained longer.
            // Decoding it per entry also put a synchronous UTF-8 decode on the replication path.
            Console.WriteLine("{0}: Log received: {1} {2} {3} ({4} bytes)", partitionId, log.Id, log.Type, log.LogType, log.LogData?.Length ?? 0);

            return Task.FromResult(true);
        };

        return node;
    });

    builder.Services.AddHostedService<ReplicationService>();
    builder.Services.AddKommanderGrpc();
    builder.Services.AddGrpcReflection();
    
    // A cleartext port beside a configured TLS port is an explicit choice, not a default. Binding
    // one unconditionally meant a fully configured mTLS node still answered Raft requests on a
    // plaintext socket nobody asked for.
    bool bindPlaintextListener = KommanderServerBindingPolicy.ShouldBindPlaintextListener(
        tlsConfigured,
        opts.AllowPlaintextListener);

    IReadOnlyList<int> httpPorts = KommanderServerBindingPolicy.ParsePorts(opts.HttpPorts, 8004, "--http-ports");
    IReadOnlyList<int> httpsPorts = KommanderServerBindingPolicy.ParsePorts(opts.HttpsPorts, 8005, "--https-ports");

    if (!bindPlaintextListener)
        Console.WriteLine("[Kommander] Cleartext HTTP listener not bound (an HTTPs certificate is configured). Pass --allow-plaintext-listener to bind it anyway.");

    builder.WebHost.ConfigureKestrel(options =>
    {
        if (bindPlaintextListener)
        {
            foreach (int port in httpPorts)
                options.Listen(bindAddress, port, listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
                });
        }

        // Only bound when a certificate exists: UseHttps with an empty path fails inside Kestrel
        // with an error that names neither the option nor the file.
        if (tlsConfigured)
        {
            foreach (int port in httpsPorts)
                options.Listen(bindAddress, port, ConfigureHttpsListener);
        }
    });

    // Configures an HTTPS listener, requiring a client certificate when mTLS is enabled.
    //
    // The certificate must be requested during the initial handshake. Optional client certificates
    // (ClientCertificateMode.AllowCertificate) rely on TLS renegotiation to ask for one mid-connection,
    // which HTTP/2 forbids and HTTP/3 cannot express at all — so on the very transport gRPC requires,
    // Connection.ClientCertificate would always be null and every Raft call would be rejected. HTTP/3
    // is dropped in this mode for the same reason.
    //
    // The consequence, which is deliberate: in MutualTls mode this HTTPS port becomes cluster-internal.
    // Every caller on it must present a certificate, not just the /v1/raft routes.
    void ConfigureHttpsListener(ListenOptions listenOptions)
    {
        bool mutualTls = authMode == RaftNodeAuthenticationMode.MutualTls;

        listenOptions.Protocols = mutualTls
            ? HttpProtocols.Http1AndHttp2
            : HttpProtocols.Http1AndHttp2AndHttp3;

        listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword, httpsOptions =>
        {
            if (!mutualTls)
                return;

            httpsOptions.ClientCertificateMode = ClientCertificateMode.RequireCertificate;

            // Accept at the TLS layer and defer the trust decision to RaftClientCertificateValidator.
            // Kestrel's default chain validation would reject self-signed per-node certificates before
            // the application could consult its thumbprint allow-list, and that is a supported
            // deployment for private clusters.
            httpsOptions.ClientCertificateValidation = (_, _, _) => true;
        });
    }
    
    ThreadPool.SetMinThreads(128, 128);

    // The REST and gRPC cluster clients configure their own certificate policy from
    // RaftConfiguration.TransportSecurity — client certificate, thumbprint pinning, and the
    // development bypass alike. This used to be installed on FlurlHttp.Clients here, which applied it
    // to every Flurl call in the process rather than only to cluster traffic; with
    // --allow-insecure-certificate-validation that silently disabled certificate validation for
    // unrelated HTTP clients too.
    if (opts.AllowInsecureCertificateValidation)
    {
        Console.WriteLine(
            "[Kommander] WARNING: --allow-insecure-certificate-validation is set. Peer TLS certificates "
            + "are NOT validated on cluster connections, so node-to-node traffic can be intercepted. "
            + "Development only.");
    }

    WebApplication app = builder.Build();

    app.MapRestRaftRoutes();
    app.MapGrpcRaftRoutes();

    app.MapGet("/", () => "Kommander Raft Node");

    app.Run();
}
catch (RaftException ex)
{
    // A configuration problem. The message is written to name the offending flag and what to do
    // about it, so the stack trace adds nothing an operator can act on while disclosing source paths
    // and assembly layout to whoever reads the container log.
    Console.Error.WriteLine("[Kommander] {0}", ex.Message);
    Environment.ExitCode = 1;
}
catch (Exception ex)
{
    // Anything else is a fault rather than a misconfiguration, and the trace is the only way to
    // diagnose it — so it is kept, on stderr.
    Console.Error.WriteLine("[Kommander] Unexpected startup failure: {0}: {1}\n{2}", ex.GetType().Name, ex.Message, ex.StackTrace);
    Environment.ExitCode = 1;
}