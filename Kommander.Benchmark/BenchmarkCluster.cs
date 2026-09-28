using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Kommander.Communication;
using Kommander.Communication.Grpc;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;

namespace Kommander.Benchmark;

/// <summary>
/// Builds, starts and disposes the benchmark's N-node cluster inside one process.
///
/// <para><b>Two transports.</b> <c>grpc</c> gives each node its own Kestrel listener on loopback and
/// a <see cref="GrpcCommunication"/> client, so every proposal pays protobuf, HTTP/2 and (unless
/// <c>--plaintext</c>) TLS on real sockets — the path Kahuna runs. <c>memory</c> shares one
/// <see cref="InMemoryCommunication"/> and is the engine-only control: the difference between the two
/// arms is the transport's share of the round.</para>
///
/// <para><b>Mutual TLS as Kahuna runs it.</b> One self-signed certificate is created per run and
/// shared by all nodes as both server and client certificate; both thumbprint allow-lists pin it.
/// Kestrel requires a client certificate at the handshake and defers trust to Kommander's
/// allow-list, the same listener setup as <c>Kommander.Server</c>.</para>
///
/// <para><b>One process, shared CPU.</b> All nodes share the process, the thread pool and the GC,
/// so CPU and allocation are reported per committed entry for the whole cluster, not per node.
/// Separate processes would split them; that is not in this version.</para>
/// </summary>
public sealed class BenchmarkCluster : IAsyncDisposable
{
    private readonly List<WebApplication> apps = [];

    private readonly List<string> walDirectories = [];

    private X509Certificate2? certificate;

    public List<RaftManager> Managers { get; } = [];

    public List<CountingCommunication> Communications { get; } = [];

    private BenchmarkCluster() { }

    /// <summary>
    /// Creates and starts the cluster, then waits until every user partition has a stable leader.
    /// Throws when the cluster is not ready within <paramref name="readyTimeout"/>.
    /// </summary>
    public static async Task<BenchmarkCluster> StartAsync(BenchmarkOptions options, ILoggerFactory loggerFactory, TimeSpan readyTimeout)
    {
        BenchmarkCluster cluster = new();

        try
        {
            await cluster.BuildAsync(options, loggerFactory).ConfigureAwait(false);
            await cluster.WaitReadyAsync(options.Partitions, readyTimeout).ConfigureAwait(false);
            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task BuildAsync(BenchmarkOptions options, ILoggerFactory loggerFactory)
    {
        bool grpc = options.Transport == "grpc";
        bool mtls = grpc && !options.Plaintext;

        if (mtls)
            certificate = CreateCertificate();

        List<int> ports = [.. Enumerable.Range(options.BasePort, options.Nodes)];
        List<RaftNode> allNodes = [.. ports.Select(p => new RaftNode($"localhost:{p}"))];

        InMemoryCommunication? memory = grpc ? null : new InMemoryCommunication();

        foreach (int port in ports)
        {
            ILogger<IRaft> logger = loggerFactory.CreateLogger<IRaft>();

            RaftConfiguration configuration = new()
            {
                NodeId = port,
                Host = "localhost",
                Port = port,
                InitialPartitions = options.Partitions,
                GrpcScheme = mtls ? "https://" : "http://",
                // A steady-state benchmark: SWIM probing and quiescence add background traffic and
                // mode changes that are not part of a proposal round.
                EnableQuiescence = false,
                PingInterval = TimeSpan.Zero,
                FanOutBeforeLocalWrite = options.FanOutBeforeLocalWrite ?? true
            };

            ApplyOverrides(configuration, options.Set);

            if (mtls)
            {
                // Kommander pins SHA-256 over the DER certificate, not the SHA-1 Thumbprint property.
                string thumbprint = Convert.ToHexString(SHA256.HashData(certificate!.RawData));
                configuration.TransportSecurity = new RaftTransportSecurityOptions
                {
                    NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
                    ClientCertificate = certificate,
                    TrustedServerCertificateThumbprints = [thumbprint],
                    TrustedClientCertificateThumbprints = [thumbprint]
                };
            }
            else if (grpc)
            {
                // Plaintext arm: no TLS, so the transport must not insist on it.
                configuration.TransportSecurity = new RaftTransportSecurityOptions
                {
                    NodeAuthenticationMode = RaftNodeAuthenticationMode.Disabled,
                    RequireTls = false
                };
            }

            ICommunication transport = grpc ? new GrpcCommunication() : memory!;
            CountingCommunication counting = new(transport);

            RaftManager manager = new(
                configuration,
                new StaticDiscovery([.. allNodes.Where(n => n.Endpoint != $"localhost:{port}")]),
                CreateWal(options, port, logger),
                counting,
                new HybridLogicalClock(),
                logger
            );

            Managers.Add(manager);
            Communications.Add(counting);

            if (grpc)
                apps.Add(BuildHost(manager, logger, port, mtls ? certificate : null));
        }

        if (memory is not null)
            memory.SetNodes(Managers.ToDictionary(m => m.GetLocalEndpoint(), m => (IRaft)m));

        foreach (WebApplication app in apps)
            await app.StartAsync().ConfigureAwait(false);

        await Task.WhenAll(Managers.Select(m => m.JoinCluster())).ConfigureAwait(false);
    }

    private static WebApplication BuildHost(RaftManager manager, ILogger<IRaft> logger, int port, X509Certificate2? serverCertificate)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();

        builder.Services.AddSingleton<IRaft>(manager);
        builder.Services.AddSingleton(logger);
        builder.Services.AddKommanderGrpc();

        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(IPAddress.Loopback, port, listen =>
            {
                listen.Protocols = HttpProtocols.Http2;

                if (serverCertificate is null)
                    return;

                // Same listener policy as Kommander.Server in MutualTls mode: the certificate is
                // required during the handshake (HTTP/2 forbids renegotiation) and Kestrel accepts
                // it so the thumbprint allow-list makes the trust decision.
                listen.UseHttps(serverCertificate, https =>
                {
                    https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
                    https.ClientCertificateValidation = (_, _, _) => true;
                });
            });
        });

        WebApplication app = builder.Build();
        app.MapGrpcRaftRoutes();
        return app;
    }

    private IWAL CreateWal(BenchmarkOptions options, int port, ILogger<IRaft> logger)
    {
        if (options.Storage == "memory")
            return new InMemoryWAL(logger);

        string parent = options.WalDir ?? Path.GetTempPath();
        string path = Path.Combine(parent, $"kommander-bench-{Environment.ProcessId}-{port}");
        Directory.CreateDirectory(path);
        walDirectories.Add(path);

        bool sync = options.SyncWrites ?? true;

        return options.Storage switch
        {
            "rocksdb" => new RocksDbWAL(path, "v1", logger, sync),
            _ => new SqliteWAL(path, "v1", logger, sync),
        };
    }

    private async Task WaitReadyAsync(int partitions, TimeSpan timeout)
    {
        using CancellationTokenSource cts = new(timeout);

        try
        {
            for (int partitionId = 1; partitionId <= partitions; partitionId++)
                await Managers[0].WaitForLeaderStableAsync(partitionId, TimeSpan.FromSeconds(1), cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            throw new InvalidOperationException($"The cluster did not elect a stable leader for every partition within {timeout.TotalSeconds:0}s.");
        }
    }

    /// <summary>
    /// Returns the manager that leads <paramref name="partitionId"/> now, or <c>null</c> when no
    /// node claims it. The workload re-resolves after a failed call, so a leader change during the
    /// run costs failed calls, not a stuck run.
    /// </summary>
    public async Task<RaftManager?> LeaderFor(int partitionId)
    {
        foreach (RaftManager manager in Managers)
        {
            if (await manager.AmILeaderQuick(partitionId).ConfigureAwait(false))
                return manager;
        }

        return null;
    }

    /// <summary>The number of partitions each node leads, keyed by endpoint (skew check).</summary>
    public async Task<Dictionary<string, int>> LeaderDistribution(int partitions)
    {
        Dictionary<string, int> result = Managers.ToDictionary(m => m.GetLocalEndpoint(), _ => 0);

        for (int partitionId = 1; partitionId <= partitions; partitionId++)
        {
            RaftManager? leader = await LeaderFor(partitionId).ConfigureAwait(false);
            if (leader is not null)
                result[leader.GetLocalEndpoint()]++;
        }

        return result;
    }

    /// <summary>
    /// A self-signed certificate for localhost and 127.0.0.1. It is exported to PKCS#12 and loaded
    /// back so the private key is persisted in a form SslStream can use on every platform (an
    /// ephemeral CertificateRequest key fails the server handshake on macOS and Windows).
    /// </summary>
    private static X509Certificate2 CreateCertificate()
    {
        using RSA key = RSA.Create(2048);

        CertificateRequest request = new("CN=kommander-bench", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

        SubjectAlternativeNameBuilder san = new();
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], critical: false));

        using X509Certificate2 created = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(7));

#pragma warning disable SYSLIB0057 // X509CertificateLoader is net9+; this project also targets net8.0.
        return new X509Certificate2(created.Export(X509ContentType.Pkcs12), (string?)null, X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057
    }

    public async ValueTask DisposeAsync()
    {
        foreach (RaftManager manager in Managers)
        {
            try { manager.Dispose(); } catch { /* best-effort */ }
        }

        foreach (WebApplication app in apps)
        {
            using CancellationTokenSource cts = new(TimeSpan.FromSeconds(2));
            try { await app.StopAsync(cts.Token).ConfigureAwait(false); } catch { /* best-effort */ }
            await app.DisposeAsync().ConfigureAwait(false);
        }

        foreach (string directory in walDirectories)
        {
            try { Directory.Delete(directory, recursive: true); } catch { /* best-effort */ }
        }

        certificate?.Dispose();
    }

    /// <summary>
    /// Applies <c>--set Name=Value</c> overrides to a node's configuration. Only simple settable
    /// properties are supported; an unknown name or an unparsable value fails the run rather than
    /// silently measuring the default.
    /// </summary>
    internal static void ApplyOverrides(RaftConfiguration configuration, IEnumerable<string> overrides)
    {
        foreach (string item in overrides)
        {
            int eq = item.IndexOf('=');
            if (eq <= 0)
                throw new ArgumentException($"--set expects Name=Value, got '{item}'");

            string name = item[..eq].Trim();
            string value = item[(eq + 1)..].Trim();

            global::System.Reflection.PropertyInfo property = typeof(RaftConfiguration).GetProperty(name)
                ?? throw new ArgumentException($"--set: RaftConfiguration has no property '{name}'");

            object parsed = property.PropertyType switch
            {
                Type t when t == typeof(bool) => bool.Parse(value),
                Type t when t == typeof(int) => int.Parse(value, global::System.Globalization.CultureInfo.InvariantCulture),
                Type t when t == typeof(long) => long.Parse(value, global::System.Globalization.CultureInfo.InvariantCulture),
                Type t when t == typeof(double) => double.Parse(value, global::System.Globalization.CultureInfo.InvariantCulture),
                Type t when t == typeof(TimeSpan) => TimeSpan.Parse(value, global::System.Globalization.CultureInfo.InvariantCulture),
                _ => throw new ArgumentException($"--set: '{name}' is a {property.PropertyType.Name}, which --set does not support"),
            };

            property.SetValue(configuration, parsed);
        }
    }
}
