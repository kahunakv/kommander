
using CommandLine;

namespace Kommander.Server;

public sealed class KommanderCommandLineOptions
{
    [Option('h', "host", Required = false, HelpText = "Host to bind incoming connections to", Default = "*")]
    public string Host { get; set; } = "*";

    [Option('p', "http-ports", Required = false, HelpText = "Ports to bind incoming HTTP connections to")]
    public IEnumerable<string>? HttpPorts { get; set; }

    [Option("https-ports", Required = false, HelpText = "Ports to bind incoming HTTPs connections to")]
    public IEnumerable<string>? HttpsPorts { get; set; }

    [Option("https-certificate", Required = false, HelpText = "Path to the HTTPs certificate")]
    public string HttpsCertificate { get; set; } = "";

    [Option("https-certificate-password", Required = false, HelpText = "Password of the HTTPs certificate", Default = "")]
    public string HttpsCertificatePassword { get; set; } = "";

    [Option("wal-adapter", Required = false, HelpText = "WAL adapter", Default = "rocksdb")]
    public string WalAdapter { get; set; } = "";

    [Option("rocksdb-wal-path", Required = false, HelpText = "RocksDB WAL path")]
    public string RocksDbWalPath { get; set; } = "";

    [Option("rocksdb-wal-revision", Required = false, HelpText = "RocksDB WAL revision")]
    public string RocksDbWalRevision{ get; set; } = "";

    [Option("sqlite-wal-path", Required = false, HelpText = "Sqlite WAL path")]
    public string SqliteWalPath { get; set; } = "";

    [Option("sqlite-wal-revision", Required = false, HelpText = "Sqlite WAL revision")]
    public string SqliteWalRevision{ get; set; } = "";

    [Option("initial-cluster", Required = false, HelpText = "Initial cluster configuration for static discovery")]
    public IEnumerable<string>? InitialCluster { get; set; }

    [Option("initial-cluster-partitions", Required = false, HelpText = "Initial cluster number of partitions", Default = 16)]
    public int InitialClusterPartitions { get; set; }

    [Option("raft-nodename", Required = false, HelpText = "Raft unique node name")]
    public string RaftNodeName { get; set; } = "";

    [Option("raft-nodeid", Required = false, HelpText = "Raft unique node id")]
    public int RaftNodeId { get; set; } = 0;

    [Option("raft-host", Required = false, HelpText = "Host to listen for Raft consensus and replication requests", Default = "localhost")]
    public string RaftHost { get; set; } = "localhost";

    [Option("raft-port", Required = false, HelpText = "Port to bind incoming Raft consensus and replication requests", Default = 2070)]
    public int RaftPort { get; set; } = 2070;

    [Option("node-auth-mode", Required = false, HelpText = "Node authentication mode: Disabled, SharedSecret, MutualTls", Default = "Disabled")]
    public string NodeAuthMode { get; set; } = "Disabled";

    [Option("node-shared-secret", Required = false, HelpText = "Shared secret for node authentication (SharedSecret mode)")]
    public string NodeSharedSecret { get; set; } = "";

    [Option("node-auth-header", Required = false, HelpText = "HTTP/gRPC header name for the cluster auth token")]
    public string NodeAuthHeader { get; set; } = "";

    [Option("allow-insecure-certificate-validation", Required = false, HelpText = "Skip TLS certificate validation (development only)", Default = false)]
    public bool AllowInsecureCertificateValidation { get; set; }

    [Option("trusted-server-cert-thumbprint", Required = false, HelpText = "Trusted server certificate thumbprints (hex)")]
    public IEnumerable<string>? TrustedServerCertThumbprints { get; set; }

    [Option("trusted-client-cert-thumbprint", Required = false, HelpText = "Trusted client certificate thumbprints (hex)")]
    public IEnumerable<string>? TrustedClientCertThumbprints { get; set; }
}