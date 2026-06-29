using System.Globalization;
using BenchmarkDotNet.Attributes;
using Grpc.Core;
using Kommander;
using Kommander.Communication.Grpc;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Models the three states of the shared-secret duplex send path to show that signing cost
/// is now paid once at stream open, not on every send.
/// <list type="bullet">
///   <item><description>
///     <see cref="OldPath_SignPerSend"/> — old hot path: every send constructed a new
///     <see cref="RaftTransportAuthenticator"/>, signed fresh headers (HMAC-SHA256 + nonce +
///     base64url), and allocated a <see cref="Metadata"/>. This is the cost removed from
///     warm-stream sends.
///   </description></item>
///   <item><description>
///     <see cref="NewPath_WarmSend"/> — new hot path on a warm pool: the metadata factory
///     was already captured into the stream slot at open time, so a send does <b>no</b> crypto and
///     no allocation. Represented here as returning the already-signed metadata.
///   </description></item>
///   <item><description>
///     <see cref="NewPath_StreamOpenSign"/> — the only place the new code signs: once per stream
///     slot creation and per <c>EnsureHealthy</c> re-open, via the real
///     <see cref="GrpcCommunication.CreateStreamingAuthFactory"/> delegate. This cost now amortizes
///     across every message on the stream instead of being paid per message.
///   </description></item>
/// </list>
/// Expected signal: <see cref="OldPath_SignPerSend"/> and <see cref="NewPath_StreamOpenSign"/> cost
/// about the same per call (both sign once), but the new code invokes the latter only at stream
/// open, while <see cref="NewPath_WarmSend"/> — what actually runs per message — is near-free.
/// </summary>
[Config(typeof(InProcessConfig))]
public class AuthMetadataBenchmarks
{
    private const string LocalEndpoint = "node-a:5000";

    private RaftTransportSecurityOptions _options = null!;
    private Func<Metadata?>? _factory;
    private Metadata _warmMetadata = null!;

    [GlobalSetup]
    public void Setup()
    {
        _options = new RaftTransportSecurityOptions
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
            SharedSecret = "top-secret-cluster-key",
            AllowedClockSkew = TimeSpan.FromSeconds(60)
        };

        // Factory built once per manager and captured into the pool.
        _factory = GrpcCommunication.CreateStreamingAuthFactory(_options, LocalEndpoint);

        // Metadata signed once at stream-open time; a warm send reuses the established stream and
        // re-sends nothing, so this stands in for the per-message cost on the new path.
        _warmMetadata = _factory!()!;
    }

    [Benchmark(Baseline = true, Description = "old: sign per send")]
    public Metadata OldPath_SignPerSend() => BuildAuthMetadataOldPath();

    [Benchmark(Description = "new: warm send (no crypto)")]
    public Metadata NewPath_WarmSend() => _warmMetadata;

    [Benchmark(Description = "new: sign at stream open")]
    public Metadata? NewPath_StreamOpenSign() => _factory!();

    /// <summary>
    /// Reproduction of the old <c>GrpcCommunication.BuildAuthMetadata</c> hot path
    /// (which is <see langword="private"/>): a new authenticator per call, fresh signature, and a
    /// freshly allocated <see cref="Metadata"/> — exactly what every streaming send used to do.
    /// </summary>
    private Metadata BuildAuthMetadataOldPath()
    {
        RaftTransportAuthenticator authenticator = new(_options);
        RaftTransportAuthenticationHeaders signed =
            authenticator.Sign("POST", "/Rafter/BatchRequests", LocalEndpoint);

        return new Metadata
        {
            { signed.SignatureHeaderName, signed.Signature },
            { RaftTransportAuthenticationHeaders.SenderNodeHeaderName, signed.SenderNode },
            {
                RaftTransportAuthenticationHeaders.TimestampHeaderName,
                signed.TimestampUnixMilliseconds.ToString(CultureInfo.InvariantCulture)
            },
            { RaftTransportAuthenticationHeaders.NonceHeaderName, signed.Nonce }
        };
    }
}
