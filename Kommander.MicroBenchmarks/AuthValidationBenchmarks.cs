using System.Globalization;
using BenchmarkDotNet.Attributes;

namespace Kommander.MicroBenchmarks;

/// <summary>
/// Measures real receiver-side shared-secret validation — the path a peer's request actually takes —
/// rather than the metadata reuse that <c>AuthMetadataBenchmarks</c> isolates on the sending side.
/// </summary>
/// <remarks>
/// <para>
/// The change under measurement is field decoding. Validation used to decode the signature and the
/// nonce through a helper that allocated a padded array and then a second exact-length array per field,
/// and to take the expected HMAC back as a third array. All three are now written into stack buffers.
/// </para>
/// <para>
/// <b>Distinct nonces, bounded cache.</b> Every invocation signs with a fresh nonce, so validation runs
/// to completion instead of short-circuiting on replay detection — a replayed request would time the
/// replay cache, not the decode-and-verify path. Fresh nonces make that cache grow, and an unbounded one
/// would make each iteration slower than the last, so the invocation count is pinned and the cache is
/// cleared between iterations. A run therefore measures a cache of at most one iteration's worth of
/// entries, which is the closest stable stand-in for a production cache under its prune interval.
/// </para>
/// <para>
/// Both benchmarks sign one request and then validate it, so each figure covers the pair. They differ
/// only in the outcome, which is the comparison worth having: the decode and HMAC work is identical.
/// </para>
/// <para>
/// <see cref="ValidateRejected"/> is the failure counterpart: same decode work, mismatched signature. It
/// exists so a change that speeds up the accept path by shifting cost onto the reject path — the one an
/// unauthenticated caller can drive — is visible rather than silent.
/// </para>
/// </remarks>
[Config(typeof(InProcessConfig))]
[InvocationCount(4096)]
public class AuthValidationBenchmarks
{
    private const string LocalEndpoint = "node-a:5000";
    private const string Method = "POST";
    private const string Path = "/v1/raft/append-logs";

    /// <summary>Body size in bytes. 0 keeps the digest cost out of the way of the field decoding.</summary>
    [Params(0, 1024, 65536)]
    public int BodySizeInBytes;

    private RaftTransportAuthenticator _authenticator = null!;
    private byte[] _body = null!;
    private byte[] _nonceBytes = null!;
    private long _nonceCounter;

    /// <summary>
    /// Clears the process-wide replay cache so its size cannot drift across iterations. The cache is
    /// static, so without this the run would measure a progressively larger dictionary.
    /// </summary>
    [IterationSetup]
    public void ResetReplayCache() => RaftTransportAuthenticator.ResetReplayCacheForTesting();

    [GlobalSetup]
    public void Setup()
    {
        _authenticator = new RaftTransportAuthenticator(new RaftTransportSecurityOptions
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
            SharedSecret = "top-secret-cluster-key",
            AllowedClockSkew = TimeSpan.FromSeconds(60),
            RequireTls = false,
        });

        _body = new byte[BodySizeInBytes];
        for (int i = 0; i < _body.Length; i++)
            _body[i] = (byte)(i & 0xFF);

        _nonceBytes = new byte[16];
    }

    /// <summary>Signs and validates one request, the way a peer and its receiver do together.</summary>
    [Benchmark(Baseline = true, Description = "sign then validate, accepted")]
    public RaftTransportAuthenticationStatus SignAndValidate()
    {
        RaftTransportAuthenticationHeaders headers = Sign();

        return _authenticator.Validate(
            Method,
            Path,
            _body,
            headers.Signature,
            headers.SenderNode,
            headers.TimestampUnixMilliseconds.ToString(CultureInfo.InvariantCulture),
            headers.Nonce,
            isSecureTransport: true).Status;
    }

    /// <summary>Validation only, with a signature that will not match.</summary>
    [Benchmark(Description = "validate, rejected signature")]
    public RaftTransportAuthenticationStatus ValidateRejected()
    {
        RaftTransportAuthenticationHeaders headers = Sign();

        // Same length and alphabet, different content: the field decodes, and the comparison rejects it.
        string tampered = headers.Signature[..^1] + (headers.Signature[^1] == 'A' ? 'Q' : 'A');

        return _authenticator.Validate(
            Method,
            Path,
            _body,
            tampered,
            headers.SenderNode,
            headers.TimestampUnixMilliseconds.ToString(CultureInfo.InvariantCulture),
            headers.Nonce,
            isSecureTransport: true).Status;
    }

    /// <summary>
    /// Signs one request with a nonce nothing has used before, in the 16-byte / 22-character base64url
    /// shape a real nonce has. Built by counter rather than at random so a run is reproducible, and into
    /// a reused array so the scaffolding adds as little as possible to the figure being read.
    /// </summary>
    private RaftTransportAuthenticationHeaders Sign()
    {
        BitConverter.TryWriteBytes(_nonceBytes, ++_nonceCounter);

        string nonce = Convert.ToBase64String(_nonceBytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

        return _authenticator.Sign(Method, Path, LocalEndpoint, _body, nonce: nonce);
    }
}
