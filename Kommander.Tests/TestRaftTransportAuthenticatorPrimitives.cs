using System.Security.Cryptography;

namespace Kommander.Tests;

/// <summary>
/// Transport authentication primitives not covered by the request-level suites: constructor
/// contracts, the caller-digested <c>SignWithBodyHash</c>/<c>ValidateWithBodyHash</c> pair, the
/// field-level validation short-circuits (mode, TLS, missing fields, malformed encodings, clock
/// skew), the base64url acceptance rules observable through the header fields, and the replay
/// cache's pruning window.
/// </summary>
[Collection(AuthTestCollection.Name)]
public sealed class TestRaftTransportAuthenticatorPrimitives
{
    private const string Sender = "primitives-node:7000";

    private static readonly DateTimeOffset Now = new(2026, 3, 9, 16, 0, 0, TimeSpan.Zero);

    // Distinct well-formed 16-byte base64url nonces, one per validating test case, so the
    // process-wide replay cache never rejects a fresh request as a replay of another case.
    private const string NonceA = "AAECAwQFBgcICQoLDA0ODw";
    private const string NonceB = "CAECAwQFBgcICQoLDA0ODw";
    private const string NonceC = "DAECAwQFBgcICQoLDA0ODw";
    private const string NonceD = "EAECAwQFBgcICQoLDA0ODw";
    private const string NonceE = "FAECAwQFBgcICQoLDA0ODw";

    // ── Constructor ───────────────────────────────────────────────────────────

    [Fact]
    public void Ctor_NullOptions_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new RaftTransportAuthenticator(null!));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Ctor_SharedSecretModeWithoutSecret_Throws(string? secret)
    {
        RaftTransportSecurityOptions options = new()
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
            SharedSecret = secret
        };

        Assert.Throws<ArgumentException>(() => new RaftTransportAuthenticator(options));
    }

    [Fact]
    public void Ctor_NonSharedSecretMode_DoesNotRequireASecret()
    {
        RaftTransportSecurityOptions options = new() { NodeAuthenticationMode = RaftNodeAuthenticationMode.Disabled };

        RaftTransportAuthenticator authenticator = new(options);

        Assert.Equal(RaftNodeAuthenticationMode.Disabled, authenticator.Options.NodeAuthenticationMode);
    }

    // ── Signing with a caller-supplied body digest ────────────────────────────

    [Fact]
    public void SignWithBodyHash_ProducesTheSameSignatureAsSigningTheBody()
    {
        // The digest-based overload exists so the gRPC path can hash a pooled buffer without
        // materializing it; both overloads must bind the exact same bytes into the signature.
        RaftTransportAuthenticator authenticator = CreateAuthenticator();
        byte[] body = "payload-for-digest"u8.ToArray();

        RaftTransportAuthenticationHeaders fromBody = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, body,
            timestampUnixMilliseconds: 1710000000000, nonce: NonceA);

        RaftTransportAuthenticationHeaders fromDigest = authenticator.SignWithBodyHash(
            "POST", "/v1/raft/append-logs", Sender, SHA256.HashData(body),
            timestampUnixMilliseconds: 1710000000000, nonce: NonceA);

        Assert.Equal(fromBody.Signature, fromDigest.Signature);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    [InlineData(33)]
    public void SignWithBodyHash_WrongDigestLength_Throws(int hashLength)
    {
        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        ArgumentException ex = Assert.Throws<ArgumentException>(() => authenticator.SignWithBodyHash(
            "POST", "/v1/raft/append-logs", Sender, new byte[hashLength]));

        Assert.Contains("32", ex.Message);
    }

    [Fact]
    public void Sign_DisabledMode_ReturnsEmptyHeadersWithoutDigestingAnything()
    {
        // Disabled mode signs nothing, so even a wrong-length digest must be accepted and
        // ignored rather than throw — the transport skips authentication entirely.
        RaftTransportAuthenticator authenticator = new(new RaftTransportSecurityOptions
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.Disabled
        });

        RaftTransportAuthenticationHeaders fromDigest = authenticator.SignWithBodyHash(
            "POST", "/v1/raft/append-logs", Sender, new byte[7]);

        Assert.Empty(fromDigest.Signature);
        Assert.Empty(fromDigest.SenderNode);
        Assert.Empty(fromDigest.Nonce);
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public void Sign_BlankRouteInputs_Throw(string method)
    {
        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        ArgumentException ex = Assert.Throws<ArgumentException>(() => authenticator.Sign(
            method, "/v1/raft/append-logs", Sender, "payload"u8.ToArray()));

        Assert.Equal("method", ex.ParamName);
    }

    [Fact]
    public void Sign_GeneratedNonce_IsWellFormedAndValidates()
    {
        // A nonce minted by the signer itself must decode to exactly 16 bytes on the peer —
        // the round trip is the observable proof that the generated encoding is valid base64url.
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        FakeTimeProvider timeProvider = new(Now);
        RaftTransportAuthenticator authenticator = CreateAuthenticator(timeProvider);

        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray());

        Assert.False(string.IsNullOrWhiteSpace(headers.Nonce));
        Assert.Equal(timeProvider.GetUtcNow().ToUnixTimeMilliseconds(), headers.TimestampUnixMilliseconds);

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", "payload"u8.ToArray(),
            headers.Signature, headers.SenderNode,
            headers.TimestampUnixMilliseconds.ToString(), headers.Nonce,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, result.Status);
    }

    [Fact]
    public void Sign_CustomHeaderName_IsCarriedOnTheHeaders()
    {
        RaftTransportAuthenticator authenticator = CreateAuthenticator(
            options: o => o.HeaderName = "X-Custom-Auth");

        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray());

        Assert.Equal("X-Custom-Auth", headers.SignatureHeaderName);
    }

    [Fact]
    public void Sign_BlankHeaderName_FallsBackToTheDefaultHeaderName()
    {
        RaftTransportAuthenticator authenticator = CreateAuthenticator(
            options: o => o.HeaderName = " ");

        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray());

        Assert.Equal(RaftTransportAuthenticationHeaders.DefaultSignatureHeaderName, headers.SignatureHeaderName);
    }

    // ── Validating against a caller-supplied body digest ──────────────────────

    [Fact]
    public void ValidateWithBodyHash_RoundTripsASignedDigest()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        RaftTransportAuthenticator authenticator = CreateAuthenticator(new FakeTimeProvider(Now));
        byte[] digest = SHA256.HashData("digest-body"u8.ToArray());

        RaftTransportAuthenticationHeaders headers = authenticator.SignWithBodyHash(
            "POST", "/v1/raft/append-logs", Sender, digest, nonce: NonceB);

        RaftTransportAuthenticationResult result = authenticator.ValidateWithBodyHash(
            "POST", "/v1/raft/append-logs", digest,
            headers.Signature, headers.SenderNode,
            headers.TimestampUnixMilliseconds.ToString(), headers.Nonce,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, result.Status);
        Assert.True(result.IsAuthenticated);
    }

    [Fact]
    public void ValidateWithBodyHash_DigestOfDifferentBody_RejectedAsInvalidSignature()
    {
        RaftTransportAuthenticator authenticator = CreateAuthenticator(new FakeTimeProvider(Now));
        byte[] signedDigest = SHA256.HashData("digest-body"u8.ToArray());
        byte[] presentedDigest = SHA256.HashData("other-body"u8.ToArray());

        RaftTransportAuthenticationHeaders headers = authenticator.SignWithBodyHash(
            "POST", "/v1/raft/append-logs", Sender, signedDigest, nonce: NonceC);

        RaftTransportAuthenticationResult result = authenticator.ValidateWithBodyHash(
            "POST", "/v1/raft/append-logs", presentedDigest,
            headers.Signature, headers.SenderNode,
            headers.TimestampUnixMilliseconds.ToString(), headers.Nonce,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.InvalidSignature, result.Status);
    }

    // ── Mode and TLS short-circuits ───────────────────────────────────────────

    [Fact]
    public void Validate_DisabledMode_ReportsDisabledWithoutInspectingFields()
    {
        RaftTransportAuthenticator authenticator = new(new RaftTransportSecurityOptions
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.Disabled,
            RequireTls = true
        });

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", bodyBytes: null,
            signature: null, senderNode: null,
            timestampUnixMilliseconds: null, nonce: null,
            isSecureTransport: false);

        Assert.Equal(RaftTransportAuthenticationStatus.Disabled, result.Status);
        Assert.True(result.IsAuthenticated);
    }

    [Fact]
    public void Validate_RequireTlsOverCleartext_IsRejectedBeforeFieldChecks()
    {
        // TlsRequired must win even with every field missing: an unauthenticated cleartext
        // caller learns nothing about which fields the validator expects.
        RaftTransportAuthenticator authenticator = CreateAuthenticator(new FakeTimeProvider(Now));

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", bodyBytes: null,
            signature: null, senderNode: Sender,
            timestampUnixMilliseconds: null, nonce: null,
            isSecureTransport: false);

        Assert.Equal(RaftTransportAuthenticationStatus.TlsRequired, result.Status);
    }

    // ── Missing and malformed fields ──────────────────────────────────────────

    [Fact]
    public void Validate_MissingField_IsRejectedAsMissingFields()
    {
        RaftTransportAuthenticator authenticator = CreateAuthenticator(new FakeTimeProvider(Now));
        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray(), nonce: NonceD);

        long timestamp = headers.TimestampUnixMilliseconds;

        Assert.Equal(RaftTransportAuthenticationStatus.MissingFields, authenticator.Validate(
            "POST", "/v1/raft/append-logs", [],
            null, Sender, timestamp.ToString(), headers.Nonce, true).Status);

        Assert.Equal(RaftTransportAuthenticationStatus.MissingFields, authenticator.Validate(
            "POST", "/v1/raft/append-logs", [],
            headers.Signature, "  ", timestamp.ToString(), headers.Nonce, true).Status);

        Assert.Equal(RaftTransportAuthenticationStatus.MissingFields, authenticator.Validate(
            "POST", "/v1/raft/append-logs", [],
            headers.Signature, Sender, "", headers.Nonce, true).Status);

        Assert.Equal(RaftTransportAuthenticationStatus.MissingFields, authenticator.Validate(
            "POST", "/v1/raft/append-logs", [],
            headers.Signature, Sender, timestamp.ToString(), null, true).Status);
    }

    [Fact]
    public void Validate_UndecodableSignatureEncoding_IsRejectedAsMalformedFields()
    {
        // One leftover char cannot be produced by any base64 encoding, so the decoder refuses it
        // before any comparison and the failure is classified as malformed.
        RaftTransportAuthenticator authenticator = CreateAuthenticator(new FakeTimeProvider(Now));

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", [],
            "A", Sender,
            Now.ToUnixTimeMilliseconds().ToString(), NonceE,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.MalformedFields, result.Status);
    }

    [Theory]
    [InlineData("AB+C")]                 // standard-alphabet '+' — decodes under the lenient decoder
    [InlineData("AB/C")]                 // standard-alphabet '/' — decodes under the lenient decoder
    [InlineData("AAECAwQFBgcICQoLDA0ODw")] // decodes, but the wrong number of bytes for a signature
    public void Validate_DecodableButWrongSignature_IsRejectedAsInvalidSignature(string signature)
    {
        // The decoder deliberately accepts the standard base64 alphabet as well as base64url, so
        // these fields decode and reach the length-sensitive constant-time comparison, which is
        // the documented classification point for anything that is not the exact HMAC.
        RaftTransportAuthenticator authenticator = CreateAuthenticator(new FakeTimeProvider(Now));

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", [],
            signature, Sender,
            Now.ToUnixTimeMilliseconds().ToString(), NonceE,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.InvalidSignature, result.Status);
    }

    [Fact]
    public void Validate_AbsurdlyLongSignature_IsRejectedBeforeAnyBufferIsSized()
    {
        // An unauthenticated header must never size a buffer: a field longer than any
        // signature could occupy is refused up front as malformed.
        RaftTransportAuthenticator authenticator = CreateAuthenticator(new FakeTimeProvider(Now));

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", [],
            new string('A', 200), Sender,
            Now.ToUnixTimeMilliseconds().ToString(), NonceE,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.MalformedFields, result.Status);
    }

    [Fact]
    public void Validate_NonceDecodingToWrongByteCount_IsRejectedAsMalformedFields()
    {
        // A nonce is a fixed 16-byte value; "AAAA" is valid base64url but only 3 bytes, which
        // is a malformed field rather than a signature mismatch.
        RaftTransportAuthenticator authenticator = CreateAuthenticator(new FakeTimeProvider(Now));
        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray(), nonce: NonceA);

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", "payload"u8.ToArray(),
            headers.Signature, Sender,
            headers.TimestampUnixMilliseconds.ToString(), "AAAA",
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.MalformedFields, result.Status);
    }

    [Fact]
    public void Validate_WellFormedWrongLengthSignature_IsRejectedAsInvalidSignature()
    {
        // 43 chars is a decodable field (33 bytes), so it reaches the constant-time compare
        // and fails there on length — classified as an invalid signature, not malformed.
        RaftTransportAuthenticator authenticator = CreateAuthenticator(new FakeTimeProvider(Now));
        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray(), nonce: NonceB);

        string wrongLengthSignature = new string('A', 43);

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", "payload"u8.ToArray(),
            wrongLengthSignature, Sender,
            headers.TimestampUnixMilliseconds.ToString(), headers.Nonce,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.InvalidSignature, result.Status);
    }

    [Fact]
    public void Validate_PaddedSignature_EncodingIsAccepted()
    {
        // The URL alphabet with or without trailing padding is the documented accepted set;
        // re-adding the stripped '=' must not change the decoded signature.
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        FakeTimeProvider timeProvider = new(Now);
        RaftTransportAuthenticator authenticator = CreateAuthenticator(timeProvider);
        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray(), nonce: NonceC);

        string padded = headers.Signature + "=";

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", "payload"u8.ToArray(),
            padded, Sender,
            headers.TimestampUnixMilliseconds.ToString(), headers.Nonce,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, result.Status);
    }

    [Fact]
    public void Validate_SignatureMintedWithDifferentSecret_IsRejectedAsInvalidSignature()
    {
        // A peer that shares no secret must fail the signature check, not the field checks:
        // this is the verify-failure direction of the HMAC.
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        FakeTimeProvider timeProvider = new(Now);
        RaftTransportAuthenticator signer = CreateAuthenticator(timeProvider, secret: "attacker-secret");
        RaftTransportAuthenticator validator = CreateAuthenticator(timeProvider);

        RaftTransportAuthenticationHeaders headers = signer.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray(), nonce: NonceD);

        RaftTransportAuthenticationResult result = validator.Validate(
            "POST", "/v1/raft/append-logs", "payload"u8.ToArray(),
            headers.Signature, Sender,
            headers.TimestampUnixMilliseconds.ToString(), headers.Nonce,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.InvalidSignature, result.Status);
        Assert.False(result.IsAuthenticated);
    }

    // ── Clock-skew window ─────────────────────────────────────────────────────

    [Fact]
    public void Validate_TimestampWithinSkewInEitherDirection_Accepts()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        FakeTimeProvider timeProvider = new(Now);
        RaftTransportAuthenticator authenticator = CreateAuthenticator(timeProvider, skew: TimeSpan.FromSeconds(60));
        long now = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

        RaftTransportAuthenticationHeaders pastHeaders = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "past"u8.ToArray(),
            timestampUnixMilliseconds: now - 59_000, nonce: NonceE);

        RaftTransportAuthenticationHeaders futureHeaders = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "future"u8.ToArray(),
            timestampUnixMilliseconds: now + 59_000, nonce: NonceA);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, authenticator.Validate(
            "POST", "/v1/raft/append-logs", "past"u8.ToArray(),
            pastHeaders.Signature, Sender, (now - 59_000).ToString(), pastHeaders.Nonce, true).Status);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, authenticator.Validate(
            "POST", "/v1/raft/append-logs", "future"u8.ToArray(),
            futureHeaders.Signature, Sender, (now + 59_000).ToString(), futureHeaders.Nonce, true).Status);
    }

    [Fact]
    public void Validate_TimestampOutsideSkew_IsRejectedAsSkewExceeded()
    {
        FakeTimeProvider timeProvider = new(Now);
        RaftTransportAuthenticator authenticator = CreateAuthenticator(timeProvider, skew: TimeSpan.FromSeconds(60));
        long now = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

        RaftTransportAuthenticationHeaders pastHeaders = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray(),
            timestampUnixMilliseconds: now - 61_000, nonce: NonceB);

        RaftTransportAuthenticationHeaders futureHeaders = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray(),
            timestampUnixMilliseconds: now + 61_000, nonce: NonceC);

        Assert.Equal(RaftTransportAuthenticationStatus.TimestampSkewExceeded, authenticator.Validate(
            "POST", "/v1/raft/append-logs", "payload"u8.ToArray(),
            pastHeaders.Signature, Sender, (now - 61_000).ToString(), pastHeaders.Nonce, true).Status);

        Assert.Equal(RaftTransportAuthenticationStatus.TimestampSkewExceeded, authenticator.Validate(
            "POST", "/v1/raft/append-logs", "payload"u8.ToArray(),
            futureHeaders.Signature, Sender, (now + 61_000).ToString(), futureHeaders.Nonce, true).Status);
    }

    // ── Replay cache pruning ──────────────────────────────────────────────────

    [Fact]
    public void Validate_NonceReusedAfterPruneIntervalSweepsTheReplayCache_IsAcceptedAgain()
    {
        // A nonce's cache entry lives for now + AllowedClockSkew. Once the retention window
        // AND the 30 s prune-interval gate have both elapsed, the next successful validation
        // sweeps the expired entry out and the same nonce may carry a fresh request.
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        FakeTimeProvider timeProvider = new(Now);
        RaftTransportAuthenticator authenticator = CreateAuthenticator(timeProvider, skew: TimeSpan.FromSeconds(60));
        long t0 = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

        RaftTransportAuthenticationHeaders original = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray(),
            timestampUnixMilliseconds: t0, nonce: NonceD);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, authenticator.Validate(
            "POST", "/v1/raft/append-logs", "payload"u8.ToArray(),
            original.Signature, Sender, t0.ToString(), original.Nonce, true).Status);

        timeProvider.Advance(TimeSpan.FromSeconds(91)); // 60 s retention + 30 s prune interval + 1 s

        long t1 = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        RaftTransportAuthenticationHeaders renewed = authenticator.Sign(
            "POST", "/v1/raft/append-logs", Sender, "payload"u8.ToArray(),
            timestampUnixMilliseconds: t1, nonce: NonceD);

        RaftTransportAuthenticationResult result = authenticator.Validate(
            "POST", "/v1/raft/append-logs", "payload"u8.ToArray(),
            renewed.Signature, Sender, t1.ToString(), renewed.Nonce, true);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, result.Status);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static RaftTransportAuthenticator CreateAuthenticator(
        TimeProvider? timeProvider = null,
        string secret = "top-secret-cluster-key",
        TimeSpan? skew = null,
        Action<RaftTransportSecurityOptions>? options = null)
    {
        RaftTransportSecurityOptions securityOptions = new()
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
            SharedSecret = secret,
            AllowedClockSkew = skew ?? TimeSpan.FromSeconds(60)
        };

        options?.Invoke(securityOptions);

        return new RaftTransportAuthenticator(securityOptions, timeProvider);
    }

    private sealed class FakeTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset current = utcNow;

        public override DateTimeOffset GetUtcNow() => current;

        public void Advance(TimeSpan delta) => current = current.Add(delta);
    }
}
