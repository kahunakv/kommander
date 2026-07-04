namespace Kommander.Tests;

[Collection(AuthTestCollection.Name)]
public sealed class TestRaftTransportAuthenticator
{
    [Fact]
    public void Sign_IsDeterministic_ForFixedInput()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        RaftTransportAuthenticationHeaders first = authenticator.Sign(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            senderNode: "node-a:5000",
            bodyBytes: "payload"u8.ToArray(),
            timestampUnixMilliseconds: 1710000000000,
            nonce: "AAECAwQFBgcICQoLDA0ODw");

        RaftTransportAuthenticationHeaders second = authenticator.Sign(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            senderNode: "node-a:5000",
            bodyBytes: "payload"u8.ToArray(),
            timestampUnixMilliseconds: 1710000000000,
            nonce: "AAECAwQFBgcICQoLDA0ODw");

        Assert.Equal(first.Signature, second.Signature);
        Assert.Equal(first.Nonce, second.Nonce);
        Assert.Equal(first.TimestampUnixMilliseconds, second.TimestampUnixMilliseconds);
    }

    [Fact]
    public void Validate_AcceptsMatchingSignedRequest()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        TestTimeProvider timeProvider = new(new DateTimeOffset(2024, 3, 9, 16, 0, 0, TimeSpan.Zero));
        RaftTransportAuthenticator authenticator = CreateAuthenticator(timeProvider);

        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            senderNode: "node-a:5000",
            bodyBytes: "payload"u8.ToArray(),
            timestampUnixMilliseconds: timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
            nonce: "AAECAwQFBgcICQoLDA0ODw");

        RaftTransportAuthenticationResult result = authenticator.Validate(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            bodyBytes: "payload"u8.ToArray(),
            signature: headers.Signature,
            senderNode: headers.SenderNode,
            timestampUnixMilliseconds: headers.TimestampUnixMilliseconds.ToString(),
            nonce: headers.Nonce,
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, result.Status);
        Assert.True(result.IsAuthenticated);
    }

    [Fact]
    public void Validate_RejectsTamperingAcrossSignedFields()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        TestTimeProvider timeProvider = new(new DateTimeOffset(2024, 3, 9, 16, 0, 0, TimeSpan.Zero));
        long timestamp = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        RaftTransportAuthenticationHeaders headers = CreateAuthenticator(timeProvider).Sign(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            senderNode: "node-a:5000",
            bodyBytes: "payload"u8.ToArray(),
            timestampUnixMilliseconds: timestamp,
            nonce: "AAECAwQFBgcICQoLDA0ODw");

        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature,
            CreateAuthenticator(timeProvider).Validate(
                "POST",
                "/v1/raft/append-logs",
                "tampered"u8.ToArray(),
                headers.Signature,
                headers.SenderNode,
                timestamp.ToString(),
                headers.Nonce,
                true).Status);

        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature,
            CreateAuthenticator(timeProvider).Validate(
                "GET",
                "/v1/raft/append-logs",
                "payload"u8.ToArray(),
                headers.Signature,
                headers.SenderNode,
                timestamp.ToString(),
                headers.Nonce,
                true).Status);

        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature,
            CreateAuthenticator(timeProvider).Validate(
                "POST",
                "/v1/raft/request-vote",
                "payload"u8.ToArray(),
                headers.Signature,
                headers.SenderNode,
                timestamp.ToString(),
                headers.Nonce,
                true).Status);

        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature,
            CreateAuthenticator(timeProvider).Validate(
                "POST",
                "/v1/raft/append-logs",
                "payload"u8.ToArray(),
                headers.Signature,
                "node-b:5000",
                timestamp.ToString(),
                headers.Nonce,
                true).Status);

        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature,
            CreateAuthenticator(timeProvider).Validate(
                "POST",
                "/v1/raft/append-logs",
                "payload"u8.ToArray(),
                headers.Signature,
                headers.SenderNode,
                (timestamp + 1).ToString(),
                headers.Nonce,
                true).Status);

        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature,
            CreateAuthenticator(timeProvider).Validate(
                "POST",
                "/v1/raft/append-logs",
                "payload"u8.ToArray(),
                headers.Signature,
                headers.SenderNode,
                timestamp.ToString(),
                "AQECAwQFBgcICQoLDA0ODw",
                true).Status);

        Assert.Equal(
            RaftTransportAuthenticationStatus.InvalidSignature,
            CreateAuthenticator(timeProvider).Validate(
                "POST",
                "/v1/raft/append-logs",
                "payload"u8.ToArray(),
                headers.Signature[..^1] + "A",
                headers.SenderNode,
                timestamp.ToString(),
                headers.Nonce,
                true).Status);
    }

    [Fact]
    public void FixedTimeEquals_RejectsLengthMismatchAndWrongBytes()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        Assert.False(RaftTransportAuthenticator.FixedTimeEquals([1, 2, 3], [1, 2]));
        Assert.False(RaftTransportAuthenticator.FixedTimeEquals([1, 2, 3], [1, 2, 4]));
        Assert.True(RaftTransportAuthenticator.FixedTimeEquals([1, 2, 3], [1, 2, 3]));
    }

    [Fact]
    public void Validate_RejectsReplayAndExpiresNonceAfterSkewWindow()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        TestTimeProvider timeProvider = new(new DateTimeOffset(2024, 3, 9, 16, 0, 0, TimeSpan.Zero));
        RaftTransportAuthenticator authenticator = CreateAuthenticator(timeProvider, TimeSpan.FromSeconds(60));

        RaftTransportAuthenticationHeaders headers = authenticator.Sign(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            senderNode: "node-a:5000",
            bodyBytes: "payload"u8.ToArray(),
            timestampUnixMilliseconds: timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
            nonce: "AAECAwQFBgcICQoLDA0ODw");

        RaftTransportAuthenticationResult first = authenticator.Validate(
            "POST",
            "/v1/raft/append-logs",
            "payload"u8.ToArray(),
            headers.Signature,
            headers.SenderNode,
            headers.TimestampUnixMilliseconds.ToString(),
            headers.Nonce,
            true);

        RaftTransportAuthenticationResult replay = authenticator.Validate(
            "POST",
            "/v1/raft/append-logs",
            "payload"u8.ToArray(),
            headers.Signature,
            headers.SenderNode,
            headers.TimestampUnixMilliseconds.ToString(),
            headers.Nonce,
            true);

        timeProvider.Advance(TimeSpan.FromSeconds(61));

        RaftTransportAuthenticationHeaders renewedHeaders = authenticator.Sign(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            senderNode: "node-a:5000",
            bodyBytes: "payload"u8.ToArray(),
            timestampUnixMilliseconds: timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
            nonce: headers.Nonce);

        RaftTransportAuthenticationResult afterExpiry = authenticator.Validate(
            "POST",
            "/v1/raft/append-logs",
            "payload"u8.ToArray(),
            renewedHeaders.Signature,
            renewedHeaders.SenderNode,
            renewedHeaders.TimestampUnixMilliseconds.ToString(),
            renewedHeaders.Nonce,
            true);

        Assert.Equal(RaftTransportAuthenticationStatus.Success, first.Status);
        Assert.Equal(RaftTransportAuthenticationStatus.ReplayDetected, replay.Status);
        Assert.Equal(RaftTransportAuthenticationStatus.Success, afterExpiry.Status);
    }

    [Fact]
    public void Validate_RejectsMalformedFields()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        RaftTransportAuthenticator authenticator = CreateAuthenticator();

        RaftTransportAuthenticationResult result = authenticator.Validate(
            method: "POST",
            pathOrGrpcMethod: "/v1/raft/append-logs",
            bodyBytes: [],
            signature: "not-base64url***",
            senderNode: "node-a:5000",
            timestampUnixMilliseconds: "abc",
            nonce: "bad",
            isSecureTransport: true);

        Assert.Equal(RaftTransportAuthenticationStatus.MalformedFields, result.Status);
    }

    private static RaftTransportAuthenticator CreateAuthenticator(
        TimeProvider? timeProvider = null,
        TimeSpan? allowedClockSkew = null)
    {
        return new RaftTransportAuthenticator(
            new RaftTransportSecurityOptions
            {
                NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
                SharedSecret = "top-secret-cluster-key",
                AllowedClockSkew = allowedClockSkew ?? TimeSpan.FromSeconds(60)
            },
            timeProvider);
    }

    private sealed class TestTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset current = utcNow;

        public override DateTimeOffset GetUtcNow() => current;

        public void Advance(TimeSpan delta) => current = current.Add(delta);
    }
}
