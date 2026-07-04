using Grpc.Core;
using Kommander.Communication.Grpc;

namespace Kommander.Tests;

/// <summary>
/// Covers the streaming-auth security invariant: the duplex BatchRequests stream pool must sign fresh per-stream
/// metadata (new nonce/timestamp) for every slot creation and re-open, rather than freezing a
/// single signed <see cref="Metadata"/> and reusing its nonce across the pool. A shared frozen
/// nonce is accepted on the first stream and then rejected as a replay on every subsequent stream
/// (and every EnsureHealthy re-open). These tests assert that the factory the pool invokes per slot
/// produces distinct, individually valid credentials.
/// </summary>
[Collection(AuthTestCollection.Name)]
public sealed class TestGrpcStreamingAuthFactory
{
    private const string Secret = "top-secret-cluster-key";
    private const string LocalEndpoint = "node-a:5000";

    [Fact]
    public void CreateStreamingAuthFactory_DisabledAuth_ReturnsNull()
    {
        RaftTransportSecurityOptions options = new()
        {
            NodeAuthenticationMode = RaftNodeAuthenticationMode.Disabled
        };

        Func<Metadata?>? factory = GrpcCommunication.CreateStreamingAuthFactory(options, LocalEndpoint);

        Assert.Null(factory);
    }

    [Fact]
    public void CreateStreamingAuthFactory_SharedSecret_ProducesFreshDistinctNoncePerInvocation()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        Func<Metadata?>? factory = GrpcCommunication.CreateStreamingAuthFactory(CreateOptions(), LocalEndpoint);
        Assert.NotNull(factory);

        Metadata first = Assert.IsType<Metadata>(factory!());
        Metadata second = Assert.IsType<Metadata>(factory!());

        // Each slot/re-open signs its own nonce; the two streams must not collide.
        Assert.NotEqual(Nonce(first), Nonce(second));
    }

    [Fact]
    public void CreateStreamingAuthFactory_SharedSecret_EachSignedMetadataValidatesWithoutReplay()
    {
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        Func<Metadata?>? factory = GrpcCommunication.CreateStreamingAuthFactory(CreateOptions(), LocalEndpoint);
        Assert.NotNull(factory);

        Metadata first = Assert.IsType<Metadata>(factory!());
        Metadata second = Assert.IsType<Metadata>(factory!());

        RaftTransportAuthenticator verifier = new(CreateOptions());

        // Both per-slot credentials validate against the receiver, and the second is NOT flagged as
        // a replay precisely because it carries a fresh nonce — this is the behavior the fix restores.
        Assert.Equal(RaftTransportAuthenticationStatus.Success, Validate(verifier, first).Status);
        Assert.Equal(RaftTransportAuthenticationStatus.Success, Validate(verifier, second).Status);
    }

    [Fact]
    public void FrozenMetadata_ReusedAcrossStreams_IsRejectedAsReplay()
    {
        // Documents the bug the fix removes: reusing one signed Metadata for a second stream
        // (the old pool behavior) is rejected as a replay on the second validation.
        RaftTransportAuthenticator.ResetReplayCacheForTesting();
        Func<Metadata?>? factory = GrpcCommunication.CreateStreamingAuthFactory(CreateOptions(), LocalEndpoint);
        Assert.NotNull(factory);

        Metadata frozen = Assert.IsType<Metadata>(factory!());
        RaftTransportAuthenticator verifier = new(CreateOptions());

        Assert.Equal(RaftTransportAuthenticationStatus.Success, Validate(verifier, frozen).Status);
        Assert.Equal(RaftTransportAuthenticationStatus.ReplayDetected, Validate(verifier, frozen).Status);
    }

    private static RaftTransportSecurityOptions CreateOptions() => new()
    {
        NodeAuthenticationMode = RaftNodeAuthenticationMode.SharedSecret,
        SharedSecret = Secret,
        AllowedClockSkew = TimeSpan.FromSeconds(60)
    };

    private static RaftTransportAuthenticationResult Validate(RaftTransportAuthenticator verifier, Metadata metadata) =>
        verifier.Validate(
            method: "POST",
            pathOrGrpcMethod: "/Rafter/BatchRequests",
            bodyBytes: null,
            signature: GetValue(metadata, RaftTransportAuthenticationHeaders.DefaultSignatureHeaderName),
            senderNode: GetValue(metadata, RaftTransportAuthenticationHeaders.SenderNodeHeaderName),
            timestampUnixMilliseconds: GetValue(metadata, RaftTransportAuthenticationHeaders.TimestampHeaderName),
            nonce: Nonce(metadata),
            isSecureTransport: true);

    private static string Nonce(Metadata metadata) =>
        GetValue(metadata, RaftTransportAuthenticationHeaders.NonceHeaderName);

    // gRPC lowercases metadata keys; GetValue performs the same normalization.
    private static string GetValue(Metadata metadata, string headerName) =>
        metadata.GetValue(headerName.ToLowerInvariant())
        ?? throw new InvalidOperationException($"Missing metadata header {headerName}.");
}
