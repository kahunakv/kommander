
using Google.Protobuf;
using Kommander;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// Covers the determinism assumption that gRPC body binding rests on: the digest a sender computes
/// over the message it is about to send must equal the digest the receiver computes over the message
/// it deserialized.
/// </summary>
/// <remarks>
/// If that ever stops holding, <em>every</em> unary gRPC call fails with <c>InvalidSignature</c> in
/// shared-secret mode — a total transport outage that reads like an attack. The round-trip tests here
/// are the cheap early warning: they exercise encode → decode → re-encode without needing a live
/// server, so a protobuf version bump or a new field type that breaks canonical encoding fails here
/// first.
/// </remarks>
public sealed class TestGrpcMessageBodyHash
{
    /// <summary>
    /// The load-bearing case: a message that has been through the wire digests the same as the
    /// original. This is exactly what sender and receiver each do.
    /// </summary>
    [Fact]
    public void Digest_SurvivesSerializationRoundTrip()
    {
        GrpcInstallSnapshotRequest original = new()
        {
            SessionId = "abc123",
            PartitionId = 7,
            SnapshotIndex = 4242,
            FollowerEndpoint = "node4:8005",
            ChunkIndex = 3,
            IsLast = true,
            Data = ByteString.CopyFrom([1, 2, 3, 4, 5]),
            Kind = 2,
            LeaderTerm = 9,
            LeaderEndpoint = "node1:8005",
            LastIncludedTerm = 8,
            SnapshotChecksum = "AABBCC",
        };

        GrpcInstallSnapshotRequest roundTripped =
            GrpcInstallSnapshotRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(Digest(original), Digest(roundTripped));
    }

    /// <summary>
    /// Two independently constructed but equal messages digest identically — the digest is over the
    /// encoded bytes, not over object identity or construction order.
    /// </summary>
    [Fact]
    public void Digest_IsEqualForEquivalentMessages()
    {
        Assert.Equal(Digest(BuildVote()), Digest(BuildVote()));
    }

    /// <summary>
    /// Any field change moves the digest. Without this the binding would exist but bind nothing.
    /// </summary>
    [Fact]
    public void Digest_ChangesWithAnyFieldChange()
    {
        GrpcVoteRequest baseline = BuildVote();

        GrpcVoteRequest differentTerm = BuildVote();
        differentTerm.Term = baseline.Term + 1;

        GrpcVoteRequest differentEndpoint = BuildVote();
        differentEndpoint.Endpoint = "other:8005";

        GrpcVoteRequest differentPartition = BuildVote();
        differentPartition.Partition = baseline.Partition + 1;

        Assert.NotEqual(Digest(baseline), Digest(differentTerm));
        Assert.NotEqual(Digest(baseline), Digest(differentEndpoint));
        Assert.NotEqual(Digest(baseline), Digest(differentPartition));
    }

    /// <summary>
    /// A large payload takes the pooled-buffer path; its digest must still match the round trip and
    /// must not depend on the pooled buffer's length, which is only guaranteed to be <i>at least</i>
    /// the message size.
    /// </summary>
    [Fact]
    public void Digest_IsStableForLargePooledPayloads()
    {
        byte[] payload = new byte[3 * 1024 * 1024];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 251);

        GrpcInstallSnapshotRequest original = new()
        {
            SessionId = "big",
            PartitionId = 1,
            Data = ByteString.CopyFrom(payload),
            IsLast = true,
        };

        GrpcInstallSnapshotRequest roundTripped =
            GrpcInstallSnapshotRequest.Parser.ParseFrom(original.ToByteArray());

        Assert.Equal(Digest(original), Digest(roundTripped));
    }

    /// <summary>
    /// A null message digests as the empty body, which is what keeps the duplex stream — signed with
    /// no request message — on the same signature format as everything else.
    /// </summary>
    [Fact]
    public void NullMessage_DigestsAsEmptyBody()
    {
        byte[] fromNull = Digest(null);

        // The value the byte[]-taking signature path produces for "no body".
        // global:: because Kommander.System shadows the BCL System namespace here.
        byte[] fromEmptyBody = global::System.Security.Cryptography.SHA256.HashData([]);

        Assert.Equal(fromEmptyBody, fromNull);
    }

    /// <summary>
    /// An all-defaults message encodes to zero bytes in protobuf, so it must also digest as the empty
    /// body rather than taking a different path.
    /// </summary>
    [Fact]
    public void DefaultMessage_DigestsAsEmptyBody()
    {
        Assert.Equal(Digest(null), Digest(new GrpcVoteRequest()));
    }

    private static GrpcVoteRequest BuildVote() => new()
    {
        Partition = 3,
        Term = 11,
        MaxLogId = 900,
        LastLogTerm = 10,
        TimeNode = 1,
        TimePhysical = 1234,
        TimeCounter = 5,
        Endpoint = "node2:8005",
        PreVote = false,
    };

    private static byte[] Digest(IMessage? message)
    {
        byte[] hash = new byte[RaftTransportAuthenticator.BodyHashSizeInBytes];
        GrpcMessageBodyHash.Compute(message, hash);
        return hash;
    }
}
