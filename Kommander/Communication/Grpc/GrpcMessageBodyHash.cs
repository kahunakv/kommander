
using System.Buffers;
using System.Security.Cryptography;
using Google.Protobuf;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Computes the body digest that binds a gRPC request message into its transport-authentication
/// signature.
/// </summary>
/// <remarks>
/// <para>
/// This is the single definition of "the bytes both peers sign" for gRPC. The sender digests the
/// message it is about to send; the receiver digests the message it deserialized. A disagreement
/// between the two produces <see cref="RaftTransportAuthenticationStatus.InvalidSignature"/> on
/// every request — the safe direction to fail, but one that looks like an attack rather than a
/// version skew, which is why both sides call this method rather than each rolling their own.
/// </para>
/// <para>
/// <b>Determinism.</b> The digest is taken over the protobuf encoding produced by Google.Protobuf,
/// which writes known fields in ascending field-number order, so two peers running the same
/// generated code encode an equal message identically. The <c>raft.proto</c> messages contain no
/// <c>map</c> fields, whose iteration order is unspecified and would break this. Do not add one to
/// a message that travels a signed unary path without revisiting this: the signature would start
/// failing intermittently and only under load, which is close to the worst possible failure shape.
/// A peer sending fields this build does not know about is the other way to break it — unknown
/// fields survive the round trip but are re-encoded after the known ones, so a sender that
/// interleaved them would not reproduce its own digest here.
/// </para>
/// <para>
/// <b>Allocation.</b> The message is serialized into a pooled buffer rather than through
/// <c>ToByteArray()</c>, which would allocate a fresh array the size of the message on every signed
/// call — up to a 3 MiB snapshot chunk, on the large-object heap, twice per chunk once both peers
/// are counted.
/// </para>
/// </remarks>
internal static class GrpcMessageBodyHash
{
    /// <summary>Length of the digest this helper writes; SHA-256.</summary>
    internal const int HashSizeInBytes = RaftTransportAuthenticator.BodyHashSizeInBytes;

    /// <summary>
    /// Writes the SHA-256 of <paramref name="message"/>'s protobuf encoding into
    /// <paramref name="destination"/>.
    /// </summary>
    /// <remarks>
    /// A null message digests as the empty body, matching <c>SHA256("")</c> — the value the REST
    /// path and the pre-body-binding signature format both use when there is nothing to bind. That
    /// keeps the duplex <c>BatchRequests</c> stream, which authenticates once at establishment with
    /// no single request message, on the same signature format as everything else.
    /// </remarks>
    /// <param name="message">Message to digest, or null for "no body".</param>
    /// <param name="destination">Buffer of exactly <see cref="HashSizeInBytes"/> bytes.</param>
    internal static void Compute(IMessage? message, Span<byte> destination)
    {
        if (message is null)
        {
            SHA256.HashData([], destination);
            return;
        }

        int size = message.CalculateSize();

        if (size == 0)
        {
            SHA256.HashData([], destination);
            return;
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(size);

        try
        {
            CodedOutputStream output = new(rented);
            message.WriteTo(output);
            output.Flush();

            // Bounded to size, not rented.Length: the pool returns a buffer at least as large as
            // requested, and digesting its unused tail would make the hash depend on the pool.
            SHA256.HashData(rented.AsSpan(0, size), destination);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
