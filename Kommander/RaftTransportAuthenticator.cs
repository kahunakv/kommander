using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Kommander;

/// <summary>
/// Signs and validates node-to-node transport authentication fields.
/// </summary>
public sealed class RaftTransportAuthenticator
{
    private const long ReplayCachePruneIntervalMilliseconds = 30_000;

    private static readonly ConcurrentDictionary<string, long> ReplayCache = new();

    private static long nextReplayCachePruneAtUnixMilliseconds;

    private readonly byte[]? sharedSecretBytes;

    private readonly string replayCacheNamespace = "disabled";
    
    private readonly TimeProvider timeProvider;

    /// <summary>
    /// Initializes a new authenticator for the provided transport security options.
    /// </summary>
    public RaftTransportAuthenticator(
        RaftTransportSecurityOptions options,
        TimeProvider? timeProvider = null)
    {
        Options = options ?? throw new ArgumentNullException(nameof(options));
        this.timeProvider = timeProvider ?? TimeProvider.System;

        if (Options.NodeAuthenticationMode == RaftNodeAuthenticationMode.SharedSecret)
        {
            if (string.IsNullOrWhiteSpace(Options.SharedSecret))
                throw new ArgumentException(
                    "SharedSecret must be configured when SharedSecret authentication is enabled.",
                    nameof(options));

            sharedSecretBytes = Encoding.UTF8.GetBytes(Options.SharedSecret);
            replayCacheNamespace = Convert.ToHexString(SHA256.HashData(sharedSecretBytes));
        }
    }

    /// <summary>
    /// Transport security settings used by this authenticator.
    /// </summary>
    public RaftTransportSecurityOptions Options { get; }

    /// <summary>
    /// Length in bytes of the body digest bound into a signature.
    /// </summary>
    public const int BodyHashSizeInBytes = 32;

    /// <summary>Length in bytes of an HMAC-SHA256 signature.</summary>
    private const int SignatureSizeInBytes = 32;

    /// <summary>Length in bytes of the random nonce carried in the authentication headers.</summary>
    private const int NonceSizeInBytes = 16;

    /// <summary>
    /// Largest signature the validator will decode into its stack buffer.
    /// </summary>
    /// <remarks>
    /// Sized well above <see cref="SignatureSizeInBytes"/> so a well-formed field of the wrong length
    /// still reaches the signature comparison and is rejected there as
    /// <see cref="RaftTransportAuthenticationStatus.InvalidSignature"/>, which is what the previous
    /// allocating decoder did. Only an absurdly long field — one that could not be a signature under any
    /// reading — is refused earlier as malformed. The bound is a compile-time constant precisely because
    /// the header it decodes is unauthenticated: no attacker-supplied length ever sizes a buffer.
    /// </remarks>
    private const int MaxDecodedSignatureSizeInBytes = 64;

    /// <summary>
    /// Creates signed authentication headers for an outgoing request, binding a body the caller has
    /// already digested.
    /// </summary>
    /// <remarks>
    /// Exists for transports that can hash their payload without first materializing it as a
    /// <c>byte[]</c> — the gRPC path serializes into a pooled buffer, so handing over a 32-byte digest
    /// avoids an allocation the size of the message (up to a 3 MiB snapshot chunk) on every call.
    /// The digest must be SHA-256 over the exact bytes the peer will verify; see
    /// <c>GrpcMessageBodyHash</c> for the gRPC definition of "exact bytes".
    /// </remarks>
    /// <param name="bodyHash">SHA-256 of the request body; must be <see cref="BodyHashSizeInBytes"/> long.</param>
    public RaftTransportAuthenticationHeaders SignWithBodyHash(
        string method,
        string pathOrGrpcMethod,
        string senderNode,
        ReadOnlySpan<byte> bodyHash,
        long? timestampUnixMilliseconds = null,
        string? nonce = null)
    {
        if (Options.NodeAuthenticationMode is RaftNodeAuthenticationMode.Disabled
            or RaftNodeAuthenticationMode.MutualTls)
        {
            return new RaftTransportAuthenticationHeaders
            {
                SignatureHeaderName = Options.HeaderName
            };
        }

        ValidateBodyHashLength(bodyHash);

        return SignCore(method, pathOrGrpcMethod, senderNode, bodyHash, timestampUnixMilliseconds, nonce);
    }

    /// <summary>
    /// Creates signed authentication headers for an outgoing request.
    /// </summary>
    public RaftTransportAuthenticationHeaders Sign(
        string method,
        string pathOrGrpcMethod,
        string senderNode,
        byte[]? bodyBytes = null,
        long? timestampUnixMilliseconds = null,
        string? nonce = null)
    {
        // MutualTls authenticates the connection during the TLS handshake, so there is nothing to
        // sign and no header to attach — the same shape as Disabled. Signing is not a security
        // decision (validation is), so returning empty headers here is safe.
        if (Options.NodeAuthenticationMode is RaftNodeAuthenticationMode.Disabled
            or RaftNodeAuthenticationMode.MutualTls)
        {
            return new RaftTransportAuthenticationHeaders
            {
                SignatureHeaderName = Options.HeaderName
            };
        }

        Span<byte> bodyHash = stackalloc byte[BodyHashSizeInBytes];
        SHA256.HashData(bodyBytes ?? [], bodyHash);

        return SignCore(method, pathOrGrpcMethod, senderNode, bodyHash, timestampUnixMilliseconds, nonce);
    }

    private RaftTransportAuthenticationHeaders SignCore(
        string method,
        string pathOrGrpcMethod,
        string senderNode,
        ReadOnlySpan<byte> bodyHash,
        long? timestampUnixMilliseconds,
        string? nonce)
    {
        ValidateInputs(method, pathOrGrpcMethod, senderNode);

        long timestamp = timestampUnixMilliseconds ?? GetUtcNowUnixMilliseconds();
        string authNonce = string.IsNullOrWhiteSpace(nonce) ? CreateNonce() : nonce;

        Span<byte> signatureBytes = stackalloc byte[SignatureSizeInBytes];
        ComputeSignature(
            method,
            pathOrGrpcMethod,
            senderNode,
            timestamp,
            authNonce,
            bodyHash,
            signatureBytes);

        return new RaftTransportAuthenticationHeaders
        {
            SignatureHeaderName = string.IsNullOrWhiteSpace(Options.HeaderName)
                ? RaftTransportAuthenticationHeaders.DefaultSignatureHeaderName
                : Options.HeaderName,
            Signature = Base64UrlEncode(signatureBytes),
            SenderNode = senderNode,
            TimestampUnixMilliseconds = timestamp,
            Nonce = authNonce
        };
    }

    /// <summary>
    /// Validates signed authentication fields for an incoming request.
    /// </summary>
    public RaftTransportAuthenticationResult Validate(
        string method,
        string pathOrGrpcMethod,
        byte[]? bodyBytes,
        string? signature,
        string? senderNode,
        string? timestampUnixMilliseconds,
        string? nonce,
        bool isSecureTransport)
    {
        RaftTransportAuthenticationResult? modeRejection = CheckMode();
        if (modeRejection is not null)
            return modeRejection;

        // Hashed after the mode checks so a Disabled or MutualTls caller never pays for digesting a
        // body its mode does not consult.
        Span<byte> bodyHash = stackalloc byte[BodyHashSizeInBytes];
        SHA256.HashData(bodyBytes ?? [], bodyHash);

        return ValidateCore(
            method,
            pathOrGrpcMethod,
            bodyHash,
            signature,
            senderNode,
            timestampUnixMilliseconds,
            nonce,
            isSecureTransport);
    }

    /// <summary>
    /// Validates signed authentication fields against a body the caller has already digested.
    /// </summary>
    /// <remarks>
    /// The counterpart to <see cref="SignWithBodyHash"/>, for transports that digest their payload
    /// without materializing it. Both sides must derive the digest the same way or every request
    /// fails with <see cref="RaftTransportAuthenticationStatus.InvalidSignature"/> — which is the
    /// correct direction to fail, but makes a mismatch look like an attack rather than a bug, so the
    /// derivation belongs in one shared helper per transport.
    /// </remarks>
    /// <param name="bodyHash">SHA-256 of the request body; must be <see cref="BodyHashSizeInBytes"/> long.</param>
    public RaftTransportAuthenticationResult ValidateWithBodyHash(
        string method,
        string pathOrGrpcMethod,
        ReadOnlySpan<byte> bodyHash,
        string? signature,
        string? senderNode,
        string? timestampUnixMilliseconds,
        string? nonce,
        bool isSecureTransport)
    {
        RaftTransportAuthenticationResult? modeRejection = CheckMode();
        if (modeRejection is not null)
            return modeRejection;

        ValidateBodyHashLength(bodyHash);

        return ValidateCore(
            method,
            pathOrGrpcMethod,
            bodyHash,
            signature,
            senderNode,
            timestampUnixMilliseconds,
            nonce,
            isSecureTransport);
    }

    /// <summary>
    /// Returns a rejection for the authentication modes that must not reach HMAC verification, or null
    /// when signature validation should proceed.
    /// </summary>
    /// <remarks>
    /// Fail closed in MutualTls mode: these overloads authenticate an HMAC signature, which mTLS never
    /// sends, so reaching one means a transport did not route to <see cref="ValidatePeerCertificate"/>.
    /// Returning Success here would hand a caller authentication without any certificate ever being
    /// presented.
    /// </remarks>
    private RaftTransportAuthenticationResult? CheckMode()
    {
        if (Options.NodeAuthenticationMode == RaftNodeAuthenticationMode.Disabled)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.Disabled
            };
        }

        if (Options.NodeAuthenticationMode == RaftNodeAuthenticationMode.MutualTls)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.CertificateRequired
            };
        }

        return null;
    }

    private static void ValidateBodyHashLength(ReadOnlySpan<byte> bodyHash)
    {
        if (bodyHash.Length != BodyHashSizeInBytes)
        {
            throw new ArgumentException(
                $"Body hash must be {BodyHashSizeInBytes} bytes (SHA-256), got {bodyHash.Length}.",
                nameof(bodyHash));
        }
    }

    private RaftTransportAuthenticationResult ValidateCore(
        string method,
        string pathOrGrpcMethod,
        ReadOnlySpan<byte> bodyHash,
        string? signature,
        string? senderNode,
        string? timestampUnixMilliseconds,
        string? nonce,
        bool isSecureTransport)
    {
        if (Options.RequireTls && !isSecureTransport)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.TlsRequired
            };
        }

        if (string.IsNullOrWhiteSpace(signature)
            || string.IsNullOrWhiteSpace(senderNode)
            || string.IsNullOrWhiteSpace(timestampUnixMilliseconds)
            || string.IsNullOrWhiteSpace(nonce))
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.MissingFields
            };
        }

        ValidateInputs(method, pathOrGrpcMethod, senderNode);

        if (!long.TryParse(
                timestampUnixMilliseconds,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out long timestamp))
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.MalformedFields
            };
        }

        // Decoded into fixed stack buffers rather than into arrays sized from the header. The previous
        // shape allocated a padded array and then a second exact-length array per field, so a successful
        // validation cost four arrays before the HMAC result.
        Span<byte> providedSignature = stackalloc byte[MaxDecodedSignatureSizeInBytes];
        Span<byte> decodedNonce = stackalloc byte[NonceSizeInBytes];

        if (!TryDecodeBase64Url(signature, providedSignature, out int providedSignatureLength)
            || !TryDecodeBase64Url(nonce, decodedNonce, out int decodedNonceLength)
            || decodedNonceLength != NonceSizeInBytes)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.MalformedFields
            };
        }

        long now = GetUtcNowUnixMilliseconds();
        long skew = Math.Abs(now - timestamp);
        long allowedSkew = (long)Math.Max(0, Options.AllowedClockSkew.TotalMilliseconds);

        if (skew > allowedSkew)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.TimestampSkewExceeded
            };
        }

        Span<byte> expectedSignature = stackalloc byte[SignatureSizeInBytes];
        ComputeSignature(
            method,
            pathOrGrpcMethod,
            senderNode,
            timestamp,
            nonce,
            bodyHash,
            expectedSignature);

        // FixedTimeEquals is length-sensitive, so a well-formed field of the wrong length fails here as
        // an invalid signature — the same classification the allocating decoder produced.
        if (!CryptographicOperations.FixedTimeEquals(
                providedSignature[..providedSignatureLength],
                expectedSignature))
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.InvalidSignature
            };
        }

        if (!TryRegisterNonce(senderNode, nonce, now, allowedSkew))
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.ReplayDetected
            };
        }

        return new RaftTransportAuthenticationResult
        {
            Status = RaftTransportAuthenticationStatus.Success
        };
    }

    /// <summary>
    /// Validates a MutualTls peer certificate for an incoming request.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Deliberately separate from the HMAC-shaped <see cref="Validate"/> overload rather than folded
    /// into it. The two modes authenticate different things — a per-request signature versus a
    /// per-connection certificate — and a single entry point that accepted either would make it
    /// possible to reach <see cref="RaftTransportAuthenticationStatus.Success"/> in MutualTls mode
    /// without a certificate ever being presented.
    /// </para>
    /// <para>
    /// The TLS handshake has already authenticated the connection before this runs; what remains is
    /// the application-level trust decision (validity window and thumbprint allow-list), which Kestrel
    /// cannot make because it would have to reject self-signed per-node certificates first.
    /// </para>
    /// </remarks>
    /// <param name="peerCertificate">Client certificate presented on the connection, if any.</param>
    /// <param name="isSecureTransport">Whether the request arrived over TLS.</param>
    public RaftTransportAuthenticationResult ValidatePeerCertificate(
        X509Certificate2? peerCertificate,
        bool isSecureTransport)
    {
        if (Options.NodeAuthenticationMode == RaftNodeAuthenticationMode.Disabled)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.Disabled
            };
        }

        // Wrong door: shared-secret callers must go through the signature overload. Reported as a
        // rejection rather than an exception so a mis-wired transport degrades to "unauthenticated"
        // instead of faulting every request.
        if (Options.NodeAuthenticationMode == RaftNodeAuthenticationMode.SharedSecret)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.MissingFields
            };
        }

        // mTLS over cleartext is a contradiction: with no handshake there is no peer identity to
        // check, so a null certificate would otherwise be the only signal.
        if (Options.RequireTls && !isSecureTransport)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.TlsRequired
            };
        }

        return new RaftTransportAuthenticationResult
        {
            Status = RaftClientCertificateValidator.Validate(peerCertificate, Options, timeProvider)
        };
    }

    /// <summary>
    /// Compares two byte arrays using a fixed-time algorithm.
    /// </summary>
    public static bool FixedTimeEquals(byte[]? left, byte[]? right)
    {
        if (left is null || right is null || left.Length != right.Length)
            return false;

        return CryptographicOperations.FixedTimeEquals(left, right);
    }

    internal static void ResetReplayCacheForTesting()
    {
        ReplayCache.Clear();
        Volatile.Write(ref nextReplayCachePruneAtUnixMilliseconds, 0);
    }

    private long GetUtcNowUnixMilliseconds() =>
        timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

    private static void ValidateInputs(string method, string pathOrGrpcMethod, string senderNode)
    {
        if (string.IsNullOrWhiteSpace(method))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(method));

        if (string.IsNullOrWhiteSpace(pathOrGrpcMethod))
        {
            throw new ArgumentException(
                "Value cannot be null or whitespace.",
                nameof(pathOrGrpcMethod));
        }

        if (string.IsNullOrWhiteSpace(senderNode))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(senderNode));
    }

    /// <summary>
    /// Writes the HMAC-SHA256 over the canonical signing representation into
    /// <paramref name="destination"/>, which must be exactly <see cref="SignatureSizeInBytes"/> long.
    /// </summary>
    /// <remarks>
    /// Takes a destination rather than returning a fresh array so neither signing nor validation
    /// allocates for the result. The canonical representation itself is unchanged — altering the field
    /// order, the separators, or the digest encoding here would invalidate every peer's signature.
    /// </remarks>
    private void ComputeSignature(
        string method,
        string pathOrGrpcMethod,
        string senderNode,
        long timestampUnixMilliseconds,
        string nonce,
        ReadOnlySpan<byte> bodyHash,
        Span<byte> destination)
    {
        int methodByteCount = Encoding.UTF8.GetByteCount(method);
        int pathByteCount = Encoding.UTF8.GetByteCount(pathOrGrpcMethod);
        int senderByteCount = Encoding.UTF8.GetByteCount(senderNode);
        int nonceByteCount = Encoding.UTF8.GetByteCount(nonce);

        Span<char> timestampChars = stackalloc char[20];
        if (!timestampUnixMilliseconds.TryFormat(
                timestampChars,
                out int timestampCharCount,
                provider: CultureInfo.InvariantCulture))
        {
            throw new InvalidOperationException("Could not format timestamp.");
        }

        int timestampByteCount = Encoding.UTF8.GetByteCount(timestampChars[..timestampCharCount]);
        int totalByteCount =
            methodByteCount
            + pathByteCount
            + senderByteCount
            + timestampByteCount
            + nonceByteCount
            + 5
            + 64;

        byte[] rentedBuffer = ArrayPool<byte>.Shared.Rent(totalByteCount);

        try
        {
            Span<byte> buffer = rentedBuffer.AsSpan(0, totalByteCount);
            int offset = 0;

            offset += WriteUtf8(buffer[offset..], method);
            buffer[offset++] = (byte)'\n';
            offset += WriteUtf8(buffer[offset..], pathOrGrpcMethod);
            buffer[offset++] = (byte)'\n';
            offset += WriteUtf8(buffer[offset..], senderNode);
            buffer[offset++] = (byte)'\n';
            offset += WriteUtf8(buffer[offset..], timestampChars[..timestampCharCount]);
            buffer[offset++] = (byte)'\n';
            offset += WriteUtf8(buffer[offset..], nonce);
            buffer[offset++] = (byte)'\n';
            offset += WriteHexLower(buffer[offset..], bodyHash);

            HMACSHA256.HashData(sharedSecretBytes!, buffer[..offset], destination);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rentedBuffer);
        }
    }

    private bool TryRegisterNonce(string senderNode, string nonce, long now, long allowedSkewMs)
    {
        long expiry = now + allowedSkewMs;
        string key = string.Create(
            CultureInfo.InvariantCulture,
            $"{replayCacheNamespace}\n{senderNode}\n{nonce}");

        PruneExpiredNonces(now);

        while (true)
        {
            if (ReplayCache.TryGetValue(key, out long existingExpiry))
            {
                if (existingExpiry > now)
                    return false;

                if (ReplayCache.TryUpdate(key, expiry, existingExpiry))
                    return true;

                continue;
            }

            return ReplayCache.TryAdd(key, expiry);
        }
    }

    private void PruneExpiredNonces(long now)
    {
        if (ReplayCache.IsEmpty)
            return;

        long scheduledPruneAt = Volatile.Read(ref nextReplayCachePruneAtUnixMilliseconds);
        if (scheduledPruneAt > now)
            return;

        long nextPruneAt = now + ReplayCachePruneIntervalMilliseconds;
        if (Interlocked.CompareExchange(ref nextReplayCachePruneAtUnixMilliseconds, nextPruneAt, scheduledPruneAt)
            != scheduledPruneAt)
        {
            return;
        }

        foreach ((string key, long expiry) in ReplayCache)
        {
            if (expiry <= now)
                ReplayCache.TryRemove(key, out _);
        }
    }

    private static string CreateNonce()
    {
        Span<byte> nonceBytes = stackalloc byte[NonceSizeInBytes];
        RandomNumberGenerator.Fill(nonceBytes);
        return Base64UrlEncode(nonceBytes);
    }

    private static string Base64UrlEncode(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
            return string.Empty;

        // Encode into a stack buffer and translate to the URL alphabet in place, allocating exactly one
        // result string — instead of the standard-base64 string plus the TrimEnd/Replace/Replace chain
        // that produced up to three more intermediate strings.
        int base64Length = ((bytes.Length + 2) / 3) * 4;
        Span<char> buffer = base64Length <= 256 ? stackalloc char[base64Length] : new char[base64Length];

        if (!Convert.TryToBase64Chars(bytes, buffer, out int written))
            return string.Empty; // unreachable: buffer is exactly sized

        int end = written;
        while (end > 0 && buffer[end - 1] == '=') // strip padding
            end--;

        for (int i = 0; i < end; i++)
        {
            buffer[i] = buffer[i] switch
            {
                '+' => '-',
                '/' => '_',
                char c => c
            };
        }

        return new string(buffer[..end]);
    }

    /// <summary>
    /// Decodes a base64url field directly into <paramref name="destination"/>, allocating nothing.
    /// Returns false when the field is not valid base64url or cannot fit.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The character buffer is bounded by <paramref name="destination"/>, never by the field. That
    /// ordering is the point: the field arrives in an unauthenticated header, so it must not be able to
    /// size a stack allocation. A field longer than the destination could hold is rejected before any
    /// buffer is touched.
    /// </para>
    /// <para>
    /// The accepted encodings are unchanged — the URL alphabet, with or without trailing padding — and
    /// so is the rejection of a length that leaves one leftover character, which no base64 encoding
    /// produces.
    /// </para>
    /// </remarks>
    private static bool TryDecodeBase64Url(string value, Span<byte> destination, out int bytesWritten)
    {
        bytesWritten = 0;

        if (string.IsNullOrWhiteSpace(value))
            return false;

        int padding = value.Length % 4;
        if (padding == 1)
            return false;

        int paddedLength = padding == 0 ? value.Length : value.Length + (4 - padding);

        // Longest padded encoding that can decode into the destination. Four base64 characters carry
        // three bytes, so a destination of n bytes admits at most ceil(n / 3) quartets.
        int maxPaddedLength = ((destination.Length + 2) / 3) * 4;
        if (paddedLength > maxPaddedLength)
            return false;

        Span<char> buffer = stackalloc char[maxPaddedLength];
        Span<char> chars = buffer[..paddedLength];

        for (int i = 0; i < value.Length; i++)
        {
            chars[i] = value[i] switch
            {
                '-' => '+',
                '_' => '/',
                char c => c
            };
        }

        for (int i = value.Length; i < paddedLength; i++)
            chars[i] = '=';

        return Convert.TryFromBase64Chars(chars, destination, out bytesWritten);
    }

    private static int WriteUtf8(Span<byte> destination, string value)
    {
        return Encoding.UTF8.GetBytes(value, destination);
    }

    private static int WriteUtf8(Span<byte> destination, ReadOnlySpan<char> value)
    {
        return Encoding.UTF8.GetBytes(value, destination);
    }

    private static int WriteHexLower(Span<byte> destination, ReadOnlySpan<byte> hash)
    {
        for (int i = 0; i < hash.Length; i++)
        {
            byte value = hash[i];
            destination[i * 2] = ToHexLower((value >> 4) & 0xF);
            destination[i * 2 + 1] = ToHexLower(value & 0xF);
        }

        return hash.Length * 2;
    }

    private static byte ToHexLower(int nibble)
    {
        return (byte)(nibble < 10 ? '0' + nibble : 'a' + (nibble - 10));
    }
}
