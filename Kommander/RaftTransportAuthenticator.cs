using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
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
        if (Options.NodeAuthenticationMode == RaftNodeAuthenticationMode.Disabled)
        {
            return new RaftTransportAuthenticationHeaders
            {
                SignatureHeaderName = Options.HeaderName
            };
        }

        EnsureSharedSecretMode();
        ValidateInputs(method, pathOrGrpcMethod, senderNode);

        long timestamp = timestampUnixMilliseconds ?? GetUtcNowUnixMilliseconds();
        string authNonce = string.IsNullOrWhiteSpace(nonce) ? CreateNonce() : nonce;
        byte[] signatureBytes = ComputeSignatureBytes(
            method,
            pathOrGrpcMethod,
            senderNode,
            timestamp,
            authNonce,
            bodyBytes);

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
        if (Options.NodeAuthenticationMode == RaftNodeAuthenticationMode.Disabled)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.Disabled
            };
        }

        EnsureSharedSecretMode();

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

        if (!TryDecodeBase64Url(signature, out byte[]? providedSignature)
            || !TryDecodeBase64Url(nonce, out byte[]? decodedNonce)
            || decodedNonce is null
            || decodedNonce.Length != 16)
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

        byte[] expectedSignature = ComputeSignatureBytes(
            method,
            pathOrGrpcMethod,
            senderNode,
            timestamp,
            nonce,
            bodyBytes);

        if (!FixedTimeEquals(providedSignature, expectedSignature))
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

    private void EnsureSharedSecretMode()
    {
        if (Options.NodeAuthenticationMode == RaftNodeAuthenticationMode.MutualTls)
            throw new NotSupportedException("Mutual TLS authentication is not implemented yet.");
    }

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

    private byte[] ComputeSignatureBytes(
        string method,
        string pathOrGrpcMethod,
        string senderNode,
        long timestampUnixMilliseconds,
        string nonce,
        byte[]? bodyBytes)
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
            offset += WriteSha256HexLower(buffer[offset..], bodyBytes);

            return HMACSHA256.HashData(sharedSecretBytes!, buffer[..offset]);
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
        Span<byte> nonceBytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(nonceBytes);
        return Base64UrlEncode(nonceBytes);
    }

    private static string Base64UrlEncode(ReadOnlySpan<byte> bytes)
    {
        return Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    private static bool TryDecodeBase64Url(string value, out byte[]? decoded)
    {
        decoded = null;

        if (string.IsNullOrWhiteSpace(value))
            return false;

        string padded = value.Replace('-', '+').Replace('_', '/');
        int padding = padded.Length % 4;

        if (padding == 1)
            return false;

        if (padding > 0)
            padded = padded.PadRight(padded.Length + (4 - padding), '=');

        try
        {
            decoded = Convert.FromBase64String(padded);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static int WriteUtf8(Span<byte> destination, string value)
    {
        return Encoding.UTF8.GetBytes(value, destination);
    }

    private static int WriteUtf8(Span<byte> destination, ReadOnlySpan<char> value)
    {
        return Encoding.UTF8.GetBytes(value, destination);
    }

    private static int WriteSha256HexLower(Span<byte> destination, byte[]? bodyBytes)
    {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(bodyBytes ?? [], hash);

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
