
using System.Net;
using System.Net.Sockets;

namespace Kommander.Server;

/// <summary>
/// Resolves which addresses and ports the server listens on, and enforces the fail-closed startup
/// rules that govern that exposure.
/// </summary>
/// <remarks>
/// <para>
/// This lives apart from <c>Program.cs</c> because the decisions here are security policy, not
/// wiring: "may this node accept unauthenticated Raft traffic from the network" and "may a
/// cleartext listener exist alongside TLS" are the two questions that decide whether the consensus
/// protocol is reachable by anyone who can route a packet to the host. Top-level statements cannot
/// be unit tested, and an untested fail-closed check is one refactor away from failing open.
/// </para>
/// <para>
/// Every method here either returns a decision or throws <see cref="RaftException"/> naming the
/// flag that resolves the problem. Nothing warns-and-continues: a warning printed at startup is
/// invisible in a container log three weeks later, which is exactly how the reviewed build shipped
/// with an unauthenticated control plane bound to every interface.
/// </para>
/// </remarks>
public static class KommanderServerBindingPolicy
{
    /// <summary>
    /// Resolves the <c>--host</c> option to the address every listener binds.
    /// </summary>
    /// <remarks>
    /// The wildcard forms (<c>*</c>, <c>+</c>, empty) and literal addresses are handled without
    /// touching DNS. A hostname is resolved once, at startup, and the first address is used: binding
    /// is a single-address operation, so a multi-homed name has to collapse to one either way, and
    /// doing it here makes the choice visible in the startup log rather than inside Kestrel.
    /// </remarks>
    /// <exception cref="RaftException">The host cannot be resolved to any address.</exception>
    public static IPAddress ResolveBindAddress(string? host)
    {
        if (string.IsNullOrWhiteSpace(host) || host is "*" or "+")
            return IPAddress.Any;

        // Covers "0.0.0.0" and "::" as well as specific interface addresses.
        if (IPAddress.TryParse(host, out IPAddress? parsed))
            return parsed;

        // Resolved explicitly rather than through DNS: "localhost" must mean loopback even on hosts
        // whose resolver maps it to something else, because the unauthenticated-exposure check below
        // treats loopback as the safe case.
        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
            return IPAddress.Loopback;

        IPAddress[] resolved;

        try
        {
            resolved = Dns.GetHostAddresses(host);
        }
        catch (SocketException ex)
        {
            throw new RaftException($"Could not resolve --host '{host}' to an IP address.", ex);
        }

        if (resolved.Length == 0)
            throw new RaftException($"--host '{host}' did not resolve to any IP address.");

        return resolved[0];
    }

    /// <summary>
    /// True when this configuration would expose an unauthenticated Raft control plane to the
    /// network — authentication disabled on a non-loopback bind.
    /// </summary>
    /// <remarks>
    /// Loopback is the exemption rather than a general "trusted network" notion because it is the
    /// only bind the kernel itself constrains. Everything else — a private subnet, a container
    /// network, a VPC — is a claim about topology that this process cannot verify.
    /// </remarks>
    public static bool IsUnauthenticatedNetworkExposure(
        RaftNodeAuthenticationMode authenticationMode,
        IPAddress bindAddress)
    {
        ArgumentNullException.ThrowIfNull(bindAddress);

        return authenticationMode == RaftNodeAuthenticationMode.Disabled
            && !IPAddress.IsLoopback(bindAddress);
    }

    /// <summary>
    /// Refuses to start an unauthenticated node that is reachable from the network.
    /// </summary>
    /// <remarks>
    /// With authentication disabled every Raft endpoint — <c>append-logs</c>, <c>install-snapshot</c>,
    /// <c>request-vote</c>, <c>gossip</c> (which accepts a full cluster roster) — is callable by any
    /// host that can reach the port, on both the gRPC and REST surfaces. That is log forgery and
    /// membership manipulation from an unauthenticated position, so the insecure configuration must
    /// be chosen explicitly rather than inherited from defaults.
    /// </remarks>
    /// <exception cref="RaftException">
    /// Authentication is disabled, the bind is not loopback, and the operator has not passed
    /// <c>--allow-unauthenticated-cluster</c>.
    /// </exception>
    public static void ValidateAuthenticationExposure(
        RaftNodeAuthenticationMode authenticationMode,
        IPAddress bindAddress,
        bool allowUnauthenticatedCluster)
    {
        if (!IsUnauthenticatedNetworkExposure(authenticationMode, bindAddress))
            return;

        if (allowUnauthenticatedCluster)
            return;

        throw new RaftException(
            $"--node-auth-mode is Disabled and the server would bind {bindAddress}, which is not "
            + "loopback: every Raft endpoint would accept unauthenticated requests from the network. "
            + "Set --node-auth-mode to SharedSecret or MutualTls, bind loopback with --host localhost, "
            + "or pass --allow-unauthenticated-cluster to accept this exposure deliberately.");
    }

    /// <summary>
    /// Refuses to start with an mTLS trust anchor that trusts everything.
    /// </summary>
    /// <remarks>
    /// The HTTPS listener accepts any client certificate at the TLS layer on purpose, so that
    /// self-signed per-node certificates survive the handshake and reach the application's
    /// allow-list. That delegation only holds up if an allow-list exists: with none configured,
    /// <see cref="RaftClientCertificateValidator"/> also accepts anything, and the effective policy
    /// becomes "any self-generated certificate is a trusted cluster node" — mTLS fully bypassed
    /// while appearing configured.
    /// </remarks>
    /// <exception cref="RaftException">
    /// <see cref="RaftNodeAuthenticationMode.MutualTls"/> is configured with no usable trusted
    /// client certificate thumbprint.
    /// </exception>
    public static void ValidateMutualTlsTrustAnchors(
        RaftNodeAuthenticationMode authenticationMode,
        IReadOnlyCollection<string>? trustedClientCertificateThumbprints)
    {
        if (authenticationMode != RaftNodeAuthenticationMode.MutualTls)
            return;

        if (HasUsableThumbprint(trustedClientCertificateThumbprints))
            return;

        throw new RaftException(
            "--node-auth-mode MutualTls requires at least one --trusted-client-cert-thumbprint. "
            + "With an empty allow-list every certificate that completes the TLS handshake is "
            + "trusted, including self-signed certificates generated by anyone who can reach the "
            + "listener, which defeats the purpose of mutual TLS.");
    }

    /// <summary>
    /// Rejects a cleartext listener in <see cref="RaftNodeAuthenticationMode.MutualTls"/> mode.
    /// </summary>
    /// <remarks>
    /// mTLS authenticates the connection during the TLS handshake. A plaintext port offers no
    /// handshake and therefore no peer identity, so binding one alongside mTLS is not a weaker
    /// configuration of the mode — it is an unauthenticated door into a node that is otherwise
    /// enforcing certificates. There is no opt-in for this combination.
    /// </remarks>
    /// <exception cref="RaftException">Both mTLS and the plaintext opt-in were requested.</exception>
    public static void ValidatePlaintextListener(
        RaftNodeAuthenticationMode authenticationMode,
        bool allowPlaintextListener)
    {
        if (authenticationMode != RaftNodeAuthenticationMode.MutualTls || !allowPlaintextListener)
            return;

        throw new RaftException(
            "--allow-plaintext-listener cannot be combined with --node-auth-mode MutualTls: the "
            + "cleartext port has no TLS handshake, so it would accept requests with no peer "
            + "certificate at all and bypass the authentication the mode exists to enforce.");
    }

    /// <summary>
    /// Decides whether to bind the cleartext HTTP listener.
    /// </summary>
    /// <remarks>
    /// When no certificate is configured the cleartext listener is the node's only transport, so it
    /// is bound. Once TLS is configured, a cleartext port sitting beside it is an opt-in: the
    /// reviewed build bound one unconditionally, which meant a fully configured mTLS node still
    /// answered on a plaintext port that no operator had asked for.
    /// </remarks>
    public static bool ShouldBindPlaintextListener(bool tlsConfigured, bool allowPlaintextListener) =>
        !tlsConfigured || allowPlaintextListener;

    /// <summary>
    /// Parses port options, falling back to <paramref name="defaultPort"/> when none were supplied.
    /// </summary>
    /// <remarks>
    /// Ports are parsed up front so a typo fails with a message naming the option and the offending
    /// value, rather than as an unattributed <see cref="FormatException"/> from inside the Kestrel
    /// configuration callback.
    /// </remarks>
    /// <exception cref="RaftException">A value is not a valid TCP port number.</exception>
    public static IReadOnlyList<int> ParsePorts(
        IEnumerable<string>? values,
        int defaultPort,
        string optionName)
    {
        List<int> ports = [];

        foreach (string value in values ?? [])
        {
            if (!int.TryParse(value, out int port) || port is < 0 or > 65535)
                throw new RaftException($"Invalid {optionName} value '{value}': expected a TCP port between 0 and 65535.");

            ports.Add(port);
        }

        return ports.Count == 0 ? [defaultPort] : ports;
    }

    /// <summary>
    /// True when the allow-list holds at least one entry that survives thumbprint normalization.
    /// </summary>
    /// <remarks>
    /// A configured value of <c>":::"</c> normalizes away to nothing, so counting raw entries would
    /// let an empty allow-list pass the check that exists to forbid exactly that.
    /// </remarks>
    private static bool HasUsableThumbprint(IReadOnlyCollection<string>? thumbprints)
    {
        if (thumbprints is null)
            return false;

        foreach (string thumbprint in thumbprints)
        {
            if (string.IsNullOrWhiteSpace(thumbprint))
                continue;

            foreach (char c in thumbprint)
            {
                if (char.IsAsciiLetterOrDigit(c))
                    return true;
            }
        }

        return false;
    }
}
