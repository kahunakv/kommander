using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using Flurl.Http;
using Flurl.Http.Configuration;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.Logging;
using Kommander.System;
using Microsoft.Extensions.Logging;
using WirePingRequest = Kommander.Data.PingRequest;
using WirePingResponse = Kommander.Data.PingResponse;
using WirePingReqRequest = Kommander.Data.PingReqRequest;
using WirePingReqResponse = Kommander.Data.PingReqResponse;

namespace Kommander.Communication.Rest;

/// <summary>
/// Allows for communication between Raft nodes using REST endpoints.
/// </summary>
public class RestCommunication : ICommunication
{
    /// <summary>Shared empty header map returned when node authentication is disabled, so the
    /// unauthenticated REST path allocates no per-request dictionary.</summary>
    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>();

    public async Task<HandshakeResponse> Handshake(RaftManager manager, RaftNode node, HandshakeRequest request)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.HandshakeRequest);
        
        try
        {
            return await CreateRaftRequest(manager, node, "/v1/raft/handshake", payload)
                .PostAsync(JsonContent(payload))
                .ReceiveJson<HandshakeResponse>().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] Handshake: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }
        
        return new();
    }
    
    public async Task<RequestVotesResponse> RequestVotes(RaftManager manager, RaftNode node, RequestVotesRequest request)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.RequestVotesRequest);
        
        try
        {
            return await CreateRaftRequest(manager, node, "/v1/raft/request-vote", payload)
                .PostAsync(JsonContent(payload))
                .ReceiveJson<RequestVotesResponse>().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] RequestVotes: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }
        
        return new();
    }

    public async Task<VoteResponse> Vote(RaftManager manager, RaftNode node, VoteRequest request)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.VoteRequest);
        
        try
        {
            return await CreateRaftRequest(manager, node, "/v1/raft/vote", payload)
                .PostAsync(JsonContent(payload))
                .ReceiveJson<VoteResponse>().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] Vote: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }

        return new();
    }

    public async Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager manager, RaftNode node, CompleteAppendLogsRequest request)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.CompleteAppendLogsRequest);
        
        try
        {
            CompleteAppendLogsResponse? response = await CreateRaftRequest(
                    manager,
                    node,
                    "/v1/raft/complete-append-logs",
                    payload)
                .PostAsync(JsonContent(payload))
                .ReceiveJson<CompleteAppendLogsResponse>().ConfigureAwait(false);

            return response;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] CompleteAppendLogs: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }

        return new();
    }

    public async Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.AppendLogsRequest);
        
        try
        {
            AppendLogsResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/append-logs", payload)
                .PostAsync(JsonContent(payload))
                .ReceiveJson<AppendLogsResponse>().ConfigureAwait(false);
            
            if (request.Logs is not null && request.Logs.Count > 0)
                manager.Logger.LogDebugLogsReplicated(manager.LocalEndpoint, request.Partition, node.Endpoint);

            return response;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}/{Partition}] AppendLogs: {Message}", manager.LocalEndpoint, request.Partition, e.Message);
        }

        return new();
    }

    public async Task<BatchRequestsResponse> BatchRequests(RaftManager manager, RaftNode node, BatchRequestsRequest request)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.BatchRequestsRequest);
        
        try
        {
            BatchRequestsResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/batch-requests", payload)
                .PostAsync(JsonContent(payload))
                .ReceiveJson<BatchRequestsResponse>().ConfigureAwait(false);

            return response;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] BatchRequestsResponse: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new();
    }

    /// <summary>
    /// Sends a <see cref="LeaveRequest"/> to <paramref name="node"/> via the
    /// <c>POST /v1/raft/leave</c> REST endpoint.  If the target is not the P0 leader it
    /// returns <see cref="LeaveResponse.LeaderHint"/> so the caller can retry against the
    /// current leader.  Returns failure on any transport or HTTP error.
    /// </summary>
    public async Task<LeaveResponse> SendLeave(RaftManager manager, RaftNode node, LeaveRequest request, CancellationToken cancellationToken = default)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.LeaveRequest);

        try
        {
            LeaveResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/leave", payload)
                .PostAsync(JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<LeaveResponse>()
                .ConfigureAwait(false);

            return response ?? new LeaveResponse(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] SendLeave: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new LeaveResponse(false);
    }

    /// <summary>
    /// Sends a <see cref="SetMemberRoleRequest"/> to <paramref name="node"/> via the
    /// <c>POST /v1/raft/set-member-role</c> REST endpoint. A pre-drain peer answers 404, which
    /// surfaces here as a failed response — the drain fails loudly instead of silently degrading
    /// into an immediate removal. Returns failure on any transport or HTTP error.
    /// </summary>
    public async Task<SetMemberRoleResponse> SendSetMemberRole(RaftManager manager, RaftNode node, SetMemberRoleRequest request, CancellationToken cancellationToken = default)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.SetMemberRoleRequest);

        try
        {
            SetMemberRoleResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/set-member-role", payload)
                .PostAsync(JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<SetMemberRoleResponse>()
                .ConfigureAwait(false);

            return response ?? new SetMemberRoleResponse(false, Status: RaftOperationStatus.Errored);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] SendSetMemberRole: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new SetMemberRoleResponse(false, Status: RaftOperationStatus.Errored);
    }

    /// <summary>
    /// Sends a gossip push to a peer via <c>POST /v1/raft/gossip</c>.
    /// The roster is encoded as JSON in <see cref="GossipRequest.RosterJson"/> so that
    /// <see cref="ClusterMembership"/> does not need to be registered in <see cref="RestJsonContext"/>.
    /// Returns an empty <see cref="GossipAck"/> on any transport or deserialization error.
    /// </summary>
    public async Task<GossipAck> SendGossip(RaftManager manager, RaftNode node, GossipMessage digest, CancellationToken cancellationToken = default)
    {
        string? rosterJson = digest.Roster is not null
            ? JsonSerializer.Serialize(digest.Roster)
            : null;

        string? loadReportJson = digest.LoadReport is not null
            ? JsonSerializer.Serialize(digest.LoadReport)
            : null;

        GossipRequest request = new(digest.SenderEndpoint, digest.MembershipVersion, rosterJson)
        {
            LoadReportJson = loadReportJson,
        };
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.GossipRequest);

        try
        {
            GossipResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/gossip", payload)
                .PostAsync(JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<GossipResponse>()
                .ConfigureAwait(false);

            if (response is null)
                return new GossipAck(0, null);

            ClusterMembership? roster = response.RosterJson is not null
                ? JsonSerializer.Deserialize<ClusterMembership>(response.RosterJson)
                : null;

            return new GossipAck(response.MembershipVersion, roster);
        }
        catch (Exception e)
        {
            manager.Logger.LogWarning("[{Endpoint}] SendGossip: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new GossipAck(0, null);
    }

    /// <summary>
    /// Sends a direct SWIM probe to <paramref name="node"/> via <c>POST /v1/raft/ping</c>.
    /// Returns <c>PingResponse(false, 0)</c> on any transport error so the caller treats
    /// the node as unreachable.
    /// </summary>
    public async Task<Gossip.PingResponse> SendPing(RaftManager manager, RaftNode node, Gossip.PingRequest request, CancellationToken cancellationToken = default)
    {
        WirePingRequest wireRequest = new(request.SenderEndpoint);
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(wireRequest, RestJsonContext.Default.PingRequest);

        try
        {
            WirePingResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/ping", payload)
                .PostAsync(JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<WirePingResponse>()
                .ConfigureAwait(false);

            return response is not null
                ? new Gossip.PingResponse(response.Alive, response.Incarnation)
                : new Gossip.PingResponse(false, 0);
        }
        catch (Exception e)
        {
            manager.Logger.LogWarning("[{Endpoint}] SendPing: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new Gossip.PingResponse(false, 0);
    }

    /// <summary>
    /// Asks <paramref name="node"/> to relay a direct probe to a third node via
    /// <c>POST /v1/raft/ping-req</c>.  Returns <c>PingReqResponse(false)</c> on any
    /// transport error.
    /// </summary>
    public async Task<Gossip.PingReqResponse> SendPingReq(RaftManager manager, RaftNode node, Gossip.PingReqRequest request, CancellationToken cancellationToken = default)
    {
        WirePingReqRequest wireRequest = new(request.SenderEndpoint, request.TargetEndpoint);
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(wireRequest, RestJsonContext.Default.PingReqRequest);

        try
        {
            WirePingReqResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/ping-req", payload)
                .PostAsync(JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<WirePingReqResponse>()
                .ConfigureAwait(false);

            return response is not null
                ? new Gossip.PingReqResponse(response.Reached)
                : new Gossip.PingReqResponse(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogWarning("[{Endpoint}] SendPingReq: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new Gossip.PingReqResponse(false);
    }

    /// <summary>
    /// Queries the remote node for the last committed log index it has recorded for
    /// <paramref name="followerEndpoint"/> on <paramref name="partitionId"/> via the
    /// <c>POST /v1/raft/get-follower-lag</c> REST endpoint.  Returns <see langword="null"/>
    /// when the remote node reports no record for the follower on that partition, or when
    /// the request fails.
    /// </summary>
    public async Task<long?> GetRemoteFollowerLag(RaftManager manager, RaftNode node, int partitionId, string followerEndpoint)
    {
        GetFollowerLagRequest request = new(partitionId, followerEndpoint);
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.GetFollowerLagRequest);

        try
        {
            GetFollowerLagResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/get-follower-lag", payload)
                .PostAsync(JsonContent(payload))
                .ReceiveJson<GetFollowerLagResponse>()
                .ConfigureAwait(false);

            return response is { HasValue: true } ? response.Value : null;
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] GetRemoteFollowerLag partition {PartitionId}: {Message}",
                manager.LocalEndpoint, partitionId, e.Message);
        }

        return null;
    }

    public async Task<SnapshotResponse> SendInstallSnapshot(
        RaftManager manager, RaftNode node, SnapshotRequest request,
        CancellationToken cancellationToken = default)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.SnapshotRequest);

        try
        {
            SnapshotResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/install-snapshot", payload)
                .PostAsync(JsonContent(payload), cancellationToken: cancellationToken)
                .ReceiveJson<SnapshotResponse>().ConfigureAwait(false);

            return response ?? new SnapshotResponse(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] SendInstallSnapshot partition {PartitionId}: {Message}",
                manager.LocalEndpoint, request.PartitionId, e.Message);
        }

        return new SnapshotResponse(false);
    }

    public async Task<JoinResponse> SendJoin(RaftManager manager, RaftNode node, JoinRequest request)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(request, RestJsonContext.Default.JoinRequest);

        try
        {
            JoinResponse? response = await CreateRaftRequest(manager, node, "/v1/raft/join", payload)
                .PostAsync(JsonContent(payload))
                .ReceiveJson<JoinResponse>().ConfigureAwait(false);

            return response ?? new JoinResponse(false);
        }
        catch (Exception e)
        {
            manager.Logger.LogError("[{Endpoint}] SendJoin: {Message}", manager.LocalEndpoint, e.Message);
        }

        return new JoinResponse(false);
    }

    /// <summary>
    /// Signs <paramref name="payload"/> — the exact bytes that will be sent as the request body — and
    /// returns the authentication headers, or an empty map when node authentication is not in shared-secret
    /// mode.
    /// </summary>
    /// <remarks>
    /// Takes bytes rather than a string because the signature must cover what the peer receives. The
    /// caller serializes once, straight to UTF-8, and hands the same buffer here and to the HTTP content;
    /// a second serialization for signing could produce a signature over bytes that were never sent, and
    /// every request would then fail authentication.
    /// </remarks>
    internal static IReadOnlyDictionary<string, string> BuildAuthenticationHeaders(
        RaftConfiguration configuration,
        string senderNode,
        string method,
        string path,
        ReadOnlySpan<byte> payload)
    {
        RaftTransportAuthenticator authenticator = configuration.GetTransportAuthenticator();

        if (authenticator.Options.NodeAuthenticationMode != RaftNodeAuthenticationMode.SharedSecret)
            return EmptyHeaders;

        // Digest here and sign the digest: SignWithBodyHash exists so a transport that already holds its
        // payload as bytes does not hand over a byte[] copy of it.
        Span<byte> bodyHash = stackalloc byte[RaftTransportAuthenticator.BodyHashSizeInBytes];
        SHA256.HashData(payload, bodyHash);

        RaftTransportAuthenticationHeaders authHeaders = authenticator.SignWithBodyHash(
            method,
            path,
            senderNode,
            bodyHash);

        return new Dictionary<string, string>
        {
            [authHeaders.SignatureHeaderName] = authHeaders.Signature,
            [RaftTransportAuthenticationHeaders.SenderNodeHeaderName] = authHeaders.SenderNode,
            [RaftTransportAuthenticationHeaders.TimestampHeaderName] =
                authHeaders.TimestampUnixMilliseconds.ToString(),
            [RaftTransportAuthenticationHeaders.NonceHeaderName] = authHeaders.Nonce
        };
    }

    /// <summary>
    /// Per-manager REST clients, configured from the manager's transport security settings.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Cluster traffic goes through these rather than Flurl's process-wide defaults. The certificate
    /// policy — presenting a client certificate under mTLS, pinning the peer's server certificate, or
    /// the development bypass that accepts anything — belongs to this cluster's configuration, and
    /// installing it on <c>FlurlHttp.Clients</c> applied it to every Flurl call anywhere in the
    /// process, including calls made by an application that merely hosts Kommander. The insecure
    /// bypass in particular then silently disabled certificate validation for unrelated HTTP clients.
    /// </para>
    /// <para>
    /// Owning it here also means an embedded host gets the same policy as <c>Kommander.Server</c>:
    /// previously the client certificate and pinning were wired up in that project's startup, so a
    /// library consumer using <see cref="RestCommunication"/> got neither.
    /// </para>
    /// <para>
    /// Keyed weakly so per-test managers do not leak clients. Security options and endpoints are
    /// immutable after <see cref="RaftManager"/> construction, which is the same contract the gRPC
    /// channel pool's per-manager caches rely on.
    /// </para>
    /// </remarks>
    private static readonly ConditionalWeakTable<RaftManager, IFlurlClientCache> clientsByManager = new();

    private static IFlurlClient GetClient(RaftManager manager, RaftNode node)
    {
        IFlurlClientCache cache = clientsByManager.GetValue(
            manager,
            static _ => new FlurlClientCache());

        RaftConfiguration configuration = manager.Configuration;
        string baseUrl = configuration.HttpScheme + node.Endpoint;

        return cache.GetOrAdd(
            node.Endpoint,
            baseUrl,
            builder => ConfigureClient(builder, configuration.GetEffectiveTransportSecurity()));
    }

    /// <summary>
    /// Applies this cluster's certificate policy to a REST client.
    /// </summary>
    /// <remarks>
    /// Presenting our certificate and validating theirs are independent, so the mTLS branch composes
    /// with whichever validation branch applies. The ordering of the validation branches matters:
    /// the insecure bypass wins over pinning, because an operator who set both has asked for the
    /// bypass explicitly and silently pinning instead would be a surprising override.
    /// </remarks>
    private static void ConfigureClient(IFlurlClientBuilder builder, RaftTransportSecurityOptions security)
    {
        builder.ConfigureInnerHandler(handler =>
        {
            if (security.NodeAuthenticationMode == RaftNodeAuthenticationMode.MutualTls)
            {
                X509Certificate2? clientCertificate = security.GetClientCertificate();

                if (clientCertificate is not null)
                    handler.ClientCertificates.Add(clientCertificate);
            }

            if (security.AllowInsecureCertificateValidation)
            {
                handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
                return;
            }

            if (security.TrustedServerCertificateThumbprints.Count > 0)
            {
                IReadOnlyCollection<string> thumbprints = security.TrustedServerCertificateThumbprints;

                handler.ServerCertificateCustomValidationCallback = (_, certificate, _, _) =>
                    RaftClientCertificateValidator.IsServerCertificateTrusted(
                        certificate,
                        thumbprints,
                        TimeProvider.System);
            }
        });
    }

    /// <summary>
    /// Builds a signed request against the per-manager client for <paramref name="node"/>.
    /// </summary>
    /// <remarks>
    /// Internal so the URL it composes can be asserted directly: routing moved from Flurl's
    /// string extensions (which resolve through the process-wide client cache) to a scoped client
    /// with a base URL, and a mistake there would silently retarget cluster traffic.
    /// </remarks>
    internal static IFlurlRequest CreateRaftRequest(
        RaftManager manager,
        RaftNode node,
        string path,
        ReadOnlySpan<byte> payload)
    {
        RaftConfiguration configuration = manager.Configuration;
        IFlurlRequest request = GetClient(manager, node)
            .Request(path.Trim('/').Split('/'))
            .WithHeader("Accept", "application/json")
            .WithHeader("Content-Type", "application/json")
            .WithTimeout(configuration.HttpTimeout)
            .WithSettings(o => o.HttpVersion = configuration.HttpVersion);

#pragma warning disable CS0618
        // Suppressed under MutualTls: the mode authenticates at the handshake and never reads this
        // header, so sending it would put a leftover legacy credential on every Raft request for no
        // benefit.
        if (!string.IsNullOrWhiteSpace(configuration.HttpAuthBearerToken)
            && configuration.TransportSecurity.NodeAuthenticationMode != RaftNodeAuthenticationMode.MutualTls)
        {
            request = request.WithOAuthBearerToken(configuration.HttpAuthBearerToken);
        }
#pragma warning restore CS0618

        foreach ((string headerName, string headerValue) in BuildAuthenticationHeaders(
                     configuration,
                     manager.LocalEndpoint,
                     "POST",
                     path,
                     payload))
        {
            request = request.WithHeader(headerName, headerValue);
        }

        return request;
    }

    /// <summary>
    /// Wraps an already-serialized UTF-8 JSON body as HTTP content.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every REST method now serializes once, directly to UTF-8, and sends those exact bytes. The
    /// previous shape encoded the same body three times: once into a UTF-16 string, once into a byte
    /// array for signing, and once again when the string was written to the wire. Snapshot chunks and
    /// log batches travel as base64 text inside the JSON, so that middle representation was the largest
    /// single allocation on the REST replication path.
    /// </para>
    /// <para>
    /// The content type is set on the content itself rather than left to the request-level header, so the
    /// media type does not depend on how Flurl reconciles a content header with a request header. The
    /// server binds these bodies as JSON and answers 415 for anything else, so the value is part of the
    /// contract, not a formality.
    /// </para>
    /// <para>
    /// The array is owned by the content and stays alive and unmodified for the send and any retry, so no
    /// pooled buffer is used here: a returned buffer could be handed to another caller while a retry still
    /// holds it.
    /// </para>
    /// </remarks>
    private static ByteArrayContent JsonContent(byte[] payload)
    {
        ByteArrayContent content = new(payload);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        return content;
    }
}
