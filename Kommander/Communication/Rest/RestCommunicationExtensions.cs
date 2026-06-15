using System.Text;
using System.Text.Json;
using Kommander.Data;
using Kommander.Gossip;
using Kommander.System;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using WirePingRequest = Kommander.Data.PingRequest;
using WirePingResponse = Kommander.Data.PingResponse;
using WirePingReqRequest = Kommander.Data.PingReqRequest;
using WirePingReqResponse = Kommander.Data.PingReqResponse;

namespace Kommander.Communication.Rest;

public static class RestCommunicationExtensions
{
    public static void MapRestRaftRoutes(this WebApplication app)
    {
        app.UseWhen(
            context => context.Request.Path.StartsWithSegments("/v1/raft"),
            branch => branch.Use(async (context, next) =>
            {
                IRaft? raft = context.RequestServices.GetService(typeof(IRaft)) as IRaft;
                if (!await AuthorizeRequestAsync(context, raft).ConfigureAwait(false))
                    return;

                await next(context).ConfigureAwait(false);
            }));

        app.MapPost("/v1/raft/handshake", async (HandshakeRequest request, IRaft raft) =>
        {
            await raft.Handshake(request);
            if (raft is RaftManager manager)
                return manager.GetHandshakeResponse(request.Partition);
            return new HandshakeResponse();
        });
        
        app.MapPost("/v1/raft/append-logs", (AppendLogsRequest request, IRaft raft) =>
        {
            raft.AppendLogs(request);
            return new AppendLogsResponse();
        });
        
        app.MapPost("/v1/raft/append-logs-batch", (AppendLogsBatchRequest request, IRaft raft) =>
        {
            if (request.AppendLogs is not null)
            {
                foreach (AppendLogsRequest req in request.AppendLogs)
                    raft.AppendLogs(req);
            }

            return new AppendLogsBatchResponse();
        });
        
        app.MapPost("/v1/raft/complete-append-logs", (CompleteAppendLogsRequest request, IRaft raft) =>
        {
            raft.CompleteAppendLogs(request);

            return new AppendLogsResponse();
        });
        
        app.MapPost("/v1/raft/complete-append-logs-batch", (CompleteAppendLogsBatchRequest request, IRaft raft) =>
        {
            if (request.CompleteLogs is not null)
            {
                foreach (CompleteAppendLogsRequest req in request.CompleteLogs)
                    raft.CompleteAppendLogs(req);
            }

            return new CompleteAppendLogsBatchResponse();
        });
        
        app.MapPost("/v1/raft/batch-requests", async (BatchRequestsRequest request, IRaft raft) =>
        {
            if (request.Requests is not null)
            {
                foreach (BatchRequestsRequestItem item in request.Requests)
                {
                    switch (item.Type)
                    {
                        case BatchRequestsRequestType.Handshake:
                            await raft.Handshake(item.Handshake!);
                            break;
                        
                        case BatchRequestsRequestType.Vote:
                            raft.Vote(item.Vote!);
                            break;
                        
                        case BatchRequestsRequestType.RequestVote:
                            raft.RequestVote(item.RequestVotes!);
                            break;

                        case BatchRequestsRequestType.StepDownNotice:
                            if (raft is RaftManager manager)
                                manager.StepDownNotice(item.StepDownNotice!);
                            break;

                        case BatchRequestsRequestType.TransferLeadership:
                            if (raft is RaftManager transferManager)
                                transferManager.TransferLeadership(item.TransferLeadership!);
                            break;
                        
                        case BatchRequestsRequestType.AppendLogs:
                            raft.AppendLogs(item.AppendLogs!);
                            break;
                        
                        case BatchRequestsRequestType.CompleteAppendLogs:
                            raft.CompleteAppendLogs(item.CompleteAppendLogs!);
                            break;
                        
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            return new CompleteAppendLogsBatchResponse();
        });

        app.MapPost("/v1/raft/request-vote", (RequestVotesRequest request, IRaft raft) =>
        {
            raft.RequestVote(request);
            
            return new RequestVotesResponse();
        });

        app.MapPost("/v1/raft/vote", (VoteRequest request, IRaft raft) =>
        {
            raft.Vote(request);
            
            return new VoteResponse();
        });
        
        app.MapGet("/v1/raft/get-leader/{partitionId}", async (int partitionId, IRaft raft) =>
        {
            return await raft.WaitForLeader(partitionId, CancellationToken.None).ConfigureAwait(false);
        });

        app.MapPost("/v1/raft/leave", async (LeaveRequest request, IRaft raft, HttpContext httpContext) =>
        {
            if (raft is not RaftManager manager)
                return new LeaveResponse(false);

            return await manager.ReceiveLeave(request, httpContext.RequestAborted).ConfigureAwait(false);
        });

        app.MapPost("/v1/raft/get-follower-lag", async (GetFollowerLagRequest request, IRaft raft) =>
        {
            if (raft is not RaftManager manager)
                return new GetFollowerLagResponse(false);

            long? lag = await manager.GetFollowerLagAsync(request.PartitionId, request.FollowerEndpoint).ConfigureAwait(false);
            return lag.HasValue ? new GetFollowerLagResponse(true, lag.Value) : new GetFollowerLagResponse(false);
        });

        app.MapPost("/v1/raft/install-snapshot", async (SnapshotRequest request, IRaft raft) =>
        {
            if (raft is not RaftManager manager)
                return new SnapshotResponse(false);

            return await manager.ReceiveInstallSnapshot(request).ConfigureAwait(false);
        });

        app.MapPost("/v1/raft/gossip", (GossipRequest request, IRaft raft) =>
        {
            if (raft is not RaftManager manager)
                return new GossipResponse(0, null);

            ClusterMembership? roster = request.RosterJson is not null
                ? JsonSerializer.Deserialize<ClusterMembership>(request.RosterJson)
                : null;

            GossipMessage digest = new(request.SenderEndpoint, request.MembershipVersion, roster);
            GossipAck ack = manager.ReceiveGossip(digest);

            string? ackRosterJson = ack.Roster is not null
                ? JsonSerializer.Serialize(ack.Roster)
                : null;

            return new GossipResponse(ack.MembershipVersion, ackRosterJson);
        });

        app.MapPost("/v1/raft/ping", (WirePingRequest request, IRaft raft) =>
        {
            if (raft is not RaftManager manager)
                return new WirePingResponse(false, 0);

            Gossip.PingResponse resp = manager.ReceivePing(new Gossip.PingRequest(request.SenderEndpoint));
            return new WirePingResponse(resp.Alive, resp.Incarnation);
        });

        app.MapPost("/v1/raft/ping-req", async (WirePingReqRequest request, IRaft raft, HttpContext httpContext) =>
        {
            if (raft is not RaftManager manager)
                return new WirePingReqResponse(false);

            Gossip.PingReqResponse resp = await manager.ReceivePingReq(
                new Gossip.PingReqRequest(request.SenderEndpoint, request.TargetEndpoint),
                httpContext.RequestAborted).ConfigureAwait(false);

            return new WirePingReqResponse(resp.Reached);
        });
    }

    internal static async Task<RaftTransportAuthenticationResult> AuthenticateRequestAsync(
        HttpContext context,
        RaftConfiguration configuration)
    {
        RaftTransportSecurityOptions transportSecurity = configuration.GetEffectiveTransportSecurity();
        RaftTransportAuthenticator authenticator = new(transportSecurity);

        if (transportSecurity.NodeAuthenticationMode == RaftNodeAuthenticationMode.Disabled)
        {
            return new RaftTransportAuthenticationResult
            {
                Status = RaftTransportAuthenticationStatus.Disabled
            };
        }

        byte[] bodyBytes = await ReadRequestBodyAsync(context.Request).ConfigureAwait(false);

        context.Request.Headers.TryGetValue(transportSecurity.HeaderName, out var signatureValues);
        context.Request.Headers.TryGetValue(
            RaftTransportAuthenticationHeaders.SenderNodeHeaderName,
            out var senderNodeValues);
        context.Request.Headers.TryGetValue(
            RaftTransportAuthenticationHeaders.TimestampHeaderName,
            out var timestampValues);
        context.Request.Headers.TryGetValue(
            RaftTransportAuthenticationHeaders.NonceHeaderName,
            out var nonceValues);

        return authenticator.Validate(
            context.Request.Method,
            context.Request.Path.Value ?? string.Empty,
            bodyBytes,
            signatureValues.ToString(),
            senderNodeValues.ToString(),
            timestampValues.ToString(),
            nonceValues.ToString(),
            context.Request.IsHttps);
    }

    internal static async Task<bool> AuthorizeRequestAsync(HttpContext context, IRaft? raft)
    {
        if (raft is not RaftManager manager)
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return false;
        }

        RaftTransportAuthenticationResult authenticationResult =
            await AuthenticateRequestAsync(context, manager.Configuration).ConfigureAwait(false);

        if (authenticationResult.IsAuthenticated)
            return true;

        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        return false;
    }

    private static async Task<byte[]> ReadRequestBodyAsync(HttpRequest request)
    {
        request.EnableBuffering();

        using MemoryStream buffer = new();
        await request.Body.CopyToAsync(buffer).ConfigureAwait(false);
        request.Body.Position = 0;
        return buffer.ToArray();
    }
}
