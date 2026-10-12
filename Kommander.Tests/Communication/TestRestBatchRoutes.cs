using System.Net;
using System.Net.Http.Json;
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Communication;

/// <summary>
/// The REST routes that carry batched and auxiliary traffic a single request at a time: the
/// leader lookup a joining node bootstraps from, the append/completion batch loops whose items
/// must each be dropped-or-applied independently, and the batch-requests surface where an item
/// the route cannot dispatch must not abort the items behind it — including advisory items
/// (step-down notices, transfers, suggestions, re-seed asks) that a follower answers with
/// nothing and a leader answers only for members of the roster.
/// </summary>
[Collection(ClusterIntegrationCollection.Name)]
public class TestRestBatchRoutes
{
    private static ILoggerFactory CreateLoggerFactory() =>
        LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

    /// <summary>
    /// The leader endpoint a node reports over HTTP must be the cluster's actual elected leader
    /// for the partition — the answer a joining node uses to route its first system requests.
    /// Ports 9790-9792.
    /// </summary>
    [Fact]
    public async Task GetLeaderRoute_ReportsTheElectedLeaderEndpoint()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync([9790, 9791, 9792], logFactory, partitions: 1, ct: ct);

        RestTestNode leader = await AwaitLeaderAsync(harness, ct);

        using HttpClient client = new();
        // The route answers a bare endpoint string, not a JSON document.
        string reported = await client.GetStringAsync($"http://{leader.Endpoint}/v1/raft/get-leader/0", ct);

        Assert.Equal(leader.Endpoint, reported);
    }

    /// <summary>
    /// The batched append and completion loops answer successfully whether the batch is empty,
    /// or names only partitions the receiver does not host — each item dropped silently, the
    /// route never faulting, and the hosted partitions untouched.
    /// Ports 9793-9795.
    /// </summary>
    [Fact]
    public async Task AppendAndCompletionBatches_ForUnhostedPartitions_AnswerWithoutFaulting()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync([9793, 9794, 9795], logFactory, partitions: 1, ct: ct);

        RestTestNode leader = await AwaitLeaderAsync(harness, ct);
        long termBefore = await PartitionTermAsync(leader, ct);

        using HttpClient client = new();
        string baseAddress = $"http://{leader.Endpoint}";

        using (HttpResponseMessage empty = await client.PostAsJsonAsync(
                   $"{baseAddress}/v1/raft/append-logs-batch", new AppendLogsBatchRequest { AppendLogs = null }, ct))
        {
            Assert.Equal(HttpStatusCode.OK, empty.StatusCode);
        }

        AppendLogsBatchRequest unhostedAppends = new()
        {
            AppendLogs =
            [
                new AppendLogsRequest(4242, 1, default, leader.Endpoint, []),
                new AppendLogsRequest(4243, 1, default, leader.Endpoint, []),
            ],
        };

        using (HttpResponseMessage appended = await client.PostAsJsonAsync($"{baseAddress}/v1/raft/append-logs-batch", unhostedAppends, ct))
        {
            Assert.Equal(HttpStatusCode.OK, appended.StatusCode);
        }

        CompleteAppendLogsBatchRequest completions = new()
        {
            CompleteLogs =
            [
                new CompleteAppendLogsRequest(4242, 1, default, leader.Endpoint, RaftOperationStatus.Success, 10),
                new CompleteAppendLogsRequest(4243, 1, default, leader.Endpoint, RaftOperationStatus.Success, 10),
            ],
        };

        using (HttpResponseMessage completed = await client.PostAsJsonAsync($"{baseAddress}/v1/raft/complete-append-logs-batch", completions, ct))
        {
            Assert.Equal(HttpStatusCode.OK, completed.StatusCode);
        }

        Assert.Equal(termBefore, await PartitionTermAsync(leader, ct));
    }

    /// <summary>
    /// A batch-requests frame whose first item is a type the route cannot dispatch must not
    /// abort the items behind it: the unsupported item is caught and logged, the advisory items
    /// (a step-down notice, a transfer, a suggestion, a re-seed ask — all from an endpoint the
    /// roster does not know) are all safely ignored by a follower, and a trailing valid
    /// handshake still runs. The cluster's leadership must be unchanged by the ghost step-down
    /// notice — the membership fence, not the route, is what holds it.
    /// Ports 9796-9798.
    /// </summary>
    [Fact]
    public async Task BatchRequests_UnsupportedAndAdvisoryItems_DoNotAbortTheBatchOrDisturbLeadership()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using ILoggerFactory logFactory = CreateLoggerFactory();

        await using RestClusterHarness harness = await RestClusterHarness.CreateAsync([9796, 9797, 9798], logFactory, partitions: 1, ct: ct);

        RestTestNode leader = await AwaitLeaderAsync(harness, ct);
        List<RestTestNode> followers = [.. harness.Nodes.Where(n => n != leader)];
        RestTestNode follower = followers[0];

        BatchRequestsRequest batch = new()
        {
            Requests =
            [
                // No dispatch exists for Ping on this route: the item must be caught, not fatal.
                new BatchRequestsRequestItem { Type = BatchRequestsRequestType.Ping },
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.StepDownNotice,
                    StepDownNotice = new StepDownNoticeRequest(0, 1, default, "ghost.nowhere:1"),
                },
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.TransferLeadership,
                    TransferLeadership = new TransferLeadershipRequest(0, 1, default, "ghost.nowhere:1", follower.Endpoint),
                },
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.TransferLeadershipSuggestion,
                    TransferLeadershipSuggestion = new TransferLeadershipSuggestionRequest(1, 1, default, "ghost.nowhere:1", follower.Endpoint),
                },
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.Reseed,
                    Reseed = new ReseedRequest(0, 1, default, "ghost.nowhere:1"),
                },
                // A valid trailing item proves the loop reached the end of the batch.
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.Handshake,
                    Handshake = new HandshakeRequest(1, 1, 0, follower.Endpoint),
                },
            ],
        };

        using HttpClient client = new();
        using HttpResponseMessage response = await client.PostAsJsonAsync(
            $"http://{follower.Endpoint}/v1/raft/batch-requests", batch, ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        await Task.Delay(300, ct);
        Assert.True(await leader.Manager.AmILeaderQuick(0), "a step-down notice from an endpoint outside the roster must not depose the leader");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static async Task<RestTestNode> AwaitLeaderAsync(RestClusterHarness harness, CancellationToken ct)
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(15));

        while (!cts.Token.IsCancellationRequested)
        {
            foreach (RestTestNode node in harness.Nodes)
            {
                try
                {
                    if (await node.Manager.AmILeaderQuick(0))
                        return node;
                }
                catch { /* node may not be ready yet */ }
            }

            await Task.Delay(200, cts.Token).ConfigureAwait(false);
        }

        throw new TimeoutException("no leader was elected for partition 0");
    }

    private static async Task<long> PartitionTermAsync(RestTestNode node, CancellationToken ct)
    {
        RaftPartitionView? view = null;

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        while (view is null && !cts.Token.IsCancellationRequested)
        {
            view = await node.Manager.GetPartitionViewAsync(0, cts.Token).ConfigureAwait(false);
            if (view is null)
                await Task.Delay(100, cts.Token).ConfigureAwait(false);
        }

        Assert.NotNull(view);
        return view.Term;
    }
}
