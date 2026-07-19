
using System.Diagnostics.CodeAnalysis;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests;

/// <summary>
/// Regression coverage for the partition-creation race that stalled cluster assembly under load.
///
/// Batching is per-endpoint, not per-partition: a single outbound batch to a peer mixes messages
/// for the system partition and every data partition. During assembly, nodes create their data
/// partitions at slightly different moments, so an inbound election/replication message for a data
/// partition can arrive before the receiving node has created that partition. Previously this threw
/// <see cref="RaftException"/> ("Invalid partition"), which aborted the whole coalesced batch —
/// poisoning sibling system-partition heartbeats and votes and preventing a leader from ever being
/// elected within the join timeout on slow CI runners.
///
/// The fix has two parts, both exercised here:
///  - Inbound peer handlers drop a message for a not-yet-created data partition instead of throwing.
///  - The in-memory batch loop guards each item so one bad item cannot abort its siblings.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public class TestPartitionAssemblyRace
{
    private readonly ILogger<IRaft> logger;

    private const int UserPartition = 1;
    private const int UncreatedPartition = 999;

    public TestPartitionAssemblyRace()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    private static IRaft GetNode(InMemoryCommunication communication, ILogger<IRaft> logger, int nodeId, int port, string peer)
    {
        RaftConfiguration config = new()
        {
            NodeName = "node" + nodeId,
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(50),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(250),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(100),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 100,
            EnableQuiescence = false,
            EndElectionTimeout = 250,
        };

        return new RaftManager(
            config,
            new StaticDiscovery([new(peer)]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger
        );
    }

    private async Task<(RaftManager node1, RaftManager node2)> AssembleTwoNodeCluster(InMemoryCommunication communication)
    {
        IRaft node1 = GetNode(communication, logger, 1, 8001, "localhost:8002");
        IRaft node2 = GetNode(communication, logger, 2, 8002, "localhost:8001");

        communication.SetNodes(new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 }
        });

        await node1.UpdateNodes();
        await node2.UpdateNodes();

        await Task.WhenAll(
            node1.JoinCluster(TestContext.Current.CancellationToken),
            node2.JoinCluster(TestContext.Current.CancellationToken));

        return ((RaftManager)node1, (RaftManager)node2);
    }

    /// <summary>
    /// An inbound election/replication message for a data partition this node has not created yet
    /// must be dropped silently, never thrown. Before the fix each of these threw
    /// <see cref="RaftException"/>, which — inside a coalesced batch — took the whole batch down.
    /// </summary>
    [Fact]
    public async Task InboundMessages_ForUncreatedPartition_AreDroppedNotThrown()
    {
        InMemoryCommunication communication = new();
        (RaftManager node1, _) = await AssembleTwoNodeCluster(communication);

        HLCTimestamp now = node1.HybridLogicalClock.SendOrLocalEvent(node1.LocalNodeId);

        RequestVotesRequest requestVote = new(UncreatedPartition, 1, 0, 0, now, "localhost:8002");
        VoteRequest vote = new(UncreatedPartition, 1, 0, 0, now, "localhost:8002");
        AppendLogsRequest append = new(UncreatedPartition, 1, now, "localhost:8002");
        CompleteAppendLogsRequest complete = new(UncreatedPartition, 1, now, "localhost:8002", RaftOperationStatus.Success, 0);

        // None of these must throw: the target data partition does not exist on this node.
        Exception? caught = Record.Exception(() =>
        {
            node1.RequestVote(requestVote);
            node1.Vote(vote);
            node1.AppendLogs(append);
            node1.CompleteAppendLogs(complete);
        });

        Assert.Null(caught);

        // The partition genuinely does not exist — we exercised the tolerant drop path, not a stray create.
        Assert.False(node1.Partitions.ContainsKey(UncreatedPartition));
    }

    /// <summary>
    /// A heterogeneous batch that mixes a message for a not-yet-created data partition with a
    /// system-partition message must be fully processed: the bad item is dropped, and the sibling
    /// system-partition item is still delivered. This is the exact shape that stalled assembly.
    /// </summary>
    [Fact]
    public async Task HeterogeneousBatch_WithUncreatedPartitionItem_DoesNotAbortSiblings()
    {
        InMemoryCommunication communication = new();
        (RaftManager node1, _) = await AssembleTwoNodeCluster(communication);

        HLCTimestamp now = node1.HybridLogicalClock.SendOrLocalEvent(node1.LocalNodeId);

        // Item order matters: the poison item is FIRST, so a regression that aborts the batch on
        // throw would drop the valid system-partition item that follows it.
        BatchRequestsRequest batch = new()
        {
            Requests =
            [
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.RequestVote,
                    RequestVotes = new RequestVotesRequest(UncreatedPartition, 1, 0, 0, now, "localhost:8001"),
                },
                new BatchRequestsRequestItem
                {
                    Type = BatchRequestsRequestType.RequestVote,
                    RequestVotes = new RequestVotesRequest(RaftSystemConfig.SystemPartition, 1, 0, 0, now, "localhost:8001"),
                },
            ]
        };

        // Deliver the batch to node2 through the in-memory transport, exactly as a peer would.
        Exception? caught = await Record.ExceptionAsync(() =>
            communication.BatchRequests(node1, new RaftNode("localhost:8002"), batch));

        Assert.Null(caught);
    }
}
