
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
/// Integration coverage for the heterogeneous write-coalescing surface
/// (<see cref="IRaft.ReplicateEntries"/>), slices 1-3: an auto-commit prefix plus an optional single trailing
/// manual group, with a per-entry generation fence. Runs on a real two-node in-memory cluster so the follower
/// actually receives the coalesced batch and restores each entry by its own <see cref="RaftLog.LogType"/>, the
/// manual ticket's commit/rollback lifecycle is exercised end-to-end, and per-entry fencing is verified to
/// drop stale entries while their siblings commit.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
[Collection(ClusterIntegrationCollection.Name)]
public class TestReplicateEntries
{
    private readonly ILogger<IRaft> logger;

    private const int UserPartition = 1;

    public TestReplicateEntries()
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        logger = loggerFactory.CreateLogger<IRaft>();
    }

    private static RaftConfiguration NodeConfig(string name, int id, int port) => new()
    {
        NodeName = name,
        NodeId = id,
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

    private IRaft NewNode(InMemoryCommunication communication, string name, int id, int port, string peer) =>
        new RaftManager(
            NodeConfig(name, id, port),
            new StaticDiscovery([new(peer)]),
            new InMemoryWAL(logger),
            communication,
            new HybridLogicalClock(),
            logger);

    private async Task<(IRaft node1, IRaft node2)> AssembleTwoNodeCluster(InMemoryCommunication communication)
    {
        IRaft node1 = NewNode(communication, "node1", 1, 8001, "localhost:8002");
        IRaft node2 = NewNode(communication, "node2", 2, 8002, "localhost:8001");

        communication.SetNodes(new()
        {
            { "localhost:8001", node1 },
            { "localhost:8002", node2 }
        });

        await node1.UpdateNodes();
        await node2.UpdateNodes();

        await Task.WhenAll(node1.JoinCluster(TestContext.Current.CancellationToken), node2.JoinCluster(TestContext.Current.CancellationToken));

        return (node1, node2);
    }

    private static async Task<IRaft> GetLeaderAsync(int partitionId, IRaft[] nodes)
    {
        for (int attempt = 0; attempt < 200; attempt++)
        {
            foreach (IRaft node in nodes)
            {
                if (await node.AmILeaderQuick(partitionId).ConfigureAwait(false))
                    return node;
            }

            await Task.Delay(10).ConfigureAwait(false);
        }

        throw new InvalidOperationException($"No leader elected for partition {partitionId}");
    }

    private static IRaft Follower(IRaft leader, IRaft node1, IRaft node2) =>
        leader.GetLocalEndpoint() == node1.GetLocalEndpoint() ? node2 : node1;

    /// <summary>
    /// A heterogeneous auto-commit batch commits as one proposal: every entry succeeds, the per-entry
    /// <see cref="RaftEntryResult.LogIndex"/> values are index-aligned to input and contiguous, tickets are
    /// <see cref="HLCTimestamp.Zero"/>, and the follower restores each entry under its own type.
    /// </summary>
    [Fact]
    public async Task HeterogeneousAutoCommitBatch_CommitsAsOneProposal_RestoresByTypeOnFollower()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);
        IRaft follower = Follower(leader, node1, node2);

        long leaderMaxBefore = leader.WalAdapter.GetMaxLog(UserPartition);

        RaftProposalEntry[] entries =
        [
            new("kv", [1], AutoCommit: true),
            new("lock", [2], AutoCommit: true),
            new("receipt", [3], AutoCommit: true),
            new("kv", [4], AutoCommit: true),
        ];

        RaftBatchReplicationResult result = await leader.ReplicateEntries(
            UserPartition, entries, TestContext.Current.CancellationToken);

        Assert.True(result.Success);
        Assert.Equal(RaftOperationStatus.Success, result.Status);
        Assert.Equal(entries.Length, result.Entries.Count);

        // Per-entry results: all success, contiguous ascending indices, no caller-held ticket.
        for (int i = 0; i < entries.Length; i++)
        {
            Assert.Equal(RaftOperationStatus.Success, result.Entries[i].Status);
            Assert.Equal(HLCTimestamp.Zero, result.Entries[i].Ticket);
            if (i > 0)
                Assert.Equal(result.Entries[i - 1].LogIndex + 1, result.Entries[i].LogIndex);
        }

        // One proposal: the whole batch advanced the leader's log by exactly its size, and the last
        // per-entry index is the new max.
        long leaderMaxAfter = leader.WalAdapter.GetMaxLog(UserPartition);
        Assert.Equal(leaderMaxBefore + entries.Length, leaderMaxAfter);
        Assert.Equal(leaderMaxAfter, result.Entries[^1].LogIndex);

        // The follower receives the coalesced batch and restores each entry under its own type.
        await WaitForAsync(() => follower.WalAdapter.GetMaxLog(UserPartition) >= leaderMaxAfter);

        List<RaftLog> followerLogs = follower.WalAdapter.ReadLogs(UserPartition);
        foreach (RaftProposalEntry entry in entries)
        {
            Assert.Contains(followerLogs, l =>
                l.LogType == entry.Type && l.LogData is not null && l.LogData.Length == 1 && l.LogData[0] == entry.Data[0]);
        }

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// A trailing manual group is valid (slice 2): the auto-commit prefix commits with the batch (Success,
    /// Ticket = Zero) while the trailing manual entries are Pending, share one non-zero ticket, and sit at the
    /// highest indices. Committing that ticket commits the manual suffix.
    /// </summary>
    [Fact]
    public async Task TrailingManualGroup_AutoCommits_ManualPending_ThenTicketCommits()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);

        RaftProposalEntry[] entries =
        [
            new("kv", [1], AutoCommit: true),
            new("lock", [2], AutoCommit: true),
            new("prepare", [3], AutoCommit: false),
            new("prepare", [4], AutoCommit: false),
        ];

        RaftBatchReplicationResult result = await leader.ReplicateEntries(
            UserPartition, entries, TestContext.Current.CancellationToken);

        Assert.True(result.Success);
        Assert.Equal(RaftOperationStatus.Success, result.Status);

        // Auto prefix: committed, no caller-held ticket.
        Assert.Equal(RaftOperationStatus.Success, result.Entries[0].Status);
        Assert.Equal(RaftOperationStatus.Success, result.Entries[1].Status);
        Assert.Equal(HLCTimestamp.Zero, result.Entries[0].Ticket);
        Assert.Equal(HLCTimestamp.Zero, result.Entries[1].Ticket);

        // Manual suffix: Pending, one shared non-zero ticket, highest indices.
        HLCTimestamp manualTicket = result.Entries[2].Ticket;
        Assert.NotEqual(HLCTimestamp.Zero, manualTicket);
        Assert.Equal(RaftOperationStatus.Pending, result.Entries[2].Status);
        Assert.Equal(RaftOperationStatus.Pending, result.Entries[3].Status);
        Assert.Equal(manualTicket, result.Entries[3].Ticket);
        Assert.Equal(manualTicket, result.TicketId);

        // All four entries are contiguous with the manual group strictly after the auto group.
        for (int i = 1; i < entries.Length; i++)
            Assert.Equal(result.Entries[i - 1].LogIndex + 1, result.Entries[i].LogIndex);

        // Committing the manual ticket commits the suffix.
        (bool commitOk, RaftOperationStatus commitStatus, long commitLogId) =
            await leader.CommitLogs(UserPartition, manualTicket, TestContext.Current.CancellationToken);

        Assert.True(commitOk);
        Assert.Equal(RaftOperationStatus.Success, commitStatus);
        Assert.True(commitLogId >= result.Entries[^1].LogIndex);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// Rolling back the trailing manual ticket truncates only the manual suffix; the committed auto-commit
    /// prefix is untouched. This is the rollback-suffix safety that the trailing-group shape guarantees.
    /// </summary>
    [Fact]
    public async Task TrailingManualGroup_TicketRollsBack_LeavesAutoPrefixIntact()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);

        RaftProposalEntry[] entries =
        [
            new("kv", [1], AutoCommit: true),
            new("prepare", [2], AutoCommit: false),
        ];

        RaftBatchReplicationResult result = await leader.ReplicateEntries(
            UserPartition, entries, TestContext.Current.CancellationToken);

        Assert.True(result.Success);
        long autoIndex = result.Entries[0].LogIndex;
        long manualIndex = result.Entries[1].LogIndex;
        HLCTimestamp manualTicket = result.Entries[1].Ticket;

        Assert.Equal(manualIndex, leader.WalAdapter.GetMaxLog(UserPartition)); // manual proposed at the tail

        (bool rollbackOk, RaftOperationStatus rollbackStatus, _) =
            await leader.RollbackLogs(UserPartition, manualTicket, TestContext.Current.CancellationToken);

        Assert.True(rollbackOk);
        Assert.Equal(RaftOperationStatus.Success, rollbackStatus);

        // Rolling back the manual ticket marks its suffix entry RolledBack; the committed auto prefix is
        // untouched. (Leader-side rollback marks the uncommitted suffix rather than deleting it; compaction
        // reclaims it later.)
        await WaitForAsync(() =>
        {
            foreach (RaftLog l in leader.WalAdapter.ReadLogs(UserPartition))
                if (l.Id == manualIndex && l.Type == RaftLogType.RolledBack)
                    return true;
            return false;
        });

        List<RaftLog> logs = leader.WalAdapter.ReadLogs(UserPartition);
        Assert.Contains(logs, l => l.Id == autoIndex && l.LogType == "kv" && l.Type == RaftLogType.Committed);
        Assert.Contains(logs, l => l.Id == manualIndex && l.Type == RaftLogType.RolledBack);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// An auto-commit entry positioned after a manual entry is a batch-level rejection: the manual entries
    /// would not form a single contiguous truncatable suffix. Nothing is appended.
    /// </summary>
    [Theory]
    [InlineData(true)]   // [manual, auto]
    [InlineData(false)]  // [auto, manual, auto]
    public async Task AutoCommitAfterManual_RejectedAtBatchLevel_NothingAppended(bool manualFirst)
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);
        long maxBefore = leader.WalAdapter.GetMaxLog(UserPartition);

        RaftProposalEntry[] entries = manualFirst
            ?
            [
                new("prepare", [1], AutoCommit: false),
                new("kv", [2], AutoCommit: true),
            ]
            :
            [
                new("kv", [1], AutoCommit: true),
                new("prepare", [2], AutoCommit: false),
                new("kv", [3], AutoCommit: true),
            ];

        RaftBatchReplicationResult result = await leader.ReplicateEntries(
            UserPartition, entries, TestContext.Current.CancellationToken);

        Assert.False(result.Success);
        Assert.Equal(RaftOperationStatus.Errored, result.Status);
        Assert.Equal(HLCTimestamp.Zero, result.TicketId);
        Assert.All(result.Entries, e =>
        {
            Assert.Equal(RaftOperationStatus.Errored, e.Status);
            Assert.Equal(-1, e.LogIndex);
        });

        Assert.Equal(maxBefore, leader.WalAdapter.GetMaxLog(UserPartition));

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// Per-entry fence (slice 3): a batch that mixes an unfenced (generation-0) entry with a stale-generation
    /// entry appends the unfenced one, reports <see cref="RaftOperationStatus.PartitionMoved"/> only in the
    /// stale entry's slot, keeps overall status Success, and leaves no orphan. (The user partition sits at
    /// generation 0, so any non-zero <c>ExpectedGeneration</c> is a fence miss.)
    /// </summary>
    [Fact]
    public async Task PerEntryFence_MixesAdmittedAndFenced_OverallSuccess()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);
        long maxBefore = leader.WalAdapter.GetMaxLog(UserPartition);

        RaftProposalEntry[] entries =
        [
            new("kv", [1], AutoCommit: true, ExpectedGeneration: 0),        // unfenced → admitted
            new("kv", [2], AutoCommit: true, ExpectedGeneration: 5),        // stale → fenced
            new("kv", [3], AutoCommit: true, ExpectedGeneration: 0),        // unfenced → admitted
        ];

        RaftBatchReplicationResult result = await leader.ReplicateEntries(
            UserPartition, entries, TestContext.Current.CancellationToken);

        Assert.True(result.Success);
        Assert.Equal(RaftOperationStatus.Success, result.Status);

        Assert.Equal(RaftOperationStatus.Success, result.Entries[0].Status);
        Assert.True(result.Entries[0].LogIndex > 0);

        // Only the stale entry is fenced; its slot is PartitionMoved with no index.
        Assert.Equal(RaftOperationStatus.PartitionMoved, result.Entries[1].Status);
        Assert.Equal(-1, result.Entries[1].LogIndex);

        Assert.Equal(RaftOperationStatus.Success, result.Entries[2].Status);
        Assert.True(result.Entries[2].LogIndex > 0);

        // Exactly the two admitted entries were appended (contiguous), the fenced one left no orphan.
        Assert.Equal(result.Entries[0].LogIndex + 1, result.Entries[2].LogIndex);
        Assert.Equal(maxBefore + 2, leader.WalAdapter.GetMaxLog(UserPartition));

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// Per-entry fence interleaved with the auto/manual split: a fenced entry between an admitted auto entry
    /// and an admitted manual entry is dropped without disturbing either surviving group. Index alignment to
    /// input is preserved.
    /// </summary>
    [Fact]
    public async Task PerEntryFence_InterleavedWithManualGroup()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);

        RaftProposalEntry[] entries =
        [
            new("kv", [1], AutoCommit: true, ExpectedGeneration: 0),        // admitted auto
            new("kv", [2], AutoCommit: true, ExpectedGeneration: 7),        // fenced (stale)
            new("prepare", [3], AutoCommit: false, ExpectedGeneration: 0),  // admitted manual
        ];

        RaftBatchReplicationResult result = await leader.ReplicateEntries(
            UserPartition, entries, TestContext.Current.CancellationToken);

        Assert.True(result.Success);
        Assert.Equal(RaftOperationStatus.Success, result.Entries[0].Status);
        Assert.Equal(HLCTimestamp.Zero, result.Entries[0].Ticket);

        Assert.Equal(RaftOperationStatus.PartitionMoved, result.Entries[1].Status);
        Assert.Equal(-1, result.Entries[1].LogIndex);

        Assert.Equal(RaftOperationStatus.Pending, result.Entries[2].Status);
        Assert.NotEqual(HLCTimestamp.Zero, result.Entries[2].Ticket);
        Assert.Equal(result.Entries[2].Ticket, result.TicketId);

        // The admitted manual entry sits above the admitted auto entry (the fenced middle entry took no index).
        Assert.True(result.Entries[2].LogIndex > result.Entries[0].LogIndex);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// When every entry is fenced out (all carry a stale non-zero generation), nothing is appended and the
    /// batch reports <see cref="RaftOperationStatus.PartitionMoved"/> overall so the caller refreshes the map.
    /// </summary>
    [Fact]
    public async Task PerEntryFence_AllFenced_ReturnsPartitionMoved_NothingAppended()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);
        long maxBefore = leader.WalAdapter.GetMaxLog(UserPartition);

        RaftProposalEntry[] entries =
        [
            new("kv", [1], AutoCommit: true, ExpectedGeneration: 3),
            new("kv", [2], AutoCommit: true, ExpectedGeneration: 4),
        ];

        RaftBatchReplicationResult result = await leader.ReplicateEntries(
            UserPartition, entries, TestContext.Current.CancellationToken);

        Assert.False(result.Success);
        Assert.Equal(RaftOperationStatus.PartitionMoved, result.Status);
        Assert.Equal(HLCTimestamp.Zero, result.TicketId);
        Assert.All(result.Entries, e =>
        {
            Assert.Equal(RaftOperationStatus.PartitionMoved, e.Status);
            Assert.Equal(-1, e.LogIndex);
        });
        Assert.Equal(maxBefore, leader.WalAdapter.GetMaxLog(UserPartition));

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>An empty batch is a success no-op with no per-entry results.</summary>
    [Fact]
    public async Task EmptyBatch_SucceedsWithNoEntries()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft leader = await GetLeaderAsync(UserPartition, [node1, node2]);

        RaftBatchReplicationResult result = await leader.ReplicateEntries(
            UserPartition, [], TestContext.Current.CancellationToken);

        Assert.True(result.Success);
        Assert.Equal(RaftOperationStatus.Success, result.Status);
        Assert.Empty(result.Entries);

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    /// <summary>
    /// The reserved system log type remains forbidden on the system partition through the batch surface,
    /// exactly as on the single-type <c>ReplicateLogs</c> path.
    /// </summary>
    [Fact]
    public async Task ReservedSystemType_OnSystemPartition_Throws()
    {
        InMemoryCommunication communication = new();
        (IRaft node1, IRaft node2) = await AssembleTwoNodeCluster(communication);

        IRaft p0Leader = await GetLeaderAsync(RaftSystemConfig.SystemPartition, [node1, node2]);

        RaftProposalEntry[] entries =
        [
            new(RaftSystemConfig.RaftLogType, [1], AutoCommit: true),
        ];

        await Assert.ThrowsAsync<RaftException>(() =>
            p0Leader.ReplicateEntries(RaftSystemConfig.SystemPartition, entries, TestContext.Current.CancellationToken));

        await node1.LeaveCluster(true, CancellationToken.None);
        await node2.LeaveCluster(true, CancellationToken.None);
    }

    private static async Task WaitForAsync(Func<bool> condition, int attempts = 200, int delayMs = 10)
    {
        for (int i = 0; i < attempts; i++)
        {
            if (condition())
                return;

            await Task.Delay(delayMs).ConfigureAwait(false);
        }

        Assert.True(condition(), "Condition not met within the allotted time");
    }
}
