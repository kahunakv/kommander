
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Tests.Scheduler;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Verifies Task 5 acceptance: <see cref="RaftPartitionStateMachine"/> is usable without Nixie.
/// </summary>
public class TestRaftPartitionStateMachine
{
    [Fact]
    public void CanInstantiateWithoutNixie()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();

        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(0L, sm.CurrentTerm);
    }

    [Fact]
    public void CheckTicketCompletion_UnknownTicket_ReturnsNotFound()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        (RaftProposalTicketState state, long commitIndex) = sm.CheckTicketCompletion(HLCTimestamp.Zero);

        Assert.Equal(RaftProposalTicketState.NotFound, state);
        Assert.Equal(-1L, commitIndex);
    }

    [Fact]
    public void ReceiveHandshake_RecordsRemoteIndexInStateMachine()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        sm.ReceiveHandshake(remoteNodeId: 2, endpoint: "node-b", remoteMaxLogId: 42);

        // Handshake is accepted without throwing; internal index tracking is private.
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    [Fact]
    public async Task ForceLeaderForTestingAsync_SingleNode_BecomesLeaderImmediately()
    {
        FakePartitionHost host = new() { NodesOverride = [] };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 7);

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Equal(host.LocalEndpoint, host.Leader);
        Assert.Collection(host.LeaderChanges,
            leader => Assert.Equal(string.Empty, leader),
            leader => Assert.Equal(host.LocalEndpoint, leader));
        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)7, reply.Id);
            Assert.Equal(RaftOperationStatus.Success, reply.Response.Status);
        });
    }

    [Fact]
    public async Task ForceLeaderForTestingAsync_OutdatedNode_ReturnsReplicationFailed()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        sm.ReceiveHandshake(remoteNodeId: 2, endpoint: "node-b", remoteMaxLogId: 42);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 9);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(string.Empty, host.Leader);
        Assert.Empty(host.LeaderChanges);
        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)9, reply.Id);
            Assert.Equal(RaftOperationStatus.ReplicationFailed, reply.Response.Status);
        });
    }

    [Fact]
    public async Task ForceLeaderForTestingAsync_MultiNode_StartsElectionAndReturnsPending()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 11);

        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.Equal(string.Empty, host.Leader);
        Assert.Single(host.LeaderChanges);
        Assert.Equal(string.Empty, host.LeaderChanges[0]);
        Assert.Contains(host.EnqueuedResponses, message => message.Type == RaftResponderRequestType.RequestVotes);
        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)11, reply.Id);
            Assert.Equal(RaftOperationStatus.Pending, reply.Response.Status);
        });
    }

    [Fact]
    public async Task ReceivedVoteAsync_FiveNodeCluster_DoesNotElectLeaderWithOnlyTwoVotes()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [new("node-b"), new("node-c"), new("node-d"), new("node-e")]
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 13);
        await sm.ReceivedVoteAsync("node-b", voteTerm: 1, remoteMaxLogId: 0);

        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.Equal(string.Empty, host.Leader);

        await sm.ReceivedVoteAsync("node-c", voteTerm: 1, remoteMaxLogId: 0);

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Equal(host.LocalEndpoint, host.Leader);
    }

    [Fact]
    public async Task StepDownAsync_Leader_BecomesFollowerAndSendsSingleNotice()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [],
            Leader = "node-a"
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 15);
        host.NodesOverride = [new("node-c"), new("node-b"), new("node-d"), new("node-e")];
        host.ClearObservations();
        sink.Completed.Clear();

        await sm.StepDownAsync(replyCorrelationId: 16);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(string.Empty, host.Leader);
        Assert.Collection(host.LeaderChanges, leader => Assert.Equal(string.Empty, leader));
        Assert.Collection(host.EnqueuedResponses, message =>
        {
            Assert.Equal("node-b", message.Endpoint);
            Assert.Equal(RaftResponderRequestType.StepDownNotice, message.Type);
        });
        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)16, reply.Id);
            Assert.Equal(RaftOperationStatus.Pending, reply.Response.Status);
        });
    }

    [Fact]
    public async Task ReceiveStepDownNoticeAsync_Follower_StartsImmediateElection()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ReceiveStepDownNoticeAsync(new StepDownNoticeRequest(
            host.PartitionId,
            term: 0,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            "node-z"));

        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.Equal(string.Empty, host.Leader);
        Assert.Contains(host.EnqueuedResponses, message => message.Type == RaftResponderRequestType.RequestVotes);
    }

    [Fact]
    public async Task TransferLeadershipAsync_Leader_WithUnknownTarget_ReturnsErrored()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [],
            Leader = "node-a"
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 17);
        host.NodesOverride = [new("node-b")];
        sink.Completed.Clear();

        await sm.TransferLeadershipAsync("node-z", replyCorrelationId: 18);

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)18, reply.Id);
            Assert.Equal(RaftOperationStatus.Errored, reply.Response.Status);
        });
    }

    [Fact]
    public async Task TransferLeadershipAsync_Leader_WithStaleTarget_ReturnsReplicationFailed()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [],
            Leader = "node-a"
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 19);
        host.NodesOverride = [new("node-b")];
        sink.Completed.Clear();

        await sm.TransferLeadershipAsync("node-b", replyCorrelationId: 20);

        Assert.Equal(RaftNodeState.Leader, sm.NodeState);
        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)20, reply.Id);
            Assert.Equal(RaftOperationStatus.ReplicationFailed, reply.Response.Status);
        });
    }

    [Fact]
    public async Task TransferLeadershipAsync_Leader_WithFreshTarget_StepsDownAndEnqueuesTransfer()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [],
            Leader = "node-a"
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 21);
        host.NodesOverride = [new("node-b")];
        sm.ReceiveHandshake(remoteNodeId: 2, endpoint: "node-b", remoteMaxLogId: 1);
        host.ClearObservations();
        sink.Completed.Clear();

        await sm.TransferLeadershipAsync("node-b", replyCorrelationId: 22);

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(string.Empty, host.Leader);
        Assert.Collection(host.LeaderChanges, leader => Assert.Equal(string.Empty, leader));
        Assert.Collection(host.EnqueuedResponses, message =>
        {
            Assert.Equal("node-b", message.Endpoint);
            Assert.Equal(RaftResponderRequestType.TransferLeadership, message.Type);
        });
        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)22, reply.Id);
            Assert.Equal(RaftOperationStatus.Pending, reply.Response.Status);
        });
    }

    [Fact]
    public async Task VoteAsync_AfterTransferLeadership_VotesForPreferredTarget()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [],
            Leader = "node-a"
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 23);
        host.NodesOverride = [new("node-b")];
        sm.ReceiveHandshake(remoteNodeId: 2, endpoint: "node-b", remoteMaxLogId: 1);
        host.ClearObservations();
        sink.Completed.Clear();

        await sm.TransferLeadershipAsync("node-b", replyCorrelationId: 24);
        host.ClearObservations();

        await sm.VoteAsync(
            new RaftNode("node-b"),
            voteTerm: 2,
            remoteMaxLogId: 1,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId));

        Assert.Collection(host.EnqueuedResponses, message =>
        {
            Assert.Equal("node-b", message.Endpoint);
            Assert.Equal(RaftResponderRequestType.Vote, message.Type);
        });
    }

    [Fact]
    public async Task ReceiveTransferLeadershipAsync_Follower_StartsImmediateElection()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ReceiveTransferLeadershipAsync(new TransferLeadershipRequest(
            host.PartitionId,
            term: 0,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            endpoint: "node-z",
            targetEndpoint: host.LocalEndpoint));

        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.Equal(string.Empty, host.Leader);
        Assert.Contains(host.EnqueuedResponses, message => message.Type == RaftResponderRequestType.RequestVotes);
    }

    [Fact]
    public async Task ReceiveTransferLeadershipAsync_WrongTarget_IgnoresRequest()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ReceiveTransferLeadershipAsync(new TransferLeadershipRequest(
            host.PartitionId,
            term: 0,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            endpoint: "node-z",
            targetEndpoint: "node-b"));

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(string.Empty, host.Leader);
        Assert.Empty(host.EnqueuedResponses);
    }

    [Fact]
    public async Task SuspendHeartbeatsAsync_Leader_SuppressesPeriodicHeartbeats()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [],
            Leader = "node-a",
            HeartbeatIntervalOverride = TimeSpan.FromMilliseconds(1)
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 25);
        host.NodesOverride = [new("node-b")];
        host.ClearObservations();
        sink.Completed.Clear();

        await sm.SuspendHeartbeatsAsync(replyCorrelationId: 26);
        await WaitForHeartbeatWindow(host.Configuration.HeartbeatInterval);
        await sm.CheckPartitionLeadershipAsync();

        Assert.Empty(host.EnqueuedResponses);
        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)26, reply.Id);
            Assert.Equal(RaftOperationStatus.Success, reply.Response.Status);
        });
    }

    [Fact]
    public async Task SuspendHeartbeatsAsync_Follower_ReturnsNodeIsNotLeader_AndDoesNotPoisonFutureLeadership()
    {
        FakePartitionHost host = new()
        {
            Leader = "node-b",
            NodesOverride = [],
            HeartbeatIntervalOverride = TimeSpan.FromMilliseconds(1)
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.SuspendHeartbeatsAsync(replyCorrelationId: 30);

        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)30, reply.Id);
            Assert.Equal(RaftOperationStatus.NodeIsNotLeader, reply.Response.Status);
        });

        host.Leader = "node-a";
        host.ClearObservations();
        sink.Completed.Clear();

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 31);
        host.NodesOverride = [new("node-b")];
        host.ClearObservations();
        sink.Completed.Clear();

        await WaitForHeartbeatWindow(host.Configuration.HeartbeatInterval);
        await sm.CheckPartitionLeadershipAsync();

        Assert.Collection(host.EnqueuedResponses, message =>
        {
            Assert.Equal("node-b", message.Endpoint);
            Assert.Equal(RaftResponderRequestType.AppendLogs, message.Type);
        });
    }

    [Fact]
    public async Task ResumeHeartbeatsAsync_Leader_SendsForcedHeartbeatImmediately()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [],
            Leader = "node-a"
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 27);
        host.NodesOverride = [new("node-b")];
        host.ClearObservations();
        sink.Completed.Clear();

        await sm.SuspendHeartbeatsAsync(replyCorrelationId: 28);
        host.ClearObservations();
        sink.Completed.Clear();

        await sm.ResumeHeartbeatsAsync(replyCorrelationId: 29);

        Assert.Collection(host.EnqueuedResponses, message =>
        {
            Assert.Equal("node-b", message.Endpoint);
            Assert.Equal(RaftResponderRequestType.AppendLogs, message.Type);
        });
        Assert.Collection(sink.Completed, reply =>
        {
            Assert.Equal((ulong)29, reply.Id);
            Assert.Equal(RaftOperationStatus.Success, reply.Response.Status);
        });
    }

    private static async Task WaitForHeartbeatWindow(TimeSpan interval)
    {
        TimeSpan delay = interval + TimeSpan.FromMilliseconds(5);
        if (delay < TimeSpan.FromMilliseconds(5))
            delay = TimeSpan.FromMilliseconds(5);

        await Task.Delay(delay);
    }

    private sealed class FakePartitionHost : IRaftPartitionHost
    {
        public int PartitionId => 1;

        public string Leader { get; set; } = "";

        public string LocalEndpoint => "node-a";

        public int LocalNodeId => 1;

        public RaftConfiguration Configuration { get; } = new()
        {
            Host = "localhost",
            Port = 8001,
            InitialPartitions = 1,
        };

        public TimeSpan? HeartbeatIntervalOverride
        {
            set
            {
                if (value is not null)
                    Configuration.HeartbeatInterval = value.Value;
            }
        }

        public HybridLogicalClock HybridLogicalClock { get; } = new();

        public IReadOnlyList<RaftNode> Nodes => NodesOverride;

        public IReadOnlyList<RaftNode> NodesOverride { get; set; } = [new("node-b")];

        public List<string> LeaderChanges { get; } = [];

        public List<(string Endpoint, RaftResponderRequestType Type)> EnqueuedResponses { get; } = [];

        public void ClearObservations()
        {
            LeaderChanges.Clear();
            EnqueuedResponses.Clear();
        }

        public HLCTimestamp GetLastNodeActivity(string endpoint) => HLCTimestamp.Zero;

        public HLCTimestamp GetLastNodeHearthbeat(string endpoint) => HLCTimestamp.Zero;

        public void UpdateLastHeartbeat(string endpoint, HLCTimestamp timestamp) { }

        public void UpdateLastNodeActivity(string endpoint, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request) => EnqueuedResponses.Add((endpoint, request.Type));

        public Task InvokeLeaderChanged(int partitionId, string leader)
        {
            LeaderChanges.Add(leader);
            return Task.CompletedTask;
        }

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public void InvokeReplicationError(int partitionId, RaftLog log) { }
    }

    private sealed class FakeWalFacade : IRaftWalFacade
    {
        private readonly FakeWAL wal = new();

        public ValueTask<IReadOnlyList<RaftLog>> LoadRestoreLogsAsync()
        {
            wal.Write([(1, [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed }])]);
            wal.DrainAll();
            IReadOnlyList<RaftLog> logs = [new RaftLog { Id = 1, Term = 1, Type = RaftLogType.Committed }];
            return ValueTask.FromResult(logs);
        }

        public ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs) => ValueTask.CompletedTask;

        public ValueTask<long> GetMaxLogAsync() => ValueTask.FromResult(wal.GetMaxLog(partitionId: 1));

        public ValueTask<long> GetCurrentTermAsync() => ValueTask.FromResult(wal.GetCurrentTerm(partitionId: 1));

        public WALWriteOperation EnqueuePropose(long term, List<RaftLog> logs, HLCTimestamp timestamp, bool autoCommit) =>
            new(null!, 1, WALWriteOperationType.LeaderPropose, (1, logs), timestamp, autoCommit: autoCommit, term: term);

        public WALWriteOperation EnqueueCommit(List<RaftLog> logs) =>
            new(null!, 2, WALWriteOperationType.LeaderCommit, (1, logs));

        public WALWriteOperation EnqueueRollback(List<RaftLog> logs) =>
            new(null!, 3, WALWriteOperationType.LeaderRollback, (1, logs));

        public WALWriteOperation? EnqueueProposeOrCommit(List<RaftLog>? logs, HLCTimestamp timestamp = default, string? endpoint = null, long term = -1) =>
            logs is null ? null : EnqueuePropose(term, logs, timestamp, autoCommit: false);

        public void NotifyCommitted() { }
    }

    private sealed class CapturingReplySink : IRaftOperationReplySink
    {
        public List<(ulong Id, RaftResponse Response)> Completed { get; } = [];

        public void TryComplete(ulong correlationId, RaftResponse response) =>
            Completed.Add((correlationId, response));
    }
}
