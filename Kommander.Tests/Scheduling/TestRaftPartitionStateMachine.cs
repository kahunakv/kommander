
using Kommander.Data;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Tests.Scheduler;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// <see cref="RaftPartitionStateMachine"/> is usable without Nixie.
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
    public async Task ReceivedVoteAsync_LearnerInNodes_DoesNotInflateElectionQuorum()
    {
        // Three peers in Nodes, but only node-b/node-c are voters; node-d is a Learner that
        // receives replication yet must not count toward quorum. The election denominator must
        // be the 3 voters (self + b + c), so self-vote + one voter grant (2 of 3) wins — a
        // learner in Nodes must not push the quorum to 3-of-4.
        FakePartitionHost host = new()
        {
            NodesOverride = [new("node-b"), new("node-c"), new("node-d")],
            VoterEndpoints = ["node-a", "node-b", "node-c"]
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 21);
        await sm.ReceivedVoteAsync("node-b", voteTerm: 1, remoteMaxLogId: 0);

        // Quorum is 2-of-3 voters; the learner does not raise it to 3, so one grant elects us.
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

    // ── PreVote tests (Raft §9.6) ────────────────────────────────────────────

    /// <summary>
    /// A pre-vote ask from a candidate with a stale log must be denied, and answering it must not
    /// mutate any real state (term/votes/expected-leaders). Proven indirectly by then granting a
    /// *real* vote to a different candidate at the same term, which is only possible if the pre-vote
    /// left <c>expectedLeaders</c> untouched.
    /// </summary>
    [Fact]
    public async Task VoteAsync_PreVote_StaleLog_DeniesAndDoesNotMutateState()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // Seed a local log so localMaxId = 1, currentTerm = 1.
        IReadOnlyList<RaftLog> restored = await sm.StartRestoreAsync();
        await sm.CompleteRestoreAsync(restored);
        long termBefore = sm.CurrentTerm;
        host.ClearObservations();

        // Candidate's log (0) is behind ours (1): pre-vote must be denied with no reply.
        await sm.VoteAsync(
            new RaftNode("node-b"),
            voteTerm: 2,
            remoteMaxLogId: 0,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            preVote: true);

        Assert.Empty(host.EnqueuedRequests);
        Assert.Equal(termBefore, sm.CurrentTerm);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);

        // Proof that expectedLeaders was not poisoned: a real vote for a *different* up-to-date
        // candidate at the same term is still granted.
        await sm.VoteAsync(
            new RaftNode("node-c"),
            voteTerm: 2,
            remoteMaxLogId: 5,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId));

        Assert.Collection(host.EnqueuedRequests, entry =>
        {
            Assert.Equal("node-c", entry.Endpoint);
            Assert.Equal(RaftResponderRequestType.Vote, entry.Request.Type);
            Assert.False(entry.Request.VoteRequest!.PreVote);
        });
    }

    /// <summary>
    /// A pre-vote ask from an up-to-date candidate, with no fresh leader, must be granted with a
    /// <see cref="VoteRequest"/> carrying <c>PreVote=true</c> — without mutating any real state.
    /// </summary>
    [Fact]
    public async Task VoteAsync_PreVote_HealthyCandidate_GrantsWithoutMutatingState()
    {
        FakePartitionHost host = new();
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        long termBefore = sm.CurrentTerm;

        await sm.VoteAsync(
            new RaftNode("node-b"),
            voteTerm: 1,
            remoteMaxLogId: 0,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            preVote: true);

        // Exactly one pre-vote grant, and no real state changed.
        Assert.Collection(host.EnqueuedRequests, entry =>
        {
            Assert.Equal("node-b", entry.Endpoint);
            Assert.Equal(RaftResponderRequestType.Vote, entry.Request.Type);
            Assert.True(entry.Request.VoteRequest!.PreVote);
            Assert.Equal(1L, entry.Request.VoteRequest!.Term);
        });
        Assert.Equal(termBefore, sm.CurrentTerm);
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);

        // Proof the pre-vote did not record an expected leader: a real vote for a *different*
        // candidate at the same term is still granted.
        host.ClearObservations();
        await sm.VoteAsync(
            new RaftNode("node-c"),
            voteTerm: 1,
            remoteMaxLogId: 0,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId));

        Assert.Collection(host.EnqueuedRequests, entry =>
        {
            Assert.Equal("node-c", entry.Endpoint);
            Assert.Equal(RaftResponderRequestType.Vote, entry.Request.Type);
            Assert.False(entry.Request.VoteRequest!.PreVote);
        });
    }

    /// <summary>
    /// Reaching a pre-vote quorum (self-grant + one peer in a 3-node cluster) promotes to exactly
    /// one real election: the term increments once, the node becomes Candidate, real (non-pre-vote)
    /// RequestVotes go out, and the pre-vote round is reset so further pre-grants are ignored.
    /// </summary>
    [Fact]
    public async Task PreVoteQuorum_PromotesToExactlyOneRealElection()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [new("node-b"), new("node-c")] // 3-node cluster → pre-vote quorum = 2
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // Election timer fires on a follower with no fresh leader: opens a side-effect-free pre-vote.
        await sm.CheckPartitionLeadershipAsync();

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.Equal(0L, sm.CurrentTerm); // pre-vote must not bump the term
        Assert.Contains(host.EnqueuedRequests, e =>
            e.Request.Type == RaftResponderRequestType.RequestVotes && e.Request.RequestVotesRequest!.PreVote);

        host.ClearObservations();

        // Self already pre-granted; one peer pre-grant reaches quorum and promotes to a real election.
        await sm.ReceivedVoteAsync("node-b", voteTerm: 1, remoteMaxLogId: 0, preVote: true);

        Assert.Equal(1L, sm.CurrentTerm); // incremented exactly once
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.Contains(host.EnqueuedRequests, e =>
            e.Request.Type == RaftResponderRequestType.RequestVotes && !e.Request.RequestVotesRequest!.PreVote);

        // The round was reset: a late pre-grant is ignored and triggers no second election.
        host.ClearObservations();
        await sm.ReceivedVoteAsync("node-c", voteTerm: 1, remoteMaxLogId: 0, preVote: true);

        Assert.Equal(1L, sm.CurrentTerm);
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
        Assert.DoesNotContain(host.EnqueuedRequests, e => e.Request.Type == RaftResponderRequestType.RequestVotes);
    }

    // ── ElectionTimeoutSeed tests ────────────────────────────────────────────

    /// <summary>
    /// Two state machines on the same partition with the same seed must receive
    /// identical initial election timeouts.
    /// </summary>
    [Fact]
    public void ElectionTimeoutSeed_SamePartition_ProducesIdenticalTimeout()
    {
        const int seed = 42;

        FakePartitionHost hostA = new() { PartitionId = 5, Configuration = { ElectionTimeoutSeed = seed } };
        FakePartitionHost hostB = new() { PartitionId = 5, Configuration = { ElectionTimeoutSeed = seed } };

        RaftPartitionStateMachine smA = new(hostA, new FakeWalFacade(), new CapturingReplySink(), NullLogger<IRaft>.Instance);
        RaftPartitionStateMachine smB = new(hostB, new FakeWalFacade(), new CapturingReplySink(), NullLogger<IRaft>.Instance);

        Assert.Equal(smA.ElectionTimeout, smB.ElectionTimeout);
    }

    /// <summary>
    /// Two partitions with the same seed but different partition IDs must receive
    /// different timeouts so they do not start elections simultaneously and
    /// deadlock.
    /// </summary>
    [Fact]
    public void ElectionTimeoutSeed_DifferentPartitions_ProducesDifferentTimeouts()
    {
        const int seed = 42;

        FakePartitionHost hostA = new() { PartitionId = 1, Configuration = { ElectionTimeoutSeed = seed } };
        FakePartitionHost hostB = new() { PartitionId = 2, Configuration = { ElectionTimeoutSeed = seed } };

        RaftPartitionStateMachine smA = new(hostA, new FakeWalFacade(), new CapturingReplySink(), NullLogger<IRaft>.Instance);
        RaftPartitionStateMachine smB = new(hostB, new FakeWalFacade(), new CapturingReplySink(), NullLogger<IRaft>.Instance);

        Assert.NotEqual(smA.ElectionTimeout, smB.ElectionTimeout);
    }

    /// <summary>
    /// Two different seeds on the same partition must produce different timeouts.
    /// </summary>
    [Fact]
    public void ElectionTimeoutSeed_DifferentSeeds_ProducesDifferentTimeouts()
    {
        FakePartitionHost hostA = new() { PartitionId = 3, Configuration = { ElectionTimeoutSeed = 1 } };
        FakePartitionHost hostB = new() { PartitionId = 3, Configuration = { ElectionTimeoutSeed = 2 } };

        RaftPartitionStateMachine smA = new(hostA, new FakeWalFacade(), new CapturingReplySink(), NullLogger<IRaft>.Instance);
        RaftPartitionStateMachine smB = new(hostB, new FakeWalFacade(), new CapturingReplySink(), NullLogger<IRaft>.Instance);

        Assert.NotEqual(smA.ElectionTimeout, smB.ElectionTimeout);
    }

    /// <summary>
    /// Without a seed the initial timeout must still fall within the configured range.
    /// </summary>
    [Fact]
    public void ElectionTimeout_NoSeed_FallsWithinConfiguredRange()
    {
        FakePartitionHost host = new();

        RaftPartitionStateMachine sm = new(host, new FakeWalFacade(), new CapturingReplySink(), NullLogger<IRaft>.Instance);

        Assert.InRange(sm.ElectionTimeout.TotalMilliseconds,
            host.Configuration.StartElectionTimeout,
            host.Configuration.EndElectionTimeout);
    }

    /// <summary>
    /// With a seed the initial timeout must still fall within the configured range.
    /// </summary>
    [Fact]
    public void ElectionTimeout_WithSeed_FallsWithinConfiguredRange()
    {
        FakePartitionHost host = new() { PartitionId = 7, Configuration = { ElectionTimeoutSeed = 99 } };

        RaftPartitionStateMachine sm = new(host, new FakeWalFacade(), new CapturingReplySink(), NullLogger<IRaft>.Instance);

        Assert.InRange(sm.ElectionTimeout.TotalMilliseconds,
            host.Configuration.StartElectionTimeout,
            host.Configuration.EndElectionTimeout);
    }

    private static async Task WaitForHeartbeatWindow(TimeSpan interval)
    {
        TimeSpan delay = interval + TimeSpan.FromMilliseconds(5);
        if (delay < TimeSpan.FromMilliseconds(5))
            delay = TimeSpan.FromMilliseconds(5);

        await Task.Delay(delay);
    }

    // ── Role gating ───────────────────────────────────────────────────

    [Fact]
    public async Task StartElection_WhenLearner_NeverEnqueuesRequestVotes()
    {
        // ReceiveStepDownNoticeAsync is the cleanest public path into StartElectionAsync
        // that does not bypass the role check (ForceLeaderForTestingAsync does bypass it).
        FakePartitionHost host = new() { LocalRole = ClusterMemberRole.Learner };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ReceiveStepDownNoticeAsync(new StepDownNoticeRequest(
            host.PartitionId, term: 0,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            "node-b"));

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.DoesNotContain(host.EnqueuedResponses,
            r => r.Type == RaftResponderRequestType.RequestVotes);
    }

    [Fact]
    public async Task StartElection_WhenNotMember_NeverEnqueuesRequestVotes()
    {
        FakePartitionHost host = new() { LocalRole = ClusterMemberRole.NotMember };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.ReceiveStepDownNoticeAsync(new StepDownNoticeRequest(
            host.PartitionId, term: 0,
            host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            "node-b"));

        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
        Assert.DoesNotContain(host.EnqueuedResponses,
            r => r.Type == RaftResponderRequestType.RequestVotes);
    }

    [Fact]
    public async Task VoteAsync_PreVote_DeniedForNonRosterEndpoint()
    {
        // Roster: only "node-a" (self) and "node-b" are voters.
        FakePartitionHost host = new()
        {
            VoterEndpoints = ["node-a", "node-b"]
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // "node-z" is not in the committed roster — pre-vote must be silently denied.
        await sm.VoteAsync(new RaftNode("node-z"), voteTerm: 1, remoteMaxLogId: 0,
            timestamp: host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            preVote: true);

        // No Vote reply should have been enqueued for node-z.
        Assert.DoesNotContain(host.EnqueuedResponses,
            r => r.Endpoint == "node-z" && r.Type == RaftResponderRequestType.Vote);
    }

    [Fact]
    public async Task VoteAsync_RealVote_DeniedForNonRosterEndpoint()
    {
        FakePartitionHost host = new()
        {
            VoterEndpoints = ["node-a", "node-b"]
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.VoteAsync(new RaftNode("node-z"), voteTerm: 1, remoteMaxLogId: 0,
            timestamp: host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            preVote: false);

        Assert.DoesNotContain(host.EnqueuedResponses,
            r => r.Endpoint == "node-z" && r.Type == RaftResponderRequestType.Vote);
    }

    [Fact]
    public async Task VoteAsync_RealVote_GrantedForRosterVoter()
    {
        // When the requesting node IS in the roster the existing election path is unchanged.
        FakePartitionHost host = new()
        {
            NodesOverride = [new("node-b")],
            VoterEndpoints = ["node-a", "node-b"]
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 1, remoteMaxLogId: 0,
            timestamp: host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            preVote: false);

        Assert.Contains(host.EnqueuedResponses,
            r => r.Endpoint == "node-b" && r.Type == RaftResponderRequestType.Vote);
    }

    [Fact]
    public async Task VoteAsync_PreVote_GrantedForRosterVoter()
    {
        FakePartitionHost host = new()
        {
            NodesOverride = [new("node-b")],
            VoterEndpoints = ["node-a", "node-b"]
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 1, remoteMaxLogId: 0,
            timestamp: host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            preVote: true);

        Assert.Contains(host.EnqueuedResponses,
            r => r.Endpoint == "node-b" && r.Type == RaftResponderRequestType.Vote);
    }

    [Fact]
    public async Task VoteAsync_NoRoster_TreatsAllEndpointsAsVoters()
    {
        // VoterEndpoints == null → pre-seed fallback → all endpoints accepted.
        FakePartitionHost host = new()
        {
            NodesOverride = [new("node-b")],
            VoterEndpoints = null
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        await sm.VoteAsync(new RaftNode("node-b"), voteTerm: 1, remoteMaxLogId: 0,
            timestamp: host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId),
            preVote: false);

        Assert.Contains(host.EnqueuedResponses,
            r => r.Endpoint == "node-b" && r.Type == RaftResponderRequestType.Vote);
    }

    [Fact]
    public async Task ReceivedVoteAsync_FromNonRosterEndpoint_IsDiscarded()
    {
        // A candidate should not tally a grant from an endpoint absent from the roster,
        // even if the grant is technically well-formed (stale node, buggy peer, etc.).
        FakePartitionHost host = new()
        {
            NodesOverride = [new("node-b")],
            VoterEndpoints = ["node-a", "node-b"] // node-z is NOT in the roster
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // Put the state machine into Candidate state so the real-vote tally branch is reachable.
        await sm.ForceLeaderForTestingAsync(replyCorrelationId: 200);
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);

        long termBefore = sm.CurrentTerm;

        // node-z sends a vote grant for the candidate's term — must be discarded.
        await sm.ReceivedVoteAsync("node-z", voteTerm: termBefore, remoteMaxLogId: 0, preVote: false);

        // Still a Candidate — the stray grant must not have pushed us to Leader.
        Assert.Equal(RaftNodeState.Candidate, sm.NodeState);
    }

    [Fact]
    public async Task ReceivedVoteAsync_PreVote_FromNonRosterEndpoint_IsDiscarded()
    {
        // 3-node cluster: self (node-a), node-b, node-c.  Quorum = 2.
        // Self already casts the first pre-grant; one more grant reaches quorum and would promote
        // to Candidate.  A non-roster grant must be discarded so promotion does not happen.
        FakePartitionHost host = new()
        {
            NodesOverride = [new("node-b"), new("node-c")],
            VoterEndpoints = ["node-a", "node-b", "node-c"] // node-z is NOT in the roster
        };
        FakeWalFacade wal = new();
        CapturingReplySink sink = new();
        RaftPartitionStateMachine sm = new(host, wal, sink, NullLogger<IRaft>.Instance);

        // CheckPartitionLeadershipAsync on a fresh follower (lastHeartbeat == Zero) opens a
        // pre-vote round, seeds self's pre-grant, and stays in Follower state.
        await sm.CheckPartitionLeadershipAsync();
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);

        // node-z sends a pre-vote grant for the open round — must be discarded without promoting.
        await sm.ReceivedVoteAsync("node-z", voteTerm: sm.CurrentTerm + 1, remoteMaxLogId: 0, preVote: true);

        // Still Follower — the stray grant must not have completed the pre-vote quorum.
        Assert.Equal(RaftNodeState.Follower, sm.NodeState);
    }

    private sealed class FakePartitionHost : IRaftPartitionHost
    {
        public int PartitionId { get; init; } = 1;

        public string Leader { get; set; } = "";

        public string LocalEndpoint => "node-a";

        public int LocalNodeId => 1;

        /// <summary>Defaults to Voter (pre-seed fallback). Override to test learner/non-member gating.</summary>
        public ClusterMemberRole LocalRole { get; set; } = ClusterMemberRole.Voter;

        /// <summary>
        /// When null every endpoint is treated as a voter (pre-seed fallback).
        /// Set to a specific set to exercise IsVoter denial.
        /// </summary>
        public HashSet<string>? VoterEndpoints { get; set; }

        public bool IsVoter(string endpoint) =>
            VoterEndpoints is null || VoterEndpoints.Contains(endpoint);

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

        /// <summary>
        /// Full enqueued requests, captured alongside <see cref="EnqueuedResponses"/> so pre-vote
        /// tests can inspect the <see cref="VoteRequest.PreVote"/> / <see cref="RequestVotesRequest.PreVote"/> flags.
        /// </summary>
        public List<(string Endpoint, RaftResponderRequest Request)> EnqueuedRequests { get; } = [];

        public void ClearObservations()
        {
            LeaderChanges.Clear();
            EnqueuedResponses.Clear();
            EnqueuedRequests.Clear();
        }

        public HLCTimestamp GetLastNodeActivity(string endpoint, int partitionId) => HLCTimestamp.Zero;

        public HLCTimestamp GetLastNodeHearthbeat(string endpoint, int partitionId) => HLCTimestamp.Zero;

        public void UpdateLastHeartbeat(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void UpdateLastNodeActivity(string endpoint, int partitionId, HLCTimestamp timestamp) { }

        public void EnqueueResponse(string endpoint, RaftResponderRequest request)
        {
            EnqueuedResponses.Add((endpoint, request.Type));
            EnqueuedRequests.Add((endpoint, request));
        }

        public Task InvokeLeaderChanged(int partitionId, string leader)
        {
            LeaderChanges.Add(leader);
            return Task.CompletedTask;
        }

        public Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log) => Task.FromResult(true);

        public void InvokeReplicationError(int partitionId, RaftLog log) { }
    }

    private sealed class FakeWalFacade : IRaftWalFacade, IDisposable
    {
        private readonly FakeWAL wal = new();

        public void Dispose() => wal.Dispose();

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

        public ValueTask<List<RaftLog>> GetRangeAsync(long startLogIndex, int maxEntries) =>
            ValueTask.FromResult(new List<RaftLog>());

        public long GetCommitIndex() => wal.GetMaxLog(partitionId: 1);

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
