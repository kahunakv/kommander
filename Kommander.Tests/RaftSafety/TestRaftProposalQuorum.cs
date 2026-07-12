using Kommander.Data;
using Kommander.Time;

namespace Kommander.Tests.RaftSafety;

public class TestRaftProposalQuorum
{
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 2)]
    [InlineData(5, 3)]
    public void HasQuorum_UsesMajorityOfFullClusterSize(int totalNodes, int requiredCompletions)
    {
        RaftProposalQuorum quorum = BuildQuorum(totalNodes);

        for (int i = 0; i < requiredCompletions - 1; i++)
            quorum.MarkNodeCompleted(NodeName(i));

        if (requiredCompletions > 1)
            Assert.False(quorum.HasQuorum());

        quorum.MarkNodeCompleted(NodeName(requiredCompletions - 1));

        Assert.True(quorum.HasQuorum());
    }

    [Theory]
    [InlineData(3, 1)]
    [InlineData(5, 2)]
    public void HasQuorum_AllowsToleratedFailures(int totalNodes, int toleratedFailures)
    {
        RaftProposalQuorum quorum = BuildQuorum(totalNodes);
        int completedNodes = totalNodes - toleratedFailures;

        for (int i = 0; i < completedNodes; i++)
            quorum.MarkNodeCompleted(NodeName(i));

        Assert.True(quorum.HasQuorum());
    }

    [Theory]
    [InlineData(3, 2)]
    [InlineData(5, 3)]
    public void HasQuorum_RejectsMoreThanToleratedFailures(int totalNodes, int failedNodes)
    {
        RaftProposalQuorum quorum = BuildQuorum(totalNodes);
        int completedNodes = totalNodes - failedNodes;

        for (int i = 0; i < completedNodes; i++)
            quorum.MarkNodeCompleted(NodeName(i));

        Assert.False(quorum.HasQuorum());
    }

    /// <summary>
    /// Duplicate follower acks must not advance the quorum: the O(1) completion count is only
    /// incremented on a false→true transition, so re-acking the same node is a no-op.
    /// </summary>
    [Fact]
    public void HasQuorum_DuplicateAcks_DoNotOverCount()
    {
        RaftProposalQuorum quorum = BuildQuorum(totalNodes: 3); // quorum = 2

        // One real node acks twice: without idempotency this would falsely reach quorum.
        quorum.MarkNodeCompleted(NodeName(0));
        quorum.MarkNodeCompleted(NodeName(0));
        quorum.MarkNodeCompleted(NodeName(0));

        Assert.False(quorum.HasQuorum());

        quorum.MarkNodeCompleted(NodeName(1)); // a distinct node — now 2/3
        Assert.True(quorum.HasQuorum());
    }

    /// <summary>
    /// Acks from nodes never registered via <see cref="RaftProposalQuorum.AddExpectedNodeCompletion"/>
    /// (e.g. learners, which receive entries but are excluded from quorum) must be ignored and must
    /// not advance the completion count.
    /// </summary>
    [Fact]
    public void HasQuorum_UnregisteredNodeAcks_AreIgnored()
    {
        RaftProposalQuorum quorum = BuildQuorum(totalNodes: 3); // quorum = 2

        quorum.MarkNodeCompleted("learner-a:5000");
        quorum.MarkNodeCompleted("learner-b:5000");
        quorum.MarkNodeCompleted(NodeName(0));

        Assert.False(quorum.HasQuorum()); // only one registered voter has acked
    }

    /// <summary>
    /// A quorum returned to the pool and rented again must start fresh: <see cref="RaftProposalQuorum.Clear"/>
    /// resets the completion count in lockstep with the nodes dictionary, so a reused instance is not
    /// pre-satisfied by a previous proposal's acks.
    /// </summary>
    [Fact]
    public void PooledQuorum_AfterReturnAndRent_StartsWithNoCompletions()
    {
        RaftProposalQuorum first = RaftProposalQuorumPool.Rent([new RaftLog { Id = 1, Term = 1 }], autoCommit: false, HLCTimestamp.Zero);
        for (int i = 0; i < 3; i++)
            first.AddExpectedNodeCompletion(NodeName(i));
        first.MarkNodeCompleted(NodeName(0));
        first.MarkNodeCompleted(NodeName(1));
        Assert.True(first.HasQuorum());

        RaftProposalQuorumPool.Return(first);

        RaftProposalQuorum reused = RaftProposalQuorumPool.Rent([new RaftLog { Id = 2, Term = 1 }], autoCommit: false, HLCTimestamp.Zero);
        for (int i = 0; i < 3; i++)
            reused.AddExpectedNodeCompletion(NodeName(i));

        // No acks yet on the reused instance — the previous proposal's completions must not carry over.
        Assert.False(reused.HasQuorum());
        reused.MarkNodeCompleted(NodeName(0));
        Assert.False(reused.HasQuorum());
        reused.MarkNodeCompleted(NodeName(1));
        Assert.True(reused.HasQuorum());
    }

    private static RaftProposalQuorum BuildQuorum(int totalNodes)
    {
        RaftProposalQuorum quorum = new([new RaftLog { Id = 1, Term = 1 }], autoCommit: false, HLCTimestamp.Zero);

        for (int i = 0; i < totalNodes; i++)
            quorum.AddExpectedNodeCompletion(NodeName(i));

        return quorum;
    }

    private static string NodeName(int index) => $"node-{index}";
}
