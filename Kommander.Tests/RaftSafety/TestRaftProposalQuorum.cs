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

    private static RaftProposalQuorum BuildQuorum(int totalNodes)
    {
        RaftProposalQuorum quorum = new([new RaftLog { Id = 1, Term = 1 }], autoCommit: false, HLCTimestamp.Zero);

        for (int i = 0; i < totalNodes; i++)
            quorum.AddExpectedNodeCompletion(NodeName(i));

        return quorum;
    }

    private static string NodeName(int index) => $"node-{index}";
}
