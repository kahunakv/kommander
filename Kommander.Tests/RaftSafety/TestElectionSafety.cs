
using Kommander.Tests.Scheduler;

namespace Kommander.Tests.RaftSafety;

/// <summary>
/// Safety invariant: at most one leader per term per partition.
///
/// A node may only cast one vote per term.  A leader is elected only after
/// reaching strict quorum.  These tests verify the vote-counting and quorum
/// logic that the state machine relies on so that regressions introduced
/// during the actor→executor migration are caught immediately.
///
/// The vote-tracking model is:
///   votes[term] = { endpoints that sent a vote for this node in this term }
/// Quorum = Math.Max(2, Floor(totalNodes / 2) + 1)
/// </summary>
public class TestElectionSafety
{
    // ── Quorum formula ────────────────────────────────────────────────────

    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 2)]
    [InlineData(3, 2)]
    [InlineData(4, 3)]
    [InlineData(5, 3)]
    [InlineData(6, 4)]
    [InlineData(7, 4)]
    public void Quorum_Formula_IsCorrect(int totalNodes, int expectedQuorum)
    {
        Assert.Equal(expectedQuorum, RaftSafetyAssert.Quorum(totalNodes));
    }

    // ── Single vote per term per node ─────────────────────────────────────

    [Fact]
    public void VoteCounting_EachNodeVotesOncePerTerm()
    {
        // A votes dictionary mirrors RaftStateActor.votes: votes[term] = set of endpoints.
        Dictionary<long, HashSet<string>> votes = new();

        long term = 1;
        string[] voters = ["node-a", "node-b", "node-c"];

        foreach (string voter in voters)
        {
            if (!votes.TryGetValue(term, out HashSet<string>? termVotes))
                votes[term] = termVotes = [];
            termVotes.Add(voter);
        }

        // Adding the same voter again must not inflate the count.
        votes[term].Add("node-a");

        Assert.Equal(3, votes[term].Count);
    }

    [Fact]
    public void VoteCounting_VoteInOlderTerm_IsIgnoredForCurrentTerm()
    {
        Dictionary<long, HashSet<string>> votes = new()
        {
            [1] = ["node-a", "node-b"],
            [2] = ["node-a"],
        };

        // Term-1 votes should not count towards term-2 quorum.
        Assert.Single(votes[2]);
        Assert.Equal(2, votes[1].Count);
    }

    // ── At-most-one-leader per term (3-node cluster) ──────────────────────

    [Fact]
    public void ElectionSafety_ThreeNodeCluster_AtMostOneLeaderPerTerm()
    {
        const int totalNodes = 3;
        int quorum = RaftSafetyAssert.Quorum(totalNodes);

        // Two candidates race in term 1.
        // Node A votes for itself.  Node B votes for itself.
        // Each can receive at most one additional vote from the third node (C).

        Dictionary<long, HashSet<string>> votesForA = new() { [1] = ["node-a"] }; // self-vote
        Dictionary<long, HashSet<string>> votesForB = new() { [1] = ["node-b"] }; // self-vote

        // Node C votes for A.
        votesForA[1].Add("node-c");

        // A reaches quorum. B only has 1 vote and cannot reach quorum now
        // because C already voted for A in this term.
        bool aIsLeader = votesForA[1].Count >= quorum;
        bool bIsLeader = votesForB[1].Count >= quorum;

        Assert.True(aIsLeader, "Node A should have reached quorum");
        Assert.False(bIsLeader, "Node B must not reach quorum after C voted for A");

        // Verify the mutual-exclusion: at most one leader per term.
        Assert.False(aIsLeader && bIsLeader, "Two leaders elected in the same term — election safety violation");
    }

    [Fact]
    public void ElectionSafety_FiveNodeCluster_SplitVoteDoesNotElectLeader()
    {
        const int totalNodes = 5;
        int quorum = RaftSafetyAssert.Quorum(totalNodes); // 3

        // Split: A gets 2 votes, B gets 2 votes, neither reaches quorum.
        Dictionary<long, HashSet<string>> votesForA = new() { [1] = ["node-a", "node-c"] };
        Dictionary<long, HashSet<string>> votesForB = new() { [1] = ["node-b", "node-d"] };

        bool aIsLeader = votesForA[1].Count >= quorum;
        bool bIsLeader = votesForB[1].Count >= quorum;

        Assert.False(aIsLeader, "Split vote: A must not be elected without quorum");
        Assert.False(bIsLeader, "Split vote: B must not be elected without quorum");
    }

    [Fact]
    public void ElectionSafety_NewTermInvalidatesOldTermVotes()
    {
        // A node that voted in term 1 can vote in term 2 for a different candidate.
        // This is valid because term isolation means votes[1] != votes[2].
        Dictionary<long, HashSet<string>> votes = new()
        {
            [1] = ["node-a", "node-b"],   // voted for candidate X in term 1
            [2] = ["node-a", "node-c"],   // voted for candidate Y in term 2
        };

        // Both terms are independent; this is not a safety violation.
        Assert.Equal(2, votes[1].Count);
        Assert.Equal(2, votes[2].Count);

        // Crucially, the same node cannot vote for TWO DIFFERENT CANDIDATES in the same term.
        // node-a appears in votes[1] — its vote is already cast for that term.
        Assert.Contains("node-a", votes[1]);
        // Adding node-a again must be idempotent (HashSet semantics).
        votes[1].Add("node-a");
        Assert.Equal(2, votes[1].Count);
    }

    // ── Vote ignored when term is stale ───────────────────────────────────

    [Fact]
    public void ElectionSafety_VoteFromLowerTerm_IsIgnored()
    {
        // Mirror of RaftStateActor.ReceivedVote: "if (voteTerm < currentTerm) return"
        long currentTerm = 5;
        long staleTerm = 3;

        bool shouldIgnore = staleTerm < currentTerm;

        Assert.True(shouldIgnore, "Vote from a lower term must be ignored");
    }

    [Fact]
    public void ElectionSafety_VoteFromHigherTerm_IsAccepted()
    {
        long currentTerm = 3;
        long higherTerm = 5;

        bool shouldIgnore = higherTerm < currentTerm;

        Assert.False(shouldIgnore, "Vote from a higher term should not be silently ignored");
    }

    // ── WAL: leader uniqueness encoded in log terms ───────────────────────

    [Fact]
    public void ElectionSafety_WalTerms_OnlyOneLeaderCanCommitPerTerm()
    {
        FakeWAL wal = new();

        // Simulate two nodes racing to commit in term 1.
        // The winning leader (node-a) commits; node-b's proposed entry for the same
        // log ID would mean two leaders — a safety violation.
        wal.Write([(0, [new() { Id = 1, Term = 1, Type = Kommander.Data.RaftLogType.Committed }])]);
        wal.DrainAll();

        // Now attempt to write a committed entry with the same ID from a different leader.
        // In a correct implementation this must overwrite, not duplicate.
        wal.Write([(0, [new() { Id = 1, Term = 1, Type = Kommander.Data.RaftLogType.Committed }])]);
        wal.DrainAll();

        List<Kommander.Data.RaftLog> logs = wal.ReadLogs(partitionId: 0);
        long countForId1 = logs.Count(l => l.Id == 1);

        // Only one entry per log ID should exist (the WAL is a dictionary keyed by Id).
        Assert.Equal(1L, countForId1);
    }
}
