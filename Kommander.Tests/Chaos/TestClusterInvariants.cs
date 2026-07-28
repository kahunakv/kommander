
using Kommander.Data;
using Kommander.System;

namespace Kommander.Tests.Chaos;

/// <summary>
/// Verifies that each of the seven continuous invariants fires on a synthetic violation and passes on a
/// clean view. Views are constructed by hand (no cluster), so each invariant is exercised as the pure
/// function it is.
/// </summary>
public class TestClusterInvariants
{
    private const int P = 1;

    private static RaftPartitionView View(
        string endpoint, RaftNodeState role, long term, string leader,
        long commit = 0, long applied = 0, long maxWal = 0, bool quiesced = false,
        ClusterMemberRole member = ClusterMemberRole.Voter, int partition = P) =>
        new(endpoint, partition, role, term, leader, commit, applied, maxWal, quiesced, member);

    private static RaftLog Log(long id, long term, byte[] payload) =>
        new() { Id = id, Term = term, Type = RaftLogType.Committed, LogType = "chaos", LogData = payload };

    private static async Task<HashChainSnapshot> Chain(string endpoint, params RaftLog[] logs)
    {
        HashChainStateMachine sm = new(endpoint, P);
        foreach (RaftLog l in logs)
            await sm.OnReplicationReceived(P, l);
        return sm.Snapshot();
    }

    private static ClusterView ViewOf(
        IEnumerable<RaftPartitionView>? views = null,
        IEnumerable<HashChainSnapshot>? chains = null,
        IEnumerable<CommitObservation>? commits = null,
        IEnumerable<CommitAck>? acks = null,
        IDictionary<string, long>? maxCommit = null,
        IDictionary<string, long>? maxApplied = null) =>
        new(1, views?.ToList() ?? [], chains?.ToList() ?? [], commits?.ToList() ?? [], acks?.ToList() ?? [],
            maxCommit is null ? new Dictionary<string, long>() : new Dictionary<string, long>(maxCommit))
        {
            MaxAppliedByNode = maxApplied is null ? new Dictionary<string, long>() : new Dictionary<string, long>(maxApplied),
        };

    // ── 1. election safety ──────────────────────────────────────────────────────

    [Fact]
    public void ElectionSafety_TwoLeadersSameTerm_Fires()
    {
        ClusterView v = ViewOf(views:
        [
            View("a:1", RaftNodeState.Leader, term: 5, leader: "a:1"),
            View("b:1", RaftNodeState.Leader, term: 5, leader: "b:1"),
        ]);
        ClusterViolation? r = new ElectionSafetyInvariant().Evaluate(null, v);
        Assert.NotNull(r);
        Assert.Contains("2 leaders", r!.Detail);
        Assert.True(r.RequiresConfirmation);
    }

    [Fact]
    public void ElectionSafety_SingleLeader_Passes()
    {
        ClusterView v = ViewOf(views:
        [
            View("a:1", RaftNodeState.Leader, 5, "a:1"),
            View("b:1", RaftNodeState.Follower, 5, "a:1"),
        ]);
        Assert.Null(new ElectionSafetyInvariant().Evaluate(null, v));
    }

    // ── 2. state machine safety ──────────────────────────────────────────────────

    [Fact]
    public async Task StateMachineSafety_PrefixDivergence_Fires()
    {
        HashChainSnapshot a = await Chain("a:1", Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [30]));
        HashChainSnapshot b = await Chain("b:1", Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [99])); // differs at 3
        ClusterViolation? r = new StateMachineSafetyInvariant().Evaluate(null, ViewOf(chains: [a, b]));
        Assert.NotNull(r);
        Assert.Contains("divergence at index 3", r!.Detail);
        Assert.False(r.RequiresConfirmation);
    }

    [Fact]
    public async Task StateMachineSafety_IdenticalHistories_Passes()
    {
        RaftLog[] logs = [Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [30])];
        HashChainSnapshot a = await Chain("a:1", logs);
        HashChainSnapshot b = await Chain("b:1", logs);
        Assert.Null(new StateMachineSafetyInvariant().Evaluate(null, ViewOf(chains: [a, b])));
    }

    // ── 3. index uniqueness ──────────────────────────────────────────────────────

    [Fact]
    public async Task IndexUniqueness_ConflictingTermAtIndex_Fires()
    {
        HashChainSnapshot a = await Chain("a:1", Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [30]));
        HashChainSnapshot b = await Chain("b:1", Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 2, [30])); // term differs at 3
        ClusterViolation? r = new IndexUniquenessInvariant().Evaluate(null, ViewOf(chains: [a, b]));
        Assert.NotNull(r);
        Assert.Contains("index 3 disagrees", r!.Detail);
    }

    [Fact]
    public async Task IndexUniqueness_Agreeing_Passes()
    {
        RaftLog[] logs = [Log(1, 1, [10]), Log(2, 1, [20])];
        HashChainSnapshot a = await Chain("a:1", logs);
        HashChainSnapshot b = await Chain("b:1", logs);
        Assert.Null(new IndexUniquenessInvariant().Evaluate(null, ViewOf(chains: [a, b])));
    }

    // ── 4. leader completeness ────────────────────────────────────────────────────

    [Fact]
    public async Task LeaderCompleteness_LeaderMissingCommittedEntry_Fires()
    {
        // Leader applied 1,2,4,5 (hole at 3) but reports applied-through 5.
        HashChainSnapshot leaderChain = await Chain("a:1", Log(1, 1, [10]), Log(2, 1, [20]), Log(4, 1, [40]), Log(5, 1, [50]));
        ClusterView v = ViewOf(
            views: [View("a:1", RaftNodeState.Leader, 5, "a:1", applied: 5)],
            chains: [leaderChain],
            commits: [new CommitObservation(P, 3, 1, 0xABCD)]);
        ClusterViolation? r = new LeaderCompletenessInvariant().Evaluate(null, v);
        Assert.NotNull(r);
        Assert.Contains("missing committed index 3", r!.Detail);
    }

    [Fact]
    public async Task LeaderCompleteness_LeaderHasEntry_Passes()
    {
        HashChainSnapshot leaderChain = await Chain("a:1", Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [30]));
        ulong digest = leaderChain.MetaByIndex[3].EntryDigest;
        ClusterView v = ViewOf(
            views: [View("a:1", RaftNodeState.Leader, 5, "a:1", applied: 3)],
            chains: [leaderChain],
            commits: [new CommitObservation(P, 3, 1, digest)]);
        Assert.Null(new LeaderCompletenessInvariant().Evaluate(null, v));
    }

    [Fact]
    public async Task LeaderCompleteness_LeaderBehind_IsTransient_Passes()
    {
        HashChainSnapshot leaderChain = await Chain("a:1", Log(1, 1, [10]));
        ClusterView v = ViewOf(
            views: [View("a:1", RaftNodeState.Leader, 5, "a:1", applied: 1)],
            chains: [leaderChain],
            commits: [new CommitObservation(P, 3, 1, 0xABCD)]); // leader hasn't applied through 3 yet
        Assert.Null(new LeaderCompletenessInvariant().Evaluate(null, v));
    }

    // ── 5. commit monotonicity ────────────────────────────────────────────────────

    [Fact]
    public void CommitMonotonicity_CommitBelowAppliedPrefix_Fires()
    {
        // Commit frontier (5) below what this (node, partition) already applied (10) — a real regression.
        ClusterView v = ViewOf(
            views: [View("a:1", RaftNodeState.Follower, 5, "b:1", commit: 5)],
            maxApplied: new Dictionary<string, long> { [InvariantPredicates.NodeKey("a:1", P)] = 10 });
        ClusterViolation? r = new CommitMonotonicityInvariant().Evaluate(null, v);
        Assert.NotNull(r);
        Assert.Contains("dropped below the durable applied prefix 10", r!.Detail);
        Assert.True(r.RequiresConfirmation);
    }

    [Fact]
    public void CommitMonotonicity_FrontierDipAboveAppliedPrefix_Passes()
    {
        // Commit frontier dipped to 8 (below a previously-seen commit) but stays at/above the applied prefix (8):
        // a benign gap-aware/rolled-back-commit artifact, not a regression.
        ClusterView v = ViewOf(
            views: [View("a:1", RaftNodeState.Follower, 5, "b:1", commit: 8)],
            maxApplied: new Dictionary<string, long> { [InvariantPredicates.NodeKey("a:1", P)] = 8 });
        Assert.Null(new CommitMonotonicityInvariant().Evaluate(null, v));
    }

    [Fact]
    public void CommitMonotonicity_OtherPartitionTrailing_DoesNotConflate()
    {
        // Same endpoint, two partitions: p2 trailing p1 must NOT read as a regression (per-partition keying).
        ClusterView v = ViewOf(
            views:
            [
                View("a:1", RaftNodeState.Leader, 5, "a:1", commit: 20, applied: 20, partition: 1),
                View("a:1", RaftNodeState.Leader, 5, "a:1", commit: 15, applied: 15, partition: 2),
            ],
            maxApplied: new Dictionary<string, long>
            {
                [InvariantPredicates.NodeKey("a:1", 1)] = 20,
                [InvariantPredicates.NodeKey("a:1", 2)] = 15,
            });
        Assert.Null(new CommitMonotonicityInvariant().Evaluate(null, v));
    }

    // ── 6. no committed rollback ──────────────────────────────────────────────────

    [Fact]
    public async Task NoCommittedRollback_EntryReplaced_Fires()
    {
        HashChainSnapshot committedView = await Chain("ref", Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [30]));
        ulong committedDigest = committedView.MetaByIndex[3].EntryDigest;

        HashChainSnapshot replaced = await Chain("a:1", Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [99])); // idx3 differs
        ClusterView v = ViewOf(
            chains: [replaced],
            commits: [new CommitObservation(P, 3, 1, committedDigest)]);
        ClusterViolation? r = new NoCommittedRollbackInvariant().Evaluate(null, v);
        Assert.NotNull(r);
        Assert.Contains("replaced a committed entry", r!.Detail);
    }

    [Fact]
    public async Task NoCommittedRollback_Consistent_Passes()
    {
        HashChainSnapshot chain = await Chain("a:1", Log(1, 1, [10]), Log(2, 1, [20]), Log(3, 1, [30]));
        ulong digest = chain.MetaByIndex[3].EntryDigest;
        ClusterView v = ViewOf(chains: [chain], commits: [new CommitObservation(P, 3, 1, digest)]);
        Assert.Null(new NoCommittedRollbackInvariant().Evaluate(null, v));
    }

    // ── 7. quorum discipline ──────────────────────────────────────────────────────

    [Fact]
    public void QuorumDiscipline_MinorityVoterAcks_Fires()
    {
        // 3 voters → majority 2. Only one voter acked; a learner ack does not count.
        ClusterView v = ViewOf(acks:
        [
            new CommitAck(P, 1, "a:1", AckerIsVoter: true, VotersTotal: 3),
            new CommitAck(P, 1, "L:1", AckerIsVoter: false, VotersTotal: 3),
        ]);
        ClusterViolation? r = new QuorumDisciplineInvariant().Evaluate(null, v);
        Assert.NotNull(r);
        Assert.Contains("only 1 voter acks (need 2 of 3)", r!.Detail);
    }

    [Fact]
    public void QuorumDiscipline_VoterMajority_Passes()
    {
        ClusterView v = ViewOf(acks:
        [
            new CommitAck(P, 1, "a:1", true, 3),
            new CommitAck(P, 1, "b:1", true, 3),
            new CommitAck(P, 1, "L:1", false, 3),
        ]);
        Assert.Null(new QuorumDisciplineInvariant().Evaluate(null, v));
    }

    // ── every invariant is represented ────────────────────────────────────────────

    [Fact]
    public void DefaultSet_ContainsAllSevenInvariants()
    {
        string[] names = ClusterInvariants.All.Select(i => i.Name).ToArray();
        Assert.Equal(7, names.Length);
        Assert.Equal(names.Length, names.Distinct().Count());
    }
}
