using Kommander.Data;
using Kommander.System;
using Kommander.Tests.Simulation.Invariants;
using Kommander.Tests.Simulation.WAL;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Tests that each invariant fires on the state it is meant to catch, and stays silent on legal
/// state.
///
/// <para><b>Why this file exists.</b> An invariant that never fires is worse than no invariant: it
/// makes a run look checked when nothing was checked, and a vacuous check is invisible in a green
/// suite. Each rule here is fed a hand-built violation and a hand-built legal state, so a rule
/// that stops working fails this file rather than quietly passing every simulation.</para>
///
/// <para>These are pure state checks. No cluster runs, so the file costs milliseconds.</para>
/// </summary>
[Trait("Category", "DSTSmoke")]
public sealed class TestClusterInvariantSet
{
    private const int PartitionId = 1;

    // ── One leader per term ───────────────────────────────────────────────

    [Fact]
    public void OneLeaderPerTerm_FiresOnTwoLeadersInOneTerm()
    {
        List<RaftPartitionView> views =
        [
            View("node1", RaftNodeState.Leader, term: 5, commitIndex: 3),
            View("node2", RaftNodeState.Leader, term: 5, commitIndex: 3),
        ];

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(
            () => ClusterInvariantSet.CheckOneLeaderPerTerm(stepNumber: 7, views));

        Assert.Equal(ClusterInvariantSet.OneLeaderPerTerm, error.InvariantName);
        Assert.Equal(7, error.StepNumber);
        Assert.Contains("node1", error.Message, StringComparison.Ordinal);
        Assert.Contains("node2", error.Message, StringComparison.Ordinal);
    }

    /// <summary>Two leaders in <b>different</b> terms is ordinary: one of them is stale.</summary>
    [Fact]
    public void OneLeaderPerTerm_AllowsLeadersInDifferentTerms()
    {
        List<RaftPartitionView> views =
        [
            View("node1", RaftNodeState.Leader, term: 5, commitIndex: 3),
            View("node2", RaftNodeState.Leader, term: 6, commitIndex: 3),
        ];

        ClusterInvariantSet.CheckOneLeaderPerTerm(stepNumber: 1, views);
    }

    // ── Committed ids monotonic ───────────────────────────────────────────

    [Fact]
    public void CommittedIdsMonotonic_FiresWhenANodeLowersItsCommitIndex()
    {
        Dictionary<string, long> highest = [];

        ClusterInvariantSet.CheckCommittedIdsMonotonic(
            1, [View("node1", RaftNodeState.Follower, term: 4, commitIndex: 10)], highest);

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(
            () => ClusterInvariantSet.CheckCommittedIdsMonotonic(
                2, [View("node1", RaftNodeState.Follower, term: 4, commitIndex: 9)], highest));

        Assert.Equal(ClusterInvariantSet.CommittedIdsMonotonic, error.InvariantName);
        Assert.Contains("9", error.Message, StringComparison.Ordinal);
        Assert.Contains("10", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CommittedIdsMonotonic_AllowsAnUnchangedOrRisingIndex()
    {
        Dictionary<string, long> highest = [];

        ClusterInvariantSet.CheckCommittedIdsMonotonic(
            1, [View("node1", RaftNodeState.Follower, term: 4, commitIndex: 10)], highest);
        ClusterInvariantSet.CheckCommittedIdsMonotonic(
            2, [View("node1", RaftNodeState.Follower, term: 4, commitIndex: 10)], highest);
        ClusterInvariantSet.CheckCommittedIdsMonotonic(
            3, [View("node1", RaftNodeState.Follower, term: 4, commitIndex: 11)], highest);
    }

    // ── Committed entries agree ───────────────────────────────────────────

    [Fact]
    public void CommittedEntriesAgree_FiresOnTwoValuesAtOneIndex()
    {
        Dictionary<long, CommittedEntryFingerprint> recorded = [];

        ClusterInvariantSet.CheckCommittedEntriesAgree(
            1, [Fingerprint("node1", index: 4, term: 2, payload: "alpha")], recorded);

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(
            () => ClusterInvariantSet.CheckCommittedEntriesAgree(
                2, [Fingerprint("node2", index: 4, term: 2, payload: "beta")], recorded));

        Assert.Equal(ClusterInvariantSet.CommittedEntriesAgree, error.InvariantName);
        Assert.Contains("node1", error.Message, StringComparison.Ordinal);
        Assert.Contains("node2", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The check compares history, not only the current step. An entry recorded long ago and
    /// contradicted much later must still fail.
    /// </summary>
    [Fact]
    public void CommittedEntriesAgree_FiresOnALateContradiction()
    {
        Dictionary<long, CommittedEntryFingerprint> recorded = [];

        ClusterInvariantSet.CheckCommittedEntriesAgree(
            1, [Fingerprint("node1", index: 1, term: 1, payload: "alpha")], recorded);

        for (int step = 2; step < 20; step++)
        {
            ClusterInvariantSet.CheckCommittedEntriesAgree(
                step, [Fingerprint("node1", index: 1, term: 1, payload: "alpha")], recorded);
        }

        Assert.Throws<InvariantViolationException>(
            () => ClusterInvariantSet.CheckCommittedEntriesAgree(
                20, [Fingerprint("node3", index: 1, term: 9, payload: "alpha")], recorded));
    }

    [Fact]
    public void CommittedEntriesAgree_AllowsTwoNodesHoldingTheSameEntry()
    {
        Dictionary<long, CommittedEntryFingerprint> recorded = [];

        ClusterInvariantSet.CheckCommittedEntriesAgree(
            1,
            [
                Fingerprint("node1", index: 4, term: 2, payload: "alpha"),
                Fingerprint("node2", index: 4, term: 2, payload: "alpha"),
            ],
            recorded);
    }

    // ── Committed terms non-decreasing ────────────────────────────────────

    [Fact]
    public void CommittedTermsNonDecreasing_FiresWhenAnOlderTermSitsAbove()
    {
        Dictionary<long, CommittedEntryFingerprint> recorded = new()
        {
            [1] = Fingerprint("node1", index: 1, term: 3, payload: "a"),
            [2] = Fingerprint("node1", index: 2, term: 2, payload: "b"),
        };

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(
            () => ClusterInvariantSet.CheckCommittedTermsNonDecreasing(stepNumber: 4, recorded));

        Assert.Equal(ClusterInvariantSet.CommittedTermsNonDecreasing, error.InvariantName);
    }

    [Fact]
    public void CommittedTermsNonDecreasing_AllowsARepeatedOrRisingTerm()
    {
        Dictionary<long, CommittedEntryFingerprint> recorded = new()
        {
            [1] = Fingerprint("node1", index: 1, term: 2, payload: "a"),
            [2] = Fingerprint("node1", index: 2, term: 2, payload: "b"),
            [3] = Fingerprint("node1", index: 3, term: 5, payload: "c"),
        };

        ClusterInvariantSet.CheckCommittedTermsNonDecreasing(stepNumber: 4, recorded);
    }

    // ── Leader completeness ───────────────────────────────────────────────

    /// <summary>
    /// The hole case. A leader whose read covered index 2 but returned nothing there is missing a
    /// committed entry, which is what the election restriction exists to prevent.
    /// </summary>
    [Fact]
    public void LeaderCompleteness_FiresWhenALeaderHasAHoleInsideItsReadRange()
    {
        List<RaftPartitionView> views = [View("node1", RaftNodeState.Leader, term: 6, commitIndex: 3)];

        ClusterInvariantSet.NodeCommittedWindow leaderWindow = new(
            "node1",
            RangeStart: 1,
            RangeEnd: 3,
            ByIndex: new Dictionary<long, CommittedEntryFingerprint>
            {
                [1] = Fingerprint("node1", index: 1, term: 1, payload: "a"),
                [3] = Fingerprint("node1", index: 3, term: 6, payload: "c"),
            });

        Dictionary<long, CommittedEntryFingerprint> recorded = new()
        {
            [2] = Fingerprint("node2", index: 2, term: 2, payload: "b"),
        };

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(
            () => ClusterInvariantSet.CheckLeaderCompleteness(9, views, [leaderWindow], recorded));

        Assert.Equal(ClusterInvariantSet.LeaderCompleteness, error.InvariantName);
        Assert.Contains("hole", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A gap <b>below</b> the read range is not a hole. The leader may have compacted the entry,
    /// which is correct behavior, and reporting it would be a false alarm on every compacted node.
    /// </summary>
    [Fact]
    public void LeaderCompleteness_AllowsAGapBelowTheReadRange()
    {
        List<RaftPartitionView> views = [View("node1", RaftNodeState.Leader, term: 6, commitIndex: 12)];

        ClusterInvariantSet.NodeCommittedWindow leaderWindow = new(
            "node1",
            RangeStart: 10,
            RangeEnd: 12,
            ByIndex: new Dictionary<long, CommittedEntryFingerprint>
            {
                [10] = Fingerprint("node1", index: 10, term: 6, payload: "j"),
            });

        Dictionary<long, CommittedEntryFingerprint> recorded = new()
        {
            [2] = Fingerprint("node2", index: 2, term: 2, payload: "b"),
        };

        ClusterInvariantSet.CheckLeaderCompleteness(9, views, [leaderWindow], recorded);
    }

    /// <summary>A follower with a hole is not this rule's business; only a leader must be complete.</summary>
    [Fact]
    public void LeaderCompleteness_IgnoresAFollower()
    {
        List<RaftPartitionView> views = [View("node1", RaftNodeState.Follower, term: 6, commitIndex: 3)];

        ClusterInvariantSet.NodeCommittedWindow window = new(
            "node1",
            RangeStart: 1,
            RangeEnd: 3,
            ByIndex: new Dictionary<long, CommittedEntryFingerprint>
            {
                [1] = Fingerprint("node1", index: 1, term: 1, payload: "a"),
            });

        Dictionary<long, CommittedEntryFingerprint> recorded = new()
        {
            [2] = Fingerprint("node2", index: 2, term: 2, payload: "b"),
        };

        ClusterInvariantSet.CheckLeaderCompleteness(9, views, [window], recorded);
    }

    // ── Quiescent convergence ─────────────────────────────────────────────

    [Fact]
    public void QuiescentConvergence_FiresOnDisagreeingFrontiers()
    {
        List<RaftPartitionView> views =
        [
            View("node1", RaftNodeState.Leader, term: 4, commitIndex: 10),
            View("node2", RaftNodeState.Follower, term: 4, commitIndex: 7),
        ];

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(
            () => ClusterInvariantSet.CheckQuiescentConvergence(30, views, []));

        Assert.Equal(ClusterInvariantSet.QuiescentConvergence, error.InvariantName);
        Assert.Contains("did not converge", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Equal frontiers are not enough. Two nodes can agree on how far they have committed and
    /// still hold different entries there, which is the worse failure of the two.
    /// </summary>
    [Fact]
    public void QuiescentConvergence_FiresWhenFrontiersAgreeButEntriesDoNot()
    {
        List<RaftPartitionView> views =
        [
            View("node1", RaftNodeState.Leader, term: 4, commitIndex: 2),
            View("node2", RaftNodeState.Follower, term: 4, commitIndex: 2),
        ];

        List<ClusterInvariantSet.NodeCommittedWindow> windows =
        [
            new("node1", 1, 2, new Dictionary<long, CommittedEntryFingerprint>
            {
                [2] = Fingerprint("node1", index: 2, term: 4, payload: "alpha"),
            }),
            new("node2", 1, 2, new Dictionary<long, CommittedEntryFingerprint>
            {
                [2] = Fingerprint("node2", index: 2, term: 4, payload: "beta"),
            }),
        ];

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(
            () => ClusterInvariantSet.CheckQuiescentConvergence(30, views, windows));

        Assert.Equal(ClusterInvariantSet.QuiescentConvergence, error.InvariantName);
        Assert.Contains("differs", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void QuiescentConvergence_AllowsAConvergedCluster()
    {
        List<RaftPartitionView> views =
        [
            View("node1", RaftNodeState.Leader, term: 4, commitIndex: 2),
            View("node2", RaftNodeState.Follower, term: 4, commitIndex: 2),
        ];

        List<ClusterInvariantSet.NodeCommittedWindow> windows =
        [
            new("node1", 1, 2, new Dictionary<long, CommittedEntryFingerprint>
            {
                [2] = Fingerprint("node1", index: 2, term: 4, payload: "alpha"),
            }),
            new("node2", 1, 2, new Dictionary<long, CommittedEntryFingerprint>
            {
                [2] = Fingerprint("node2", index: 2, term: 4, payload: "alpha"),
            }),
        ];

        ClusterInvariantSet.CheckQuiescentConvergence(30, views, windows);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    // ── Committed prefix present ──────────────────────────────────────────

    /// <summary>
    /// A node committed past an id it does not hold. This is the per-step form of the state DST
    /// FINDING 1 reached, which until now only the run-level convergence check could see.
    /// </summary>
    [Fact]
    public void CommittedPrefixPresent_FiresOnAHoleBelowTheFrontier()
    {
        List<RaftPartitionView> views = [View("node1", RaftNodeState.Follower, term: 3, commitIndex: 4)];

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(() =>
            ClusterInvariantSet.CheckCommittedPrefixPresent(
                stepNumber: 9,
                views,
                Stores(Store("node1", firstLogId: 1, maxLogId: 5, lastCheckpoint: -1, missing: [3]))));

        Assert.Equal(ClusterInvariantSet.CommittedPrefixPresent, error.InvariantName);
        Assert.Contains("no entry at 3", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A gap above the frontier is legal. Entries above the commit index are proposed, and two nodes
    /// are entitled to differ there; calling it a hole would fail every run with an in-flight tail.
    /// </summary>
    [Fact]
    public void CommittedPrefixPresent_IsSilentOnAGapAboveTheFrontier()
    {
        List<RaftPartitionView> views = [View("node1", RaftNodeState.Follower, term: 3, commitIndex: 2)];

        ClusterInvariantSet.CheckCommittedPrefixPresent(
            stepNumber: 9,
            views,
            Stores(Store("node1", firstLogId: 1, maxLogId: 6, lastCheckpoint: -1, missing: [5])));
    }

    /// <summary>A compacted prefix is not a hole: the node removed those ids deliberately.</summary>
    [Fact]
    public void CommittedPrefixPresent_IsSilentOnACompactedPrefix()
    {
        List<RaftPartitionView> views = [View("node1", RaftNodeState.Follower, term: 3, commitIndex: 9)];

        ClusterInvariantSet.CheckCommittedPrefixPresent(
            stepNumber: 9,
            views,
            Stores(Store("node1", firstLogId: 7, maxLogId: 9, lastCheckpoint: 7, missing: [],
                compactedThrough: 6)));
    }

    /// <summary>
    /// A node claiming a frontier over a head it never received. This is the case that needs the
    /// compaction record: the log looks identical to a compacted one, and only what the store
    /// actually removed tells the two apart.
    /// </summary>
    [Fact]
    public void CommittedPrefixPresent_FiresOnAHeadThatWasNeverReceived()
    {
        List<RaftPartitionView> views = [View("node1", RaftNodeState.Follower, term: 3, commitIndex: 9)];

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(() =>
            ClusterInvariantSet.CheckCommittedPrefixPresent(
                stepNumber: 9,
                views,
                Stores(Store("node1", firstLogId: 7, maxLogId: 9, lastCheckpoint: -1, missing: [],
                    compactedThrough: 0))));

        Assert.Equal(ClusterInvariantSet.CommittedPrefixPresent, error.InvariantName);
        Assert.Contains("never received", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A node whose head is absent but whose frontier stays below it is legal, and this is the state
    /// DST FINDING 1 reached. It is a stranded replica, not a lying one — which is exactly why no
    /// per-node rule caught that defect and the run-level convergence check did.
    /// </summary>
    [Fact]
    public void CommittedPrefixPresent_IsSilentWhenTheFrontierStaysBelowTheMissingHead()
    {
        List<RaftPartitionView> views = [View("node1", RaftNodeState.Follower, term: 3, commitIndex: 0)];

        ClusterInvariantSet.CheckCommittedPrefixPresent(
            stepNumber: 9,
            views,
            Stores(Store("node1", firstLogId: 2, maxLogId: 2, lastCheckpoint: -1, missing: [],
                compactedThrough: 0)));
    }

    /// <summary>
    /// A node whose store the harness cannot read is skipped, not assumed empty. Assuming would
    /// report a hole on every node running a plain in-memory log.
    /// </summary>
    [Fact]
    public void CommittedPrefixPresent_SkipsANodeWithNoReadableStore()
    {
        List<RaftPartitionView> views = [View("node1", RaftNodeState.Follower, term: 3, commitIndex: 4)];

        ClusterInvariantSet.CheckCommittedPrefixPresent(
            stepNumber: 9,
            views,
            new Dictionary<string, SimulatedWalPartitionSnapshot>());
    }

    // ── Compaction floor ──────────────────────────────────────────────────

    /// <summary>A caller asked to compact past the checkpoint that was certified at the time.</summary>
    [Fact]
    public void CompactionFloorRespected_FiresOnARequestAboveTheCertifiedFloor()
    {
        InvariantViolationException error = Assert.Throws<InvariantViolationException>(() =>
            ClusterInvariantSet.CheckCompactionFloorRespected(
                stepNumber: 4,
                Stores(Store("node1", firstLogId: 1, maxLogId: 20, lastCheckpoint: 6, missing: [],
                    compactionsAboveFloor: 1, worstRequest: 12, worstCertified: 6))));

        Assert.Equal(ClusterInvariantSet.CompactionFloorRespected, error.InvariantName);
        Assert.Contains("compact below 12", error.Message, StringComparison.Ordinal);
    }

    /// <summary>Compaction that stayed at or under the floor is legal, and is the normal case.</summary>
    [Fact]
    public void CompactionFloorRespected_IsSilentWhenEveryRequestStayedUnderTheFloor()
    {
        ClusterInvariantSet.CheckCompactionFloorRespected(
            stepNumber: 4,
            Stores(Store("node1", firstLogId: 6, maxLogId: 20, lastCheckpoint: 6, missing: [])));
    }

    /// <summary>
    /// A log that starts above its first id is not evidence of anything on its own. The head may
    /// have been compacted, or may never have been written — a follower whose write was refused
    /// produces exactly this. The earlier version of the rule compared these indices and fired on
    /// every storage-fault scenario, which is why the rule reads the request instead.
    /// </summary>
    [Fact]
    public void CompactionFloorRespected_IsSilentOnALogThatNeverReceivedItsHead()
    {
        ClusterInvariantSet.CheckCompactionFloorRespected(
            stepNumber: 4,
            Stores(Store("node1", firstLogId: 2, maxLogId: 2, lastCheckpoint: -1, missing: [])));
    }

    private static Dictionary<string, SimulatedWalPartitionSnapshot> Stores(
        params (string Endpoint, SimulatedWalPartitionSnapshot Snapshot)[] entries) =>
        entries.ToDictionary(entry => entry.Endpoint, entry => entry.Snapshot);

    private static (string, SimulatedWalPartitionSnapshot) Store(
        string endpoint,
        long firstLogId,
        long maxLogId,
        long lastCheckpoint,
        long[] missing,
        int compactionsAboveFloor = 0,
        long worstRequest = -1,
        long worstCertified = -1,
        long compactedThrough = 0) =>
        (endpoint, new SimulatedWalPartitionSnapshot(
            PartitionId,
            EntryCount: (int)(maxLogId - firstLogId + 1 - missing.Length),
            FirstLogId: firstLogId,
            MaxLogId: maxLogId,
            LastCheckpoint: lastCheckpoint,
            CountByType: new Dictionary<RaftLogType, int>(),
            MissingIds: missing,
            NonDurableIds: [],
            CompactionsAboveFloor: compactionsAboveFloor,
            WorstCompactionRequest: worstRequest,
            WorstCompactionCertifiedFloor: worstCertified,
            CompactedThrough: compactedThrough));

    private static RaftPartitionView View(string endpoint, RaftNodeState role, long term, long commitIndex) =>
        new(
            endpoint,
            PartitionId,
            role,
            term,
            role == RaftNodeState.Leader ? endpoint : "",
            commitIndex,
            commitIndex,
            commitIndex,
            Quiesced: false,
            ClusterMemberRole.Voter);

    private static CommittedEntryFingerprint Fingerprint(string endpoint, long index, long term, string payload) =>
        CommittedEntryFingerprint.From(
            endpoint,
            new RaftLog
            {
                Id = index,
                Term = term,
                Type = RaftLogType.Committed,
                LogType = "test",
                // global:: because Kommander.System shadows the BCL System namespace here.
                LogData = global::System.Text.Encoding.UTF8.GetBytes(payload),
            });
}
