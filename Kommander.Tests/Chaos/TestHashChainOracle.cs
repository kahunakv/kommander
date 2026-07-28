
using Kommander.Data;

namespace Kommander.Tests.Chaos;

/// <summary>
/// Unit tests for the hash-chain oracle: each synthetic corruption (reordering, gap, conflicting payload,
/// conflicting term, duplicate delivery) must be caught, and identical histories must pass cleanly. These
/// drive <see cref="HashChainStateMachine"/> instances directly with crafted <see cref="RaftLog"/> entries,
/// no cluster required.
/// </summary>
public class TestHashChainOracle
{
    private const int Partition = 1;

    private static RaftLog UserLog(long id, long term, byte[] payload, string logType = "chaos") =>
        new() { Id = id, Term = term, Type = RaftLogType.Committed, LogType = logType, LogData = payload };

    private static async Task Feed(HashChainStateMachine sm, params RaftLog[] logs)
    {
        foreach (RaftLog log in logs)
            await sm.OnReplicationReceived(sm.PartitionId, log);
    }

    private static HashChainStateMachine Node(string endpoint) => new(endpoint, Partition);

    // ── clean baseline ───────────────────────────────────────────────────────────

    [Fact]
    public async Task IdenticalHistories_NoDivergence_Converged_NoDuplicate()
    {
        HashChainStateMachine a = Node("a:1"), b = Node("b:1");
        RaftLog[] logs =
        [
            UserLog(1, 1, [10]), UserLog(2, 1, [20]), UserLog(3, 1, [30]),
            UserLog(4, 2, [40]), UserLog(5, 2, [50]),
        ];
        await Feed(a, logs);
        await Feed(b, logs);

        HashChainAssert.NoDivergence([a, b], Partition);
        HashChainAssert.ConvergedToIndex([a, b], Partition, 5);
        HashChainAssert.NoDuplicateApply([a, b], Partition);

        // Same history ⇒ identical current hash and prefix digests.
        Assert.Equal(a.Snapshot().CurrentHash, b.Snapshot().CurrentHash);
    }

    // ── conflicting content ────────────────────────────────────────────────────────

    [Fact]
    public async Task ConflictingPayload_AtSameIndex_IsDivergence()
    {
        HashChainStateMachine a = Node("a:1"), b = Node("b:1");
        await Feed(a, UserLog(1, 1, [10]), UserLog(2, 1, [20]), UserLog(3, 1, [30]));
        await Feed(b, UserLog(1, 1, [10]), UserLog(2, 1, [20]), UserLog(3, 1, [99])); // payload differs at 3

        Exception ex = Assert.ThrowsAny<Exception>(() => HashChainAssert.NoDivergence([a, b], Partition, seed: 42));
        Assert.Contains("index 3", ex.Message);
        Assert.Contains("divergence", ex.Message);
        Assert.Contains("seed=42", ex.Message);
    }

    [Fact]
    public async Task ConflictingTerm_AtSameIndex_IsDivergence()
    {
        HashChainStateMachine a = Node("a:1"), b = Node("b:1");
        await Feed(a, UserLog(1, 1, [10]), UserLog(2, 1, [20]), UserLog(3, 1, [30]));
        await Feed(b, UserLog(1, 1, [10]), UserLog(2, 1, [20]), UserLog(3, 2, [30])); // term differs at 3

        Exception ex = Assert.ThrowsAny<Exception>(() => HashChainAssert.NoDivergence([a, b], Partition));
        Assert.Contains("index 3", ex.Message);
    }

    // ── gap / hole ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Gap_InOneHistory_IsDetectedAsHole()
    {
        HashChainStateMachine a = Node("a:1"), b = Node("b:1");
        await Feed(a, UserLog(1, 1, [10]), UserLog(2, 1, [20]), UserLog(3, 1, [30]));
        await Feed(b, UserLog(1, 1, [10]), /* skip 2 */ UserLog(3, 1, [30]));  // hole at 2

        Exception ex = Assert.ThrowsAny<Exception>(() => HashChainAssert.NoDivergence([a, b], Partition));
        Assert.Contains("hole", ex.Message);
        Assert.Contains("index 2", ex.Message);
    }

    // ── reordering ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Reordering_RecordsOrderingViolation()
    {
        HashChainStateMachine a = Node("a:1");
        // 1, 3, then a late 2 (index 2 <= lastApplied 3, not previously seen) → ordering violation.
        await Feed(a, UserLog(1, 1, [10]), UserLog(3, 1, [30]), UserLog(2, 1, [20]));

        HashChainSnapshot s = a.Snapshot();
        Assert.Contains(s.OrderingViolations, v => v.Index == 2 && v.LastAppliedIndex == 3);
        // The late entry was NOT folded, so index 2 is absent from the chain.
        Assert.False(s.PrefixHashByIndex.ContainsKey(2));
    }

    // ── duplicate delivery ───────────────────────────────────────────────────────────

    [Fact]
    public async Task IdenticalDuplicate_IsIdempotencyViolation_NotFoldedTwice()
    {
        HashChainStateMachine a = Node("a:1");
        await Feed(a, UserLog(1, 1, [10]), UserLog(2, 1, [20]));
        ulong before = a.Snapshot().CurrentHash;
        long countBefore = a.Snapshot().AppliedCount;

        await Feed(a, UserLog(2, 1, [20])); // exact duplicate of index 2

        HashChainSnapshot s = a.Snapshot();
        Assert.Contains(2L, s.IdempotencyViolations);
        Assert.Empty(s.ConflictingDuplicates);
        Assert.Equal(before, s.CurrentHash);        // not folded again
        Assert.Equal(countBefore, s.AppliedCount);

        Exception ex = Assert.ThrowsAny<Exception>(() => HashChainAssert.NoDuplicateApply([a], Partition));
        Assert.Contains("IDENTICAL duplicate", ex.Message);
    }

    [Fact]
    public async Task ConflictingDuplicate_IsDivergence()
    {
        HashChainStateMachine a = Node("a:1");
        await Feed(a, UserLog(1, 1, [10]), UserLog(2, 1, [20]));
        await Feed(a, UserLog(2, 1, [99])); // same index, different content

        HashChainSnapshot s = a.Snapshot();
        Assert.Contains(s.ConflictingDuplicates, d => d.Index == 2);

        Exception ex = Assert.ThrowsAny<Exception>(() => HashChainAssert.NoDuplicateApply([a], Partition));
        Assert.Contains("CONFLICTING duplicate", ex.Message);
    }

    // ── convergence coordinate ────────────────────────────────────────────────────────

    [Fact]
    public async Task ConvergedToIndex_FailsWhenANodeIsBehind()
    {
        HashChainStateMachine a = Node("a:1"), b = Node("b:1");
        await Feed(a, UserLog(1, 1, [10]), UserLog(2, 1, [20]), UserLog(3, 1, [30]));
        await Feed(b, UserLog(1, 1, [10]), UserLog(2, 1, [20]));  // behind

        HashChainAssert.NoDivergence([a, b], Partition);  // agree on their common prefix (1,2)
        Exception ex = Assert.ThrowsAny<Exception>(() => HashChainAssert.ConvergedToIndex([a, b], Partition, 3));
        Assert.Contains("has not applied through index 3", ex.Message);
    }
}
