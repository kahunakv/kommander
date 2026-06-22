
using Kommander.Gossip;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Unit tests for <see cref="LivenessTable"/> focused on the newly-returned transition
/// sets from <see cref="LivenessTable.AdvanceExpiry"/> and
/// <see cref="LivenessTable.ApplyUpdates"/>, which are used by
/// <see cref="RaftManager.WakePartitionsForLeader"/> to restore quiesced partitions to
/// the hot set when their believed leader node goes Suspect or Dead.
/// </summary>
public sealed class TestLivenessTable
{
    // ── AdvanceExpiry ──────────────────────────────────────────────────────

    [Fact]
    public void AdvanceExpiry_NoSuspects_ReturnsEmpty()
    {
        LivenessTable t = new();
        IReadOnlyList<string> died = t.AdvanceExpiry(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(1));
        Assert.Empty(died);
    }

    [Fact]
    public void AdvanceExpiry_SuspectNotYetExpired_ReturnsEmpty()
    {
        LivenessTable t = new();
        t.MarkSuspect("node-a");

        // Expiry window has not elapsed — should not transition.
        IReadOnlyList<string> died = t.AdvanceExpiry(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(60));
        Assert.Empty(died);
        Assert.Equal(MemberLivenessState.Suspect, t.GetState("node-a"));
    }

    [Fact]
    public void AdvanceExpiry_SuspectExpired_ReturnsDeadEndpoint()
    {
        LivenessTable t = new();
        t.MarkSuspect("node-a");

        // Advance clock past suspicion timeout.
        DateTimeOffset future = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
        IReadOnlyList<string> died = t.AdvanceExpiry(future, TimeSpan.FromSeconds(1));

        Assert.Single(died);
        Assert.Equal("node-a", died[0]);
        Assert.Equal(MemberLivenessState.Dead, t.GetState("node-a"));
    }

    [Fact]
    public void AdvanceExpiry_MultipleSuspects_ReturnsAllExpired()
    {
        LivenessTable t = new();
        t.MarkSuspect("node-a");
        t.MarkSuspect("node-b");

        DateTimeOffset future = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
        IReadOnlyList<string> died = t.AdvanceExpiry(future, TimeSpan.FromSeconds(1));

        Assert.Equal(2, died.Count);
        Assert.Contains("node-a", died);
        Assert.Contains("node-b", died);
    }

    [Fact]
    public void AdvanceExpiry_AlreadyDead_NotReturned()
    {
        // An already-Dead node is not re-reported by subsequent AdvanceExpiry calls.
        LivenessTable t = new();
        t.MarkSuspect("node-a");

        DateTimeOffset future = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
        t.AdvanceExpiry(future, TimeSpan.FromSeconds(1)); // first call → transitions to Dead

        IReadOnlyList<string> died2 = t.AdvanceExpiry(future + TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(1));
        Assert.Empty(died2); // already Dead — no new transition
    }

    // ── ApplyUpdates ───────────────────────────────────────────────────────

    [Fact]
    public void ApplyUpdates_EmptyList_ReturnsNoSuspects()
    {
        LivenessTable t = new();
        (bool selfSuspected, IReadOnlyList<string> suspects) = t.ApplyUpdates("self", []);
        Assert.False(selfSuspected);
        Assert.Empty(suspects);
    }

    [Fact]
    public void ApplyUpdates_AliveUpdate_ReturnsNoSuspects()
    {
        LivenessTable t = new();
        MemberLivenessEntry[] updates =
        [
            new("node-a", MemberLivenessState.Alive, 1, DateTimeOffset.UtcNow),
        ];

        (bool selfSuspected, IReadOnlyList<string> suspects) = t.ApplyUpdates("self", updates);
        Assert.False(selfSuspected);
        Assert.Empty(suspects);
    }

    [Fact]
    public void ApplyUpdates_SuspectPeer_ReturnedInList()
    {
        LivenessTable t = new();
        MemberLivenessEntry[] updates =
        [
            new("node-a", MemberLivenessState.Suspect, 1, DateTimeOffset.UtcNow),
        ];

        (bool selfSuspected, IReadOnlyList<string> suspects) = t.ApplyUpdates("self", updates);
        Assert.False(selfSuspected);
        Assert.Single(suspects);
        Assert.Equal("node-a", suspects[0]);
    }

    [Fact]
    public void ApplyUpdates_DeadPeer_ReturnedInList()
    {
        LivenessTable t = new();
        MemberLivenessEntry[] updates =
        [
            new("node-a", MemberLivenessState.Dead, 1, DateTimeOffset.UtcNow),
        ];

        (bool selfSuspected, IReadOnlyList<string> suspects) = t.ApplyUpdates("self", updates);
        Assert.False(selfSuspected);
        Assert.Single(suspects);
        Assert.Equal("node-a", suspects[0]);
    }

    [Fact]
    public void ApplyUpdates_SelfSuspected_NotInPeerList()
    {
        LivenessTable t = new();
        MemberLivenessEntry[] updates =
        [
            new("self", MemberLivenessState.Suspect, 1, DateTimeOffset.UtcNow),
        ];

        (bool selfSuspected, IReadOnlyList<string> suspects) = t.ApplyUpdates("self", updates);
        Assert.True(selfSuspected);
        Assert.Empty(suspects); // self is handled by selfSuspected, not the peer list
    }

    [Fact]
    public void ApplyUpdates_MixedUpdates_CorrectlySeparated()
    {
        LivenessTable t = new();
        DateTimeOffset now = DateTimeOffset.UtcNow;
        MemberLivenessEntry[] updates =
        [
            new("node-a", MemberLivenessState.Alive,   1, now),
            new("node-b", MemberLivenessState.Suspect,  1, now),
            new("node-c", MemberLivenessState.Dead,     1, now),
            new("self",   MemberLivenessState.Suspect,  1, now),
        ];

        (bool selfSuspected, IReadOnlyList<string> suspects) = t.ApplyUpdates("self", updates);
        Assert.True(selfSuspected);
        Assert.Equal(2, suspects.Count);
        Assert.Contains("node-b", suspects);
        Assert.Contains("node-c", suspects);
        Assert.DoesNotContain("node-a", suspects);
        Assert.DoesNotContain("self", suspects);
    }

    [Fact]
    public void ApplyUpdates_AlreadySuspect_ShouldUpdateFiltersNoop()
    {
        // A node already in Suspect state: a new Suspect update with the same incarnation
        // is a no-op (ShouldUpdate returns false), so it must NOT appear in the returned list.
        LivenessTable t = new();
        DateTimeOffset now = DateTimeOffset.UtcNow;
        t.MarkSuspect("node-a"); // puts incarnation=0, Suspect

        MemberLivenessEntry[] updates =
        [
            new("node-a", MemberLivenessState.Suspect, 0, now), // same incarnation — no-op
        ];

        (_, IReadOnlyList<string> suspects) = t.ApplyUpdates("self", updates);
        Assert.Empty(suspects); // no state change — not a new wakeup trigger
    }
}
