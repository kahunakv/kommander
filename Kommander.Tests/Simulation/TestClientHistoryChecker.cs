using Kommander.Data;
using Kommander.Tests.Simulation.History;

namespace Kommander.Tests.Simulation;

/// <summary>
/// Tests that each append-model rule fires on the history it is meant to catch and stays silent on
/// a legal one.
///
/// <para><b>Why the history checker exists.</b> Every per-step invariant reads node state, and a
/// node can be perfectly consistent with itself and with its peers while having told a client
/// something untrue. The lost write is the clearest case: an acknowledgement the log does not
/// honour leaves no trace in any node's state, because there is nothing there to be inconsistent
/// with.</para>
///
/// <para>Pure history checks. No cluster runs, so the file costs milliseconds.</para>
/// </summary>
[Trait("Category", "DSTSmoke")]
public sealed class TestClientHistoryChecker
{
    // ── Acknowledged appends are present ──────────────────────────────────

    /// <summary>A lost write: the client was told yes and the log holds nothing at that index.</summary>
    [Fact]
    public void AcknowledgedAppendPresent_FiresOnALostWrite()
    {
        ClientHistory history = History(Ok(id: 0, index: 1, payload: "a"));

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(() =>
            ClientHistoryChecker.Check(history, [], stepNumber: 5));

        Assert.Equal(ClientHistoryChecker.AcknowledgedAppendPresent, error.InvariantName);
        Assert.Contains("holds no committed entry at 1", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The index is occupied by somebody else's payload. This is worse than a lost write, because the
    /// client will read back a value it never sent and has no reason to doubt it.
    /// </summary>
    [Fact]
    public void AcknowledgedAppendPresent_FiresWhenTheIndexHoldsAnotherPayload()
    {
        ClientHistory history = History(Ok(id: 0, index: 1, payload: "a"));

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(() =>
            ClientHistoryChecker.Check(history, [Entry(1, "b")], stepNumber: 5));

        Assert.Equal(ClientHistoryChecker.AcknowledgedAppendPresent, error.InvariantName);
        Assert.Contains("a different payload", error.Message, StringComparison.Ordinal);
    }

    /// <summary>A history whose acknowledgements the log honours passes.</summary>
    [Fact]
    public void AcknowledgedAppendPresent_IsSilentOnAnHonouredHistory()
    {
        ClientHistory history = History(
            Ok(id: 0, index: 1, payload: "a"),
            Ok(id: 1, index: 2, payload: "b", invokedAt: 2, completedAt: 3));

        ClientHistoryChecker.Check(history, [Entry(1, "a"), Entry(2, "b")], stepNumber: 5);
    }

    /// <summary>
    /// An operation whose outcome the client never learned is not required to be present. A timeout
    /// may leave the entry committed with only the acknowledgement lost, and demanding its absence
    /// would fail every run that timed out on a healthy commit.
    /// </summary>
    [Fact]
    public void AnUnknownOutcome_IsAllowedToBeEitherPresentOrAbsent()
    {
        ClientHistory present = History(Info(id: 0, payload: "a"));
        ClientHistoryChecker.Check(present, [Entry(1, "a")], stepNumber: 5);

        ClientHistory absent = History(Info(id: 0, payload: "a"));
        ClientHistoryChecker.Check(absent, [], stepNumber: 5);
    }

    // ── Acknowledged appends are unique ───────────────────────────────────

    /// <summary>One append applied twice.</summary>
    [Fact]
    public void AcknowledgedAppendUnique_FiresOnADuplicatedEntry()
    {
        ClientHistory history = History(Ok(id: 0, index: 1, payload: "a"));

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(() =>
            ClientHistoryChecker.Check(history, [Entry(1, "a"), Entry(2, "a")], stepNumber: 5));

        Assert.Equal(ClientHistoryChecker.AcknowledgedAppendUnique, error.InvariantName);
        Assert.Contains("2 times", error.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// One index promised to two clients. This is the shape of the reissued-index defect: both
    /// clients were told yes, and only one of them is going to be right.
    /// </summary>
    [Fact]
    public void AcknowledgedAppendUnique_FiresWhenTwoAppendsShareAnIndex()
    {
        ClientHistory history = History(
            Ok(id: 0, index: 1, payload: "a"),
            Ok(id: 1, index: 1, payload: "b", invokedAt: 2, completedAt: 3));

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(() =>
            ClientHistoryChecker.Check(history, [Entry(1, "a")], stepNumber: 5));

        Assert.Equal(ClientHistoryChecker.AcknowledgedAppendUnique, error.InvariantName);
    }

    // ── Real-time order ───────────────────────────────────────────────────

    /// <summary>
    /// An append that began only after another was acknowledged took the lower index. The client saw
    /// one finish before it started the next, so the log may not reorder them.
    /// </summary>
    [Fact]
    public void AppendOrderRespectsRealTime_FiresOnAReorderedPair()
    {
        ClientHistory history = History(
            Ok(id: 0, index: 5, payload: "a", invokedAt: 0, completedAt: 1),
            Ok(id: 1, index: 4, payload: "b", invokedAt: 2, completedAt: 3));

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(() =>
            ClientHistoryChecker.Check(history, [Entry(4, "b"), Entry(5, "a")], stepNumber: 5));

        Assert.Equal(ClientHistoryChecker.AppendOrderRespectsRealTime, error.InvariantName);
    }

    /// <summary>
    /// Two appends that overlapped may land in either order. Ordering them anyway would report a
    /// failure on every healthy run with more than one client in flight.
    /// </summary>
    [Fact]
    public void AppendOrderRespectsRealTime_IsSilentOnConcurrentAppends()
    {
        ClientHistory history = History(
            Ok(id: 0, index: 5, payload: "a", invokedAt: 0, completedAt: 4),
            Ok(id: 1, index: 4, payload: "b", invokedAt: 1, completedAt: 3));

        ClientHistoryChecker.Check(history, [Entry(4, "b"), Entry(5, "a")], stepNumber: 5);
    }

    // ── Refused appends are absent ────────────────────────────────────────

    /// <summary>A client told no whose write is in the log anyway.</summary>
    [Fact]
    public void RefusedAppendAbsent_FiresOnAPhantomWrite()
    {
        ClientHistory history = History(
            Fail(id: 0, payload: "a", status: RaftOperationStatus.NodeIsNotLeader));

        InvariantViolationException error = Assert.Throws<InvariantViolationException>(() =>
            ClientHistoryChecker.Check(history, [Entry(1, "a")], stepNumber: 5));

        Assert.Equal(ClientHistoryChecker.RefusedAppendAbsent, error.InvariantName);
        Assert.Contains("was refused", error.Message, StringComparison.Ordinal);
    }

    /// <summary>A refusal the log honours passes.</summary>
    [Fact]
    public void RefusedAppendAbsent_IsSilentWhenTheRefusalHeld()
    {
        ClientHistory history = History(
            Fail(id: 0, payload: "a", status: RaftOperationStatus.NodeIsNotLeader));

        ClientHistoryChecker.Check(history, [Entry(1, "b")], stepNumber: 5);
    }

    // ── Classification ────────────────────────────────────────────────────

    /// <summary>
    /// Only statuses that cannot have reached the log are refusals. Everything ambiguous is unknown,
    /// because calling an ambiguous answer a refusal reports a phantom write every time a quorum
    /// commits an entry whose acknowledgement was lost.
    /// </summary>
    [Theory]
    [InlineData(RaftOperationStatus.Success, ClientOperationOutcome.Ok)]
    [InlineData(RaftOperationStatus.NodeIsNotLeader, ClientOperationOutcome.Fail)]
    [InlineData(RaftOperationStatus.ProposalQueueFull, ClientOperationOutcome.Fail)]
    [InlineData(RaftOperationStatus.RestoreInProgress, ClientOperationOutcome.Fail)]
    [InlineData(RaftOperationStatus.ProposalTimeout, ClientOperationOutcome.Info)]
    [InlineData(RaftOperationStatus.OperationCancelled, ClientOperationOutcome.Info)]
    [InlineData(RaftOperationStatus.ReplicationFailed, ClientOperationOutcome.Info)]
    [InlineData(RaftOperationStatus.Errored, ClientOperationOutcome.Info)]
    [InlineData(RaftOperationStatus.LeaderInOldTerm, ClientOperationOutcome.Info)]
    public void Classify_TreatsAnAmbiguousAnswerAsUnknown(
        RaftOperationStatus status, ClientOperationOutcome expected) =>
        Assert.Equal(expected, ClientHistory.Classify(status));

    // ── Helpers ───────────────────────────────────────────────────────────

    /// <summary>
    /// Builds a history from hand-written operations. The recorder normally issues the calls itself,
    /// so this reaches the same list through reflection-free construction: the checks read
    /// <see cref="ClientHistory.Operations"/> and nothing else.
    /// </summary>
    private static ClientHistory History(params ClientOperation[] operations)
    {
        ClientHistory history = new();

        foreach (ClientOperation operation in operations)
            history.Record(operation);

        return history;
    }

    private static ClientOperation Ok(
        int id, long index, string payload, int invokedAt = 0, int completedAt = 1) =>
        new(id, "Test", Bytes(payload), invokedAt, completedAt, invokedAt, completedAt,
            ClientOperationOutcome.Ok, RaftOperationStatus.Success, index);

    private static ClientOperation Fail(
        int id, string payload, RaftOperationStatus status, int invokedAt = 0, int completedAt = 1) =>
        new(id, "Test", Bytes(payload), invokedAt, completedAt, invokedAt, completedAt,
            ClientOperationOutcome.Fail, status, LogIndex: -1);

    private static ClientOperation Info(
        int id, string payload, int invokedAt = 0, int completedAt = 1) =>
        new(id, "Test", Bytes(payload), invokedAt, completedAt, invokedAt, completedAt,
            ClientOperationOutcome.Info, RaftOperationStatus.ProposalTimeout, LogIndex: -1);

    private static RaftLog Entry(long id, string payload) =>
        new() { Id = id, Term = 1, Type = RaftLogType.Committed, LogData = Bytes(payload) };

    private static byte[] Bytes(string value) => global::System.Text.Encoding.UTF8.GetBytes(value);
}
