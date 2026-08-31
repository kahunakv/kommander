using Kommander.Data;

namespace Kommander.Tests.Simulation.History;

/// <summary>
/// Checks a run's client history against the log the cluster ended up with.
///
/// <para><b>The model is a replicated log, not a register.</b> Kommander's client surface appends
/// entries and returns the index each one took, so the properties worth checking are the ones an
/// append model states: an acknowledged append is present, it is present once, acknowledged appends
/// appear in the order the client observed, and a refused append is absent. A register model with
/// reads and compare-and-set would be a better fit for a key-value store built on top, and a worse
/// fit for the library itself.</para>
///
/// <para><b>What the checks are read against.</b> One node's committed entries. This is sound only
/// because the run-level convergence invariant separately establishes that live nodes hold identical
/// committed prefixes — the two checks are meant to be run together, and the history check alone
/// would be reading one replica's opinion.</para>
/// </summary>
public static class ClientHistoryChecker
{
    /// <summary>
    /// An acknowledged append is in the log at the index the client was given.
    ///
    /// <para>This is the promise the acknowledgement made. Breaking it is a lost write, which is the
    /// most serious thing a log can do and the least visible: every node can be internally
    /// consistent and agree with every other while the entry a client was promised is gone.</para>
    /// </summary>
    public const string AcknowledgedAppendPresent = "acknowledged-append-present";

    /// <summary>
    /// An acknowledged append appears exactly once, and no two of them share an index.
    ///
    /// <para>Two entries carrying one client's single append means the log applied it twice, which
    /// for a non-idempotent consumer is a different kind of corruption from losing it. Two
    /// acknowledgements naming one index means the log promised one slot to two clients, which is
    /// the shape <c>6e659a78</c> produced.</para>
    /// </summary>
    public const string AcknowledgedAppendUnique = "acknowledged-append-unique";

    /// <summary>
    /// If one append was acknowledged before another was issued, the first took the lower index.
    ///
    /// <para>Only non-overlapping operations are compared. Two appends in flight at the same time
    /// may land in either order — that is concurrency, not a violation — and a checker that ordered
    /// them anyway would report a failure on every healthy run with two clients.</para>
    /// </summary>
    public const string AppendOrderRespectsRealTime = "append-order-respects-real-time";

    /// <summary>
    /// A refused append is not in the log.
    ///
    /// <para>Only appends whose status cannot have reached the log count as refused; see
    /// <see cref="ClientHistory.Classify"/> for why the classification is deliberately pessimistic.
    /// A client told "no" that later finds its write applied has been lied to, and it will have
    /// retried on that basis.</para>
    /// </summary>
    public const string RefusedAppendAbsent = "refused-append-absent";

    /// <summary>
    /// Runs every append-model check. Call at the end of a run, after the cluster has converged.
    /// </summary>
    /// <param name="history">What the clients were told.</param>
    /// <param name="committed">
    /// The committed entries of one converged node, in index order. Entries of other types are
    /// ignored: a proposed entry is not yet a promise to anybody.
    /// </param>
    /// <param name="stepNumber">Step to name in a violation, for the failure report.</param>
    public static void Check(ClientHistory history, IReadOnlyList<RaftLog> committed, int stepNumber)
    {
        List<RaftLog> entries = committed
            .Where(entry => entry.Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint)
            .ToList();

        // Uniqueness first, and the order is not arbitrary. When one index is promised to two
        // clients, both rules are genuinely broken — the log cannot hold both payloads, so one of
        // them is also a lost write. "Two appends took one index" names the cause; "an
        // acknowledged entry is missing" names the symptom, and only the first tells the reader
        // where to look.
        CheckAcknowledgedUnique(history, entries, stepNumber);
        CheckAcknowledgedPresent(history, entries, stepNumber);
        CheckOrderRespectsRealTime(history, stepNumber);
        CheckRefusedAbsent(history, entries, stepNumber);
    }

    private static void CheckAcknowledgedPresent(
        ClientHistory history, List<RaftLog> entries, int stepNumber)
    {
        foreach (ClientOperation operation in history.Operations)
        {
            if (operation.Outcome != ClientOperationOutcome.Ok)
                continue;

            RaftLog? entry = entries.FirstOrDefault(candidate => candidate.Id == operation.LogIndex);

            if (entry is null)
            {
                throw Violation(
                    AcknowledgedAppendPresent,
                    stepNumber,
                    $"{operation} was acknowledged but the log holds no committed entry at " +
                    $"{operation.LogIndex}.");
            }

            if (!Matches(entry, operation))
            {
                throw Violation(
                    AcknowledgedAppendPresent,
                    stepNumber,
                    $"{operation} was acknowledged at index {operation.LogIndex} but that index " +
                    $"holds a different payload.");
            }
        }
    }

    private static void CheckAcknowledgedUnique(
        ClientHistory history, List<RaftLog> entries, int stepNumber)
    {
        Dictionary<long, ClientOperation> byIndex = new();

        foreach (ClientOperation operation in history.Operations)
        {
            if (operation.Outcome != ClientOperationOutcome.Ok)
                continue;

            if (byIndex.TryGetValue(operation.LogIndex, out ClientOperation? other))
            {
                throw Violation(
                    AcknowledgedAppendUnique,
                    stepNumber,
                    $"{operation} and {other} were both acknowledged at index {operation.LogIndex}.");
            }

            byIndex[operation.LogIndex] = operation;

            int copies = entries.Count(entry => Matches(entry, operation));

            if (copies > 1)
            {
                throw Violation(
                    AcknowledgedAppendUnique,
                    stepNumber,
                    $"{operation} appears in the log {copies} times.");
            }
        }
    }

    private static void CheckOrderRespectsRealTime(ClientHistory history, int stepNumber)
    {
        foreach (ClientOperation earlier in history.Operations)
        {
            if (earlier.Outcome != ClientOperationOutcome.Ok)
                continue;

            foreach (ClientOperation later in history.Operations)
            {
                if (later.Id == earlier.Id || later.Outcome != ClientOperationOutcome.Ok)
                    continue;

                // Only operations that did not overlap, ordered on the history's own sequence
                // rather than on the step number: a client can issue and complete several appends
                // inside one step, and step numbers would call those concurrent. Concurrent appends
                // may land in either order, so they are skipped.
                if (later.InvokedAtSequence < earlier.CompletedAtSequence)
                    continue;

                if (later.LogIndex > earlier.LogIndex)
                    continue;

                throw Violation(
                    AppendOrderRespectsRealTime,
                    stepNumber,
                    $"{later} began after {earlier} was acknowledged, yet took the lower index.");
            }
        }
    }

    private static void CheckRefusedAbsent(
        ClientHistory history, List<RaftLog> entries, int stepNumber)
    {
        foreach (ClientOperation operation in history.Operations)
        {
            if (operation.Outcome != ClientOperationOutcome.Fail)
                continue;

            RaftLog? entry = entries.FirstOrDefault(candidate => Matches(candidate, operation));

            if (entry is not null)
            {
                throw Violation(
                    RefusedAppendAbsent,
                    stepNumber,
                    $"{operation} was refused, yet its payload is committed at index {entry.Id}.");
            }
        }
    }

    private static bool Matches(RaftLog entry, ClientOperation operation) =>
        entry.LogData is not null && entry.LogData.AsSpan().SequenceEqual(operation.Payload);

    private static InvariantViolationException Violation(string name, int stepNumber, string message) =>
        new(name, message, stepNumber, selectedEvent: null, lastValidSnapshot: null, failingSnapshot: null);
}
