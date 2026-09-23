using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;

namespace Kommander.Tests.Simulation.History;

/// <summary>
/// Checks a run's client history against the log the cluster ended up with.
///
/// <para><b>The model is a replicated log with confirmed reads, not a register.</b> Kommander's
/// client surface appends entries and returns the index each one took, so the properties worth
/// checking are the ones an append model states: an acknowledged append is present, it is present
/// once, acknowledged appends appear in the order the client observed, and a refused append is
/// absent. A register model with compare-and-set would be a better fit for a key-value store built
/// on top, and a worse fit for the library itself.</para>
///
/// <para><b>Reads.</b> A consumer serves a read from its own application state after
/// <c>ConfirmLocalApplicationAsync</c> returns true, so the read of the log is that state. Three
/// rules judge it: a read holds every append acknowledged before it began, a read holds no append
/// that was refused, and a read holds everything an earlier read held. Together these are
/// linearizability for reads of a log whose committed prefixes agree, which the run-level rules
/// establish separately (<c>committed-entries-agree</c>, <c>applied-state-agrees</c>). The first rule is
/// the one a deposed leader breaks when it answers a read without a quorum round
/// (<c>bf275e4a</c>).</para>
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
    /// A served read holds every append that was acknowledged before the read began, with the
    /// payload the client sent.
    ///
    /// <para>The promise of a confirmed read. A node that confirmed it is current and then served
    /// state without an acknowledged write is serving stale state as an authoritative answer: the
    /// Jepsen <c>register / partition</c> violation behind <c>bf275e4a</c>, where a leader cut off from
    /// the majority answered reads for eleven seconds.</para>
    /// </summary>
    public const string ReadObservesAcknowledgedAppends = "read-observes-acknowledged-appends";

    /// <summary>
    /// A served read holds no append that was refused.
    ///
    /// <para>A refused append must never take effect, so a read that shows one has shown the client
    /// a write it was told did not happen. <see cref="RefusedAppendAbsent"/> judges the final log;
    /// this judges what a client saw on the way.</para>
    /// </summary>
    public const string ReadObservesNoRefusedAppend = "read-observes-no-refused-append";

    /// <summary>
    /// A served read holds every entry that an earlier served read held, with the same content.
    ///
    /// <para>Two reads that did not overlap in time must not go backwards. A client that saw an
    /// entry and then, on its next read at another node, did not see it, has watched a write
    /// disappear, whether or not that write was its own.</para>
    /// </summary>
    public const string ReadsRespectRealTime = "reads-respect-real-time";

    /// <summary>
    /// Runs every append-model check. Call at the end of a run, after the cluster has converged.
    /// </summary>
    /// <param name="history">What the clients were told.</param>
    /// <param name="committed">
    /// The committed entries of one converged node, in index order. Entries of other types are
    /// ignored: a proposed entry is not yet a promise to anybody.
    /// </param>
    /// <param name="stepNumber">Step to name in a violation, for the failure report.</param>
    /// <param name="compactedThrough">
    /// Highest index compaction has removed from the reader's log, or -1 when nothing was compacted.
    ///
    /// <para><b>Why the checker has to be told.</b> Compaction deletes committed entries on purpose.
    /// Without this, the presence rule reports every compacted acknowledgement as a lost write, and
    /// it does so the first time a run ever compacts — which is exactly what happened when
    /// checkpoints entered the fault vocabulary. The rule was not wrong before; it was only sound
    /// because compaction never happened.</para>
    ///
    /// <para>The refusal rule is left alone. Compaction can hide a wrongly-refused write by removing
    /// it, which costs the rule some power and never makes it raise a false alarm — the safe
    /// direction of the two.</para>
    /// </param>
    public static void Check(
        ClientHistory history,
        IReadOnlyList<RaftLog> committed,
        int stepNumber,
        long compactedThrough = -1)
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
        CheckAcknowledgedPresent(history, entries, stepNumber, compactedThrough);
        CheckOrderRespectsRealTime(history, stepNumber);
        CheckRefusedAbsent(history, entries, stepNumber);
        CheckReads(history, stepNumber);
    }

    /// <summary>
    /// Runs the three read rules. They need no log: each read carries the state it returned.
    ///
    /// <para>Order matters for the same reason as for appends. A read that misses an acknowledged
    /// append names the cause at the read; a later read that goes backwards would name only a
    /// symptom.</para>
    /// </summary>
    public static void CheckReads(ClientHistory history, int stepNumber)
    {
        List<ClientOperation> reads = history.Operations
            .Where(op => op.Kind == ClientOperationKind.Read && op.Outcome == ClientOperationOutcome.Ok)
            .ToList();

        if (reads.Count == 0)
            return;

        List<ClientOperation> appends = Appends(history).ToList();

        foreach (ClientOperation read in reads)
        {
            IReadOnlyDictionary<long, ulong> observed = read.Observed!;

            foreach (ClientOperation append in appends)
            {
                ulong expected = SimulatedPartitionStateTransfer.Hash(append.Type, append.Payload);

                if (append.Outcome == ClientOperationOutcome.Ok
                    && append.CompletedAtSequence < read.InvokedAtSequence)
                {
                    if (!observed.TryGetValue(append.LogIndex, out ulong held))
                    {
                        throw Violation(
                            ReadObservesAcknowledgedAppends,
                            stepNumber,
                            $"{read} began after {append} was acknowledged, yet the state it " +
                            $"returned holds nothing at {append.LogIndex}. The node served a stale read.");
                    }

                    if (held != expected)
                    {
                        throw Violation(
                            ReadObservesAcknowledgedAppends,
                            stepNumber,
                            $"{read} began after {append} was acknowledged, yet the state it " +
                            $"returned holds a different entry at {append.LogIndex}.");
                    }
                }

                if (append.Outcome == ClientOperationOutcome.Fail && observed.Values.Contains(expected))
                {
                    throw Violation(
                        ReadObservesNoRefusedAppend,
                        stepNumber,
                        $"{read} returned the payload of {append}, which the cluster refused.");
                }
            }
        }

        foreach (ClientOperation earlier in reads)
        {
            foreach (ClientOperation later in reads)
            {
                if (later.Id == earlier.Id || later.InvokedAtSequence < earlier.CompletedAtSequence)
                    continue;

                foreach ((long id, ulong hash) in earlier.Observed!)
                {
                    if (later.Observed!.TryGetValue(id, out ulong held) && held == hash)
                        continue;

                    throw Violation(
                        ReadsRespectRealTime,
                        stepNumber,
                        $"{later} began after {earlier} returned, yet it " +
                        (later.Observed.ContainsKey(id) ? "holds a different entry" : "holds nothing") +
                        $" at {id}, which the earlier read held.");
                }
            }
        }
    }

    /// <summary>The appends of a history. The append rules ignore reads.</summary>
    private static IEnumerable<ClientOperation> Appends(ClientHistory history) =>
        history.Operations.Where(op => op.Kind == ClientOperationKind.Append);

    private static void CheckAcknowledgedPresent(
        ClientHistory history, List<RaftLog> entries, int stepNumber, long compactedThrough)
    {
        foreach (ClientOperation operation in Appends(history))
        {
            if (operation.Outcome != ClientOperationOutcome.Ok)
                continue;

            // Compaction removed it deliberately. Its absence says nothing about whether the write
            // took effect, so the rule has nothing to check here.
            if (operation.LogIndex <= compactedThrough)
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

        foreach (ClientOperation operation in Appends(history))
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
        foreach (ClientOperation earlier in Appends(history))
        {
            if (earlier.Outcome != ClientOperationOutcome.Ok)
                continue;

            foreach (ClientOperation later in Appends(history))
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
        foreach (ClientOperation operation in Appends(history))
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
