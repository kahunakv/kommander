using System.Globalization;
using System.Text;
using Kommander.Data;
using Kommander.Diagnostics;

namespace Kommander.Tests.Simulation.Trace;

/// <summary>
/// Records one simulated node's write-ahead events for the TLA+ trace validator
/// (<c>formal/tla/WalDurabilityTrace.tla</c>), across every life of the node.
///
/// <para><b>Why one recorder per node and not per manager.</b> A restart builds a new
/// <see cref="RaftManager"/> over the same store. The validator's model of the disk must carry over
/// the restart, so the trace must too: the recorder lives with the store, and every manager the node
/// builds gets the same instance.</para>
///
/// <para><b>Order.</b> Events arrive from the partition executors and from the WAL worker. They are
/// appended under one lock in arrival order. For one operation that order is causal: the scheduler
/// records its Write event before the completion that triggers the Durable and Resolution events is
/// even posted.</para>
///
/// <para><b>Output.</b> <see cref="WriteModules"/> writes one TLA+ module per partition. A module
/// rather than JSON, so the validator needs nothing beyond the pinned <c>tla2tools.jar</c>.</para>
/// </summary>
public sealed class WalTraceRecorder : IWalTraceSink
{
    private readonly object gate = new();

    private readonly List<WalTraceEvent> events = [];

    public WalTraceRecorder(string endpoint)
    {
        Endpoint = endpoint;
    }

    /// <summary>The node this trace belongs to.</summary>
    public string Endpoint { get; }

    /// <summary>Events recorded so far, all partitions.</summary>
    public int Count
    {
        get
        {
            lock (gate)
                return events.Count;
        }
    }

    void IWalTraceSink.Record(WalTraceEvent traceEvent)
    {
        lock (gate)
            events.Add(traceEvent);
    }

    /// <summary>
    /// Records a crash for every partition. A crash the model cannot describe is recorded as
    /// unsupported instead, and validation of the node stops there: a store with a write latency
    /// loses writes it reported as synced, and the lying-disk fault loses resolutions it fsynced.
    /// Both break the fsync contract the model assumes, on purpose.
    /// </summary>
    public void RecordCrash(bool breaksFsyncContract)
    {
        lock (gate)
        {
            events.Add(breaksFsyncContract
                ? new WalTraceEvent(-1, WalTraceEventKind.Unsupported, Detail: "crash outside the fsync contract")
                : new WalTraceEvent(-1, WalTraceEventKind.Crash));
        }
    }

    /// <summary>Copy of the events of one partition, with node-wide events (crashes) included.</summary>
    internal IReadOnlyList<WalTraceEvent> EventsFor(int partitionId)
    {
        lock (gate)
            return events.Where(e => e.PartitionId == partitionId || e.PartitionId == -1).ToList();
    }

    /// <summary>The partitions that recorded at least one event.</summary>
    public IReadOnlyList<int> Partitions()
    {
        lock (gate)
            return events.Where(e => e.PartitionId >= 0).Select(e => e.PartitionId).Distinct().Order().ToList();
    }

    /// <summary>
    /// Writes one module per partition into <paramref name="directory"/> and returns the paths.
    /// Each module defines <c>TraceN</c> (an id bound for the model's constant N) and
    /// <c>TraceLog</c> (the events as records with every field present).
    /// </summary>
    public IReadOnlyList<string> WriteModules(string directory)
    {
        global::System.IO.Directory.CreateDirectory(directory);
        List<string> paths = [];

        foreach (int partitionId in Partitions())
        {
            string module = $"WalTrace_{Sanitize(Endpoint)}_p{partitionId}";
            string path = global::System.IO.Path.Combine(directory, module + ".tla");
            global::System.IO.File.WriteAllText(path, ToModule(module, EventsFor(partitionId)));
            paths.Add(path);
        }

        return paths;
    }

    /// <summary>Renders events as a TLA+ module. Internal so a test can render a hand-built trace.</summary>
    internal static string ToModule(string moduleName, IReadOnlyList<WalTraceEvent> trace)
    {
        // N must cover every id and every exclusive frontier minus one. long.MaxValue (the empty
        // riding floor) is written as the model's Inf, N + 2.
        long maxId = 1;
        foreach (WalTraceEvent e in trace)
        {
            if (e.Rows is not null)
                foreach (WalTraceRow row in e.Rows)
                    maxId = Math.Max(maxId, row.Id);
            maxId = Math.Max(maxId, e.Max);
            if (e.SparseIds is not null)
                foreach (long id in e.SparseIds)
                    maxId = Math.Max(maxId, id);
            if (e.Frontiers is { } f)
            {
                maxId = Math.Max(maxId, f.CommitIndex - 1);
                maxId = Math.Max(maxId, f.PresentIndex - 1);
                maxId = Math.Max(maxId, f.DurablePresentIndex - 1);
                maxId = Math.Max(maxId, f.DurableResolvedIndex - 1);
                maxId = Math.Max(maxId, f.RidingResolvedIndex - 1);
                maxId = Math.Max(maxId, f.WrittenResolvedIndex - 1);
                if (f.RidingResolvedFloor != long.MaxValue)
                    maxId = Math.Max(maxId, f.RidingResolvedFloor);
            }
        }

        long inf = maxId + 2;

        StringBuilder text = new();
        text.Append("---- MODULE ").Append(moduleName).AppendLine(" ----");
        text.AppendLine("EXTENDS Integers");
        text.AppendLine("\\* Generated by Kommander.Tests WalTraceRecorder. Validate with formal/run-tlc.sh --trace <this file>.");
        text.Append("TraceN == ").AppendLine(maxId.ToString(CultureInfo.InvariantCulture));
        text.AppendLine("TraceLog == <<");

        for (int i = 0; i < trace.Count; i++)
        {
            WalTraceEvent e = trace[i];
            WalTraceFrontiers f = e.Frontiers ?? default;

            text.Append("  [kind |-> \"").Append(KindName(e.Kind)).Append('"');
            text.Append(", hasState |-> ").Append(e.Frontiers is null ? "FALSE" : "TRUE");
            text.Append(", commit |-> ").Append(f.CommitIndex);
            text.Append(", present |-> ").Append(f.PresentIndex);
            text.Append(", durPresent |-> ").Append(f.DurablePresentIndex);
            text.Append(", durRes |-> ").Append(f.DurableResolvedIndex);
            text.Append(", ridingRes |-> ").Append(f.RidingResolvedIndex);
            text.Append(", ridingFloor |-> ").Append(f.RidingResolvedFloor == long.MaxValue ? inf : f.RidingResolvedFloor);
            text.Append(", writtenRes |-> ").Append(f.WrittenResolvedIndex);
            text.Append(", reported |-> ").Append(f.ReportedDurable);
            text.Append(", synced |-> ").Append(e.Synced ? "TRUE" : "FALSE");
            text.Append(", min |-> ").Append(e.Min);
            text.Append(", max |-> ").Append(e.Max);
            text.Append(", sparse |-> <<").Append(string.Join(", ", e.SparseIds ?? [])).Append(">>");
            text.Append(", rows |-> <<");
            if (e.Rows is not null)
                text.Append(string.Join(", ", e.Rows.Select(row => $"<<{row.Id}, \"{RowStatus(row.Type)}\">>")));
            text.Append(">>");
            text.Append(", detail |-> \"").Append((e.Detail ?? "").Replace("\"", "'")).Append("\"]");
            text.AppendLine(i < trace.Count - 1 ? "," : "");
        }

        text.AppendLine(">>");
        text.AppendLine("====");
        return text.ToString();
    }

    private static string KindName(WalTraceEventKind kind) => kind switch
    {
        WalTraceEventKind.Frontiers => "frontiers",
        WalTraceEventKind.Write => "write",
        WalTraceEventKind.Truncate => "truncate",
        WalTraceEventKind.Durable => "durable",
        WalTraceEventKind.Resolution => "resolution",
        WalTraceEventKind.Restore => "restore",
        WalTraceEventKind.Crash => "crash",
        _ => "unsupported",
    };

    // The model's row statuses. A checkpoint row behaves like its plain type everywhere the model
    // looks; a certified checkpoint changes restore, and that restore is traced as unsupported.
    private static string RowStatus(RaftLogType type) => type switch
    {
        RaftLogType.Proposed or RaftLogType.ProposedCheckpoint => "P",
        RaftLogType.Committed or RaftLogType.CommittedCheckpoint => "C",
        _ => "R",
    };

    private static string Sanitize(string endpoint) =>
        new(endpoint.Select(c => char.IsLetterOrDigit(c) ? c : '_').ToArray());
}
