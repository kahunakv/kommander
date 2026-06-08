
using Kommander.Data;
using Kommander.WAL;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Deterministic in-memory WAL for use in scheduler harness tests.
///
/// <para>Writes are not applied immediately.  Each call to <see cref="Write"/> enqueues
/// a <see cref="PendingWrite"/> that stays pending until the test calls
/// <see cref="DrainOne"/> or <see cref="DrainAll"/>.  This lets tests verify behaviour
/// under delayed WAL writes without any real threading or I/O.</para>
///
/// <para>Failure injection is per-operation: configure <see cref="NextWriteResult"/>
/// before the call that should fail; the harness will dequeue the pending write and
/// return the configured status.</para>
/// </summary>
public sealed class FakeWAL : IWAL
{
    // ── Storage ───────────────────────────────────────────────────────────────

    private readonly Dictionary<int, SortedDictionary<long, RaftLog>> _logs = new();
    private readonly Dictionary<string, string> _meta = new();

    // ── Pending write queue ───────────────────────────────────────────────────

    /// <summary>Represents a write that has been submitted but not yet applied.</summary>
    public sealed class PendingWrite
    {
        /// <summary>Unique sequence number (1-based, assigned on enqueue).</summary>
        public long SequenceNumber { get; init; }

        /// <summary>The log batches to be written when this operation is drained.</summary>
        public List<(int PartitionId, List<RaftLog> Logs)> Batches { get; init; } = [];

        /// <summary>
        /// Result that will be returned when this write is drained.
        /// Defaults to <see cref="RaftOperationStatus.Success"/>.
        /// </summary>
        public RaftOperationStatus InjectedResult { get; init; } = RaftOperationStatus.Success;
    }

    private readonly Queue<PendingWrite> _pending = new();
    private long _sequenceCounter;

    // ── Observation ───────────────────────────────────────────────────────────

    /// <summary>All writes that have been completed (drained), in order.</summary>
    public List<PendingWrite> CompletedWrites { get; } = [];

    /// <summary>All writes that are currently waiting to be drained.</summary>
    public IReadOnlyCollection<PendingWrite> PendingWrites => _pending;

    /// <summary>Total number of times <see cref="Write"/> has been called.</summary>
    public int WriteCallCount { get; private set; }

    // ── Failure injection ─────────────────────────────────────────────────────

    /// <summary>
    /// When set, the <em>next enqueued</em> write will use this status instead of
    /// <see cref="RaftOperationStatus.Success"/>.  Automatically resets to
    /// <c>null</c> after one write is enqueued.
    /// </summary>
    public RaftOperationStatus? NextWriteResult { get; set; }

    /// <summary>
    /// When true, every subsequent <see cref="Write"/> call will throw
    /// <see cref="InvalidOperationException"/> to simulate a storage crash.
    /// </summary>
    public bool SimulateCrash { get; set; }

    // ── IWAL implementation ───────────────────────────────────────────────────

    /// <inheritdoc/>
    /// <remarks>
    /// Enqueues a pending write rather than applying it immediately.
    /// Call <see cref="DrainOne"/> or <see cref="DrainAll"/> to commit.
    /// </remarks>
    public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
    {
        if (SimulateCrash)
            throw new InvalidOperationException("FakeWAL: storage crash simulated.");

        WriteCallCount++;

        RaftOperationStatus injected = NextWriteResult ?? RaftOperationStatus.Success;
        NextWriteResult = null;

        List<(int, List<RaftLog>)> snapshot = logs
            .Select(t => (t.Item1, t.Item2.Select(l => CloneLog(l)).ToList()))
            .ToList();

        PendingWrite pending = new()
        {
            SequenceNumber = ++_sequenceCounter,
            Batches = snapshot.Select(t => (t.Item1, t.Item2)).ToList(),
            InjectedResult = injected,
        };

        _pending.Enqueue(pending);
        return RaftOperationStatus.Pending;
    }

    /// <inheritdoc/>
    public List<RaftLog> ReadLogs(int partitionId)
    {
        if (_logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? dict))
            return dict.Values.ToList();
        return [];
    }

    /// <inheritdoc/>
    public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex)
    {
        if (_logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? dict))
            return dict.Where(kv => kv.Key >= startLogIndex).Select(kv => kv.Value).ToList();
        return [];
    }

    /// <inheritdoc/>
    public long GetMaxLog(int partitionId)
    {
        if (_logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? dict) && dict.Count > 0)
            return dict.Keys.Max();
        return 0;
    }

    /// <inheritdoc/>
    public long GetCurrentTerm(int partitionId)
    {
        if (_logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? dict) && dict.Count > 0)
            return dict[dict.Keys.Max()].Term;
        return 0;
    }

    /// <inheritdoc/>
    public long GetLastCheckpoint(int partitionId) => -1;

    /// <inheritdoc/>
    public int CountPersistedLogs(int partitionId) =>
        _logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? dict) ? dict.Count : 0;

    /// <inheritdoc/>
    public int CountRemovableLogs(int partitionId) => 0;

    /// <inheritdoc/>
    public RaftOperationStatus DeletePartitionWAL(int partitionId)
    {
        _logs.Remove(partitionId);
        return RaftOperationStatus.Success;
    }

    /// <inheritdoc/>
    public string? GetMetaData(string key) => _meta.GetValueOrDefault(key);

    /// <inheritdoc/>
    public bool SetMetaData(string key, string value)
    {
        _meta[key] = value;
        return true;
    }

    /// <inheritdoc/>
    public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries)
    {
        if (!_logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? dict))
            return (RaftOperationStatus.Success, 0);

        long[] toRemove = dict.Keys.Where(k => k < lastCheckpoint).Take(compactNumberEntries).ToArray();
        foreach (long key in toRemove)
            dict.Remove(key);

        return (RaftOperationStatus.Success, toRemove.Length);
    }

    /// <inheritdoc/>
    public void Dispose() { }

    // ── Step-by-step drain helpers ────────────────────────────────────────────

    /// <summary>
    /// Applies the oldest pending write and returns it.
    /// Returns <c>null</c> if there are no pending writes.
    /// </summary>
    public PendingWrite? DrainOne()
    {
        if (!_pending.TryDequeue(out PendingWrite? write))
            return null;

        if (write.InjectedResult == RaftOperationStatus.Success)
            ApplyWrite(write);

        CompletedWrites.Add(write);
        return write;
    }

    /// <summary>
    /// Drains all pending writes in submission order and returns them.
    /// </summary>
    public IReadOnlyList<PendingWrite> DrainAll()
    {
        List<PendingWrite> drained = [];
        while (DrainOne() is { } w)
            drained.Add(w);
        return drained;
    }

    /// <summary>Discards the oldest pending write without applying it (simulates a lost write).</summary>
    public PendingWrite? DropOne()
    {
        if (!_pending.TryDequeue(out PendingWrite? write))
            return null;
        CompletedWrites.Add(write);
        return write;
    }

    /// <summary>
    /// Returns true if there are any writes waiting to be drained.
    /// </summary>
    public bool HasPendingWrites => _pending.Count > 0;

    // ── Internal helpers ──────────────────────────────────────────────────────

    private void ApplyWrite(PendingWrite write)
    {
        foreach ((int partitionId, List<RaftLog> logs) in write.Batches)
        {
            if (!_logs.TryGetValue(partitionId, out SortedDictionary<long, RaftLog>? dict))
            {
                dict = new SortedDictionary<long, RaftLog>();
                _logs[partitionId] = dict;
            }

            foreach (RaftLog log in logs)
                dict[log.Id] = log;
        }
    }

    private static RaftLog CloneLog(RaftLog src) => new()
    {
        Id = src.Id,
        Type = src.Type,
        Term = src.Term,
        Time = src.Time,
        LogType = src.LogType,
        LogData = src.LogData is null ? null : src.LogData[..],
    };
}
