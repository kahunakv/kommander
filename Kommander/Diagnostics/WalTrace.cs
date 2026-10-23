using Kommander.Data;

namespace Kommander.Diagnostics;

/// <summary>
/// Receives the write-ahead events that the TLA+ trace validator replays
/// (<c>formal/tla/WalDurabilityTrace.tla</c>).
///
/// <para><b>Why this exists.</b> The TLA+ model in <c>formal/tla</c> states how the durable
/// frontiers of <see cref="RaftWriteAhead"/> must move and what a follower may report as durable.
/// A model checker proves those rules on a small model, but a model can drift away from the code.
/// The deterministic simulation records these events from the real code, and the validator checks
/// that every frontier the code computed equals the value the model computes from the same events,
/// and that the reported frontier never exceeds what a restart would restore. The model and the
/// code are then compared on every traced run, not only when someone reads both.</para>
///
/// <para><b>Cost.</b> Off unless a test sets <see cref="RaftConfiguration.WalTraceSink"/>: each
/// emission point is one null check. Never set it in production. The sink is called on the
/// partition executor (frontier events) and on the WAL worker (write and truncate events), so an
/// implementation must be thread-safe and must not block.</para>
/// </summary>
internal interface IWalTraceSink
{
    void Record(WalTraceEvent traceEvent);
}

/// <summary>The kinds of event the trace validator understands.</summary>
internal enum WalTraceEventKind
{
    /// <summary>The frontiers after a change that the validator takes as input (an enqueue, a failed-write regression, an inherited commit).</summary>
    Frontiers,

    /// <summary>The scheduler wrote one operation's rows (<see cref="WalTraceEvent.Rows"/>) with or without an fsync.</summary>
    Write,

    /// <summary>The scheduler removed every Proposed row above <see cref="WalTraceEvent.Max"/>.</summary>
    Truncate,

    /// <summary><see cref="RaftWriteAhead.MarkDurablyWritten"/> ran.</summary>
    Durable,

    /// <summary><see cref="RaftWriteAhead.MarkResolutionWritten(long, long, long[], bool)"/> ran; <see cref="WalTraceEvent.SparseIds"/> carries the exact resolved ids when they are not contiguous.</summary>
    Resolution,

    /// <summary><see cref="RaftWriteAhead.CompleteRestoreAsync"/> re-derived the frontiers from the disk.</summary>
    Restore,

    /// <summary>The process lost its memory and the store lost everything inside its fsync window. Recorded by the harness.</summary>
    Crash,

    /// <summary>Something the model does not cover happened (a snapshot install, a compaction, a suffix truncation). The validator stops at this event.</summary>
    Unsupported,
}

/// <summary>One row a write carried: its id and its type.</summary>
internal readonly record struct WalTraceRow(long Id, RaftLogType Type);

/// <summary>
/// The raw frontier fields of <see cref="RaftWriteAhead"/> after an event, in the field's own
/// EXCLUSIVE convention (the next id not yet covered). The validator compares them one for one
/// with the model's fields of the same names.
/// </summary>
internal readonly record struct WalTraceFrontiers(
    long CommitIndex,
    long PresentIndex,
    long DurablePresentIndex,
    long DurableResolvedIndex,
    long RidingResolvedIndex,
    long RidingResolvedFloor,
    long WrittenResolvedIndex,
    long ReportedDurable);

/// <summary>One event. Fields that do not apply to the kind are left at their defaults.</summary>
internal sealed record WalTraceEvent(
    int PartitionId,
    WalTraceEventKind Kind,
    WalTraceFrontiers? Frontiers = null,
    WalTraceRow[]? Rows = null,
    bool Synced = false,
    long Min = -1,
    long Max = -1,
    long[]? SparseIds = null,
    string? Detail = null);
