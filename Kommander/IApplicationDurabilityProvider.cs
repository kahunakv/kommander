
namespace Kommander;

/// <summary>
/// Consumer-supplied, per-partition <b>application-durability floor</b>: reports the highest WAL
/// log id whose committed prefix the application has durably applied to its <em>own</em> storage.
/// <para>
/// Raft's <c>CommittedCheckpoint</c> certifies <b>consensus</b> durability only — it says nothing
/// about whether an asynchronously-applying consumer (in-memory overlay + background flush) has
/// persisted that prefix. Anchoring restart replay and WAL compaction to the checkpoint alone
/// therefore opens a window, as wide as the consumer's flush cadence, in which a killed node
/// silently loses committed entries: replay starts above them and compaction may have removed
/// them. This provider closes that window by letting Kommander widen replay down to the floor and
/// fence compaction above it. See the "Application-durability floor for restart replay and WAL
/// compaction" spec.
/// </para>
/// <para>
/// The floor also acts as a <b>soft checkpoint</b> in the opposite direction: when it sits above
/// the last hard checkpoint, restart replay starts at the floor instead of the checkpoint and
/// committed entries at or below the floor are not redelivered (they are already durably applied).
/// Nothing is removed from the WAL — compaction keeps its own conservative fences — so a stalled
/// or lagging compaction no longer makes cold-restart time proportional to the retained log
/// length. See the "Rethink WAL replay" spec.
/// </para>
/// <para>
/// Wire an implementation via <see cref="RaftConfiguration.ApplicationDurabilityProvider"/>. A
/// <see langword="null"/> provider keeps the checkpoint-anchored behavior byte-for-byte.
/// </para>
/// </summary>
public interface IApplicationDurabilityProvider
{
    /// <summary>
    /// Returns the highest WAL log id <c>X</c> for <paramref name="partitionId"/> such that every
    /// committed entry with id &lt;= <c>X</c> is durably applied in the application's own storage.
    /// Return <c>-1</c> for "no opinion" (checkpoint-anchored behavior, exactly as without a
    /// provider). <c>0</c> means "nothing durably applied yet" and is honored as a real floor.
    /// <para>
    /// Contract:
    /// <list type="bullet">
    ///   <item>Must be cheap and non-blocking — called once per partition restore and once per
    ///         compaction pass, never on the propose/commit hot path.</item>
    ///   <item>The value must come from the application's <b>durable</b> storage: it is consulted
    ///         at restart, before any in-memory state exists to re-assert it.</item>
    ///   <item>May be stale-<b>low</b> (a crash between the application's data flush and its floor
    ///         record): entries in <c>(floor, actuallyFlushed]</c> are then redelivered via
    ///         <c>OnLogRestored</c>, so consumers must make restore application idempotent.
    ///         It must <b>never</b> be high — a too-high floor recreates the data-loss window this
    ///         provider exists to close, both through compaction (entries removed before they were
    ///         applied) and through restore (entries at or below the floor are not redelivered).</item>
    ///   <item>When one logical partition feeds several application stores, the floor must be the
    ///         <b>minimum</b> across those stores — no store may sit behind the reported value,
    ///         because restore will not redeliver anything at or below it.</item>
    /// </list>
    /// </para>
    /// </summary>
    long GetDurablyAppliedIndex(int partitionId);
}
