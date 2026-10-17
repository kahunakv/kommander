namespace Kommander.WAL.IO;

/// <summary>
/// Tells <see cref="IRaftReadScheduler"/> where a read may run.
///
/// <para>
/// A worker hand-off costs microseconds of wall time and, on a loaded host, several times that in CPU
/// on the threads that wake and spin. For a read that touches memory only, the hand-off costs more than
/// the read. <see cref="InlineWhenIdle"/> lets the caller skip it when that is safe; the scheduler still
/// decides, under the partition lock, so ordering and back-pressure do not change.
/// </para>
/// </summary>
public enum ReadExecutionHint
{
    /// <summary>
    /// Always run the operation on a scheduler worker. The default, and the only correct choice for any
    /// operation that can block (a RocksDB or SQLite read, a lock that a worker can hold).
    /// </summary>
    Queue = 0,

    /// <summary>
    /// Run the operation on the calling thread when its partition is idle: nothing is queued for it and,
    /// for an ordered scheduler, nothing is in flight. Otherwise the operation queues exactly as with
    /// <see cref="Queue"/>.
    ///
    /// <para><b>The hint is the caller's promise</b>, and the scheduler cannot check it:</para>
    /// <list type="bullet">
    ///   <item>the operation reads memory and does not block;</item>
    ///   <item>it takes no lock that a scheduler worker can hold;</item>
    ///   <item>it does not call back into a read scheduler and wait for the result.</item>
    /// </list>
    /// <para>
    /// An admission made from inside a scheduler-run operation or completion callback (on a worker, or
    /// nested inside another inline run) always queues, so inline runs never nest. The task-returning
    /// overloads complete their task before they return when the operation ran inline; a caller that
    /// attaches a <c>ContinueWith</c> still pays a pool hop, so test <c>IsCompletedSuccessfully</c> first.
    /// </para>
    /// </summary>
    InlineWhenIdle = 1,
}
