
using Kommander.Data;
using Kommander.Scheduling;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Assembles <see cref="FakeClock"/>, <see cref="FakeWAL"/>, and
/// <see cref="FakeTransport"/> into a single test harness that supports
/// deterministic, step-by-step execution of partition operations.
///
/// <para>The harness maintains an ordered queue of <see cref="RaftPartitionOperation"/>
/// items.  Tests enqueue operations and then advance execution one step at a time
/// so they can inspect intermediate state and assert event ordering without
/// relying on wall-clock delays or background threads.</para>
///
/// <para>Usage pattern:
/// <code>
/// var h = new SchedulerHarness();
/// h.Enqueue(partitionId: 0, RaftRequestType.ReplicateLogs, logs);
/// h.Clock.AdvanceMs(100);
/// h.StepOnce();
/// Assert.Single(h.Wal.CompletedWrites);
/// </code>
/// </para>
/// </summary>
public sealed class SchedulerHarness
{
    // ── Components ─────────────────────────────────────────────────────────

    /// <summary>Deterministic clock shared by all harness components.</summary>
    public FakeClock Clock { get; } = new();

    /// <summary>Fake WAL with pending-write queue and failure injection.</summary>
    public FakeWAL Wal { get; } = new();

    /// <summary>Fake transport with partition/drop simulation.</summary>
    public FakeTransport Transport { get; } = new();

    // ── Operation queue ────────────────────────────────────────────────────

    private readonly Queue<HarnessStep> _steps = new();
    private long _sequenceCounter;

    /// <summary>All steps that have been executed, in execution order.</summary>
    public List<HarnessStep> ExecutedSteps { get; } = [];

    /// <summary>Number of steps currently waiting to be executed.</summary>
    public int PendingStepCount => _steps.Count;

    /// <summary>
    /// A single scheduled unit of work in the harness: a
    /// <see cref="RaftPartitionOperation"/> plus an optional WAL write that
    /// should be triggered as part of executing it.
    /// </summary>
    public sealed class HarnessStep
    {
        /// <summary>The operation that was enqueued.</summary>
        public RaftPartitionOperation Operation { get; init; } = null!;

        /// <summary>
        /// Simulated time (from <see cref="FakeClock"/>) at which this step was enqueued.
        /// </summary>
        public DateTimeOffset EnqueuedAt { get; init; }

        /// <summary>
        /// Simulated time at which this step was executed (set by <see cref="SchedulerHarness.StepOnce"/>).
        /// </summary>
        public DateTimeOffset? ExecutedAt { get; set; }

        /// <summary>
        /// WAL write triggered during this step's execution (if any).
        /// </summary>
        public FakeWAL.PendingWrite? TriggeredWalWrite { get; set; }
    }

    // ── Enqueue helpers ────────────────────────────────────────────────────

    /// <summary>
    /// Enqueues an operation built from an existing <see cref="RaftRequest"/> using
    /// <see cref="RaftOperationMapper.CreateOperation"/>.
    /// </summary>
    public HarnessStep Enqueue(int partitionId, RaftRequest request)
    {
        RaftPartitionOperation op = RaftOperationMapper.CreateOperation(partitionId, request, ++_sequenceCounter);
        return EnqueueOperation(op);
    }

    /// <summary>
    /// Convenience overload: enqueues an operation with no extra payload fields.
    /// </summary>
    public HarnessStep Enqueue(int partitionId, RaftRequestType type)
        => Enqueue(partitionId, new RaftRequest(type));

    /// <summary>
    /// Enqueues a pre-built <see cref="RaftPartitionOperation"/> directly.
    /// </summary>
    public HarnessStep EnqueueOperation(RaftPartitionOperation operation)
    {
        HarnessStep step = new()
        {
            Operation = operation,
            EnqueuedAt = Clock.UtcNow,
        };
        _steps.Enqueue(step);
        return step;
    }

    // ── Step-by-step execution ─────────────────────────────────────────────

    /// <summary>
    /// Executes the next pending step and returns it.  Returns <c>null</c> when
    /// there are no pending steps.
    ///
    /// <para>For WAL-backed operation kinds (Replication, Client), the step
    /// automatically submits the operation's logs to the <see cref="FakeWAL"/> as a
    /// pending write.  The write is <em>not</em> drained automatically; call
    /// <see cref="FakeWAL.DrainOne"/> or <see cref="FakeWAL.DrainAll"/> to commit
    /// it and simulate WAL completion.</para>
    /// </summary>
    public HarnessStep? StepOnce()
    {
        if (!_steps.TryDequeue(out HarnessStep? step))
            return null;

        step.ExecutedAt = Clock.UtcNow;

        // For replication and client operations that carry logs, submit a WAL write
        // so tests can exercise the pending-write flow.
        RaftPartitionOperation op = step.Operation;
        if (op.Kind is RaftOperationKind.Replication or RaftOperationKind.Client
            && op.Request.Logs is { Count: > 0 } logs)
        {
            Wal.Write([(op.PartitionId, logs)]);
        }

        ExecutedSteps.Add(step);
        return step;
    }

    /// <summary>
    /// Executes all pending steps and returns the count.
    /// WAL writes are still not drained automatically.
    /// </summary>
    public int StepAll()
    {
        int count = 0;
        while (StepOnce() is not null)
            count++;
        return count;
    }

    /// <summary>
    /// Executes all pending steps <em>and</em> immediately drains all WAL writes
    /// they produce.  Equivalent to calling <see cref="StepAll"/> followed by
    /// <see cref="FakeWAL.DrainAll"/>.
    /// </summary>
    public void RunToCompletion()
    {
        StepAll();
        Wal.DrainAll();
    }

    // ── Assertion helpers ──────────────────────────────────────────────────

    /// <summary>
    /// Returns all executed steps for the given partition, in execution order.
    /// </summary>
    public IEnumerable<HarnessStep> StepsForPartition(int partitionId) =>
        ExecutedSteps.Where(s => s.Operation.PartitionId == partitionId);

    /// <summary>
    /// Returns all executed steps of a given <see cref="RaftOperationKind"/>, in
    /// execution order.
    /// </summary>
    public IEnumerable<HarnessStep> StepsOfKind(RaftOperationKind kind) =>
        ExecutedSteps.Where(s => s.Operation.Kind == kind);

    /// <summary>
    /// Returns the execution-order indices of all executed steps, keyed by kind.
    /// Useful for asserting that control-plane steps ran before client steps.
    /// </summary>
    public IReadOnlyDictionary<RaftOperationKind, List<int>> ExecutionOrderByKind()
    {
        Dictionary<RaftOperationKind, List<int>> result = new();
        for (int i = 0; i < ExecutedSteps.Count; i++)
        {
            RaftOperationKind k = ExecutedSteps[i].Operation.Kind;
            if (!result.TryGetValue(k, out List<int>? list))
                result[k] = list = [];
            list.Add(i);
        }
        return result;
    }
}
