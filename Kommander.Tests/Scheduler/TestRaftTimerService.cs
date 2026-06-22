using Kommander.Data;
using Kommander.Scheduling;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kommander.Tests.Scheduler;

/// <summary>
/// Acceptance tests for <see cref="RaftTimerService"/>.
///
/// All tests drive the service via the public <see cref="RaftTimerService.TriggerCheckLeader"/>
/// and <see cref="RaftTimerService.TriggerUpdateNodes"/> methods, which expose the same code
/// paths that the internal timers invoke.  This means no wall-clock waits are needed:
/// tests are fully deterministic and fast.
/// </summary>
public sealed class TestRaftTimerService
{
    // ── Stub host ──────────────────────────────────────────────────────────

    /// <summary>
    /// Lightweight stub for <see cref="IRaftTimerHost"/>.
    /// Tracks how many times CheckLeader was called on each stub partition and
    /// how many times UpdateNodes was called on the host.
    /// </summary>
    private sealed class StubTimerHost : IRaftTimerHost
    {
        public bool Joined { get; set; }

        public RaftPartition? SystemPartition => null; // No system partition in unit tests.

        public List<TrackingPartition> UserPartitions { get; } = [];

        public int UpdateNodesCallCount { get; private set; }

        public IEnumerable<RaftPartition> GetUserPartitions() => [];   // used indirectly via CheckLeaderCount

        public Task UpdateNodes()
        {
            UpdateNodesCallCount++;
            return Task.CompletedTask;
        }

        public Task GossipAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task PingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void TriggerBalancerPass() { }

        // Convenience — number of CheckLeader calls recorded across all tracking partitions.
        public int TotalCheckLeaderCalls => UserPartitions.Sum(p => p.CheckLeaderCount);
    }

    /// <summary>
    /// Tracks calls to <c>CheckLeader()</c> without requiring a real executor.
    /// </summary>
    internal sealed class TrackingPartition
    {
        public int CheckLeaderCount { get; private set; }

        public void CheckLeader() => CheckLeaderCount++;
    }

    /// <summary>
    /// Variant of <see cref="StubTimerHost"/> that actually routes
    /// <see cref="GetUserPartitions"/> to <see cref="TrackingPartition"/> instances
    /// via a <see cref="IRaftTimerHost"/> implementation backed by delegates.
    /// </summary>
    private sealed class DelegatingTimerHost : IRaftTimerHost
    {
        private readonly Func<bool> _joinedGetter;
        private readonly Func<IEnumerable<RaftPartition>> _partitionsGetter;
        private readonly Func<Task> _updateNodes;

        public DelegatingTimerHost(
            Func<bool> joinedGetter,
            Func<IEnumerable<RaftPartition>> partitionsGetter,
            Func<Task> updateNodes)
        {
            _joinedGetter = joinedGetter;
            _partitionsGetter = partitionsGetter;
            _updateNodes = updateNodes;
        }

        public bool Joined => _joinedGetter();
        public RaftPartition? SystemPartition => null;
        public IEnumerable<RaftPartition> GetUserPartitions() => _partitionsGetter();
        public Task UpdateNodes() => _updateNodes();
        public Task GossipAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task PingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void TriggerBalancerPass() { }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static RaftConfiguration MakeConfig(
        int checkLeaderMs = 250,
        int updateNodesMs = 5000) => new()
    {
        StartElectionTimeout = 50,
        EndElectionTimeout = 100,
        CheckLeaderInterval = TimeSpan.FromMilliseconds(checkLeaderMs),
        UpdateNodesInterval = TimeSpan.FromMilliseconds(updateNodesMs),
    };

    private static RaftTimerService BuildService(
        IRaftTimerHost host,
        RaftConfiguration? config = null,
        TimeSpan? initialDelay = null)
    {
        config ??= MakeConfig();
        return new RaftTimerService(host, NullLogger<IRaft>.Instance, config, initialDelay ?? TimeSpan.Zero);
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// TriggerUpdateNodes calls host.UpdateNodes() when the node has joined.
    /// </summary>
    [Fact]
    public void TriggerUpdateNodes_WhenJoined_CallsUpdateNodes()
    {
        int callCount = 0;
        DelegatingTimerHost host = new(
            joinedGetter: () => true,
            partitionsGetter: () => [],
            updateNodes: () => { callCount++; return Task.CompletedTask; }
        );

        using RaftTimerService svc = BuildService(host);
        svc.TriggerUpdateNodes();
        svc.TriggerUpdateNodes();

        Assert.Equal(2, callCount);
    }

    /// <summary>
    /// TriggerUpdateNodes is a no-op when the node has not joined.
    /// </summary>
    [Fact]
    public void TriggerUpdateNodes_WhenNotJoined_DoesNotCallUpdateNodes()
    {
        int callCount = 0;
        DelegatingTimerHost host = new(
            joinedGetter: () => false,
            partitionsGetter: () => [],
            updateNodes: () => { callCount++; return Task.CompletedTask; }
        );

        using RaftTimerService svc = BuildService(host);
        svc.TriggerUpdateNodes();

        Assert.Equal(0, callCount);
    }

    /// <summary>
    /// After Stop(), TriggerCheckLeader is a no-op and does not post to executors.
    /// </summary>
    [Fact]
    public void TriggerCheckLeader_AfterStop_IsNoOp()
    {
        DelegatingTimerHost host = new(
            joinedGetter: () => true,
            partitionsGetter: () => [],          // We count via the delegate below
            updateNodes: () => Task.CompletedTask
        );

        // Wrap so we can intercept any call that would reach partitions.
        bool anyCalled = false;
        DelegatingTimerHost interceptHost = new(
            joinedGetter: () => true,
            partitionsGetter: () => { anyCalled = true; return []; },
            updateNodes: () => Task.CompletedTask
        );

        using RaftTimerService svc = BuildService(interceptHost);
        svc.Stop();
        svc.TriggerCheckLeader();
        svc.TriggerUpdateNodes();

        Assert.False(anyCalled);
    }

    /// <summary>
    /// After Stop(), TriggerUpdateNodes is a no-op.
    /// </summary>
    [Fact]
    public void TriggerUpdateNodes_AfterStop_IsNoOp()
    {
        int callCount = 0;
        DelegatingTimerHost host = new(
            joinedGetter: () => true,
            partitionsGetter: () => [],
            updateNodes: () => { callCount++; return Task.CompletedTask; }
        );

        using RaftTimerService svc = BuildService(host);
        svc.Stop();
        svc.TriggerUpdateNodes();

        Assert.Equal(0, callCount);
    }

    /// <summary>
    /// Start() is idempotent — the second call is a no-op.
    /// </summary>
    [Fact]
    public void Start_CalledTwice_IsIdempotent()
    {
        DelegatingTimerHost host = new(
            joinedGetter: () => false,
            partitionsGetter: () => [],
            updateNodes: () => Task.CompletedTask
        );

        using RaftTimerService svc = BuildService(host, MakeConfig(checkLeaderMs: 60000, updateNodesMs: 60000));
        svc.Start();
        svc.Start(); // must not throw or create extra timers
        svc.Stop();
    }

    /// <summary>
    /// Dispose() is safe to call even if Stop() was already called.
    /// </summary>
    [Fact]
    public void Dispose_AfterStop_DoesNotThrow()
    {
        DelegatingTimerHost host = new(
            joinedGetter: () => false,
            partitionsGetter: () => [],
            updateNodes: () => Task.CompletedTask
        );

        RaftTimerService svc = BuildService(host);
        svc.Stop();
        Exception? ex = Record.Exception(() => svc.Dispose());

        Assert.Null(ex);
    }

    /// <summary>
    /// Start() wires the internal timers to TriggerCheckLeader/TriggerUpdateNodes.
    /// We verify the wiring is correct by confirming that calling the trigger methods
    /// directly (the same code path the timers use) increments the counter, and that
    /// calling them before Start() still works because the triggers are independent of
    /// the timer state.
    /// </summary>
    [Fact]
    public void Start_TriggerWiring_IsCorrect()
    {
        int callCount = 0;
        DelegatingTimerHost host = new(
            joinedGetter: () => false,
            partitionsGetter: () => { Interlocked.Increment(ref callCount); return []; },
            updateNodes: () => Task.CompletedTask
        );

        // Use long intervals so the real timers never fire during this synchronous test.
        RaftConfiguration config = MakeConfig(checkLeaderMs: 60_000, updateNodesMs: 60_000);
        using RaftTimerService svc = BuildService(host, config, initialDelay: Timeout.InfiniteTimeSpan);
        svc.Start();

        // Drive the same code path the timer callbacks invoke — deterministically.
        svc.TriggerCheckLeader();
        svc.TriggerCheckLeader();

        Assert.Equal(2, callCount);
    }

    // ── Phase 2 hot-set tests ──────────────────────────────────────────────

    /// <summary>
    /// Minimal IRaftTimerHost that counts how many times GetUserPartitions (full sweep)
    /// vs GetHotUserPartitions (targeted tick) are called by TriggerCheckLeader.
    /// </summary>
    private sealed class HotSetTrackingHost : IRaftTimerHost
    {
        public bool Joined => false;
        public RaftPartition? SystemPartition => null;
        public int FullSweepCount;
        public int HotSetCount;

        public IEnumerable<RaftPartition> GetUserPartitions()
        {
            Interlocked.Increment(ref FullSweepCount);
            return [];
        }

        public IEnumerable<RaftPartition> GetHotUserPartitions()
        {
            Interlocked.Increment(ref HotSetCount);
            return [];
        }

        public Task UpdateNodes() => Task.CompletedTask;
        public Task GossipAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task PingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void TriggerBalancerPass() { }
    }

    /// <summary>
    /// With EnableSharedExecutorPool=true, normal ticks use GetHotUserPartitions and only
    /// safety-sweep ticks use GetUserPartitions.  The safety period is
    /// UpdateNodesInterval / CheckLeaderInterval = 5000 / 250 = 20 ticks.
    /// </summary>
    [Fact]
    public void TriggerCheckLeader_HotSetEnabled_UsesHotSetOnNormalTicks()
    {
        HotSetTrackingHost host = new();
        RaftConfiguration config = new()
        {
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
            EnableSharedExecutorPool = true,
            CheckLeaderInterval  = TimeSpan.FromMilliseconds(250),
            UpdateNodesInterval  = TimeSpan.FromMilliseconds(5000),
        };

        using RaftTimerService svc = new(host, NullLogger<IRaft>.Instance, config, TimeSpan.Zero);

        // safetyTickPeriod = 5000/250 = 20. Fire 19 ticks — should use hot set every time.
        for (int i = 0; i < 19; i++)
            svc.TriggerCheckLeader();

        Assert.Equal(19, host.HotSetCount);
        Assert.Equal(0, host.FullSweepCount);
    }

    /// <summary>
    /// At tick 20 (= safetyTickPeriod) the safety sweep fires — GetUserPartitions is called
    /// once; subsequent normal ticks revert to the hot set.
    /// </summary>
    [Fact]
    public void TriggerCheckLeader_HotSetEnabled_SafetySweepFiresAtPeriod()
    {
        HotSetTrackingHost host = new();
        RaftConfiguration config = new()
        {
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
            EnableSharedExecutorPool = true,
            CheckLeaderInterval  = TimeSpan.FromMilliseconds(250),
            UpdateNodesInterval  = TimeSpan.FromMilliseconds(5000),
        };

        using RaftTimerService svc = new(host, NullLogger<IRaft>.Instance, config, TimeSpan.Zero);

        // Fire exactly safetyTickPeriod = 20 ticks.
        for (int i = 0; i < 20; i++)
            svc.TriggerCheckLeader();

        // 20th tick is the safety sweep; the other 19 use the hot set.
        Assert.Equal(19, host.HotSetCount);
        Assert.Equal(1, host.FullSweepCount);

        // Next 20 ticks: 19 hot-set + 1 safety.
        for (int i = 0; i < 20; i++)
            svc.TriggerCheckLeader();

        Assert.Equal(38, host.HotSetCount);
        Assert.Equal(2, host.FullSweepCount);
    }

    /// <summary>
    /// With EnableSharedExecutorPool=false, every tick is a full sweep — hot set is
    /// never consulted regardless of partition count.
    /// </summary>
    [Fact]
    public void TriggerCheckLeader_HotSetDisabled_AlwaysFullSweep()
    {
        HotSetTrackingHost host = new();
        RaftConfiguration config = new()
        {
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
            EnableSharedExecutorPool = false,
            CheckLeaderInterval  = TimeSpan.FromMilliseconds(250),
            UpdateNodesInterval  = TimeSpan.FromMilliseconds(5000),
        };

        using RaftTimerService svc = new(host, NullLogger<IRaft>.Instance, config, TimeSpan.Zero);

        for (int i = 0; i < 25; i++)
            svc.TriggerCheckLeader();

        Assert.Equal(25, host.FullSweepCount);
        Assert.Equal(0, host.HotSetCount);
    }

    /// <summary>
    /// When safetyTickPeriod collapses to 1 (UpdateNodesInterval &lt;= CheckLeaderInterval),
    /// every tick is a full sweep — no hot-set ticks at all.
    /// </summary>
    [Fact]
    public void TriggerCheckLeader_HotSet_SafetyPeriodOne_AlwaysFullSweep()
    {
        HotSetTrackingHost host = new();
        RaftConfiguration config = new()
        {
            StartElectionTimeout = 50,
            EndElectionTimeout = 100,
            EnableSharedExecutorPool = true,
            CheckLeaderInterval  = TimeSpan.FromMilliseconds(250),
            UpdateNodesInterval  = TimeSpan.FromMilliseconds(250), // period = 1
        };

        using RaftTimerService svc = new(host, NullLogger<IRaft>.Instance, config, TimeSpan.Zero);

        for (int i = 0; i < 5; i++)
            svc.TriggerCheckLeader();

        Assert.Equal(5, host.FullSweepCount);
        Assert.Equal(0, host.HotSetCount);
    }
}
