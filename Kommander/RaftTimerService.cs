using Kommander.Scheduling;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Explicit, actor-free timer service that replaces <see cref="RaftLeaderSupervisor"/>.
///
/// <para>Two periodic timers drive two recurring operations:</para>
/// <list type="bullet">
///   <item>
///     <term>CheckLeader</term>
///     <description>
///       Posts a <c>CheckLeader</c> message into every partition executor so each
///       partition's state machine can independently decide whether to start an
///       election, send heartbeats, or step down.  The callback never mutates
///       partition state directly — it only posts into the partition's serial queue.
///     </description>
///   </item>
///   <item>
///     <term>UpdateNodes</term>
///     <description>
///       Refreshes the cluster node registry via discovery.
///       Does not touch partition state.
///     </description>
///   </item>
/// </list>
///
/// <para>The public trigger methods (<see cref="TriggerCheckLeader"/> and
/// <see cref="TriggerUpdateNodes"/>) let tests drive the same code paths
/// synchronously without relying on wall-clock waits — no fake clock required.</para>
/// </summary>
public sealed class RaftTimerService : IDisposable
{
    private readonly IRaftTimerHost _host;
    private readonly ILogger<IRaft> _logger;

    private Timer? _checkLeaderTimer;
    private Timer? _updateNodesTimer;
    private Timer? _gossipTimer;
    private Timer? _pingTimer;

    private volatile bool _started;
    private volatile bool _stopped;
    private int _updateNodesRunning;  // 0 = idle, 1 = running; prevents overlapping ticks
    private int _gossipRunning;       // 0 = idle, 1 = running; prevents overlapping ticks
    private int _pingRunning;         // 0 = idle, 1 = running; prevents overlapping ticks

    private readonly TimeSpan _checkLeaderInterval;
    private readonly TimeSpan _updateNodesInterval;
    private readonly TimeSpan _gossipInterval;
    private readonly int _gossipFanout;
    private readonly TimeSpan _pingInterval;
    private readonly TimeSpan _initialDelay;

    public RaftTimerService(
        IRaftTimerHost host,
        ILogger<IRaft> logger,
        RaftConfiguration configuration,
        TimeSpan? initialDelay = null)
    {
        _host = host;
        _logger = logger;
        _checkLeaderInterval = configuration.CheckLeaderInterval;
        _updateNodesInterval = configuration.UpdateNodesInterval;
        _gossipInterval = configuration.GossipInterval;
        _gossipFanout = configuration.GossipFanout;
        _pingInterval = configuration.PingInterval;
        _initialDelay = initialDelay ?? configuration.TimerInitialDelay;
    }

    /// <summary>Starts the periodic timers.  Safe to call only once.</summary>
    public void Start()
    {
        if (_started)
            return;

        _started = true;

        _checkLeaderTimer = new Timer(
            _ => TriggerCheckLeader(),
            null,
            _initialDelay,
            _checkLeaderInterval
        );

        _updateNodesTimer = new Timer(
            _ => TriggerUpdateNodes(),
            null,
            _initialDelay,
            _updateNodesInterval
        );

        // Gossip is disabled when GossipFanout is 0.
        if (_gossipFanout > 0)
        {
            _gossipTimer = new Timer(
                _ => TriggerGossip(),
                null,
                _initialDelay,
                _gossipInterval
            );
        }

        // SWIM failure detector is disabled when PingInterval is non-positive.
        if (_pingInterval > TimeSpan.Zero)
        {
            _pingTimer = new Timer(
                _ => TriggerPing(),
                null,
                _initialDelay,
                _pingInterval
            );
        }
    }

    /// <summary>
    /// Stops both timers.  In-flight callbacks that have already started are
    /// allowed to complete.  The service cannot be restarted after this call.
    /// </summary>
    public void Stop()
    {
        if (_stopped)
            return;

        _stopped = true;
        _checkLeaderTimer?.Change(Timeout.Infinite, Timeout.Infinite);
        _updateNodesTimer?.Change(Timeout.Infinite, Timeout.Infinite);
        _gossipTimer?.Change(Timeout.Infinite, Timeout.Infinite);
        _pingTimer?.Change(Timeout.Infinite, Timeout.Infinite);
    }

    /// <summary>
    /// Posts a <c>CheckLeader</c> message into every partition executor.
    ///
    /// <para>Invoked by the internal timer, but public so tests can drive it
    /// synchronously without waiting for a real timer tick.</para>
    /// </summary>
    public void TriggerCheckLeader()
    {
        if (_stopped)
            return;

        try
        {
            _host.SystemPartition?.CheckLeader();

            foreach (RaftPartition partition in _host.GetUserPartitions())
                partition.CheckLeader();
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "RaftTimerService.TriggerCheckLeader: {Message}\n{StackTrace}",
                ex.Message, ex.StackTrace);
        }
    }

    /// <summary>
    /// Refreshes the cluster node registry via discovery.
    ///
    /// <para>Invoked by the internal timer, but public so tests can drive it
    /// synchronously without waiting for a real timer tick.</para>
    /// </summary>
    public void TriggerUpdateNodes()
    {
        if (_stopped)
            return;

        if (!_host.Joined)
            return;

        // Skip this tick if a prior refresh is still in-flight so discovery calls
        // never stack up behind a slow network response.
        if (Interlocked.CompareExchange(ref _updateNodesRunning, 1, 0) != 0)
            return;

        // Fire-and-forget with full async exception coverage: UpdateNodesAsync awaits
        // _host.UpdateNodes() and catches all exceptions — including those thrown after
        // the first await — so the discarded Task is always in a completed (not faulted)
        // state when it is collected.
        _ = UpdateNodesAsync();
    }

    private async Task UpdateNodesAsync()
    {
        try
        {
            await _host.UpdateNodes().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "RaftTimerService.TriggerUpdateNodes: {Message}\n{StackTrace}",
                ex.Message, ex.StackTrace);
        }
        finally
        {
            Volatile.Write(ref _updateNodesRunning, 0);
        }
    }

    /// <summary>
    /// Runs one gossip round, contacting random peers to exchange membership versions
    /// and pulling any newer committed roster into the local cache.
    ///
    /// <para>Invoked by the internal gossip timer, but public so tests can drive it
    /// synchronously without waiting for a real timer tick.</para>
    /// </summary>
    public void TriggerGossip()
    {
        if (_stopped || !_host.Joined)
            return;

        if (Interlocked.CompareExchange(ref _gossipRunning, 1, 0) != 0)
            return;

        _ = GossipTickAsync();
    }

    private async Task GossipTickAsync()
    {
        try
        {
            await _host.GossipAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "RaftTimerService.TriggerGossip: {Message}\n{StackTrace}",
                ex.Message, ex.StackTrace);
        }
        finally
        {
            Volatile.Write(ref _gossipRunning, 0);
        }
    }

    /// <summary>
    /// Runs one SWIM probe round.
    ///
    /// <para>Invoked by the internal ping timer, but public so tests can drive it
    /// synchronously without waiting for a real timer tick.</para>
    /// </summary>
    public void TriggerPing()
    {
        if (_stopped || !_host.Joined)
            return;

        if (Interlocked.CompareExchange(ref _pingRunning, 1, 0) != 0)
            return;

        _ = PingTickAsync();
    }

    private async Task PingTickAsync()
    {
        try
        {
            await _host.PingAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                "RaftTimerService.TriggerPing: {Message}\n{StackTrace}",
                ex.Message, ex.StackTrace);
        }
        finally
        {
            Volatile.Write(ref _pingRunning, 0);
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        Stop();
        _checkLeaderTimer?.Dispose();
        _updateNodesTimer?.Dispose();
        _gossipTimer?.Dispose();
        _pingTimer?.Dispose();
    }
}
