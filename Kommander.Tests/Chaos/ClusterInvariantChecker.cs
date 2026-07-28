
using System.Threading.Channels;
using Kommander.Data;

namespace Kommander.Tests.Chaos;

/// <summary>
/// The single background task that continuously evaluates <see cref="IClusterInvariant"/>s against
/// point-in-time <see cref="ClusterView"/>s. It samples on two triggers — a coalesced, non-blocking
/// notification after each nemesis decision/delivery, and a low-frequency poll to catch state changes that
/// send no message — and preserves temporal history (the previous view) so exactly one task orders the
/// snapshots.
///
/// <para>Because cluster snapshots are not globally atomic, an apparent violation flagged
/// <see cref="ClusterViolation.RequiresConfirmation"/> (e.g. two leaders, from transient skew) is
/// re-sampled and re-evaluated before it is recorded; historical violations (commit regression, conflicting
/// applied prefixes) fail immediately.</para>
///
/// <para>The checker never backpressures the Raft transport: notifications are dropped when one is already
/// pending, so a flood of decisions coalesces into a single re-evaluation.</para>
/// </summary>
public sealed class ClusterInvariantChecker : IAsyncDisposable
{
    private readonly Func<CancellationToken, Task<ClusterView>> _sampler;
    private readonly IReadOnlyList<IClusterInvariant> _invariants;
    private readonly TimeSpan _pollInterval;
    private readonly TimeSpan _confirmDelay;
    private readonly Channel<byte> _notifications =
        Channel.CreateBounded<byte>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<string, long> _maxCommit = new();

    private Task? _loop;
    private ClusterView? _previous;
    private volatile ClusterViolation? _firstViolation;

    public ClusterInvariantChecker(
        Func<CancellationToken, Task<ClusterView>> sampler,
        IReadOnlyList<IClusterInvariant>? invariants = null,
        TimeSpan? pollInterval = null)
    {
        _sampler = sampler;
        _invariants = invariants ?? ClusterInvariants.All;
        _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(100);
        // Give a transient condition (leader skew, a gap-aware commit dip mid log-hole repair) time to heal
        // before the confirmation resample: a couple of poll intervals, floored so it always clears a few
        // heartbeats even when polling fast.
        _confirmDelay = TimeSpan.FromMilliseconds(Math.Max(100, _pollInterval.TotalMilliseconds * 2));
    }

    public ClusterViolation? FirstViolation => _firstViolation;

    public void Start() => _loop ??= Task.Run(() => RunAsync(_cts.Token));

    /// <summary>Non-blocking coalesced nudge; safe to call from the nemesis decision/delivery path.</summary>
    public void Notify() => _notifications.Writer.TryWrite(0);

    /// <summary>Throws if any invariant has been confirmed violated.</summary>
    public void ThrowIfViolated()
    {
        ClusterViolation? v = _firstViolation;
        if (v is not null)
            Assert.Fail($"Cluster invariant violated: {v}");
    }

    private async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && _firstViolation is null)
        {
            // Wake on a notification or the poll interval, whichever comes first.
            try
            {
                await _notifications.Reader.ReadAsync(ct).AsTask().WaitAsync(_pollInterval, ct).ConfigureAwait(false);
            }
            catch (TimeoutException) { /* poll cadence */ }
            catch (OperationCanceledException) { break; }

            // Drain any coalesced extra notifications.
            while (_notifications.Reader.TryRead(out _)) { }

            await EvaluateOnceAsync(ct).ConfigureAwait(false);
        }
    }

    private async Task EvaluateOnceAsync(CancellationToken ct)
    {
        ClusterView current;
        try { current = Augment(await _sampler(ct).ConfigureAwait(false)); }
        catch (OperationCanceledException) { return; }
        catch { return; } // a transient sampling failure (e.g. an executor mid-dispose) is not a violation

        foreach (IClusterInvariant inv in _invariants)
        {
            ClusterViolation? v = inv.Evaluate(_previous, current);
            if (v is null)
                continue;

            if (v.RequiresConfirmation)
            {
                // Re-sample and re-evaluate to filter non-atomic-snapshot false positives (transient leader
                // skew, a gap-aware commit-frontier dip during log-hole repair). Wait briefly first so a
                // genuinely transient condition has time to heal before we look again; a real violation
                // persists across the wait and is recorded.
                try { await Task.Delay(_confirmDelay, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }

                ClusterView confirm;
                try { confirm = Augment(await _sampler(ct).ConfigureAwait(false)); }
                catch { continue; }
                ClusterViolation? again = inv.Evaluate(current, confirm);
                if (again is null)
                    continue;
                v = again;
            }

            _firstViolation = v;
            return;
        }

        _previous = current;
    }

    /// <summary>
    /// Injects the running per-node max commit index (as seen BEFORE this view) so the monotonicity invariant
    /// can detect a regression, then advances the running max.
    /// </summary>
    private ClusterView Augment(ClusterView view)
    {
        Dictionary<string, long> snapshot = new(_maxCommit);
        ClusterView augmented = view with { MaxCommitByNode = snapshot };

        foreach (RaftPartitionView v in view.PartitionViews)
        {
            long prior = _maxCommit.GetValueOrDefault(v.Endpoint, long.MinValue);
            if (v.CommitIndex > prior)
                _maxCommit[v.Endpoint] = v.CommitIndex;
        }
        return augmented;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_loop is not null)
        {
            try { await _loop.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
        _cts.Dispose();
    }
}
