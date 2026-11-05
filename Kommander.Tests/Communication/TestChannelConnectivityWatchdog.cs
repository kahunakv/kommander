
using System.Diagnostics;
using Grpc.Core;
using Kommander.Communication.Grpc;

namespace Kommander.Tests.Communication;

/// <summary>
/// The stuck-channel verdict of <see cref="ChannelConnectivityWatchdog"/>: a channel that sits in
/// TransientFailure with no transition for the whole limit has a dead connect loop; any transition
/// inside the limit, including a flicker straight back into TransientFailure, means the loop is
/// alive and the channel must be left alone.
/// </summary>
public sealed class TestChannelConnectivityWatchdog
{
    /// <summary>Scripted connectivity: the test moves the state and waiters wake like the real channel's do.</summary>
    private sealed class ScriptedConnectivity(ConnectivityState initial) : IChannelConnectivity
    {
        private readonly object sync = new();
        private ConnectivityState state = initial;
        private TaskCompletionSource changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ConnectivityState State
        {
            get
            {
                lock (sync)
                    return state;
            }
        }

        public int Waits { get; private set; }

        public void Set(ConnectivityState next)
        {
            TaskCompletionSource previous;

            lock (sync)
            {
                if (state == next)
                    return;

                state = next;
                previous = changed;
                changed = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            previous.TrySetResult();
        }

        public Task WaitForStateChangedAsync(ConnectivityState lastObservedState, CancellationToken cancellationToken)
        {
            Task pending;

            lock (sync)
            {
                Waits++;

                if (state != lastObservedState)
                    return Task.CompletedTask;

                pending = changed.Task;
            }

            return pending.WaitAsync(cancellationToken);
        }
    }

    [Fact]
    public async Task NoTransitionForTheWholeLimit_IsStuck()
    {
        ScriptedConnectivity channel = new(ConnectivityState.TransientFailure);
        TimeSpan limit = TimeSpan.FromMilliseconds(150);

        long started = Stopwatch.GetTimestamp();
        bool stuck = await ChannelConnectivityWatchdog.StaysInTransientFailureAsync(channel, limit, TestContext.Current.CancellationToken);
        TimeSpan elapsed = Stopwatch.GetElapsedTime(started);

        Assert.True(stuck);
        Assert.True(elapsed >= limit - TimeSpan.FromMilliseconds(20), $"verdict arrived after {elapsed.TotalMilliseconds:F0} ms, before the {limit.TotalMilliseconds:F0} ms limit");
    }

    [Fact]
    public async Task LeavingTransientFailureInsideTheLimit_IsAlive()
    {
        ScriptedConnectivity channel = new(ConnectivityState.TransientFailure);

        Task<bool> verdict = ChannelConnectivityWatchdog.StaysInTransientFailureAsync(channel, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await Task.Delay(30, TestContext.Current.CancellationToken);
        channel.Set(ConnectivityState.Connecting);

        Assert.False(await verdict.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A live loop to a dead peer flips Connecting → TransientFailure on every attempt. The first
    /// transition settles the verdict even when the state is back in TransientFailure by the time
    /// anyone looks: it is the transition, not the state, that proves the loop is alive.
    /// </summary>
    [Fact]
    public async Task FlickerBackIntoTransientFailure_IsStillAlive()
    {
        ScriptedConnectivity channel = new(ConnectivityState.TransientFailure);

        Task<bool> verdict = ChannelConnectivityWatchdog.StaysInTransientFailureAsync(channel, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await Task.Delay(30, TestContext.Current.CancellationToken);
        channel.Set(ConnectivityState.Connecting);
        channel.Set(ConnectivityState.TransientFailure);

        Assert.False(await verdict.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
        Assert.Equal(ConnectivityState.TransientFailure, channel.State);
    }

    [Fact]
    public async Task NotInTransientFailure_IsAliveAtOnceWithoutWaiting()
    {
        ScriptedConnectivity channel = new(ConnectivityState.Idle);

        bool stuck = await ChannelConnectivityWatchdog.StaysInTransientFailureAsync(channel, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.False(stuck);
        Assert.Equal(0, channel.Waits);
    }

    [Fact]
    public async Task CallerCancellation_Propagates()
    {
        ScriptedConnectivity channel = new(ConnectivityState.TransientFailure);
        using CancellationTokenSource cts = new();

        Task<bool> verdict = ChannelConnectivityWatchdog.StaysInTransientFailureAsync(channel, TimeSpan.FromSeconds(5), cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => verdict.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
    }
}
