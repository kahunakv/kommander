
using Grpc.Core;
using Grpc.Net.Client;

namespace Kommander.Communication.Grpc;

/// <summary>
/// The connectivity view of one pooled channel, narrowed to the two reads the stuck-channel
/// detector makes. <see cref="GrpcChannel"/> is sealed, so the detector is written against this
/// seam; the pool wraps the real channel in <see cref="GrpcChannelConnectivity"/> and tests drive
/// the detector with a scripted state.
/// </summary>
internal interface IChannelConnectivity
{
    ConnectivityState State { get; }

    /// <summary>
    /// Completes when the state leaves <paramref name="lastObservedState"/>, at once when it
    /// already has, and as cancelled when <paramref name="cancellationToken"/> fires first.
    /// </summary>
    Task WaitForStateChangedAsync(ConnectivityState lastObservedState, CancellationToken cancellationToken);
}

/// <summary>
/// <see cref="IChannelConnectivity"/> over a real <see cref="GrpcChannel"/>. Both members are the
/// channel's own connectivity API, which grpc-dotnet backs with its load-balancing infrastructure
/// for every plain <c>http(s)://host:port</c> address on a <c>SocketsHttpHandler</c>.
/// </summary>
internal sealed class GrpcChannelConnectivity(GrpcChannel channel) : IChannelConnectivity
{
    public ConnectivityState State => channel.State;

    public Task WaitForStateChangedAsync(ConnectivityState lastObservedState, CancellationToken cancellationToken) =>
        channel.WaitForStateChangedAsync(lastObservedState, cancellationToken);
}

/// <summary>
/// Tells a channel whose subchannel connect loop has died from one that is merely retrying.
///
/// <para><b>Why this exists.</b> grpc-dotnet runs one connect loop per subchannel: attempt,
/// publish <see cref="ConnectivityState.TransientFailure"/>, back off, attempt again. One
/// <c>SocketsHttpHandler.ConnectTimeout</c> budget covers the whole loop, so a healthy loop to a
/// dead peer can never sit in TransientFailure for longer than that budget: within it the state
/// flips to Connecting on every attempt, and when it runs out the loop publishes Idle and the next
/// call starts a fresh one. Before Grpc.Net.Client 2.83 the loop could exit through a catch that
/// left the state at TransientFailure with no loop behind it (grpc-dotnet issue #2734): every later
/// call failed at pick time with the cached <c>Error connecting to subchannel</c> status, and
/// nothing in the library restarts a loop from that state. A generic exception inside the loop
/// still ends it the same way on every version. The 1.13.3 inst3 soak lost a voter for 53 minutes
/// to exactly this: the new leader's channel to the restarted peer never reconnected while the
/// peer's own channels back were fine.</para>
///
/// <para><b>The test.</b> A channel that stays in TransientFailure, with no transition at all, for
/// longer than the connect budget has no loop running. The pool then disposes it and opens a new
/// one, which is the only recovery the library offers. The limit is deliberately a multiple of the
/// budget so a loop that is alive but slow is never mistaken for a dead one.</para>
/// </summary>
internal static class ChannelConnectivityWatchdog
{
    /// <summary>
    /// How long a channel may sit in TransientFailure without any transition before it is
    /// rebuilt: three times the handler's 10-second <c>ConnectTimeout</c>, the longest a live
    /// connect loop can hold that state once.
    /// </summary>
    public static readonly TimeSpan DefaultTransientFailureLimit = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Waits for the channel to leave <see cref="ConnectivityState.TransientFailure"/>. Returns
    /// <see langword="false"/> as soon as it does (the connect loop is alive; the caller re-arms on
    /// the next observation) or at once when it is not in that state. Returns
    /// <see langword="true"/> when <paramref name="limit"/> passes with no transition at all: the
    /// loop is dead and the channel will never recover on its own.
    ///
    /// <para>A channel disposed while this waits cancels its waiters without a token; that lands
    /// here as a stuck verdict, which is the right answer for a disposed channel too. Only the
    /// caller's own <paramref name="cancellationToken"/> propagates as an exception.</para>
    /// </summary>
    public static async Task<bool> StaysInTransientFailureAsync(
        IChannelConnectivity connectivity,
        TimeSpan limit,
        CancellationToken cancellationToken = default)
    {
        if (connectivity.State != ConnectivityState.TransientFailure)
            return false;

        using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(limit);

        try
        {
            await connectivity.WaitForStateChangedAsync(ConnectivityState.TransientFailure, deadline.Token).ConfigureAwait(false);
            return false;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return connectivity.State == ConnectivityState.TransientFailure;
        }
    }
}
