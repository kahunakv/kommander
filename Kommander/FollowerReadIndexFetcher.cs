
using Kommander.Data;

namespace Kommander;

/// <summary>
/// Shares one <c>GetReadIndex</c> RPC among the concurrent follower reads of a partition, so a
/// burst of <see cref="IRaft.ConfirmLocalApplicationAsync"/> calls on a non-leader costs one
/// round trip to the leader instead of one per read. The leader already coalesces concurrent
/// confirmations into one quorum round (<c>ReadIndexCoordinator</c>); this is the follower half.
/// <para>
/// <b>Safety rule: a read joins only a fetch that is not sent yet.</b> A read index is valid for
/// a read only when the leader captured it after the read arrived. A read that joined a fetch
/// already on the wire could get an index the leader captured before that read existed, and a
/// write acknowledged in between would then be invisible to it — a linearizability violation.
/// So each key has at most one fetch in flight and one batch that waits for the next fetch. A
/// read always joins the waiting batch. The batch is detached under the lock before its RPC is
/// sent, so every read in it arrived before the send.
/// </para>
/// <para>
/// <b>Cost.</b> When no fetch is in flight the read starts one at once, so a quiet follower sees
/// no added latency. Under load a read can wait for the fetch in flight to finish before its own
/// is sent: up to one extra round trip, in exchange for one RPC per round trip instead of one
/// per read.
/// </para>
/// <para>
/// <b>Bounds.</b> The key is (partition, believed leader), so a leader change starts a separate
/// chain and never mixes answers from two endpoints. The shared RPC runs under its own timeout,
/// never under one caller's token: a caller that gives up must not fail the reads that share its
/// fetch, and a fetch that hangs must not park every later read of the partition. Each caller
/// still waits only as long as its own token allows. Every failure, timeout included, completes
/// the batch with an unsuccessful response, so the callers fail closed.
/// </para>
/// </summary>
internal sealed class FollowerReadIndexFetcher
{
    /// <summary>
    /// Fetch state of one (partition, leader) key. Guarded by <see cref="sync"/>.
    /// </summary>
    private sealed class FetchChain
    {
        /// <summary>True while a drain loop owns this key and sends its batches in order.</summary>
        public bool Running;

        /// <summary>
        /// The batch of reads that arrived after the last send. Null when no read waits.
        /// </summary>
        public TaskCompletionSource<GetReadIndexResponse>? Waiting;
    }

    private readonly Func<RaftNode, GetReadIndexRequest, CancellationToken, Task<GetReadIndexResponse>> fetch;
    private readonly Func<TimeSpan> fetchTimeout;
    private readonly object sync = new();
    private readonly Dictionary<(int PartitionId, string Leader), FetchChain> chains = new();

    /// <param name="fetch">The transport call. It must map every failure to an unsuccessful
    /// response; an exception is also mapped here, as a second line of defense.</param>
    /// <param name="fetchTimeout">The bound for one shared RPC, read at each send so a
    /// configuration change applies to the next fetch.</param>
    internal FollowerReadIndexFetcher(
        Func<RaftNode, GetReadIndexRequest, CancellationToken, Task<GetReadIndexResponse>> fetch,
        Func<TimeSpan> fetchTimeout)
    {
        this.fetch = fetch;
        this.fetchTimeout = fetchTimeout;
    }

    /// <summary>
    /// Returns a read index that <paramref name="node"/> confirmed after this call started, shared
    /// with the other reads of the same partition and leader that joined the same batch.
    /// Cancellation of <paramref name="cancellationToken"/> ends only this caller's wait.
    /// </summary>
    internal Task<GetReadIndexResponse> FetchAsync(RaftNode node, GetReadIndexRequest request, CancellationToken cancellationToken = default)
    {
        (int, string) key = (request.PartitionId, node.Endpoint);
        Task<GetReadIndexResponse> batch;
        FetchChain? startChain = null;

        lock (sync)
        {
            if (!chains.TryGetValue(key, out FetchChain? chain))
            {
                chain = new();
                chains[key] = chain;
            }

            chain.Waiting ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            batch = chain.Waiting.Task;

            if (!chain.Running)
            {
                chain.Running = true;
                startChain = chain;
            }
        }

        if (startChain is not null)
            _ = DrainAsync(key, node, request, startChain);

        return batch.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Sends the waiting batches of one key, one at a time, until no read waits. Exactly one drain
    /// runs per key (<see cref="FetchChain.Running"/>), which is what makes "a batch is sent only
    /// after the previous fetch completes" hold. The key is removed when the drain ends, so idle
    /// partitions and old leaders keep no state.
    /// </summary>
    private async Task DrainAsync((int, string) key, RaftNode node, GetReadIndexRequest request, FetchChain chain)
    {
        while (true)
        {
            TaskCompletionSource<GetReadIndexResponse>? batch;

            lock (sync)
            {
                batch = chain.Waiting;
                chain.Waiting = null;

                if (batch is null)
                {
                    chain.Running = false;
                    chains.Remove(key);
                    return;
                }
            }

            // Every read in `batch` joined it under the lock above, before this send.
            GetReadIndexResponse response;

            try
            {
                using CancellationTokenSource cts = new(fetchTimeout());
                response = await fetch(node, request, cts.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                response = new GetReadIndexResponse(false);
            }

            batch.TrySetResult(response);
        }
    }
}
