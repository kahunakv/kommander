
using Kommander.Communication.Grpc;
using Kommander.Data;

namespace Kommander;

/// <summary>
/// Per-heartbeat-round memo of bounded backfill batches, keyed by the batch start index.
/// </summary>
/// <remarks>
/// <para>Every lagging follower used to drive its own <c>GetRangeAsync</c> on every heartbeat, even
/// though followers catching up are almost always anchored at the same index — so the leader re-read
/// and re-decoded an identical range from the WAL once per follower per round, and then re-encoded it
/// once per follower on the gRPC side. This memo makes the round read each distinct range once and fan
/// the result out, mirroring what the live-propose path already does with a single shared list plus an
/// <see cref="AppendLogsGrpcLogCache"/>.</para>
///
/// <para><b>Lifetime is one round, deliberately.</b> The cached entries are only valid for the WAL
/// state observed at the moment they were read; a longer-lived cache would have to be invalidated on
/// every commit, truncation and compaction. A fresh instance per heartbeat loop keeps the memo
/// trivially correct.</para>
///
/// <para><b>Not thread-safe</b> — it is confined to a single heartbeat loop, which runs on the
/// partition executor and is therefore already serialized.</para>
///
/// <para>Empty batches are memoized too: an empty read means the leader has compacted past the anchor,
/// and every follower sitting at that same anchor would otherwise repeat the same fruitless read before
/// each falls through to the snapshot path.</para>
/// </remarks>
internal sealed class BackfillRoundBatches
{
    /// <summary>
    /// A batch shared by every follower anchored at the same start index. <see cref="Logs"/> is handed
    /// to multiple outbound requests, so it must be treated as read-only once memoized;
    /// <see cref="GrpcLogCache"/> ensures the Protobuf form is built once for the whole fan-out.
    /// </summary>
    internal sealed record Batch(long From, List<RaftLog> Logs, long PrevTerm, AppendLogsGrpcLogCache? GrpcLogCache);

    // A round touches at most one entry per node, and nodes anchored at the same index collapse onto a
    // single entry, so a linear scan over a handful of items beats a dictionary's hashing and allocation.
    private readonly List<Batch> batches = [];

    /// <summary>Returns the batch already read for <paramref name="from"/> in this round, if any.</summary>
    internal bool TryGet(long from, out Batch? batch)
    {
        foreach (Batch candidate in batches)
        {
            if (candidate.From == from)
            {
                batch = candidate;
                return true;
            }
        }

        batch = null;
        return false;
    }

    /// <summary>
    /// Memoizes the batch read for <paramref name="from"/> and returns it. A gRPC cache is attached only
    /// for non-empty batches, since an empty one is never shipped.
    /// </summary>
    internal Batch Add(long from, List<RaftLog> logs, long prevTerm)
    {
        Batch batch = new(from, logs, prevTerm, logs.Count > 0 ? new AppendLogsGrpcLogCache() : null);
        batches.Add(batch);
        return batch;
    }
}
