
namespace Kommander;

/// <summary>
/// Decides whether a seed-joining Learner is still catching up or has stalled, from what the joiner
/// can observe locally. <see cref="ClusterJoinService"/> feeds it one sample per poll of its promotion
/// wait and fails the join only when no sample has shown progress for
/// <see cref="RaftConfiguration.JoinPromotionStallTimeout"/>.
/// <para>
/// Why progress and not time: promotion waits for the learner to come within
/// <see cref="RaftConfiguration.LearnerPromotionLag"/> of the leader and stay there for the stable
/// window. On a partition with real state that is a snapshot install plus a backfill of whatever
/// committed meanwhile — a function of data size and write rate, not of liveness. A fixed bound failed
/// every replacement of a node on a cluster that had run under load (Caraxes, 2026-10-14).
/// </para>
/// <para>
/// Progress is any of: a partition's published commit frontier rising above the highest value seen
/// for it, a partition seen for the first time, or the snapshot receiver's progress marker changing
/// (a chunk staged, an install started or completed, an import consuming its snapshot). Frontiers are
/// compared against the best value seen, not the previous sample, so a frontier that dips and recovers
/// is not counted twice. The snapshot marker is compared for inequality only; see
/// <see cref="SnapshotReceiver.ProgressMarker"/> for why it can fall.
/// </para>
/// <para>
/// The first sample is the baseline and counts as progress at its own time, so the stall clock starts
/// at the first observation rather than at construction. Not thread-safe: one wait loop owns it.
/// </para>
/// </summary>
internal sealed class JoinCatchUpProgress
{
    private readonly Dictionary<int, long> bestFrontiers = new();
    private long snapshotMarker;
    private bool hasSnapshotMarker;
    private bool hasBaseline;
    private long lastProgressMs;

    /// <summary>Elapsed time, in ms on the caller's clock, of the last sample that showed progress.</summary>
    internal long LastProgressMs => lastProgressMs;

    /// <summary>What the last progress was, for the log and the stall message.</summary>
    internal string LastProgress { get; private set; } = "none observed";

    /// <summary>
    /// Records one sample and returns true when it shows progress (the first sample always does).
    /// </summary>
    /// <param name="frontiers">Published commit frontier of every partition hosted now, system partition included.</param>
    /// <param name="snapshotProgressMarker">
    /// The current <see cref="SnapshotReceiver.ProgressMarker"/>, or null when it could not be read;
    /// a null sample neither shows progress nor replaces the last value read.
    /// </param>
    /// <param name="nowMs">Elapsed time on a monotonic clock the caller owns.</param>
    internal bool Observe(IEnumerable<(int PartitionId, long CommitIndex)> frontiers, long? snapshotProgressMarker, long nowMs)
    {
        string? progress = null;

        foreach ((int partitionId, long commitIndex) in frontiers)
        {
            if (!bestFrontiers.TryGetValue(partitionId, out long best))
            {
                bestFrontiers[partitionId] = commitIndex;

                if (hasBaseline)
                    progress ??= $"partition {partitionId} appeared at commit index {commitIndex}";

                continue;
            }

            if (commitIndex > best)
            {
                bestFrontiers[partitionId] = commitIndex;
                progress ??= $"partition {partitionId} commit index {best} -> {commitIndex}";
            }
        }

        if (snapshotProgressMarker is { } marker)
        {
            if (hasSnapshotMarker && marker != snapshotMarker)
                progress ??= "snapshot receipt or install advanced";

            snapshotMarker = marker;
            hasSnapshotMarker = true;
        }

        if (!hasBaseline)
        {
            hasBaseline = true;
            progress = "baseline";
        }

        if (progress is null)
            return false;

        lastProgressMs = nowMs;
        LastProgress = progress;
        return true;
    }

    /// <summary>Time, in ms, since the last sample that showed progress.</summary>
    internal long MillisecondsSinceProgress(long nowMs) => Math.Max(0, nowMs - lastProgressMs);

    /// <summary>The highest commit frontier seen per partition, as "p0=12 p1=340", for diagnostics.</summary>
    internal string DescribeFrontiers() =>
        bestFrontiers.Count == 0
            ? "none"
            : string.Join(' ', bestFrontiers.OrderBy(static kv => kv.Key).Select(static kv => $"p{kv.Key}={kv.Value}"));
}
