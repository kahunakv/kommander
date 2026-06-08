
namespace Kommander.System;

/// <summary>
/// JSON serialization wrapper for the partition map stored under
/// <see cref="RaftSystemConfigKeys.Partitions"/>. The flat list was replaced by
/// this envelope so that <see cref="MapVersion"/> travels with the data and any
/// node can detect that its in-memory copy is stale by comparing versions.
/// </summary>
public sealed class RaftPartitionMap
{
    /// <summary>
    /// Monotonically increasing counter bumped on every write to the partition map.
    /// Initialized to 1 when the map is first created; never decremented or reset.
    /// </summary>
    public long MapVersion { get; set; }

    public List<RaftPartitionRange> Partitions { get; set; } = [];
}
