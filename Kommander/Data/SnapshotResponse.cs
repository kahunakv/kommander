
namespace Kommander.Data;

/// <summary>
/// Result returned by a follower after attempting to install a snapshot received via
/// <see cref="SnapshotRequest"/>.
/// </summary>
public sealed class SnapshotResponse
{
    public bool Success { get; init; }

    public SnapshotResponse() { }

    public SnapshotResponse(bool success) => Success = success;
}
