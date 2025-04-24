
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// Represents a log entry in the Raft log.
/// </summary>
public sealed class RaftLog
{
    /// <summary>
    /// Gets or sets the unique identifier for a Raft log entry.
    /// </summary>
    public long Id { get; set; }

    /// <summary>
    /// Gets or sets the type of the Raft log, indicating its state or purpose.
    /// </summary>
    public RaftLogType Type { get; set; } = RaftLogType.Proposed;

    /// <summary>
    /// Gets or sets the term associated with a Raft log entry.
    /// </summary>
    public long Term { get; set; }

    /// <summary>
    /// Gets or sets the timestamp associated with the Raft log entry.
    /// </summary>
    public HLCTimestamp Time { get; set; }

    /// <summary>
    /// Gets or sets the type of the Raft log entry, indicating the purpose or category of the log.
    /// </summary>
    public string? LogType { get; set; }

    /// <summary>
    /// Gets or sets the binary data associated with a Raft log entry.
    /// </summary>
    public byte[]? LogData { get; set; }
}
