
using Kommander.Time;

namespace Kommander.Data;

/// <summary>
/// Represents a log entry in the Raft log.
/// </summary>
public sealed class RaftLog
{
    public long Id { get; set; }
    
    public RaftLogType Type { get; set; } = RaftLogType.Commited;
    
    public long Term { get; set; }
    
    public HLCTimestamp Time { get; set; }
    
    public string? LogType { get; set; }
    
    public byte[]? LogData { get; set; }
}
