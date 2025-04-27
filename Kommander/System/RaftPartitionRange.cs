
namespace Kommander.System;

/// <summary>
/// Represents a Raft partition range with a unique identifier, start, and end range values.
/// </summary>
public sealed class RaftPartitionRange
{
    public int PartitionId { get; set; }
    
    public int StartRange { get; set; }
    
    public int EndRange { get; set; }
}