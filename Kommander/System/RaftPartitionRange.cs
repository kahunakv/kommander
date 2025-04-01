
namespace Kommander.System;

public sealed class RaftPartitionRange
{
    public int PartitionId { get; set; }
    
    public int StartRange { get; set; }
    
    public int EndRange { get; set; }
}