
namespace Kommander;

/// <summary>
/// Raft configuration
/// </summary>
public class RaftConfiguration
{
    public string? Host { get; set; }
    
    public int Port { get; set; }

    public int MaxPartitions { get; set; } = 1;
}