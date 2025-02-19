
namespace Lux;

public class RaftConfiguration
{
    public string? Host { get; set; }
    
    public int Port { get; set; }

    public int MaxPartitions { get; set; } = 1;
}