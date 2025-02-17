
namespace Lux.Data;

public readonly struct RaftWALResponse
{
    public ulong NextId { get; }
    
    public RaftWALResponse(ulong nextId)
    {
        NextId = nextId;
    }
}