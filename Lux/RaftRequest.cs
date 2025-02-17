
namespace Lux;

public readonly struct RaftRequest
{
    public RaftRequestType Type { get; }

    public long Term { get; } = -1;

    public string? Endpoint { get; } = null; 

    public List<RaftLog>? Log { get; } = null;

    public RaftRequest(RaftRequestType type, long term = -1, string? endpoint = null, List<RaftLog>? log = null)
    {
        Type = type;
        Term = term;
        Endpoint = endpoint;
        Log = log;
    }

    public RaftRequest(RaftRequestType type, List<RaftLog> log)
    {
        Type = type;
        Log = log;
    }
}

