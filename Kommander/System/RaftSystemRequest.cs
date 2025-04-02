
using Kommander.System.Protos;

namespace Kommander.System;

public sealed class RaftSystemRequest
{
    public RaftSystemRequestType Type { get; }
    
    public int PartitionId { get; }
    
    public string? LeaderNode { get; }

    public byte[]? LogData { get; }
    
    public RaftSystemRequest(RaftSystemRequestType type)
    {
        Type = type;
    }

    public RaftSystemRequest(RaftSystemRequestType type, byte[] logData)
    {
        Type = type;
        LogData = logData;
    }
    
    public RaftSystemRequest(RaftSystemRequestType type, string leaderNode)
    {
        Type = type;
        LeaderNode = leaderNode;
    }
    
    public RaftSystemRequest(RaftSystemRequestType type, int partitionId)
    {
        Type = type;
        PartitionId = partitionId;
    }
}