namespace Kommander.Logging;

public static partial class RaftLoggerExtensions
{
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] WAL restored at #{NextId} in {ElapsedMs}ms")]
    public static partial void LogInfoWalRestored(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, long nextId, long elapsedMs);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Voting concluded after {Elapsed}ms. No quorum available")]
    public static partial void LogInfoVotingConcluded(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, double elapsed);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Voted to become leader after {LastHeartbeat}ms. Term={CurrentTerm}")]
    public static partial void LogInfoVotedToBecomeLeader(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, double lastHeartbeat, long currentTerm);
}