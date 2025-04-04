
using Kommander.Time;

namespace Kommander.Logging;

public static partial class RaftLoggerExtensions
{
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] WAL restored at #{NextId} in {ElapsedMs}ms")]
    public static partial void LogInfoWalRestored(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, long nextId, long elapsedMs);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Voting concluded after {Elapsed}ms. No quorum available")]
    public static partial void LogInfoVotingConcluded(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, double elapsed);
    
    [LoggerMessage(Level = LogLevel.Warning, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Voted to become leader after {LastHeartbeat}ms. Term={CurrentTerm}")]
    public static partial void LogWarnVotedToBecomeLeader(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, double lastHeartbeat, long currentTerm);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposed logs {Timestamp} Logs={Logs}")]
    public static partial void LogDebugProposedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, string logs);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposed checkpoint logs {Timestamp} Logs={Logs}")]
    public static partial void LogInfoProposedCheckpointLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, int logs);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Successfully completed logs from {Endpoint} Timestamp={Timestamp} CommitedIndex={Index}")]
    public static partial void LogInfoSuccessfullyCompletedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, HLCTimestamp timestamp, long index);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposal partially completed at {Timestamp}")]
    public static partial void LogInfoProposalPartiallyCompletedAt(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposal completed at {Timestamp}")]
    public static partial void LogInfoProposalCompletedAt(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Committed proposal {Timestamp} Logs={Logs}")]
    public static partial void LogDebugCommittedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, string logs);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Rolled back proposal {Timestamp} Logs={Logs}")]
    public static partial void LogDebugRolledbackLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, string logs);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Asked {Endpoint} for votes on Term={CurrentTerm}")]
    public static partial void LogInfoAskedForVotes(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long currentTerm);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Sending vote to {Endpoint} on Term={CurrentTerm}")]
    public static partial void LogInfoSendingVote(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long currentTerm);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received logs from leader {Endpoint} with Term={Term} Timestamp={Timestamp} Logs={Logs}")]
    public static partial void LogDebugReceivedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term, HLCTimestamp timestamp, string logs);
}