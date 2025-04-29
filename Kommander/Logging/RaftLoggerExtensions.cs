
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Logging;

/// <summary>
/// Provides extension methods for logging various Raft-specific events in a high-performant way
/// in implementations of the <see cref="IRaft"/> interface using the <see cref="ILogger"/> framework.
/// </summary>
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
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Successfully completed logs from {Endpoint} Timestamp={Timestamp} CommitedIndex={Index} Time={Elapsed}ms")]
    public static partial void LogInfoSuccessfullyCompletedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, HLCTimestamp timestamp, long index, double elapsed);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposal partially completed at {Timestamp} Time={Elapsed}ms")]
    public static partial void LogInfoProposalPartiallyCompletedAt(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, double elapsed);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Proposal completed at {Timestamp} Time={Elapsed}ms")]
    public static partial void LogInfoProposalCompletedAt(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, double elapsed);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Committed proposal {Timestamp} Logs={Logs} Time={Elapsed}ms")]
    public static partial void LogDebugCommittedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, string logs, double elapsed);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Rolled back proposal {Timestamp} Logs={Logs}")]
    public static partial void LogDebugRolledbackLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, HLCTimestamp timestamp, string logs);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Asked {Endpoint} for votes on Term={CurrentTerm}")]
    public static partial void LogInfoAskedForVotes(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long currentTerm);
    
    [LoggerMessage(Level = LogLevel.Information, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Sending vote to {Endpoint} on Term={CurrentTerm}")]
    public static partial void LogInfoSendingVote(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long currentTerm);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{LocalEndpoint}/{PartitionId}/{State}] Received logs from leader {Endpoint} with Term={Term} Timestamp={Timestamp} Logs={Logs}")]
    public static partial void LogDebugReceivedLogs(this ILogger<IRaft> logger, string localEndpoint, int partitionId, RaftNodeState state, string endpoint, long term, HLCTimestamp timestamp, string logs);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}/{PartitionId}] Committed log #{Id}")]
    public static partial void LogDebugCommittedLogs(this ILogger<IRaft> logger, string endpoint, int partitionId, long id);
    
    [LoggerMessage(Level = LogLevel.Debug, Message = "[{Endpoint}/{PartitionId}] Proposed log #{Id}")]
    public static partial void LogDebugProposedLogs(this ILogger<IRaft> logger, string endpoint, int partitionId, long id);
}