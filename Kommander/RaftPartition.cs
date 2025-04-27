
using Nixie;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;

namespace Kommander;

/// <summary>
/// Represents a partition in a Raft system. This class is responsible for managing
/// the lifecycle and operational aspects of a Raft partition, including log replication,
/// voting processes, and state management.
/// </summary>
public sealed class RaftPartition : IDisposable
{
    private static readonly RaftRequest RaftStateRequest = new(RaftRequestType.GetNodeState);

    private readonly ActorSystem actorSystem;

    private readonly ICommunication communication;
    
    private readonly SemaphoreSlim semaphore = new(1, 1);

    private readonly IActorRefAggregate<RaftStateActor, RaftRequest, RaftResponse> raftActor;

    private readonly RaftManager manager;

    internal string Leader { get; set; } = "";
    
    public int PartitionId { get; }
    
    public int StartRange { get; internal set; }
    
    public int EndRange { get; internal set; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="manager"></param>
    /// <param name="walAdapter"></param>
    /// <param name="communication"></param>
    /// <param name="partitionId"></param>
    public RaftPartition(
        ActorSystem actorSystem, 
        RaftManager manager, 
        IWAL walAdapter, 
        ICommunication communication, 
        int partitionId,
        int startRange,
        int endRange,
        ILogger<IRaft> logger
    )
    {
        this.actorSystem = actorSystem;
        this.manager = manager;
        this.communication = communication;
        
        PartitionId = partitionId;
        StartRange = startRange;
        EndRange = endRange;

        raftActor = actorSystem.SpawnAggregate<RaftStateActor, RaftRequest, RaftResponse>(
            "raft-partition-" + partitionId, 
            manager, 
            this,
            walAdapter,
            communication,
            logger
        );
    }

    /// <summary>
    /// Enqueues a handshake message from the partition.
    /// </summary>
    /// <param name="request"></param>
    public void Handshake(HandshakeRequest request)
    {
        raftActor.Send(new(
            RaftRequestType.ReceiveHandshake, 
            0, 
            request.MaxLogId, 
            HLCTimestamp.Zero, 
            request.Endpoint
        ));
    }

    /// <summary>
    /// Enqueues a "request a vote" message from the partition.
    /// </summary>
    /// <param name="request"></param>
    public void RequestVote(RequestVotesRequest request)
    {
        raftActor.Send(new(
            RaftRequestType.RequestVote, 
            request.Term, 
            request.MaxLogId, 
            request.Time, 
            request.Endpoint
        ));
    }

    /// <summary>
    /// Enqueues a "vote to become leader" message in a partition.
    /// </summary>
    /// <param name="request"></param>
    public void Vote(VoteRequest request)
    {
        raftActor.Send(new(
            RaftRequestType.ReceiveVote, 
            request.Term, 
            request.MaxLogId, 
            request.Time, 
            request.Endpoint
        ));
    }

    /// <summary>
    /// Append logs to the partition returning the commited index.
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void AppendLogs(AppendLogsRequest request)
    {
        raftActor.Ask(new(
            RaftRequestType.AppendLogs,
            request.Term,
            0,
            request.Time,
            request.Endpoint,
            RaftOperationStatus.Success,
            request.Logs
        ));
    }
    
    /// <summary>
    /// Complete the append logs request
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    public void CompleteAppendLogs(CompleteAppendLogsRequest request)
    {
        raftActor.Ask(new(
            RaftRequestType.CompleteAppendLogs,
            request.Term,
            request.CommitIndex,
            request.Time,
            request.Endpoint,
            request.Status,
            null
        ));
    }

    /// <summary>
    /// Replicates logs to the cluster, ensuring log consistency according to the Raft consensus algorithm.
    /// (Replicates a single log to the partition)
    /// </summary>
    /// <param name="type">The type of the log entry to be replicated.</param>
    /// <param name="data">The byte array containing the data of the log entry.</param>
    /// <param name="autoCommit">A boolean value indicating whether the log should be committed automatically upon replication success.</param>
    /// <returns>A task that represents the asynchronous operation, containing a tuple with a boolean indicating success,
    /// a <see cref="RaftOperationStatus"/> indicating the result status, and an <see cref="HLCTimestamp"/> representing the ticket ID for the log entry.</returns>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateLogs(string type, byte[] data, bool autoCommit)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        List<RaftLog> logsToReplicate = [new() { Type = RaftLogType.Proposed, LogType = type, LogData = data }];
        
        RaftResponse? response = await raftActor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate, autoCommit)).ConfigureAwait(false);
        
        if (response is null)
            return (false, RaftOperationStatus.Errored, HLCTimestamp.Zero);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);
        
        return (false, response.Status, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Replicates logs across the Raft cluster.
    /// </summary>
    /// <param name="type">The type of the logs to be replicated, identified by a string.</param>
    /// <param name="logs">A collection of log entries, each represented as a byte array, to be replicated.</param>
    /// <param name="autoCommit">A boolean indicating whether the logs should be automatically committed after replication.</param>
    /// <returns>A tuple containing a boolean indicating success, the status of the operation as a <see cref="RaftOperationStatus"/> value, and the <see cref="HLCTimestamp"/> ticket ID of the operation.</returns>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateLogs(string type, IEnumerable<byte[]> logs, bool autoCommit = true)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        List<RaftLog> logsToReplicate = logs.Select(data => new RaftLog { Type = RaftLogType.Proposed, LogType = type, LogData = data }).ToList();
        
        RaftResponse? response = await raftActor.Ask(new(RaftRequestType.ReplicateLogs, logsToReplicate, autoCommit)).ConfigureAwait(false);
        
        if (response is null)
            return (false, RaftOperationStatus.Errored, HLCTimestamp.Zero);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);
        
        return (false, response.Status, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Commits logs for the specified ticket identifier if the current node is the leader and notifies followers.
    /// </summary>
    /// <param name="ticketId">The logical timestamp associated with the logs to be committed.</param>
    /// <returns>A tuple containing a boolean indicating success, the status of the operation as <see cref="RaftOperationStatus"/>, and the commit index of the logs.</returns>
    public async Task<(bool success, RaftOperationStatus status, long commitIndex)> CommitLogs(HLCTimestamp ticketId)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, 0);
        
        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, 0);
        
        RaftResponse? response = await raftActor.Ask(new(RaftRequestType.CommitLogs, ticketId, false)).ConfigureAwait(false);
        
        if (response is null)
            return (false, RaftOperationStatus.Errored, 0);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.LogIndex);
        
        return (false, response.Status, 0);
    }

    /// <summary>
    /// Attempts to roll back logs to a specific timestamp for the current Raft partition.
    /// This operation can only be performed by the leader node of the partition.
    /// </summary>
    /// <param name="ticketId">The timestamp indicating the point to roll back the logs to.</param>
    /// <returns>A tuple indicating the success of the operation, the operation status, and the commit index.</returns>
    public async Task<(bool success, RaftOperationStatus status, long commitIndex)> RollbackLogs(HLCTimestamp ticketId)
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, 0);
        
        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, 0);
        
        RaftResponse? response = await raftActor.Ask(new(RaftRequestType.RollbackLogs, ticketId, false)).ConfigureAwait(false);
        
        if (response is null)
            return (false, RaftOperationStatus.Errored, 0);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.LogIndex);
        
        return (false, response.Status, 0);
    }

    /// <summary>
    /// Attempts to replicate a checkpoint across the Raft cluster in the specified partition
    /// Checkpoints must be added to the Raft Log when the leader has replicated the logs to an external store.
    /// </summary>
    /// <returns>
    /// A tuple containing a boolean indicating success, the status of the operation, and the associated HLCTimestamp for the checkpoint.
    /// </returns>
    public async Task<(bool success, RaftOperationStatus status, HLCTimestamp ticketId)> ReplicateCheckpoint()
    {
        if (string.IsNullOrEmpty(Leader))
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        if (Leader != manager.LocalEndpoint)
            return (false, RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        RaftResponse? response = await raftActor.Ask(new(RaftRequestType.ReplicateCheckpoint)).ConfigureAwait(false);
        
        if (response is null)
            return (false, RaftOperationStatus.Errored, HLCTimestamp.Zero);
        
        if (response.Status == RaftOperationStatus.Success)
            return (true, response.Status, response.TicketId);
        
        return (false, response.Status, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Retrieves the current state of the Raft node.
    /// </summary>
    /// <returns>The current state of the node as <see cref="RaftNodeState"/>.</returns>
    /// <exception cref="RaftException">
    /// Thrown if the response is invalid or cannot determine the node state.
    /// </exception>
    public async ValueTask<RaftNodeState> GetState()
    {
        if (!string.IsNullOrEmpty(Leader) && Leader == manager.LocalEndpoint)
            return RaftNodeState.Leader;

        RaftResponse? response = await raftActor.Ask(RaftStateRequest).ConfigureAwait(false);
        
        if (response is null)
            throw new RaftException("Unknown response (1)");
        
        if (response.Type != RaftResponseType.NodeState)
            throw new RaftException("Unknown response (2)");

        return response.NodeState;
    }

    /// <summary>
    /// Retrieves the state of a ticket and its associated commit ID from the Raft partition.
    /// Proposals produce a ticket ID that can be used to track the state of the proposal.
    /// </summary>
    /// <param name="ticketId">The unique identifier of the ticket based on a hybrid logical clock timestamp.</param>
    /// <param name="autoCommit">A flag indicating whether the ticket should be automatically committed if not found in a final state.</param>
    /// <returns>A tuple containing the state of the ticket (<see cref="RaftTicketState"/>) and the commit ID as a long value.</returns>
    /// <exception cref="RaftException">Thrown when an unexpected or unknown response is received from the Raft actor.</exception>
    public async Task<(RaftTicketState state, long commitId)> GetTicketState(HLCTimestamp ticketId, bool autoCommit)
    {
        RaftResponse? response = await raftActor.Ask(new(RaftRequestType.GetTicketState, ticketId, autoCommit)).ConfigureAwait(false);
        
        if (response is null)
            throw new RaftException("Unknown response (1)");
        
        if (response.Type != RaftResponseType.TicketState)
            throw new RaftException("Unknown response (2)");

        return (response.TicketState, response.LogIndex);
    }
    
    /// <summary>
    /// Sends a CheckLeader message to the raft state actor to check for leader changes
    /// </summary>
    public void CheckLeader()
    {
        raftActor.Send(new(RaftRequestType.CheckLeader));
    }

    public void Dispose()
    {
        semaphore.Dispose();
    }
}