using Kommander.Data;
using Kommander.Tests.Simulation.Cluster;

namespace Kommander.Tests.Simulation.History;

/// <summary>
/// Records what a run's clients asked for and what they were told.
///
/// <para><b>Why this exists at all.</b> Every per-step invariant reads node state. None of them can
/// see what a client was told, and a node whose own state is perfectly consistent can still have
/// answered a client wrongly. DST FINDING 1 made the point from the other side: the stranded replica
/// broke no per-node rule because it never claimed the entry it was missing. A node that is honest
/// about being behind is invisible to state checks, and a node that is dishonest to a client is
/// invisible to them too.</para>
///
/// <para><b>The history drives the call.</b> Operations are issued through this class rather than
/// recorded alongside them, because a call that escapes the record is worse than no record: the
/// checker would then verify a history that is missing exactly the operation that went wrong.</para>
/// </summary>
public sealed class ClientHistory
{
    private readonly List<ClientOperation> operations = [];

    /// <summary>
    /// Advances on every invocation and every completion, giving the history a real-time order the
    /// step counter is too coarse to provide.
    /// </summary>
    private int sequence;

    /// <summary>Every operation, in the order it was issued.</summary>
    public IReadOnlyList<ClientOperation> Operations => operations;

    /// <summary>How many operations the run issued.</summary>
    public int Count => operations.Count;

    /// <summary>Operations the cluster acknowledged.</summary>
    public int AcknowledgedCount => operations.Count(op => op.Outcome == ClientOperationOutcome.Ok);

    /// <summary>Operations whose outcome the client never learned.</summary>
    public int UnknownCount => operations.Count(op => op.Outcome == ClientOperationOutcome.Info);

    /// <summary>
    /// Issues one append through <paramref name="node"/> and records the answer.
    ///
    /// <para>The call goes through <see cref="SimulationCluster.DriveAsync"/> because in driven mode
    /// the calling thread is the only one that can serve the executor the proposal is queued on;
    /// awaiting it directly would park the driver inside the work it has not yet driven.</para>
    /// </summary>
    public async Task<ClientOperation> AppendAsync(
        SimulationCluster cluster,
        SimulationNode node,
        int partitionId,
        string type,
        byte[] payload,
        CancellationToken cancellationToken)
    {
        int invokedAtStep = cluster.StepNumber;
        int invokedAtSequence = sequence++;

        RaftReplicationResult result = await cluster.DriveAsync(
            () => node.Manager.ReplicateLogs(partitionId, type, payload, cancellationToken: cancellationToken),
            cancellationToken).ConfigureAwait(false);

        ClientOperation operation = new(
            operations.Count,
            type,
            payload,
            invokedAtStep,
            cluster.StepNumber,
            invokedAtSequence,
            sequence++,
            Classify(result.Status),
            result.Status,
            result.LogIndex);

        operations.Add(operation);
        return operation;
    }

    /// <summary>
    /// Issues an append carrying a payload unique to this history, so the checker can tell one
    /// operation's entry from another's without needing the caller to manage identifiers.
    /// </summary>
    public Task<ClientOperation> AppendUniqueAsync(
        SimulationCluster cluster,
        SimulationNode node,
        int partitionId,
        string type,
        CancellationToken cancellationToken) =>
        AppendAsync(
            cluster,
            node,
            partitionId,
            type,
            global::System.Text.Encoding.UTF8.GetBytes($"op-{operations.Count}"),
            cancellationToken);

    /// <summary>
    /// Adds an operation the caller built itself.
    ///
    /// <para>Present for tests that need a specific history rather than a real run. A scenario
    /// should use <see cref="AppendAsync"/>, which cannot leave an operation unrecorded.</para>
    /// </summary>
    public void Record(ClientOperation operation)
    {
        operations.Add(operation);
        sequence = Math.Max(sequence, operation.CompletedAtSequence + 1);
    }

    /// <summary>
    /// Decides what an answer means.
    ///
    /// <para><b>Deliberately pessimistic.</b> Only a status that cannot have reached the log is
    /// treated as a refusal; everything else, including a plain error, is unknown. The asymmetry is
    /// on purpose. Calling an unknown outcome a refusal makes the checker report a phantom write
    /// every time a quorum commits an entry whose acknowledgement was lost, which is a normal thing
    /// for a distributed system to do. Calling one an acknowledgement would hide a lost write. The
    /// first mistake is loud and wrong; the second is silent and wrong.</para>
    /// </summary>
    public static ClientOperationOutcome Classify(RaftOperationStatus status) => status switch
    {
        RaftOperationStatus.Success => ClientOperationOutcome.Ok,

        // Refused before the proposal could enter the log.
        RaftOperationStatus.NodeIsNotLeader => ClientOperationOutcome.Fail,
        RaftOperationStatus.ProposalQueueFull => ClientOperationOutcome.Fail,
        RaftOperationStatus.RestoreInProgress => ClientOperationOutcome.Fail,
        RaftOperationStatus.PartitionMoved => ClientOperationOutcome.Fail,
        RaftOperationStatus.StaleMembership => ClientOperationOutcome.Fail,
        RaftOperationStatus.ConcurrentMembershipChange => ClientOperationOutcome.Fail,
        RaftOperationStatus.InsufficientVoters => ClientOperationOutcome.Fail,

        // Everything else may have taken effect: a timeout, a cancellation, a lost acknowledgement,
        // a leader deposed after its entry reached a quorum.
        _ => ClientOperationOutcome.Info,
    };
}
