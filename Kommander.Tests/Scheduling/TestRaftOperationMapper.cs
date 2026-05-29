
using Kommander.Data;
using Kommander.Scheduling;

namespace Kommander.Tests.Scheduling;

/// <summary>
/// Unit tests for <see cref="RaftOperationMapper"/>.
///
/// These tests codify the mapping from <see cref="RaftRequestType"/> to
/// <see cref="RaftOperationKind"/> and <see cref="RaftOperationPriority"/> so that
/// future refactoring tasks can verify the scheduling classification remains correct.
/// </summary>
public class TestRaftOperationMapper
{
    // ── GetKind: control group ────────────────────────────────────────────────

    [Theory]
    [InlineData(RaftRequestType.CheckLeader)]
    [InlineData(RaftRequestType.ReceiveHandshake)]
    [InlineData(RaftRequestType.RequestVote)]
    [InlineData(RaftRequestType.ReceiveVote)]
    public void GetKind_ControlRequests_ReturnsControl(RaftRequestType type)
    {
        Assert.Equal(RaftOperationKind.Control, RaftOperationMapper.GetKind(type));
    }

    // ── GetKind: replication group ────────────────────────────────────────────

    [Theory]
    [InlineData(RaftRequestType.AppendLogs)]
    [InlineData(RaftRequestType.CompleteAppendLogs)]
    [InlineData(RaftRequestType.WriteOperationCompleted)]
    public void GetKind_ReplicationRequests_ReturnsReplication(RaftRequestType type)
    {
        Assert.Equal(RaftOperationKind.Replication, RaftOperationMapper.GetKind(type));
    }

    // ── GetKind: client group ─────────────────────────────────────────────────

    [Theory]
    [InlineData(RaftRequestType.ReplicateLogs)]
    [InlineData(RaftRequestType.ReplicateCheckpoint)]
    [InlineData(RaftRequestType.CommitLogs)]
    [InlineData(RaftRequestType.RollbackLogs)]
    [InlineData(RaftRequestType.GetNodeState)]
    [InlineData(RaftRequestType.GetTicketState)]
    public void GetKind_ClientRequests_ReturnsClient(RaftRequestType type)
    {
        Assert.Equal(RaftOperationKind.Client, RaftOperationMapper.GetKind(type));
    }

    // ── GetKind: unknown value ────────────────────────────────────────────────

    [Fact]
    public void GetKind_UnknownValue_Throws()
    {
        RaftRequestType unknown = (RaftRequestType)999;
        Assert.Throws<ArgumentOutOfRangeException>(() => RaftOperationMapper.GetKind(unknown));
    }

    // ── GetPriority: weight values ────────────────────────────────────────────

    [Theory]
    [InlineData(RaftRequestType.CheckLeader, RaftOperationPriority.Control)]
    [InlineData(RaftRequestType.ReceiveHandshake, RaftOperationPriority.Control)]
    [InlineData(RaftRequestType.RequestVote, RaftOperationPriority.Control)]
    [InlineData(RaftRequestType.ReceiveVote, RaftOperationPriority.Control)]
    [InlineData(RaftRequestType.AppendLogs, RaftOperationPriority.Replication)]
    [InlineData(RaftRequestType.CompleteAppendLogs, RaftOperationPriority.Replication)]
    [InlineData(RaftRequestType.WriteOperationCompleted, RaftOperationPriority.Replication)]
    [InlineData(RaftRequestType.ReplicateLogs, RaftOperationPriority.Client)]
    [InlineData(RaftRequestType.ReplicateCheckpoint, RaftOperationPriority.Client)]
    [InlineData(RaftRequestType.CommitLogs, RaftOperationPriority.Client)]
    [InlineData(RaftRequestType.RollbackLogs, RaftOperationPriority.Client)]
    [InlineData(RaftRequestType.GetNodeState, RaftOperationPriority.Client)]
    [InlineData(RaftRequestType.GetTicketState, RaftOperationPriority.Client)]
    public void GetPriority_KnownType_ReturnsExpectedWeight(RaftRequestType type, RaftOperationPriority expected)
    {
        Assert.Equal(expected, RaftOperationMapper.GetPriority(type));
    }

    // ── Priority weight ordering invariant ───────────────────────────────────

    [Fact]
    public void PriorityWeights_ControlHighestClientLowest()
    {
        int control = (int)RaftOperationPriority.Control;
        int replication = (int)RaftOperationPriority.Replication;
        int client = (int)RaftOperationPriority.Client;
        int maintenance = (int)RaftOperationPriority.Maintenance;

        Assert.True(control > replication, "Control must outweigh Replication");
        Assert.True(replication > client, "Replication must outweigh Client");
        Assert.True(client > maintenance, "Client must outweigh Maintenance");
    }

    // ── CreateOperation: field propagation ───────────────────────────────────

    [Fact]
    public void CreateOperation_CheckLeader_PopulatesAllFields()
    {
        RaftRequest request = new(RaftRequestType.CheckLeader);
        RaftPartitionOperation op = RaftOperationMapper.CreateOperation(partitionId: 7, request, sequenceNumber: 42);

        Assert.Equal(7, op.PartitionId);
        Assert.Same(request, op.Request);
        Assert.Equal(RaftOperationKind.Control, op.Kind);
        Assert.Equal(RaftOperationPriority.Control, op.Priority);
        Assert.Equal(42L, op.SequenceNumber);
    }

    [Fact]
    public void CreateOperation_ReplicateLogs_IsClientKind()
    {
        RaftRequest request = new(RaftRequestType.ReplicateLogs, new List<RaftLog>(), autoCommit: false);
        RaftPartitionOperation op = RaftOperationMapper.CreateOperation(partitionId: 3, request, sequenceNumber: 1);

        Assert.Equal(RaftOperationKind.Client, op.Kind);
        Assert.Equal(RaftOperationPriority.Client, op.Priority);
    }

    [Fact]
    public void CreateOperation_WriteOperationCompleted_IsReplicationKind()
    {
        RaftRequest request = new(RaftRequestType.WriteOperationCompleted);
        RaftPartitionOperation op = RaftOperationMapper.CreateOperation(partitionId: 1, request, sequenceNumber: 100);

        Assert.Equal(RaftOperationKind.Replication, op.Kind);
        Assert.Equal(RaftOperationPriority.Replication, op.Priority);
    }

    // ── All defined RaftRequestType values are covered ───────────────────────

    [Fact]
    public void GetKind_AllDefinedEnumValues_DoNotThrow()
    {
        foreach (RaftRequestType type in Enum.GetValues<RaftRequestType>())
        {
            RaftOperationKind kind = RaftOperationMapper.GetKind(type);
            Assert.True(Enum.IsDefined(kind));
        }
    }

    // ── Legacy actor bridge (temporary extraction aid) ────────────────────────

    [Theory]
    [InlineData(RaftRequestType.CheckLeader, RaftStatePriority.High)]
    [InlineData(RaftRequestType.ReceiveHandshake, RaftStatePriority.High)]
    [InlineData(RaftRequestType.RequestVote, RaftStatePriority.High)]
    [InlineData(RaftRequestType.ReceiveVote, RaftStatePriority.High)]
    [InlineData(RaftRequestType.GetNodeState, RaftStatePriority.Low)]
    [InlineData(RaftRequestType.GetTicketState, RaftStatePriority.Low)]
    [InlineData(RaftRequestType.AppendLogs, RaftStatePriority.Mid)]
    [InlineData(RaftRequestType.CompleteAppendLogs, RaftStatePriority.Mid)]
    [InlineData(RaftRequestType.ReplicateLogs, RaftStatePriority.Mid)]
    [InlineData(RaftRequestType.ReplicateCheckpoint, RaftStatePriority.Mid)]
    [InlineData(RaftRequestType.CommitLogs, RaftStatePriority.Mid)]
    [InlineData(RaftRequestType.RollbackLogs, RaftStatePriority.Mid)]
    [InlineData(RaftRequestType.WriteOperationCompleted, RaftStatePriority.Mid)]
    public void GetLegacyStatePriority_KnownType_ReturnsExpectedBucket(RaftRequestType type, RaftStatePriority expected)
    {
        Assert.Equal(expected, RaftOperationMapper.GetLegacyStatePriority(type));
    }

    [Fact]
    public void UsesLegacySinglePrioritySlot_OnlyCheckLeader()
    {
        Assert.True(RaftOperationMapper.UsesLegacySinglePrioritySlot(RaftRequestType.CheckLeader));

        foreach (RaftRequestType type in Enum.GetValues<RaftRequestType>())
        {
            if (type == RaftRequestType.CheckLeader)
                continue;

            Assert.False(RaftOperationMapper.UsesLegacySinglePrioritySlot(type));
        }
    }

    [Fact]
    public void GetLegacyStatePriority_AllDefinedEnumValues_DoNotThrow()
    {
        foreach (RaftRequestType type in Enum.GetValues<RaftRequestType>())
            Assert.True(Enum.IsDefined(RaftOperationMapper.GetLegacyStatePriority(type)));
    }

    // ── RaftOperationCompletion carries fencing metadata ─────────────────────

    [Fact]
    public void OperationCompletion_RecordEquality_ByValue()
    {
        RaftOperationCompletion a = new(PartitionId: 2, OperationId: 55, Term: 3, LogIndexStart: 10, LogIndexEnd: 20, OperationKind: RaftOperationKind.Replication, Status: RaftOperationStatus.Success);
        RaftOperationCompletion b = new(PartitionId: 2, OperationId: 55, Term: 3, LogIndexStart: 10, LogIndexEnd: 20, OperationKind: RaftOperationKind.Replication, Status: RaftOperationStatus.Success);

        Assert.Equal(a, b);
    }

    [Fact]
    public void OperationCompletion_DifferentTerm_NotEqual()
    {
        RaftOperationCompletion a = new(PartitionId: 1, OperationId: 1, Term: 2, LogIndexStart: 0, LogIndexEnd: 0, OperationKind: RaftOperationKind.Replication, Status: RaftOperationStatus.Success);
        RaftOperationCompletion b = a with { Term = 3 };

        Assert.NotEqual(a, b);
    }
}
