
namespace Kommander.Data;

public enum RaftRequestType
{
    CheckLeader,
    ForceLeaderForTesting,

    /// <summary>
    /// Test-only: injects quiesced state directly into the state machine via
    /// <see cref="RaftPartitionStateMachine.SetQuiescedForTesting"/>, triggering the
    /// quiesce callback so the hot-set bookkeeping in <see cref="RaftManager"/> is
    /// updated under the single-owner guarantee.
    /// The <see cref="RaftRequest.Quiesce"/> field carries the target value.
    /// </summary>
    SetQuiescedForTesting,
    StepDown,
    TransferLeadership,
    SuspendHeartbeats,
    ResumeHeartbeats,
    ReceiveStepDownNotice,
    ReceiveTransferLeadership,
    ReceiveHandshake,
    RequestVote,
    ReceiveVote,
    AppendLogs,
    CompleteAppendLogs,
    ReplicateLogs,
    ReplicateCheckpoint,
    CommitLogs,
    RollbackLogs,
    GetNodeState,
    GetTicketState,
    WriteOperationCompleted,
    DrainBarrier,

    /// <summary>
    /// Posted by the async restore task back to the executor once the WAL logs
    /// have been loaded from storage.  The executor processes this on its own
    /// worker thread so that log replay callbacks run under the single-owner
    /// guarantee, satisfying correctness rule 1.
    /// </summary>
    RestoreLogsLoaded,

    /// <summary>
    /// Returns the last commit index reported by a specific follower endpoint via
    /// <c>CompleteAppendLogs</c> acknowledgements.  Used by the promotion driver to
    /// measure learner lag without reading WAL storage.
    /// </summary>
    GetFollowerCommittedIndex,

    /// <summary>
    /// Discards all per-follower replication progress the leader has recorded for the endpoint
    /// carried on <see cref="RaftRequest.Endpoint"/>. Posted when the committed roster (re)adds a
    /// member: any retained progress predates the (re)admission and may describe state the member
    /// no longer holds — a node evicted during a restart typically returns with a reset log, and a
    /// leader that still "remembers" it as caught-up would neither un-quiesce nor backfill it,
    /// starving the member indefinitely. Clearing the entries makes the member read as
    /// "no recorded progress = lagging", which re-arms heartbeats and anchors backfill from the
    /// follower's actually-reported frontier.
    /// </summary>
    ResetFollowerProgress,

    /// <summary>
    /// Returns the event-driven completion task for an active proposal ticket.
    /// The returned <see cref="System.Threading.Tasks.Task{T}"/> completes when the
    /// proposal reaches a terminal state (committed, rolled-back, or invalidated by
    /// leader loss), eliminating the need to poll <see cref="GetTicketState"/>.
    /// </summary>
    GetTicketWaiterTask,

    /// <summary>
    /// Posted by the background snapshot-transfer task back to the partition executor once the
    /// target follower has confirmed the snapshot was installed.  The executor processes this on
    /// its worker thread to safely advance <c>lastCommitIndexes[endpoint]</c> to the snapshot
    /// index, allowing normal backfill to resume from the next entry.
    /// </summary>
    SnapshotInstalled,

    /// <summary>
    /// Follower-side snapshot install, asked from the transport thread once the final chunk of a
    /// snapshot session has been received and staged. The executor runs the Raft "Rule 7" validation
    /// (leader-term check / durable step-down), the application import, the durable WAL boundary install,
    /// and the cursor reconstruction on the partition's single-writer path via
    /// <see cref="RaftPartitionStateMachine.InstallSnapshotAsync"/>. The staged snapshot and its metadata
    /// are carried on <see cref="RaftRequest.SnapshotInstall"/>. Distinct from <see cref="SnapshotInstalled"/>,
    /// which is the leader/sender-side ACK that a follower finished installing.
    /// </summary>
    InstallSnapshot,

    /// <summary>
    /// Test-only: snapshots this partition's consensus state (role, term, leader, commit/applied/max-WAL
    /// indexes, quiesced, member role) into an immutable <see cref="RaftPartitionView"/> on the executor
    /// thread, so the chaos harness never reads mutable state-machine fields from a polling thread.
    /// </summary>
    GetPartitionView,

    /// <summary>
    /// Read-index leadership confirmation (Raft §6.4). Asked on behalf of a consumer that wants to
    /// serve a local read as authoritative: the state machine captures the current commit frontier,
    /// proves it is still the leader with a same-term quorum ack round, and completes the reply once
    /// the applied frontier covers the captured commit index. Replies non-success when the node is
    /// not the (published) leader or quorum cannot confirm within
    /// <see cref="RaftConfiguration.LeadershipConfirmationTimeout"/> — a minority-partitioned leader
    /// must fail reads the same way it fails writes instead of serving stale state.
    /// Concurrent requests coalesce into a single in-flight ack round.
    /// </summary>
    ConfirmLeadership,

    /// <summary>
    /// Follower half of the non-leader read-index primitive
    /// (<c>IRaft.ConfirmLocalApplicationAsync</c>): waits until this node's applied frontier
    /// covers the leader-confirmed commit index carried on <see cref="RaftRequest.CommitIndex"/>.
    /// The safety proof lives on the leader side (the quorum ack round that produced the index) —
    /// this operation only supplies the bounded local wait, so it is valid in any node state.
    /// Replies non-success on expiry (<c>RaftConfiguration.LeadershipConfirmationTimeout</c>) or
    /// when a leadership transition fails all read-index waiters — callers must treat that as
    /// "not confirmed" and skip their destructive action.
    /// </summary>
    WaitLocalApplication,
}
