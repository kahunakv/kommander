namespace Kommander.Tests.Simulation.Scenarios.Random;

/// <summary>
/// The vocabulary a random run draws from.
///
/// <para><b>Why an enumeration and not a delegate list.</b> Every action a run took must be
/// printable, comparable, and writable to a file. A run that failed is only useful if a reader can
/// see the sequence that produced it, and a closed vocabulary is what makes the sequence short
/// enough to read.</para>
///
/// <para>The four families are deliberate. Client operations produce the promises the history
/// checker measures. Network and storage faults damage what a node hears and what it holds, and
/// those are different failures. Lifecycle faults damage the node itself. Every family is paired
/// with the action that ends it, because a fault that never ends turns the rest of the run into a
/// study of one wedged cluster.</para>
/// </summary>
public enum RandomScenarioActionKind
{
    /// <summary>Let simulated time pass with the network working.</summary>
    Idle,

    /// <summary>
    /// Start a client append at the leader, cut the leader off while the call is still in flight,
    /// and take whatever answer the client eventually gets.
    ///
    /// <para>The only action that puts a client operation and a leadership change in the same
    /// moment. Everything else in this vocabulary happens between operations, and a client that is
    /// never mid-call cannot be told the wrong thing about one: the answers that matter are decided
    /// exactly when a proposal outlives the leader that accepted it. A whole family of defects — an
    /// appended entry reported as refused and then committed by the next leader — is unreachable
    /// without it.</para>
    /// </summary>
    AppendAcrossOutage,

    /// <summary>
    /// Start a client append at the leader, drop every acknowledgement coming back to it for a short
    /// window, then let the replies through again.
    ///
    /// <para>The followers receive the entries; only the replies are lost. The leader keeps its
    /// leadership and believes nobody took its proposal, so the proposal is resolved by a retry or
    /// by nothing at all — which is exactly the state a defect class lives in, where an unresolved
    /// proposal wedges a partition one index below its own.</para>
    ///
    /// <para>The acknowledgement direction alone, and that is the whole design. Cutting both
    /// directions drops the entries too, and ordinary replication then sends them again, so the
    /// leader never has to remember anything and the state is never reached. Measured: the
    /// both-directions version found nothing.</para>
    ///
    /// <para><b>Why this is not the same as the outage.</b> That action waits for a replacement
    /// leader, so the original never has to finish anything. This one gives the leader its cluster
    /// back and requires it to. The distinction is the whole point: one tests what the next leader
    /// inherits, the other tests what this leader still owes.</para>
    ///
    /// <para>The window is short on purpose. A leader with no quorum waits ten <b>real</b> seconds
    /// inside its quorum wait before reporting a timeout, so a long loss stops exploring and starts
    /// paying. Reconnecting within a couple of steps keeps the cost in simulated time.</para>
    /// </summary>
    AppendAcrossQuorumLoss,

    /// <summary>
    /// Cut the leader off in both directions until the rest of the cluster elects another one, then
    /// let it back.
    ///
    /// <para>This is how a run reaches an election deliberately, and it is the action that matters
    /// most. Several of this project's defects need a leader elected <b>after</b> the damage was
    /// done: a leader that committed the entries itself repairs a follower through a path a
    /// post-outage leader does not have, so a search that never changes leadership under fault
    /// cannot reach them.</para>
    ///
    /// <para>The leader alone, rather than the whole wire. Holding every link produces the same
    /// election and an unbounded backlog with it: nothing can complete, the senders keep sending,
    /// and releasing the pile costs minutes of real time per run. Measured at six minutes for one
    /// twelve-action plan before this became a targeted cut.</para>
    /// </summary>
    LeaderOutage,

    /// <summary>Append through the node that believes it leads.</summary>
    AppendAtLeader,

    /// <summary>
    /// Append through a node that does not lead. The answer must be a refusal, and a refused append
    /// must not appear in the log — which is a rule the checker owns and this action feeds.
    /// </summary>
    AppendAtFollower,

    /// <summary>Kill a node outright. It loses its fsync window and every belief it held.</summary>
    CrashNode,

    /// <summary>Bring a crashed node back over the store the crash left behind.</summary>
    RestartNode,

    /// <summary>Freeze a node the way <c>SIGSTOP</c> does: its traffic is stored, not lost.</summary>
    PauseNode,

    /// <summary>Thaw a paused node. Its whole backlog arrives at once.</summary>
    ResumeNode,

    /// <summary>Drop everything sent one way along one link.</summary>
    BlockLink,

    /// <summary>Restore a blocked link.</summary>
    UnblockLink,

    /// <summary>
    /// Deliver every message on one link several times. Raft claims its remote calls are
    /// idempotent, and a claim of idempotence is worth testing.
    ///
    /// <para>A copy count of one restores ordinary delivery, so this action also ends the fault it
    /// starts. That is why there is no separate name for the repair.</para>
    /// </summary>
    DuplicateLink,

    /// <summary>Refuse every write to one node's data partition, the way a full disk does.</summary>
    StarveDisk,

    /// <summary>Free a starved disk.</summary>
    FreeDisk,

    /// <summary>Fail a bounded number of writes and then recover on its own.</summary>
    FailWrites,

    /// <summary>Widen one node's fsync window, so a later crash takes more with it.</summary>
    SlowDisk,

    /// <summary>
    /// Ask the leader to write a checkpoint, which lets every node compact the log below it.
    ///
    /// <para>Without this a generated run never compacts at all, and the two rules about compaction
    /// — the floor rule and the committed-prefix rule's tolerance for a compacted head — sit there
    /// describing something that never happens. A rule nothing exercises is decoration.</para>
    ///
    /// <para>Compaction is where a follower can be left behind for good: once the leader has thrown
    /// away what a lagging replica still needs, the only way back is a snapshot. That is a whole
    /// family of failures the harness could not reach.</para>
    /// </summary>
    Checkpoint,

    /// <summary>
    /// Pin one node's log against compaction, whatever checkpoint it is told about.
    ///
    /// <para>The interesting state is a hold and a floor that disagree — a node keeping entries the
    /// cluster believes are gone. It impairs nothing on its own, so it does not spend the quorum
    /// budget.</para>
    /// </summary>
    HoldRetention,

    /// <summary>Let a pinned node compact again.</summary>
    ReleaseRetention,

    /// <summary>Close the fsync window again.</summary>
    FastDisk,
}
