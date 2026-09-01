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

    /// <summary>Close the fsync window again.</summary>
    FastDisk,
}
