using Kommander.Scheduling;
using Kommander.Time;

namespace Kommander.Consensus;

/// <summary>
/// The consensus nucleus of one partition: the handful of fields that every concern in the
/// partition state machine reads — term, role, commit/apply frontiers, quiescence, and the
/// promotion barrier.
///
/// <para><b>Why this type exists.</b> <see cref="RaftPartitionStateMachine"/> was a single class
/// holding ~50 mutable fields with no type boundary between concerns, so any method could reach
/// any field and the coupling was invisible. Splitting it required first naming the state that
/// genuinely IS shared: <see cref="NodeState"/> and <see cref="CurrentTerm"/> alone are read from
/// essentially every concern, so a collaborator that owned no shared state at all was not
/// achievable. This object is that shared surface, and it is deliberately the ONLY shared surface:
/// a collaborator takes this plus explicit dependencies, never a back-reference to the state
/// machine. Anything a collaborator needs beyond these fields must be declared, which is the whole
/// point — it makes coupling visible in the constructor instead of discoverable by reading 5,000
/// lines.</para>
///
/// <para><b>Concurrency.</b> Mutated only from the partition executor's single-writer thread and
/// holds no locks by design. The one exception is <see cref="NodeState"/>, which is published
/// through a volatile backing field because it is read off-thread (see below). Do not add
/// synchronization here: the single-owner guarantee is what makes the whole consensus path
/// lock-free, and a lock in this object would sit on every hot path in the partition.</para>
/// </summary>
internal sealed class RaftPartitionCoreState
{
    /// <summary>
    /// Volatile backing store for <see cref="NodeState"/>. The state machine itself only ever
    /// mutates this from the executor's single-writer thread, but the role is read off-thread by
    /// <c>RaftPartition.GetState</c> (the snapshot path that keeps hot <c>AmILeader</c> pollers off
    /// the executor queue), so every transition must be published with release semantics rather
    /// than left in a core-local write buffer.
    /// </summary>
    private volatile int nodeStateValue = (int)RaftNodeState.Follower;

    /// <summary>
    /// The raw role of this node. Kept as a property over a volatile field so that all the
    /// existing transition sites publish the new role to off-thread readers automatically — do not
    /// reintroduce a plain field here.
    /// <para>Any assignment to a non-Leader role also kills the published leadership lease
    /// (<see cref="LeadershipLease"/>) before the role is published, so every demotion path
    /// invalidates the off-thread confirmation fast path structurally — no per-site call needed.</para>
    /// </summary>
    public RaftNodeState NodeState
    {
        get => (RaftNodeState)nodeStateValue;
        set
        {
            if (value != RaftNodeState.Leader)
                leadershipLease = null;
            nodeStateValue = (int)value;
        }
    }

    /// <summary>Backing store for <see cref="CurrentTerm"/>. Plain (non-volatile) because only the
    /// executor's single-writer thread reads or writes the term; off-thread readers must never
    /// consume it directly (they read the lease, whose lifecycle the setter controls).</summary>
    private long currentTermValue;

    /// <summary>Current Raft term. Persisted through the WAL hard state; see
    /// <c>CompleteRestoreAsync</c> for why the log tail alone is not authoritative.
    /// <para>A property so every term change also kills the published leadership lease
    /// (<see cref="LeadershipLease"/>): a term change is exactly the event that makes a cached
    /// confirmation stale, and routing it through the setter covers every adoption path —
    /// including follower-side adoptions that never call <c>FailAllActiveProposalWaiters</c>.</para>
    /// </summary>
    public long CurrentTerm
    {
        get => currentTermValue;
        set
        {
            if (currentTermValue == value)
                return;
            leadershipLease = null;
            currentTermValue = value;
        }
    }

    /// <summary>
    /// The published leadership-confirmation lease, or <see langword="null"/> when none is live.
    /// Written only by the executor thread; read from arbitrary threads. Immutable snapshot behind
    /// a volatile reference so an off-thread reader never observes a torn (term, ticks) pair.
    /// Invalidated structurally by the <see cref="NodeState"/> and <see cref="CurrentTerm"/>
    /// setters, and explicitly by <c>ReadIndexCoordinator.FailAllWaiters</c> /
    /// <c>ResetForNewLeadership</c>. Freshness (heartbeat-interval window) is the reader's check.
    /// </summary>
    private volatile LeadershipLease? leadershipLease;

    /// <summary>Reads the current leadership lease snapshot (may be <see langword="null"/>).</summary>
    public LeadershipLease? LeadershipLease => leadershipLease;

    /// <summary>Publishes a fresh leadership lease. Executor thread only; called from the
    /// read-index quorum confirmation, which is the only event that proves leadership.</summary>
    public void PublishLeadershipLease(long term, long confirmedTicks) =>
        leadershipLease = new(term, confirmedTicks);

    /// <summary>Kills the published lease. Idempotent; safe to call from any demotion/reset path.</summary>
    public void InvalidateLeadershipLease() => leadershipLease = null;

    /// <summary>
    /// Set once the WAL restore has completed. Guards <c>CompleteRestoreAsync</c> against a second
    /// replay.
    /// </summary>
    public bool Restored;

    /// <summary>
    /// When <see langword="true"/> this partition is quiesced: no per-partition heartbeats are
    /// expected or sent.  Followers gate elections on SWIM node state instead of the heartbeat
    /// timer.  Only set when <see cref="RaftConfiguration.EnableQuiescence"/> is on.
    /// <para>Assign through <see cref="SetQuiesced"/> rather than directly, so the hot-set
    /// tracking stays consistent.</para>
    /// </summary>
    public bool Quiesced { get; private set; }

    /// <summary>
    /// Optional callback invoked whenever <see cref="Quiesced"/> transitions.
    /// <see langword="true"/> = just quiesced (leave hot set); <see langword="false"/> = just
    /// un-quiesced (re-enter hot set).  Fired under the single-owner guarantee so no extra
    /// locking is required in the callback implementation.  Set by <see cref="RaftPartition"/>
    /// via <c>SetOnQuiesceChanged</c>; <see langword="null"/> in unit tests that use stub hosts.
    /// </summary>
    public Action<bool>? OnQuiesceChanged;

    /// <summary>
    /// Assigns <paramref name="value"/> to <see cref="Quiesced"/> and notifies
    /// <see cref="OnQuiesceChanged"/> on an actual state change.  Must be called instead of
    /// directly assigning the property so the hot-set tracking stays consistent.
    /// </summary>
    public void SetQuiesced(bool value)
    {
        if (Quiesced == value)
            return;

        Quiesced = value;
        OnQuiesceChanged?.Invoke(value);
    }

    /// <summary>
    /// HLC timestamp of the last real proposal enqueued on this leader.
    /// Zero until the first proposal arrives after winning election.
    /// Used to determine when the partition has been idle long enough to quiesce:
    /// once <c>now - LastProposalAt &gt; QuiesceAfter</c> and no proposals are in flight,
    /// the leader sends a quiesce marker and stops heartbeating.
    /// </summary>
    public HLCTimestamp LastProposalAt;

    /// <summary>
    /// Monotonic-tick shadow of <see cref="LastProposalAt"/> used by the quiesce-after GATE, so an
    /// idle leader quiesces after a true local elapsed interval rather than one distorted by peer skew.
    /// 0 until the first proposal after winning election.
    /// </summary>
    public long LastProposalAtTicks;

    /// <summary>
    /// Highest log index the leader has durably committed (set by <c>CompleteLeaderCommit</c>).
    /// Compared against the per-peer commit frontiers in <c>SendHeartbeat</c> to decide whether
    /// a follower gap warrants backfill. Intentionally excludes in-flight proposed-but-uncommitted
    /// entries so healthy followers don't trigger spurious WAL reads under write load.
    /// Reset to -1 on every leader→follower transition.
    /// <para>Volatile-backed (like <see cref="NodeState"/>) because the leadership-lease fast path
    /// reads it off-thread; the executor thread stays the only writer.</para>
    /// </summary>
    public long LocalCommittedIndex
    {
        get => Volatile.Read(ref localCommittedIndexValue);
        set => Volatile.Write(ref localCommittedIndexValue, value);
    }

    private long localCommittedIndexValue = -1;

    /// <summary>
    /// The committed frontier captured at the moment this node became leader — the boundary between
    /// <em>restored</em> committed entries (present in the WAL at promotion) and entries committed
    /// <em>live</em> during this leadership term. The idle-tail backfill in <c>SendHeartbeat</c> only
    /// re-ships a sub-threshold follower gap when <see cref="LocalCommittedIndex"/> has advanced past
    /// this floor, i.e. a live commit exists that a healthy follower would already have received.
    /// This preserves the invariant that a leader does not push restored local state to followers
    /// until new entries are proposed, while still healing a genuinely-committed tail entry a follower
    /// missed once writes go quiet. Re-anchored at every promotion (both <c>BecomeLeader</c>
    /// paths) and only ever read while <c>NodeState == Leader</c>, so it needs no follower-side reset —
    /// a stale value from a prior term is always overwritten before the next leader can heartbeat.
    /// </summary>
    public long LiveCommitFloor = -1;

    /// <summary>
    /// Highest log index that has been delivered to the consumer via
    /// <see cref="IRaftPartitionHost.InvokeReplicationReceived"/> or
    /// <see cref="IRaftPartitionHost.InvokeSystemReplicationReceived"/>.
    /// Initialized to -1 (nothing applied yet).
    ///
    /// <para>Maintained on both the follower append path and the leader commit path, and
    /// <b>seeded on restore</b>: <c>CompleteRestoreAsync</c> sets it to the reconstructed
    /// commit frontier because the WAL restore already delivered every committed entry below that
    /// frontier to the consumer. Skipping this seed would make promotion re-deliver the retained log.</para>
    ///
    /// <para>On promotion, <c>DrainCommittedAppliesAsync</c> uses this as the start
    /// of a WAL range-read so that every committed entry between the current cursor
    /// and the commit frontier is delivered to the consumer before the partition is
    /// advertised as the serving leader — a no-op for entries already applied during restore.</para>
    ///
    /// <para>Volatile-backed (like <see cref="NodeState"/>) because the leadership-lease fast path
    /// reads it off-thread; the executor thread stays the only writer. The lease fast path must
    /// read <see cref="LocalCommittedIndex"/> BEFORE this — commit-index monotonicity is what makes
    /// that order the safe one.</para>
    /// </summary>
    public long LastAppliedIndex
    {
        get => Volatile.Read(ref lastAppliedIndexValue);
        set => Volatile.Write(ref lastAppliedIndexValue, value);
    }

    private long lastAppliedIndexValue = -1;

    /// <summary>
    /// Ticket of the in-flight promotion-barrier no-op, or <see cref="HLCTimestamp.Zero"/> when no
    /// barrier is pending. Armed by <c>BecomeLeaderAsync</c> when the election winner's WAL
    /// holds entries above the known commit frontier (inherited prior-term entries whose commit
    /// broadcast never reached this node). While armed, <c>NodeState == Leader</c> but
    /// <see cref="IRaftPartitionHost.Leader"/> stays unpublished, so <c>AmILeader</c> is false and
    /// the node does not serve; heartbeats still flow (they key off the role) so rival
    /// elections stay suppressed. <c>CompleteLeaderCommit</c> publishes leadership when the
    /// matching commit lands — its inherited-entry drain has by then applied every prior-term entry.
    /// Cleared on every leader→follower transition via <c>FailAllActiveProposalWaiters</c>.
    /// </summary>
    public HLCTimestamp LeadershipBarrierTicket = HLCTimestamp.Zero;

    /// <summary>Term the pending barrier was proposed in; the publish is fenced on it so a stale
    /// barrier completion from a superseded term can never publish leadership.</summary>
    public long LeadershipBarrierTerm = -1;

    /// <summary>
    /// HLC timestamp of the last accepted heartbeat (or equivalent leader contact). Stamped onto the
    /// wire for ordering; every elapsed-time GATE measures against
    /// <see cref="LastHeartbeatTicks"/> instead.
    /// </summary>
    public HLCTimestamp LastHeartbeat = HLCTimestamp.Zero;

    /// <summary>
    /// Monotonic shadow of <see cref="LastHeartbeat"/>: "how long since we last heard from a
    /// leader" on the follower side, and "when did we last beat" on the leader side. Both the
    /// follower election trigger and the leader heartbeat interval gate on this, which is why it
    /// lives here rather than with either one — a local quantity, deliberately not an HLC
    /// subtraction, which would inherit a remote peer's clock skew and could make a stale heartbeat
    /// look fresh. 0 means unset.
    /// </summary>
    public long LastHeartbeatTicks;

    /// <summary>HLC timestamp of the last vote this node granted.</summary>
    public HLCTimestamp LastVotation = HLCTimestamp.Zero;

    /// <summary>Monotonic shadow of <see cref="LastVotation"/>, driving the recent-vote cooldown
    /// that keeps a node that just granted a vote from immediately campaigning against it.</summary>
    public long LastVotationTicks;

    /// <summary>HLC timestamp when the current candidacy began; Zero when not campaigning.</summary>
    public HLCTimestamp VotingStartedAt = HLCTimestamp.Zero;

    /// <summary>Monotonic shadow of <see cref="VotingStartedAt"/>, bounding how long a candidacy
    /// may stall before the node gives up and retries.</summary>
    public long VotingStartedTicks;

    /// <summary>
    /// This node's current randomized election timeout. Re-drawn per election round so a symmetric
    /// split vote does not repeat forever; read by the follower election trigger, the recent-vote
    /// cooldown and the pre-vote leader-freshness gate.
    /// </summary>
    public TimeSpan ElectionTimeout;

    /// <summary>Monotonic timestamp when the barrier was armed; drives the
    /// <see cref="RaftConfiguration.LeadershipBarrierTimeout"/> revert in the leader tick.</summary>
    public long LeadershipBarrierArmedTicks;
}
