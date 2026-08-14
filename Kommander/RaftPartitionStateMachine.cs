using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using Kommander.Communication.Grpc;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Gossip;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.Support.Collections;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.Data;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Plain Raft partition state machine extracted from <see cref="RaftStateActor"/>.
/// Has no Nixie dependency and can be instantiated directly in tests.
/// </summary>
public sealed class RaftPartitionStateMachine
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly IRaftOperationReplySink replySink;
    private readonly ILogger<IRaft> logger;

    private readonly Dictionary<long, HashSet<string>> votes = [];

    /// <summary>
    /// Per-peer commit frontier as the peer last reported it about itself: the highest log id the
    /// follower's gap-aware WAL frontier had committed when it acknowledged an AppendLogs.
    /// This is what <see cref="SendHeartbeat"/> computes <c>followerGap</c> from, so every backfill
    /// trigger derives from it. Invariants: written only from a <see cref="RaftOperationStatus.Success"/>
    /// ack's self-report (or a confirmed snapshot install boundary) — never from a rejection ack,
    /// whose committedIndex field carries the follower's raw max log id, an over-estimate of the
    /// frontier whenever the log has an uncommitted or non-contiguous tail — and last-writer-wins,
    /// so a genuinely regressed follower (crash-restart) can lower it and become visible as behind
    /// again. Violating either invariant pins an over-estimate no later ack can correct, and the
    /// peer is then never backfilled however far behind it really is (Jepsen stranded-replica
    /// findings: commit frontier stalls while the log keeps growing and the leader sees no gap).
    /// </summary>
    private readonly Dictionary<string, long> lastCommitIndexes = [];
    private readonly Dictionary<string, long> startCommitIndexes = [];

    /// <summary>
    /// Per-follower replication cursor: the index of the next log entry to send to each peer.
    /// Seeded to <c>leaderMaxLog + 1</c> on election win (optimistic: assume peer is in sync).
    /// Backtracked on <see cref="RaftOperationStatus.LogMismatch"/> and advanced on
    /// <see cref="RaftOperationStatus.Success"/> replies.  Only meaningful while this node is
    /// Leader; cleared on every leader→follower transition so stale progress never leaks across terms.
    /// </summary>
    private readonly Dictionary<string, long> nextIndex = [];

    /// <summary>
    /// Per-follower highest log index known to be replicated on that peer.
    /// Zero until the peer confirms receipt of at least one entry.  Advanced in lock-step with
    /// <see cref="nextIndex"/> on a success reply; used to detect full catch-up
    /// (<c>matchIndex[peer] == leaderMaxLog</c>).
    /// </summary>
    private readonly Dictionary<string, long> matchIndex = [];
    /// <summary>
    /// Per-follower backfill cooldown: the monotonic tick before which this leader will not
    /// send another entry-carrying batch to that peer, set when the peer reports
    /// <see cref="RaftOperationStatus.FollowerWalSaturated"/>.
    /// </summary>
    /// <remarks>
    /// Deliberately *not* cleared on a leader transition, unlike <see cref="nextIndex"/> and
    /// <see cref="matchIndex"/>. Those are Raft state and a stale value is a correctness
    /// hazard; this is a transient throttle whose entries are absolute deadlines, so a stale
    /// one expires on its own within the backoff window and can at worst delay one batch. The
    /// alternative — clearing it at all thirteen sites that reset peer progress — buys nothing
    /// and is one forgotten site away from a bug.
    /// </remarks>
    private readonly Dictionary<string, long> backfillPausedUntilTicks = [];

    private readonly Dictionary<long, string> expectedLeaders = [];
    private readonly Dictionary<HLCTimestamp, RaftProposalQuorum> activeProposals = [];

    // Reusable scratch buffer for PruneSettledProposals so the periodic drain does not allocate a
    // collection every sweep. Only ever touched on the executor thread (single-threaded per partition),
    // so it needs no synchronization; always cleared before use.
    private readonly List<HLCTimestamp> settledProposalScratch = [];

    // WAL-saturation log throttle. A saturated partition rejects every inbound append, so the
    // condition is worth one line a second carrying a count, not one line per rejection: the log
    // is I/O contending with the very WAL writes whose slowness caused the saturation, so logging
    // each occurrence makes the condition it reports worse. 0 means "never logged" (mirrors the
    // Stopwatch-tick convention used elsewhere in this class). Only touched on the executor thread
    // (single-threaded per partition), so neither field needs synchronization.
    private long lastWalSaturatedLogTicks;
    private int suppressedWalSaturatedLogs;

    // Same throttle on the leader's side of the same conversation. A saturated follower rejects
    // every batch it is sent, and the leader logged one warning per rejection: 15,484 in a run,
    // 2,365 within a single second. Keyed on the status so a *different* failure appearing during
    // a saturation storm is still reported at once rather than swallowed by the window. Only
    // touched on the executor thread, as above.
    private RaftOperationStatus? lastLoggedAckStatus;
    private long lastFailedAckLogTicks;
    private int suppressedFailedAckLogs;

    // Diagnostic throttle for the backfill-decision probe. It fires on a hot path (once per peer
    // per heartbeat round), so it collapses to one line a second. Executor thread only, as with
    // the throttles above.
    private long lastBackfillTraceTicks;
    private int suppressedBackfillTraces;

    private readonly Dictionary<long, Scheduling.RaftPendingWalOperation> pendingWalOperations = [];

    // Per-instance pool for the pending-WAL-op metadata objects. Rented on insert, returned once the
    // completion has drained the entry. Safe without synchronization because the state machine runs
    // single-threaded on its partition executor; bounded so a burst of in-flight ops cannot retain an
    // unbounded number of pooled objects. A benchmark chose pooling over a struct value type, which
    // regressed by enlarging every dictionary entry (see RaftPendingWalOperation remarks).
    private readonly Stack<Scheduling.RaftPendingWalOperation> _pendingWalOpPool = new();
    private const int MaxPendingWalOpPool = 256;

    private Scheduling.RaftPendingWalOperation RentPendingWalOp() =>
        _pendingWalOpPool.Count > 0 ? _pendingWalOpPool.Pop() : new();

    private void ReturnPendingWalOp(Scheduling.RaftPendingWalOperation op)
    {
        op.Reset();
        if (_pendingWalOpPool.Count < MaxPendingWalOpPool)
            _pendingWalOpPool.Push(op);
    }

    /// <summary>
    /// Encapsulates in-flight snapshot-send guard, chunked send loop, and
    /// install-complete callback. Initialized in constructor after <c>postToExecutor</c>
    /// is wired so the sender can always read the current value via the closure.
    /// </summary>
    private readonly SnapshotSender snapshotSender;

    /// <summary>
    /// Posts a <see cref="RaftRequest"/> back to the partition executor from a background thread
    /// so completions such as <see cref="RaftRequestType.SnapshotInstalled"/> can update state
    /// under the single-owner guarantee without introducing re-entrancy.
    /// Set by <see cref="RaftPartition"/> at construction time; <see langword="null"/> in unit tests.
    /// </summary>
    private Action<RaftRequest>? postToExecutor;

    private readonly Random random;

    /// <summary>
    /// Volatile backing store for <see cref="nodeState"/>. The state machine itself only ever
    /// mutates this from the executor's single-writer thread, but <see cref="NodeState"/> is read
    /// off-thread by <c>RaftPartition.GetState</c> (the snapshot path that keeps hot
    /// <c>AmILeader</c> pollers off the executor queue), so every transition must be published
    /// with release semantics rather than left in a core-local write buffer.
    /// </summary>
    private volatile int nodeStateValue = (int)RaftNodeState.Follower;

    /// <summary>
    /// The raw role of this node. Kept as a property over a volatile field so that all the
    /// existing <c>nodeState = ...</c> transition sites publish the new role to off-thread
    /// readers automatically — do not reintroduce a plain field here.
    /// </summary>
    private RaftNodeState nodeState
    {
        get => (RaftNodeState)nodeStateValue;
        set => nodeStateValue = (int)value;
    }

    private long currentTerm;

    /// <summary>
    /// Gates whether an incoming <c>Vote(PreVote=true)</c> reply is tallied as a pre-grant.
    /// Pre-vote-only bookkeeping: it is never persisted and answering a pre-vote never mutates it.
    /// </summary>
    private RaftElectionPhase electionPhase = RaftElectionPhase.None;

    /// <summary>
    /// The hypothetical <c>currentTerm + 1</c> the currently-open pre-vote round is for.
    /// <c>-1</c> when no round is open. Pre-vote-only and side-effect-free: the real
    /// <see cref="currentTerm"/> is only bumped once a pre-vote quorum promotes to a real election.
    /// </summary>
    private long preVoteTerm = -1;

    /// <summary>
    /// Endpoints (including self) that pre-granted for <see cref="preVoteTerm"/>.
    /// Pre-vote-only and side-effect-free; separate from the real-election <see cref="votes"/> tally.
    /// </summary>
    private readonly HashSet<string> preVotes = [];
    private HLCTimestamp lastHeartbeat = HLCTimestamp.Zero;
    private HLCTimestamp lastVotation = HLCTimestamp.Zero;
    private HLCTimestamp votingStartedAt = HLCTimestamp.Zero;

    // B3: monotonic local-clock shadows of the HLC duration anchors above. Every elapsed-time GATE
    // (follower election timeout, leader heartbeat interval, voting timeout, quiesce-after, votation
    // back-off, the pre-vote "is our leader still fresh" check) measures against these ticks instead of
    // subtracting HLC timestamps — HLC subtraction is frozen by a remote peer's clock skew and stalls
    // elections. The HLC fields are retained ONLY where a timestamp is stamped onto the wire / WAL for
    // ordering. A value of 0 means "unset" (mirrors HLCTimestamp.Zero); Stopwatch.GetTimestamp never
    // returns 0 in practice, so the sentinel is unambiguous.
    private long lastHeartbeatTicks;
    private long lastVotationTicks;
    private long votingStartedTicks;
    private TimeSpan electionTimeout;
    private bool heartbeatsSuspendedForTesting;
    private bool restored;

    /// <summary>
    /// When <see langword="true"/> this partition is quiesced: no per-partition heartbeats are
    /// expected or sent.  Followers gate elections on SWIM node state instead of the heartbeat
    /// timer.  Only set when <see cref="RaftConfiguration.EnableQuiescence"/> is on.
    /// </summary>
    private bool quiesced;

    /// <summary>
    /// Optional callback invoked whenever <see cref="quiesced"/> transitions.
    /// <see langword="true"/> = just quiesced (leave hot set); <see langword="false"/> = just
    /// un-quiesced (re-enter hot set).  Fired under the single-owner guarantee so no extra
    /// locking is required in the callback implementation.  Set by <see cref="RaftPartition"/>
    /// via <see cref="SetOnQuiesceChanged"/>; <see langword="null"/> in unit tests that use stub hosts.
    /// </summary>
    private Action<bool>? _onQuiesceChanged;

    /// <summary>
    /// HLC timestamp of the last real proposal enqueued on this leader.
    /// Zero until the first proposal arrives after winning election.
    /// Used to determine when the partition has been idle long enough to quiesce:
    /// once <c>now - lastProposalAt &gt; QuiesceAfter</c> and no proposals are in flight,
    /// the leader sends a quiesce marker and stops heartbeating.
    /// </summary>
    private HLCTimestamp lastProposalAt;

    /// <summary>
    /// B3: monotonic-tick shadow of <see cref="lastProposalAt"/> used by the quiesce-after GATE, so an
    /// idle leader quiesces after a true local elapsed interval rather than one distorted by peer skew.
    /// 0 until the first proposal after winning election.
    /// </summary>
    private long lastProposalAtTicks;


    /// <summary>
    /// Highest log index the leader has durably committed (set by <see cref="CompleteLeaderCommit"/>).
    /// Compared against <see cref="lastCommitIndexes"/> in <c>SendHeartbeat</c> to decide whether
    /// a follower gap warrants backfill. Intentionally excludes in-flight proposed-but-uncommitted
    /// entries so healthy followers don't trigger spurious WAL reads under write load.
    /// Reset to -1 on every leader→follower transition.
    /// </summary>
    private long localCommittedIndex = -1;

    /// <summary>
    /// The committed frontier captured at the moment this node became leader — the boundary between
    /// <em>restored</em> committed entries (present in the WAL at promotion) and entries committed
    /// <em>live</em> during this leadership term. The idle-tail backfill in <c>SendHeartbeat</c> only
    /// re-ships a sub-threshold follower gap when <see cref="localCommittedIndex"/> has advanced past
    /// this floor, i.e. a live commit exists that a healthy follower would already have received.
    /// This preserves the invariant that a leader does not push restored local state to followers
    /// until new entries are proposed, while still healing a genuinely-committed tail entry a follower
    /// missed once writes go quiet. Re-anchored at every promotion (both <see cref="BecomeLeader"/>
    /// paths) and only ever read while <c>nodeState == Leader</c>, so it needs no follower-side reset —
    /// a stale value from a prior term is always overwritten before the next leader can heartbeat.
    /// </summary>
    private long liveCommitFloor = -1;

    /// <summary>
    /// Per-peer note of a detected commit-frontier regression (crash-restart signature): the endpoint
    /// maps to the committed frontier the peer reported below its recorded <see cref="matchIndex"/>.
    ///
    /// <para>Written by <see cref="CompleteAppendLogsAsync"/> on the hot ack path (detection only, cheap)
    /// and consumed by <see cref="SendHeartbeat"/> once per interval, which performs the actual
    /// (WAL-read + AppendLogs) re-supply anchored at the recorded frontier and then clears the entry.
    /// The split is deliberate: doing the re-supply inline on every ack livelocked the cluster under load
    /// (an anchored re-ship fighting an in-flight catch-up starved the executor and stalled elections).
    /// The ack path also CLEARS the note when a peer reports normal progress, so a transient reordered
    /// ack that momentarily looked like a regression self-heals before the next heartbeat acts.</para>
    /// </summary>
    private readonly Dictionary<string, long> regressedFrontiers = [];

    /// <summary>
    /// Highest log index that has been delivered to the consumer via
    /// <see cref="IRaftPartitionHost.InvokeReplicationReceived"/> or
    /// <see cref="IRaftPartitionHost.InvokeSystemReplicationReceived"/>.
    /// Initialized to -1 (nothing applied yet).
    ///
    /// <para>Maintained on both the follower append path and the leader commit path, and
    /// <b>seeded on restore</b>: <see cref="CompleteRestoreAsync"/> sets it to the reconstructed
    /// commit frontier because the WAL restore already delivered every committed entry below that
    /// frontier to the consumer. Skipping this seed would make promotion re-deliver the retained log.</para>
    ///
    /// <para>On promotion, <see cref="DrainCommittedAppliesAsync"/> uses this as the start
    /// of a WAL range-read so that every committed entry between the current cursor
    /// and the commit frontier is delivered to the consumer before the partition is
    /// advertised as the serving leader — a no-op for entries already applied during restore.</para>
    /// </summary>
    private long lastAppliedIndex = -1;

    /// <summary>
    /// Leader batches whose WAL completion arrived while an earlier current-term proposal was
    /// still unresolved below them, keyed by the batch's lowest log id. With pipelined proposals,
    /// quorum acks complete in network order, not log order: a later proposal can commit while an
    /// earlier one is still in flight. Delivering the later batch immediately would advance
    /// <see cref="lastAppliedIndex"/> over the in-flight entry, and the exactly-once guard in
    /// <see cref="ApplyLogToConsumerAsync"/> would then suppress that entry's own delivery forever —
    /// a permanent hole in the leader's applied sequence (the Jepsen Log Matching violation).
    /// Batches parked here are flushed in id order by <see cref="FlushDeferredLeaderAppliesAsync"/>
    /// as the blocking proposals resolve (commit or rollback).
    /// </summary>
    private readonly SortedDictionary<long, List<RaftLog>> deferredLeaderApplies = [];

    /// <summary>
    /// Term the entries in <see cref="deferredLeaderApplies"/> were deferred in. A term change
    /// invalidates the buffer: after a step-down the WAL-based drains (follower append or the next
    /// promotion) own in-order delivery, and a rolled-back id from the stale tenure could be
    /// re-proposed with a different payload, so flushing stale advance-only ranges would skip real
    /// entries. Checked lazily on every defer/flush rather than at each of the many
    /// leader→follower transition sites.
    /// </summary>
    private long deferredLeaderAppliesTerm = -1;

    /// <summary>
    /// Ticket of the in-flight promotion-barrier no-op, or <see cref="HLCTimestamp.Zero"/> when no
    /// barrier is pending. Armed by <see cref="BecomeLeaderAsync"/> when the election winner's WAL
    /// holds entries above the known commit frontier (inherited prior-term entries whose commit
    /// broadcast never reached this node). While armed, <c>nodeState == Leader</c> but
    /// <see cref="IRaftPartitionHost.Leader"/> stays unpublished, so <c>AmILeader</c> is false and
    /// the node does not serve; heartbeats still flow (they key off <c>nodeState</c>) so rival
    /// elections stay suppressed. <see cref="CompleteLeaderCommit"/> publishes leadership when the
    /// matching commit lands — its inherited-entry drain has by then applied every prior-term entry.
    /// Cleared on every leader→follower transition via <see cref="FailAllActiveProposalWaiters"/>.
    /// </summary>
    private HLCTimestamp leadershipBarrierTicket = HLCTimestamp.Zero;

    /// <summary>Term the pending barrier was proposed in; the publish is fenced on it so a stale
    /// barrier completion from a superseded term can never publish leadership.</summary>
    private long leadershipBarrierTerm = -1;

    /// <summary>Monotonic timestamp when the barrier was armed; drives the
    /// <see cref="RaftConfiguration.LeadershipBarrierTimeout"/> revert in the leader tick.</summary>
    private long leadershipBarrierArmedTicks;

    /// <summary>
    /// The in-flight read-index confirmation round, or <see langword="null"/> when none is open.
    /// All <see cref="ConfirmLeadershipAsync"/> callers that arrive while a round is in flight
    /// share its ack round (or chain into <see cref="readIndexPendingWaiters"/> when the commit
    /// frontier has moved past the round's capture), so steady-state cost is ~one quorum
    /// round-trip per heartbeat interval regardless of read volume. Failed wholesale on every
    /// leadership-loss transition via <see cref="FailAllReadIndexWaiters"/>, so a surviving round
    /// always belongs to <see cref="currentTerm"/>.
    /// </summary>
    private ReadIndexRound? readIndexRound;

    /// <summary>
    /// Confirmation callers that arrived while a round was in flight but could NOT join it because
    /// the commit frontier had advanced past the round's captured read index — joining would let
    /// their read miss a write that completed before the read started. They form the next round,
    /// started as soon as the current one confirms or expires. StartedTicks is each caller's own
    /// arrival time, so its timeout budget spans the full wait, not just its own round.
    /// </summary>
    private readonly List<(ulong CorrelationId, long StartedTicks)> readIndexPendingWaiters = [];

    /// <summary>
    /// Readers waiting for <see cref="lastAppliedIndex"/> to cover a quorum-confirmed commit
    /// index — the second half of the read-index contract: counting acks proves leadership, but
    /// the local applied state must also contain everything committed at capture time before a
    /// local read is linearizable. Shared by two producers: leader-side
    /// <see cref="ConfirmLeadershipAsync"/> waiters (their round confirmed here) and
    /// non-leader <see cref="WaitLocalApplication"/> waiters (their index was confirmed on the
    /// remote leader), so expiry must run from the tick in every node state, not just Leader.
    /// </summary>
    private readonly List<(ulong CorrelationId, long RequiredIndex, long StartedTicks)> readIndexApplyWaiters = [];

    /// <summary>Monotonic timestamp of the last completed quorum confirmation (0 = none). A
    /// confirmation no older than one heartbeat interval is reused as a fast path — it is exactly
    /// as fresh as the acks it counted, and pre-vote leader stickiness prevents any rival from
    /// assembling an election quorum inside that window.</summary>
    private long lastLeadershipConfirmedTicks;

    /// <summary>Term the last quorum confirmation was completed in; the fast path is fenced on it
    /// so a confirmation from a previous leadership stint can never be reused.</summary>
    private long lastLeadershipConfirmedTerm = -1;

    /// <summary>
    /// Per-peer monotonic timestamp of the last same-term successful append/heartbeat ack.
    /// Feeds the check-quorum window (<see cref="RaftConfiguration.EnableCheckQuorum"/>): unlike
    /// <see cref="matchIndex"/>/<see cref="lastCommitIndexes"/> these carry recency, which is what
    /// an isolated leader lacks. Only term-stamped acks are recorded — an unstamped ack passed the
    /// term fence by default and proves nothing about this term. Cleared on every leadership
    /// transition.
    /// </summary>
    private readonly Dictionary<string, long> lastVoterAckTicks = [];

    /// <summary>Monotonic timestamp when this leader last heard same-term acks from a majority of
    /// voters (refreshed while quiesced, since a quiesced leader legitimately receives none).
    /// When it falls behind the check-quorum window the leader steps down.</summary>
    private long lastQuorumContactTicks;

    /// <summary>
    /// Externally visible node state (served to <c>GetNodeState</c>, which backs the
    /// <c>AmILeader</c> fallback path). Reports <see cref="RaftNodeState.Candidate"/> while this
    /// node has won an election but has not yet published leadership (promotion barrier pending):
    /// the raw <c>nodeState</c> is already <c>Leader</c> so replication acks and heartbeats work,
    /// but leaking <c>Leader</c> here would reopen the inherited-entry serving hole that gating
    /// <see cref="IRaftPartitionHost.Leader"/> closes — <c>AmILeaderQuick</c> treats a
    /// <c>Leader</c> state reply as authoritative.
    /// <para>Safe to read from any thread: <see cref="nodeState"/> is volatile-published and
    /// <see cref="IRaftPartitionHost.Leader"/> is a volatile reference, so an off-thread reader
    /// sees a recent (possibly one-transition-stale) role but never a torn or resurrected one.
    /// The <c>Leader != LocalEndpoint</c> demotion to <c>Candidate</c> is what makes the
    /// off-thread read safe to expose: a role read that races ahead of the leadership
    /// publication point degrades to <c>Candidate</c>, never to a premature <c>Leader</c>.</para>
    /// </summary>
    public RaftNodeState NodeState =>
        nodeState == RaftNodeState.Leader && host.Leader != host.LocalEndpoint
            ? RaftNodeState.Candidate
            : nodeState;
    public long CurrentTerm => currentTerm;

    /// <summary>
    /// Test-only: snapshots this partition's consensus state into an immutable <see cref="RaftPartitionView"/>.
    /// Runs on the executor's single-writer thread (dispatched via <see cref="RaftRequestType.GetPartitionView"/>),
    /// so all mutable fields are read consistently and never observed torn by a polling thread. The WAL max
    /// index is read through the facade on the same thread.
    /// </summary>
    public async Task<RaftPartitionView> GetPartitionView()
    {
        long maxWal = await wal.GetMaxLogAsync().ConfigureAwait(false);
        return new RaftPartitionView(
            Endpoint: host.LocalEndpoint,
            Partition: host.PartitionId,
            Role: nodeState,
            Term: currentTerm,
            Leader: host.Leader,
            CommitIndex: wal.GetCommitIndex(),
            LastAppliedIndex: lastAppliedIndex,
            MaxWalIndex: maxWal,
            Quiesced: quiesced,
            MemberRole: host.LocalRole);
    }

    /// <summary>
    /// The current election timeout for this partition. Exposed so callers with access to a seeded
    /// configuration can verify reproducibility without depending on wall-clock behaviour.
    /// </summary>
    public TimeSpan ElectionTimeout => electionTimeout;

    /// <summary>
    /// Deterministically combines the configured election seed with the partition id and local node id
    /// so each node gets a distinct-but-reproducible election-timeout RNG sequence.
    /// <para>Uses a fixed integer hash-combine (<c>* 397 ^ …</c>) rather than
    /// <see cref="HashCode.Combine{T1,T2,T3}(T1,T2,T3)"/>, which seeds itself from a per-process random
    /// value and would therefore make the "seeded" timeouts differ across restarts — breaking the
    /// reproducibility the <see cref="RaftConfiguration.ElectionTimeoutSeed"/> knob exists to provide.</para>
    /// </summary>
    private static int DeriveElectionSeed(int configuredSeed, int partitionId, int localNodeId)
    {
        unchecked
        {
            int mixed = configuredSeed;
            mixed = mixed * 397 ^ partitionId;
            mixed = mixed * 397 ^ localNodeId;
            return mixed;
        }
    }

    public RaftPartitionStateMachine(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        IRaftOperationReplySink replySink,
        ILogger<IRaft> logger)
    {
        this.host = host;
        this.wal = wal;
        this.replySink = replySink;
        this.logger = logger;

        // Mix a STABLE, per-node identity into the seed so nodes in the same partition don't draw an
        // identical election-timeout sequence. The old `seed ^ partitionId` gave every node in a
        // partition the same sequence, so after a symmetric split vote they'd keep choosing identical
        // retry timeouts and fire simultaneously forever — defeating the randomization meant to break the
        // tie. Folding in host.LocalNodeId gives each node its own reproducible sequence (deterministic
        // given the node's identity, so seeded runs stay repeatable per node). Only applies when a seed is
        // configured; the production default (null) already uses per-node Random.Shared.
        random = host.Configuration.ElectionTimeoutSeed is int seed
            ? new Random(DeriveElectionSeed(seed, host.PartitionId, host.LocalNodeId))
            : Random.Shared;

        electionTimeout = TimeSpan.FromMilliseconds(random.Next(
            host.Configuration.StartElectionTimeout,
            host.Configuration.EndElectionTimeout));

        snapshotSender = new SnapshotSender(
            host,
            logger,
            () => nodeState,
            () => postToExecutor,
            (endpoint, idx) =>
            {
                if (!lastCommitIndexes.TryGetValue(endpoint, out long cur) || idx > cur)
                    lastCommitIndexes[endpoint] = idx;
            });
    }

    /// <summary>
    /// Wires the callback used to post messages back to the partition executor from background
    /// tasks (e.g., <see cref="RaftRequestType.SnapshotInstalled"/>).  Called once by
    /// <see cref="RaftPartition"/> immediately after the executor is created.
    /// </summary>
    public void SetPostToExecutor(Action<RaftRequest> post) => postToExecutor = post;

    /// <summary>
    /// Wires the quiesce-state-change callback.  Called once by <see cref="RaftPartition"/>
    /// at construction time so the manager's hot set stays in sync.
    /// </summary>
    internal void SetOnQuiesceChanged(Action<bool> callback) => _onQuiesceChanged = callback;

    /// <summary>
    /// Assigns <paramref name="value"/> to <see cref="quiesced"/> and notifies
    /// <see cref="_onQuiesceChanged"/> on an actual state change.  Must be called instead of
    /// directly assigning <c>quiesced</c> so the hot-set tracking stays consistent.
    /// </summary>
    private void SetQuiesced(bool value)
    {
        if (quiesced == value)
            return;
            
        quiesced = value;
        _onQuiesceChanged?.Invoke(value);
    }

    private void CompleteReply(ulong? correlationId, RaftResponse response)
    {
        if (correlationId is not null)
            replySink.TryComplete(correlationId.Value, response);
    }

    /// <summary>
    /// Clears all bookkeeping for the current pre-vote round. Called when a candidacy is
    /// abandoned (Candidate→Follower) and when a real election begins, so a stale pre-vote
    /// round for an old term can never leak into and falsely promote a later term.
    /// Side-effect-free with respect to real Raft state (term/votes/leader are untouched).
    /// </summary>
    private void ResetPreVoteRound()
    {
        preVotes.Clear();
        preVoteTerm = -1;
        electionPhase = RaftElectionPhase.None;
    }

    /// <summary>
    /// Phase 1 of the nonblocking restore.  Initialises the heartbeat timestamp and
    /// loads the raw WAL entries through the I/O scheduler.  The returned list must be
    /// delivered back to the executor as a
    /// <see cref="RaftRequestType.RestoreLogsLoaded"/> maintenance event so that
    /// <see cref="CompleteRestoreAsync"/> runs under the single-owner guarantee.
    /// </summary>
    public ValueTask<IReadOnlyList<RaftLog>> StartRestoreAsync()
    {
        lastHeartbeat = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        lastHeartbeatTicks = host.GetMonotonicTimestamp();
        return wal.LoadRestoreLogsAsync();
    }

    /// <summary>
    /// Phase 2 of the nonblocking restore.  Called on the executor thread after
    /// <see cref="StartRestoreAsync"/> has loaded logs from storage.  Replays the
    /// committed entries via the application replication callbacks, updates the
    /// current term, and sends the initial handshake.
    /// </summary>
    public async ValueTask CompleteRestoreAsync(IReadOnlyList<RaftLog> logs)
    {
        if (restored)
            return;

        await wal.CompleteRestoreAsync(logs).ConfigureAwait(false);

        currentTerm = await wal.GetCurrentTermAsync().ConfigureAwait(false);

        // B2b: seed durable Raft hard state. The term inferred from the log tail (above) can LAG the true
        // term — a crash after a term bump or a granted vote but before the next log write leaves the tail
        // behind. Trusting the tail alone would let the node regress its term and re-vote for a different
        // candidate in a term it already voted in (a split-brain hazard). The persisted hard state is
        // authoritative for the term; votedFor is restored into expectedLeaders so the "already voted for
        // someone else this term" guard in VoteAsync rejects a different candidate after a restart.
        // (Durability is the lighter, WAL-cadence guarantee — the very last vote before a power loss may
        // not have reached disk; see IWAL.PersistHardState.)
        (long CurrentTerm, string? VotedFor)? hardState = await wal.LoadHardStateAsync().ConfigureAwait(false);
        if (hardState is { } hs)
        {
            if (hs.CurrentTerm > currentTerm)
                currentTerm = hs.CurrentTerm;

            if (!string.IsNullOrEmpty(hs.VotedFor))
                expectedLeaders[hs.CurrentTerm] = hs.VotedFor;
        }

        // Seed the applied cursor to the frontier restore just replayed. wal.CompleteRestoreAsync
        // delivered every committed entry below the reconstructed commit frontier to the consumer (via
        // InvokeLogRestored / InvokeSystemLogRestored), but lastAppliedIndex stayed at its -1 init.
        // Without this seed, a restarted node that later wins an election would re-drain the entire
        // retained log from index 0 on promotion (BecomeLeaderAsync → DrainCommittedAppliesAsync),
        // delivering every committed entry to the consumer a SECOND time and holding the serial
        // partition executor for the full backlog before sending its first heartbeat — long enough to
        // risk another election round. GetCommitIndex() returns the highest committed id restore
        // applied (0 when none, since log ids start at 1), and ApplyLogToConsumerAsync applies the
        // identical committed-only filter, so seeding here makes that promotion drain a precise no-op
        // for already-restored entries while still draining anything committed after restore.
        lastAppliedIndex = wal.GetCommitIndex();

        logger.LogInfoWalRestored(host.LocalEndpoint, host.PartitionId, nodeState, logs.Count, 0L);

        await SendHandshakeAsync().ConfigureAwait(false);

        restored = true;
    }

    /// <summary>
    /// Periodically checks partition leadership and drives elections when necessary.
    /// <para>
    /// <b>Quiesced followers</b> (when <see cref="RaftConfiguration.EnableQuiescence"/> is on
    /// and <see cref="quiesced"/> is <see langword="true"/>): the per-partition heartbeat timer
    /// is ignored.  Instead, an election is triggered only when the SWIM failure detector marks
    /// the expected leader's node as <see cref="MemberLivenessState.Suspect"/> or
    /// <see cref="MemberLivenessState.Dead"/> — i.e. <c>GetNodeLiveness(leader) != Alive</c>.
    /// This relies on the invariant <c>PingInterval &lt; StartElectionTimeout</c>
    /// (validated at startup by <see cref="RaftConfiguration.Validate"/>): a SWIM Suspect fires
    /// after approximately one <c>PingInterval</c>, comfortably under <c>StartElectionTimeout</c>,
    /// so quiesced failover is not slower than normal election timeout.
    /// </para>
    /// <para>
    /// <b>Live-but-stalled-partition caveat:</b> if a leader's node stays <c>Alive</c> in SWIM
    /// but stops driving a specific partition (e.g. its executor wedges) while still answering
    /// SWIM pings, quiesced followers will not elect a new leader for that partition.  Accepted
    /// limitation for v1; mitigation would require per-partition sequence numbers in the SWIM
    /// ping payload.
    /// </para>
    /// </summary>
    public async Task CheckPartitionLeadershipAsync()
    {
        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();

        // Read-index expiry runs in every node state, before any early return (including the
        // leader's quiesced one): on the leader it bounds rounds and chained waiters; on
        // followers it bounds WaitLocalApplication applied-frontier waiters — a stable follower
        // whose apply stalls has no leadership-loss transition to fail them, so the tick is the
        // only bound on their wait.
        if (readIndexRound is not null || readIndexPendingWaiters.Count > 0 || readIndexApplyWaiters.Count > 0)
            await ExpireReadIndexWaitersAsync(nowTicks).ConfigureAwait(false);

        // Retry a withheld committed drain. DrainCommittedAppliesAsync stops without delivering
        // when the next id is absent above the snapshot floor or is still Proposed — correct, since
        // delivering past it would skip it — on the understanding that a later drain picks the
        // entries up. On a follower the *only* thing that starts a drain is a WAL write completing
        // for this partition, and that is precisely what stops arriving: the leader ships entries
        // until the follower's commit index catches up, which happens whether or not anything was
        // applied, and from then on sends empty heartbeats that enqueue no write and complete
        // nothing. The blocking condition clears with nothing left to notice, and the applied
        // frontier stays where it stopped for good.
        //
        // Observed in Jepsen run 31750742525 as a node holding 74 entries having delivered none of
        // them, while the leader correctly computed gap=0 from the commit index that follower
        // itself reported. Every one of the 889 losses in that run was of this kind.
        //
        // Leaders are excluded: their mid-tenure delivery runs through CompleteLeaderCommit and the
        // deferred-applies buffer, and a second drain racing that is how the finding-1 hole was
        // created in the first place. This is a no-op when the frontier is already covered.
        if (nodeState != RaftNodeState.Leader)
        {
            long commitFrontier = wal.GetCommitIndex();

            if (commitFrontier > lastAppliedIndex)
                await DrainCommittedAppliesAsync(commitFrontier).ConfigureAwait(false);
        }

        switch (nodeState)
        {
            // if node is leader just send hearthbeats every Configuration.HeartbeatInterval
            case RaftNodeState.Leader:
            {
                // Promotion-barrier liveness bound: a leader whose barrier no-op never commits
                // (quorum lost right after the election) would otherwise heartbeat forever without
                // ever publishing leadership — followers stay suppressed and the partition wedges
                // with no serving leader. Revert to Follower so the election timeout can pick a
                // replacement (or re-elect this node, which arms a fresh barrier).
                if (leadershipBarrierTicket != HLCTimestamp.Zero
                    && MonotonicElapsed(leadershipBarrierArmedTicks, nowTicks) >= host.Configuration.LeadershipBarrierTimeout)
                {
                    await RevertUnpublishedPromotionAsync("barrier commit timed out").ConfigureAwait(false);
                    return;
                }

                // Check-quorum: step down once no majority of voters has acked within the window.
                // This does not close the stale-read hole (ConfirmLeadershipAsync does); it bounds
                // how long an isolated leader lingers so minority-side callers fail fast.
                if (host.Configuration.EnableCheckQuorum)
                {
                    if (quiesced)
                    {
                        // A quiesced leader stops heartbeating, so an absence of acks proves
                        // nothing; keep the grace window fresh so it restarts on un-quiesce.
                        lastQuorumContactTicks = nowTicks;
                    }
                    else
                    {
                        TimeSpan window = host.Configuration.HeartbeatInterval * host.Configuration.CheckQuorumIntervalMultiplier;
                        int votersTotal = 1;    // the local leader
                        int reachable = 1;
                        foreach (RaftNode node in host.Nodes)
                        {
                            if (!host.IsVoter(node.Endpoint))
                                continue;
                            votersTotal++;
                            if (lastVoterAckTicks.TryGetValue(node.Endpoint, out long ackTicks)
                                && MonotonicElapsed(ackTicks, nowTicks) < window)
                                reachable++;
                        }

                        if (reachable >= votersTotal / 2 + 1)
                            lastQuorumContactTicks = nowTicks;
                        else if (lastQuorumContactTicks != 0
                            && MonotonicElapsed(lastQuorumContactTicks, nowTicks) >= window)
                        {
                            await StepDownOnQuorumLossAsync().ConfigureAwait(false);
                            return;
                        }
                    }
                }

                if (quiesced)
                {
                    // Gating entry into quiescence is only half the guarantee. A peer can appear or fall
                    // behind AFTER we quiesced — a node joining an idle cluster is the common case — and
                    // quiescence suppresses SendHeartbeat, the only catch-up path, so that peer would be
                    // stranded permanently with no propose traffic coming to wake anything up. Re-arm
                    // heartbeats as soon as any peer is behind (or newly present with no recorded
                    // progress); we quiesce again on a later tick once everyone has converged.
                    if (!HasLaggingPeer())
                        return;

                    SetQuiesced(false);
                }

                // B3: heartbeat cadence measured on the monotonic clock — a heartbeat received from a
                // skewed peer must not inflate the interval and suppress our own heartbeats.
                if (currentTime != HLCTimestamp.Zero && (MonotonicElapsed(lastHeartbeatTicks, nowTicks) >= host.Configuration.HeartbeatInterval))
                {
                    // Drain settled proposals on the heartbeat cadence. Under load ReplicateLogs already
                    // sweeps; this covers the idle tail — a leader that stopped proposing would otherwise
                    // retain its last batch's log payloads and, because a non-empty map blocks the quiesce
                    // gate below, never quiesce. Once drained, activeProposals.Count reaches 0 and the
                    // quiesce check can fire in this same tick.
                    if (activeProposals.Count > 0)
                        PruneSettledProposals(currentTime);

                    // When quiescence is on and the partition has been idle longer than QuiesceAfter,
                    // send a quiesce marker to followers and stop heartbeating.  Followers switch to
                    // SWIM-based election gating once they receive the marker.
                    if (host.Configuration.EnableQuiescence
                        && !quiesced
                        && activeProposals.Count == 0
                        && lastProposalAtTicks != 0
                        && !HasLaggingPeer()
                        && (MonotonicElapsed(lastProposalAtTicks, nowTicks) >= host.Configuration.QuiesceAfter))
                    {
                        SetQuiesced(true);
                        lastHeartbeat = currentTime;
                        lastHeartbeatTicks = nowTicks;
                        SendQuiesceMarker(currentTime);
                    }
                    else
                    {
                        await SendHeartbeat(false).ConfigureAwait(false);
                    }
                }

                return;
            }

            // Wait Configuration.VotingTimeout seconds after the voting process starts to check if a quorum is available
            case RaftNodeState.Candidate when votingStartedTicks != 0 && MonotonicElapsed(votingStartedTicks, nowTicks) < host.Configuration.VotingTimeout:
                return;

            case RaftNodeState.Candidate:

                double votingElapsedMs = MonotonicElapsed(votingStartedTicks, nowTicks).TotalMilliseconds;
                logger.LogInfoVotingConcluded(host.LocalEndpoint, host.PartitionId, nodeState, votingElapsedMs);

                nodeState = RaftNodeState.Follower;
                host.Leader = "";
                lastHeartbeat = currentTime;
                lastHeartbeatTicks = nowTicks;
                // Pick a fresh random timeout in the full [StartElectionTimeout, EndElectionTimeout)
                // range rather than capping an incremented value. Incremental backoff converges
                // both nodes to EndElectionTimeout after just one or two failed elections, causing
                // a persistent split-vote livelock because they fire at the same instant every time.
                electionTimeout = TimeSpan.FromMilliseconds(
                    random.Next(host.Configuration.StartElectionTimeout, host.Configuration.EndElectionTimeout));
                expectedLeaders.Clear();
                lastCommitIndexes.Clear();
                nextIndex.Clear();
                matchIndex.Clear();
                regressedFrontiers.Clear();
                localCommittedIndex = -1;
                FailAllActiveProposalWaiters();
                activeProposals.Clear();
                lastProposalAt = HLCTimestamp.Zero;
                lastProposalAtTicks = 0;
                SetQuiesced(false);
                ResetPreVoteRound();

                await host.InvokeLeaderChanged(host.PartitionId, "");
                return;
            
            // Quiesced follower: per-partition heartbeat timer is suppressed.
            // Gate elections on SWIM node state instead — Suspect or Dead triggers failover.
            case RaftNodeState.Follower when quiesced && host.Configuration.EnableQuiescence:
            {
                string expectedLeaderNode = expectedLeaders.GetValueOrDefault(currentTerm, "");
                if (string.IsNullOrEmpty(expectedLeaderNode) ||
                    host.GetNodeLiveness(expectedLeaderNode) == MemberLivenessState.Alive)
                    return; // leader's node is Alive per SWIM — stay calm
                // Leader node is Suspect or Dead — un-quiesce and challenge leadership.
                SetQuiesced(false);
                await StartPreVoteAsync(currentTime).ConfigureAwait(false);
                break;
            }

            // if node is follower and leader is not sending hearthbeats, start an election.
            // B3: elapsed-since-last-contact is measured on the monotonic clock, so a leader whose HLC
            // ran ahead of ours cannot freeze this gate and delay failover for the length of the skew.
            case RaftNodeState.Follower when (lastHeartbeatTicks != 0 && (MonotonicElapsed(lastHeartbeatTicks, nowTicks) < electionTimeout)):
                return;

            case RaftNodeState.Follower:
                // Run a side-effect-free pre-vote first; only a pre-vote quorum promotes to a
                // real election (Raft §9.6), so a stale node can't disrupt a healthy leader.
                await StartPreVoteAsync(currentTime).ConfigureAwait(false);
                break;
            
            default:
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Unknown node state. Term={CurrentTerm}", host.LocalEndpoint, host.PartitionId, nodeState, currentTerm);
                break;
        }
    }

    public async Task StepDownAsync(ulong? replyCorrelationId)
    {
        if (nodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        RaftNode? stepDownTarget = SelectStepDownTarget();

        long nowTicks = host.GetMonotonicTimestamp();

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        lastHeartbeat = currentTime;
        lastVotation = currentTime;
        lastHeartbeatTicks = nowTicks;
        lastVotationTicks = nowTicks;
        votingStartedAt = HLCTimestamp.Zero;
        votingStartedTicks = 0;
        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        localCommittedIndex = -1;
        FailAllActiveProposalWaiters();
        activeProposals.Clear();
        lastProposalAt = HLCTimestamp.Zero;
        lastProposalAtTicks = 0;
        SetQuiesced(false);

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

        if (stepDownTarget is not null)
        {
            host.EnqueueResponse(stepDownTarget.Endpoint, new(
                RaftResponderRequestType.StepDownNotice,
                stepDownTarget,
                new StepDownNoticeRequest(host.PartitionId, currentTerm, currentTime, host.LocalEndpoint)));
        }

        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Pending, 0L));
    }

    public async Task TransferLeadershipAsync(string targetEndpoint, ulong? replyCorrelationId)
    {
        if (nodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return;
        }

        if (string.IsNullOrWhiteSpace(targetEndpoint))
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Errored, 0L));
            return;
        }

        if (targetEndpoint == host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.LeaderAlreadyElected, 0L));
            return;
        }

        RaftNode? targetNode = host.Nodes.FirstOrDefault(node => node.Endpoint == targetEndpoint);
        if (targetNode is null)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Errored, 0L));
            return;
        }

        // The adapter read races the WAL write queue (appends still queued in the write scheduler
        // are invisible to it), so take the enqueue-advanced presence frontier when it is higher —
        // an understated local max would hand leadership to a target that is actually behind.
        long localMaxLogId = await wal.GetMaxLogAsync().ConfigureAwait(false);
        long presentIndex = wal.GetPresentIndex();
        if (presentIndex > localMaxLogId)
            localMaxLogId = presentIndex;

        long targetMaxLogId = GetKnownRemoteMaxLogId(targetEndpoint);
        if (targetMaxLogId < localMaxLogId)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.ReplicationFailed, 0L));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();
        long targetTerm = currentTerm + 1;

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        lastHeartbeat = currentTime;
        lastVotation = currentTime;
        lastHeartbeatTicks = nowTicks;
        lastVotationTicks = nowTicks;
        votingStartedAt = HLCTimestamp.Zero;
        votingStartedTicks = 0;
        expectedLeaders.Clear();
        expectedLeaders[targetTerm] = targetEndpoint;
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        localCommittedIndex = -1;
        FailAllActiveProposalWaiters();
        activeProposals.Clear();

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

        host.EnqueueResponse(targetNode.Endpoint, new(
            RaftResponderRequestType.TransferLeadership,
            targetNode,
            new TransferLeadershipRequest(host.PartitionId, currentTerm, currentTime, host.LocalEndpoint, targetEndpoint)));

        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Pending, 0L));
    }

    public Task SuspendHeartbeatsAsync(ulong? replyCorrelationId)
    {
        if (nodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return Task.CompletedTask;
        }

        heartbeatsSuspendedForTesting = true;
        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
        return Task.CompletedTask;
    }

    public async Task ResumeHeartbeatsAsync(ulong? replyCorrelationId)
    {
        heartbeatsSuspendedForTesting = false;

        if (nodeState == RaftNodeState.Leader && host.Leader == host.LocalEndpoint)
            await SendHeartbeat(true).ConfigureAwait(false);

        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
    }

    public void ResetTestingState()
    {
        heartbeatsSuspendedForTesting = false;
    }

    /// <summary>
    /// Forces the partition into quiesced state for unit testing without going through the
    /// full leader-side quiesce path that suppresses heartbeats.  Also records the expected
    /// leader so the quiesced follower branch can look up the SWIM state.
    /// </summary>
    public void SetQuiescedForTesting(bool value, string? leaderEndpoint = null, long term = 1)
    {
        SetQuiesced(value);
        if (value && leaderEndpoint is not null)
            expectedLeaders[term] = leaderEndpoint;
    }

    /// <summary>
    /// Forces the leader's committed frontier for unit testing so the quiesce gate's
    /// <see cref="HasLaggingPeer"/> check can be exercised without driving a full propose/commit
    /// cycle. A peer with no recorded progress counts as lagging once this is above zero, which is
    /// what re-arms heartbeats on the periodic tick after the leader has quiesced.
    /// </summary>
    public void SetLocalCommittedIndexForTesting(long committedIndex)
    {
        localCommittedIndex = committedIndex;
    }

    /// <summary>
    /// Seeds the state shared by all become-leader paths: advances the HLC, marks the node as
    /// Leader, records the durable committed index for backfill, and starts both the heartbeat
    /// timer and the idle-quiesce clock at the same election timestamp.  Per-follower cursors
    /// (<see cref="nextIndex"/>, <see cref="matchIndex"/>) remain the caller's responsibility
    /// because they differ between the single-node fast-path and the quorum-win path.
    /// <para>
    /// Seeding <see cref="lastProposalAt"/> here ensures that a partition that wins an election
    /// and receives no client writes still quiesces after <see cref="RaftConfiguration.QuiesceAfter"/>,
    /// which is the common case for idle partitions in large multi-partition deployments.
    /// </para>
    /// </summary>
    private HLCTimestamp BecomeLeader()
    {
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();
        nodeState = RaftNodeState.Leader;
        localCommittedIndex = wal.GetCommitIndex();
        liveCommitFloor = localCommittedIndex;
        host.Leader = host.LocalEndpoint;
        lastHeartbeat = ts;
        lastProposalAt = ts;
        lastHeartbeatTicks = nowTicks;
        lastProposalAtTicks = nowTicks;
        ResetLeadershipConfirmationState(nowTicks);
        SetQuiesced(false);
        return ts;
    }

    /// <summary>
    /// Synchronously forces the node into Leader state for the given term.  Test-only.
    ///
    /// <para>Intentionally uses the synchronous <see cref="BecomeLeader"/> (no WAL drain)
    /// rather than <see cref="BecomeLeaderAsync"/>.  Tests that use this helper are testing
    /// quiescence, heartbeat timing, or other leader-side behaviour that does not depend on
    /// the apply-before-leader-changed ordering guarantee.  Using the async drain path here
    /// would introduce ReadScheduler round-trips and consumer callbacks that are irrelevant
    /// to — and would slow — those tests.</para>
    ///
    /// <para>Tests that need to exercise the real promotion sequence (drain ordering,
    /// inherited-entry delivery, etc.) must use <see cref="ForceLeaderForTestingAsync"/>
    /// instead, which calls <see cref="BecomeLeaderAsync"/>.</para>
    /// </summary>
    public void SetLeaderForTesting(long term)
    {
        currentTerm = term;
        BecomeLeader();
    }

    /// <summary>
    /// Promotion helper used by all real election paths. Performs the same internal
    /// bookkeeping as <see cref="BecomeLeader"/> but defers publishing
    /// <see cref="IRaftPartitionHost.Leader"/> until the consumer projection provably
    /// covers every entry committed before this node was promoted.
    ///
    /// <para>Two cases, split on whether the WAL tail extends past the known commit frontier:</para>
    ///
    /// <para><b>No inherited tail</b> (presence frontier &lt;= commit frontier): every entry the previous
    /// leader could have committed is already commit-marked locally — election log-freshness
    /// guarantees the winner's log contains every quorum-durable entry, so an empty tail proves
    /// there is nothing inherited. The committed drain (<see cref="DrainCommittedAppliesAsync"/>)
    /// suffices and leadership is published before returning (<see langword="true"/>), exactly the
    /// pre-barrier behavior. This keeps the common idle-failover and clean single-node startup at
    /// zero added latency.</para>
    ///
    /// <para><b>Inherited tail present</b>: entries above the frontier are prior-term
    /// <c>Proposed</c> entries that may be quorum-committed elsewhere (their commit broadcast never
    /// reached this node — e.g. it raced the previous leader's death, or the single-fsync fast path
    /// crashed before writing lazy commit markers). Serving before applying them is the
    /// inherited-entry hole: a lock consumer would double-grant, a KV consumer would serve a stale
    /// read. Raft's remedy is committing a no-op in the new term, which commits the whole prior-term
    /// prefix. This method proposes that no-op (a consumer-invisible
    /// <see cref="RaftSystemConfig.LeadershipBarrierLogType"/> entry, auto-commit) through the
    /// normal proposal path and returns <see langword="false"/> WITHOUT publishing leadership. The
    /// quorum acks for the no-op arrive as later executor operations, so the commit cannot be
    /// awaited here — <see cref="CompleteLeaderCommit"/> publishes <c>host.Leader</c> when the
    /// barrier ticket commits, after its inherited-entry drain has applied every prior-term entry.
    /// Callers must fire <c>InvokeLeaderChanged(self)</c> only when this returns
    /// <see langword="true"/>; the barrier completion fires it otherwise.</para>
    ///
    /// <para>During the barrier window <c>nodeState == Leader</c> (so acks, heartbeats and the
    /// leader tick run — heartbeats suppress rival elections) but <c>host.Leader</c> is unset and
    /// <see cref="NodeState"/> reports <c>Candidate</c>, so both <c>AmILeader</c> paths stay
    /// closed. Writes routed here are refused at the manager layer for the same reason; the window
    /// is bounded by <see cref="RaftConfiguration.LeadershipBarrierTimeout"/>, after which the
    /// leader tick reverts the node to Follower (see <see cref="RevertUnpublishedPromotionAsync"/>).</para>
    ///
    /// <para><b>Atomicity:</b> the promotion is all-or-nothing. If the drain or the barrier propose
    /// throws (WAL read backpressure, scheduler shutdown, etc.), the node is reverted to
    /// <see cref="RaftNodeState.Follower"/> so it is never left in a half-promoted state.  The
    /// exception is re-thrown; the cluster self-heals by electing a replacement in the next
    /// term. Barrier failures after this method returns (propose/commit failure, rollback,
    /// timeout) revert through <see cref="RevertUnpublishedPromotionAsync"/>.</para>
    ///
    /// <para><b>Latency note:</b> the drain holds the partition executor for the full
    /// duration of the backlog (one <c>ReadScheduler</c> round-trip per 512-entry batch
    /// plus one consumer callback per committed entry).  <c>SendHeartbeat</c> runs only
    /// after this method returns, so a node with a large unapplied backlog will delay
    /// its first heartbeat and risk triggering a further election round.  No explicit
    /// bound on the drain is enforced today; a future improvement could cap the drain
    /// and resume it in background once leadership is established.</para>
    /// </summary>
    /// <returns><see langword="true"/> when leadership was published before returning;
    /// <see langword="false"/> when a promotion barrier is pending and
    /// <see cref="CompleteLeaderCommit"/> will publish it.</returns>
    private async Task<bool> BecomeLeaderAsync()
    {
        HLCTimestamp ts = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();
        nodeState = RaftNodeState.Leader;
        long commitFrontier = wal.GetCommitIndex();
        localCommittedIndex = commitFrontier;
        liveCommitFloor = commitFrontier;
        lastHeartbeat = ts;
        lastProposalAt = ts;
        lastHeartbeatTicks = nowTicks;
        lastProposalAtTicks = nowTicks;
        ResetLeadershipConfirmationState(nowTicks);
        SetQuiesced(false);

        long maxLog;

        try
        {
            // Drain all committed entries up to the promotion frontier. By the time this
            // succeeds every InvokeReplicationReceived call has completed, so the consumer
            // projection is current before the partition is advertised as the serving leader.
            // Consumer exceptions are caught inside ApplyLogToConsumerAsync and do not
            // propagate here; only WAL-level errors (backpressure, shutdown) can reach this
            // catch block.
            //
            // The drain withholds rather than skips, so it reports whether it actually REACHED
            // the frontier — and its backend reads race the WAL write queue (the in-memory
            // frontiers advance at enqueue time), so an entry still queued in the write scheduler
            // is invisible and indistinguishable from a hole. Retry until the writes land and the
            // drain covers the frontier, bounded by the barrier timeout; each retry makes forward
            // progress as writes apply and exits as soon as the frontier is reached, so the common
            // case adds no latency. A frontier still unreached at the deadline means entries below
            // it are genuinely absent or unresolved: serving would fix an incomplete consumer
            // projection for the whole tenure — a leader is never backfilled.
            // A sole voter (e.g. the last survivor of graceful leaves) needs only its own write
            // queue to drain — nothing external can arrive — so its bound is short; with voter
            // peers the full barrier timeout is worth spending, because refusing hands leadership
            // to a peer that may hold the missing entries.
            bool hasVoterPeers = HasVoterPeer();
            TimeSpan drainBound = hasVoterPeers ? host.Configuration.LeadershipBarrierTimeout : TimeSpan.FromMilliseconds(250);
            long drainDeadlineTicks = Stopwatch.GetTimestamp();

            while (!await DrainCommittedAppliesAsync(commitFrontier).ConfigureAwait(false))
            {
                if (Stopwatch.GetElapsedTime(drainDeadlineTicks) > drainBound)
                {
                    // Refuse only when another voter exists that could hold the missing entries —
                    // reverting lets it win the next term with a complete log. A sole voter has no
                    // such peer: the departed quorum took the entries with it, refusing forever
                    // would leave the partition permanently leaderless, and the gap is
                    // unrecoverable either way. Serve, and say so loudly.
                    if (hasVoterPeers)
                        throw new RaftException(
                            $"Promotion refused: committed drain stopped at {lastAppliedIndex} below the frontier {commitFrontier}");

                    logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Committed drain stopped at {LastApplied} below the frontier {Frontier} with no voter peers to defer to — proceeding as sole voter; entries in the gap are unrecoverable.",
                        host.LocalEndpoint, host.PartitionId, nodeState, lastAppliedIndex, commitFrontier);

                    // Deliver everything this survivor DOES hold past the gap, so only the
                    // genuinely absent entries are lost rather than the whole suffix.
                    await DrainCommittedAppliesAsync(commitFrontier, skipGaps: true).ConfigureAwait(false);
                    break;
                }

                await Task.Delay(2).ConfigureAwait(false);
            }

            maxLog = await wal.GetMaxLogAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Revert to Follower so the node is not left in a half-promoted state.
            // host.Leader was never set (we had not reached that line), so the gate for
            // AmILeader / leader-routed reads remains closed. The election timeout will
            // trigger a new round in the next term.
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Promotion drain failed — reverting to Follower. {Message}\n{Stacktrace}",
                host.LocalEndpoint, host.PartitionId, nodeState, ex.Message, ex.StackTrace);
            nodeState = RaftNodeState.Follower;
            localCommittedIndex = -1;
            throw;
        }

        // Completeness gate: a WAL hole below maxLog means this node is missing entries that may be
        // committed elsewhere (the lone-high-entry-over-a-gap shape the unanchored live-propose
        // broadcast can leave). Serving as leader would fix an incomplete consumer projection for
        // the whole tenure — a leader is never backfilled, and neither drain can deliver entries it
        // does not have. Revert so a node with a contiguous log wins the next term; the gap-aware
        // election freshness normally prevents this node from winning at all, so this gate is the
        // defense-in-depth backstop (e.g. a hole opened by an append that raced the election).
        long presentId = wal.GetPresentIndex();
        if (presentId >= 0 && presentId < maxLog)
        {
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Promotion refused: WAL has a hole below the max id (contiguous through {PresentId}, max {MaxLog}) — reverting to Follower.",
                host.LocalEndpoint, host.PartitionId, nodeState, presentId, maxLog);
            nodeState = RaftNodeState.Follower;
            localCommittedIndex = -1;
            throw new RaftException($"Promotion refused: WAL hole below max id (contiguous through {presentId}, max {maxLog})");
        }

        // The inherited-tail decision must come from the enqueue-advanced presence frontier, not
        // the adapter read: GetMaxLogAsync goes through the ReadScheduler straight to the backend
        // and cannot see an append still queued in the write scheduler, so under write-worker
        // latency it under-reports and would skip the barrier — publishing leadership while a
        // prior-term entry that is committed on quorum sits unapplied for the entire tenure (a
        // leader is never backfilled). GetPresentIndex advances at enqueue time and can never
        // under-report a queued append, and the hole gate above already established
        // presentId >= maxLog whenever presence is tracked. Facades that do not track presence
        // (presentId == -1, test stubs) fall back to the adapter read, which is exact for them.
        long inheritedTail = presentId >= 0 ? presentId : maxLog;

        if (inheritedTail <= commitFrontier)
        {
            // No inherited tail: the drain above already proved the consumer projection complete.
            // Publish leader status only after the drain. host.Leader is the gate for
            // AmILeader, so no external observer can see leader == self while the drain
            // is in progress.
            host.Leader = host.LocalEndpoint;
            return true;
        }

        // Inherited prior-term entries exist above the commit frontier. Commit a new-term no-op
        // before serving: CompleteLeaderCommit's inherited drain applies the whole prefix, then
        // publishes leadership. The entry rides the normal proposal path so quorum, durability
        // and commit broadcast all behave exactly like a client write.
        RaftLog barrierLog = new()
        {
            LogType = RaftSystemConfig.LeadershipBarrierLogType,
            LogData = [],
        };

        RaftOperationStatus status;
        HLCTimestamp barrierTicket;

        try
        {
            (status, barrierTicket) = ReplicateLogs([barrierLog], autoCommit: true);
        }
        catch (Exception ex)
        {
            // Same all-or-nothing contract as the drain: a barrier that cannot even be enqueued
            // (WAL backpressure, shutdown) must not leave a Leader that will never publish.
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Promotion barrier propose failed — reverting to Follower. {Message}\n{Stacktrace}",
                host.LocalEndpoint, host.PartitionId, nodeState, ex.Message, ex.StackTrace);
            nodeState = RaftNodeState.Follower;
            localCommittedIndex = -1;
            throw;
        }

        if (status != RaftOperationStatus.Pending)
        {
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Promotion barrier propose rejected ({Status}) — reverting to Follower.",
                host.LocalEndpoint, host.PartitionId, nodeState, status);
            nodeState = RaftNodeState.Follower;
            localCommittedIndex = -1;
            throw new RaftException($"Promotion barrier propose rejected: {status}");
        }

        leadershipBarrierTicket = barrierTicket;
        leadershipBarrierTerm = currentTerm;
        leadershipBarrierArmedTicks = host.GetMonotonicTimestamp();

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Promotion barrier armed at ticket {Ticket} (inherited tail {Frontier}..{MaxLog}); leadership publishes on commit",
                host.LocalEndpoint, host.PartitionId, nodeState, barrierTicket, commitFrontier + 1, inheritedTail);

        return false;
    }

    /// <summary>
    /// Reverts a promoted-but-unpublished leader (promotion barrier pending) back to Follower.
    /// Mirrors the bookkeeping of the Candidate-timeout demotion in
    /// <see cref="CheckPartitionLeadershipAsync"/>: leadership was never published
    /// (<c>host.Leader</c> was never set to self), so consumers already observe an empty leader —
    /// the <c>InvokeLeaderChanged("")</c> here is a harmless re-notification kept for consistency
    /// with every other demotion path. The election timeout then drives a fresh election in a new
    /// term (possibly won by this same node, which will arm a new barrier).
    /// </summary>
    private async Task RevertUnpublishedPromotionAsync(string reason)
    {
        logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Reverting unpublished promotion (barrier {Ticket}, term {Term}): {Reason}",
            host.LocalEndpoint, host.PartitionId, nodeState, leadershipBarrierTicket, leadershipBarrierTerm, reason);

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        lastHeartbeat = currentTime;
        lastHeartbeatTicks = nowTicks;
        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        localCommittedIndex = -1;
        FailAllActiveProposalWaiters();     // also clears the barrier fields
        activeProposals.Clear();
        lastProposalAt = HLCTimestamp.Zero;
        lastProposalAtTicks = 0;
        SetQuiesced(false);

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);
    }

    /// <summary>
    /// Delivers every committed WAL entry from <c>lastAppliedIndex + 1</c> through
    /// <paramref name="upToIndex"/> (inclusive) to the consumer via
    /// <see cref="ApplyLogToConsumerAsync"/>.  Reads the WAL in bounded batches to
    /// avoid loading the full tail into memory.
    ///
    /// <para>Reads ALL entry types so that resolved-but-not-committed entries (rolled back)
    /// advance the cursor instead of reading as gaps — a committed-only read made every
    /// rolled-back id in the range look absent and withheld the drain forever behind it.
    /// Only <c>Committed</c> entries are delivered (<see cref="ApplyLogToConsumerAsync"/>
    /// filters); checkpoints and rolled-back entries just advance the cursor.</para>
    ///
    /// <para>Returns <see langword="false"/> when the drain could not reach
    /// <paramref name="upToIndex"/>: an id absent above the snapshot floor, an unresolved
    /// (<c>Proposed</c>) entry inside the resolved range (its commit marker not yet visible), or
    /// a missing tail. On the follower path this is routine — reads race the write queue and the
    /// leader's re-ship/backfill retries the drain — but at promotion (after the WAL write queue
    /// is fenced) it means the projection genuinely cannot cover the frontier and the caller must
    /// not serve. A no-op returning <see langword="true"/> when <see cref="lastAppliedIndex"/>
    /// already covers <paramref name="upToIndex"/>.</para>
    /// </summary>
    private async Task<bool> DrainCommittedAppliesAsync(long upToIndex, bool skipGaps = false)
    {
        if (upToIndex < 0 || lastAppliedIndex >= upToIndex)
            return true;

        const int BatchSize = 512;
        long from = lastAppliedIndex + 1;

        while (from <= upToIndex)
        {
            List<RaftLog> batch = await wal.GetRangeAllTypesAsync(from, BatchSize).ConfigureAwait(false);
            if (batch.Count == 0)
                break;

            foreach (RaftLog log in batch)
            {
                if (log.Id > upToIndex)
                    return true;
                if (log.Id <= lastAppliedIndex)
                    continue;                       // already applied (defensive; the read starts at 'from')
                if (log.Id != lastAppliedIndex + 1 && !skipGaps)
                {
                    // The expected next id (lastAppliedIndex+1) is absent from the range. Classify by
                    // the snapshot floor rather than by "does an entry exist" — the in-memory commit frontier
                    // (upToIndex) can transiently lead the durable WAL (a write still queued in the WAL
                    // scheduler, or an entry that hole-repair truncated after the frontier overshot it), so an
                    // absent id is ambiguous on its own:
                    //   * expected ABOVE the floor → a real gap (unapplied write lag OR a truncated hole). Both
                    //     are re-shipped by the leader's backfill, so WITHHOLD and let a later drain deliver the
                    //     id and the tail in order. Delivering past it would skip it permanently.
                    //   * expected AT/BELOW the floor, or the -1/0 pre-restore sentinel (below the first log id):
                    //     the id was compacted by a snapshot or never existed. Not a gap — ACCEPT this entry as
                    //     the next contiguous delivery (the cursor advances to it below).
                    // With skipGaps (a sole voter proceeding past an unrecoverable gap), every present
                    // entry is delivered regardless of holes.
                    long floor = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
                    if (lastAppliedIndex + 1 > 0 && lastAppliedIndex + 1 > floor)
                        return false;
                }

                // An unresolved entry inside the resolved range: its commit (or rollback) marker has
                // not landed in the backend yet. Withhold — delivering past it would skip it, and
                // advancing the cursor over it would mark it applied without delivery. With skipGaps
                // the marker is unrecoverable (no peer will re-commit it for a sole voter mid-tenure);
                // the entry stays undelivered but the cursor moves on.
                if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                {
                    if (!skipGaps)
                        return false;

                    if (log.Id > lastAppliedIndex)
                        lastAppliedIndex = log.Id;
                    continue;
                }

                await ApplyLogToConsumerAsync(log).ConfigureAwait(false);
            }

            long next = lastAppliedIndex + 1;
            if (next <= from)   // guard: lastAppliedIndex did not advance past 'from' — would loop forever
                break;
            from = next;
        }

        // The loop can exit with the range uncovered (an empty batch: the tail of the range is
        // absent). A missing tail above the floor is a gap exactly like an interior hole.
        if (lastAppliedIndex < upToIndex && !skipGaps)
        {
            long expected = lastAppliedIndex + 1;
            long floor = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
            if (expected > 0 && expected > floor)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Delivers a single committed WAL entry to the consumer state machine and
    /// advances <see cref="lastAppliedIndex"/>.
    ///
    /// <para>Skips entries whose <see cref="RaftLog.Type"/> is not
    /// <see cref="RaftLogType.Committed"/> (e.g. <c>CommittedCheckpoint</c>), but
    /// still advances the cursor so they are not re-read on subsequent drain calls.</para>
    /// </summary>
    private async Task ApplyLogToConsumerAsync(RaftLog log)
    {
        // Deliver each committed index to the consumer at most once. The cursor still advances below for
        // any id past the frontier (including CommittedCheckpoint entries, which are not delivered), but a
        // re-delivery of an already-applied index — which the follower path can see because the leader
        // re-sends committed entries (commit broadcast + backfill/idle re-ship) — must not reach the
        // consumer twice. See CompleteFollowerAppend for the primary site this guards.
        // Promotion-barrier no-ops are consensus-internal: never delivered, cursor still advances.
        if (log.Type == RaftLogType.Committed && log.Id > lastAppliedIndex
            && log.LogType != RaftSystemConfig.LeadershipBarrierLogType)
        {
            try
            {
                bool ok;
                if (host.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
                    ok = await host.InvokeSystemReplicationReceived(host.PartitionId, log).ConfigureAwait(false);
                else
                    ok = await host.InvokeReplicationReceived(host.PartitionId, log).ConfigureAwait(false);

                if (!ok)
                    host.InvokeReplicationError(host.PartitionId, log);
            }
            catch (Exception ex)
            {
                // A throwing consumer bypasses the false-return InvokeReplicationError path;
                // catch here to ensure the error is always reported and the drain continues.
                logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Consumer threw during apply of log {LogId}: {Message}\n{Stacktrace}",
                    host.LocalEndpoint, host.PartitionId, nodeState, log.Id, ex.Message, ex.StackTrace);
                host.InvokeReplicationError(host.PartitionId, log);
            }
        }

        if (log.Id > lastAppliedIndex)
            lastAppliedIndex = log.Id;

        CompleteReadIndexApplyWaiters();
    }

    /// <summary>
    /// Applies inherited Proposed entries from a prior term in the gap
    /// [<paramref name="from"/>, <paramref name="upToIndex"/>] to the consumer state
    /// machine.  Called at the head of <see cref="CompleteLeaderCommit"/> to deliver
    /// entries that are committed by quorum (the new leader won election with this log)
    /// but have no local proposal waiter and were never touched by
    /// <see cref="CompleteFollowerAppend"/>.
    ///
    /// <para>Only entries from a strictly older term (<see cref="RaftLog.Term"/> &lt;
    /// <c>currentTerm</c>) are delivered; current-term Proposed entries are in-flight
    /// writes that have not yet reached quorum and must not be applied prematurely.</para>
    ///
    /// <para>Reads the WAL via <see cref="IRaftWalFacade.GetRangeAllTypesAsync"/> so that
    /// Proposed entries (whose lazy-commit markers may be absent after a crash on the
    /// single-fsync fast path) are visible.</para>
    ///
    /// <para><b>Gap contract:</b> returns <see cref="InheritedDrainStatus.Hole"/> when an id in the
    /// range is absent above the snapshot floor — a WAL hole. Advancing over it (the old behavior)
    /// would silently skip entries that may be committed elsewhere and mark them applied forever,
    /// leaving the consumer projection permanently incomplete on this node. The caller must not
    /// treat a <c>Hole</c> drain as proof of projection completeness (the barrier completion
    /// reverts the promotion). Ids at/below the floor were compacted and are accepted, exactly as
    /// in <see cref="DrainCommittedAppliesAsync"/>.</para>
    ///
    /// <para><b>In-flight contract:</b> returns <see cref="InheritedDrainStatus.BlockedByInFlight"/>
    /// (without advancing the cursor) when it reaches a current-term <c>Proposed</c> entry. That is
    /// not an inherited orphan but a pipelined proposal still awaiting quorum, and its own
    /// commit/rollback completion delivers it. Advancing the cursor over it here would make that
    /// later delivery hit the exactly-once guard in <see cref="ApplyLogToConsumerAsync"/> and skip
    /// the entry permanently — the leader-only applied-sequence hole found by Jepsen. This applies
    /// even with <paramref name="skipGaps"/>: a sole voter's in-flight proposals still resolve via
    /// self-quorum, so they must not be advanced over either.</para>
    /// </summary>
    private async Task<InheritedDrainStatus> DrainInheritedAppliesAsync(long from, long upToIndex, bool skipGaps = false)
    {
        const int BatchSize = 512;
        long expected = from;

        // Prior-term entries this drain advances over while they are still Proposed on disk. The
        // drain treats them as committed (delivers them to the consumer; the caller serves reads
        // from that state), so their WAL records must be committed DURABLY as well — collected here
        // and re-committed at every exit. Leaving them Proposed is not merely a restart hazard: the
        // backfill read (GetRangeAsync) filters uncommitted entries, so a leader whose inherited
        // range is Proposed on disk silently ships followers an anchored batch that SKIPS that
        // range — the batch lands above the followers' gap, no frontier ever advances, and the
        // partition wedges with no error anywhere (the Jepsen one-stuck-entry shape).
        List<RaftLog>? recommit = null;

        while (from <= upToIndex)
        {
            List<RaftLog> batch = await wal.GetRangeAllTypesAsync(from, BatchSize).ConfigureAwait(false);
            if (batch.Count == 0)
                break;

            foreach (RaftLog log in batch)
            {
                if (log.Id > upToIndex)
                {
                    EnqueueInheritedRecommitMarkers(recommit);
                    return InheritedDrainStatus.Covered;
                }

                if (log.Id > expected && !skipGaps)
                {
                    long floor = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
                    if (expected > 0 && expected > floor)
                    {
                        logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Inherited-entry drain found a WAL hole: expected {Expected}, next present {Present} (floor {Floor}).",
                            host.LocalEndpoint, host.PartitionId, nodeState, expected, log.Id, floor);
                        EnqueueInheritedRecommitMarkers(recommit);
                        return InheritedDrainStatus.Hole;
                    }
                    // Compacted below the floor: accept this entry as the next contiguous delivery.
                }

                // A current-term unresolved entry is a pipelined proposal still in flight, not an
                // inherited orphan: stop without advancing the cursor over it (see the in-flight
                // contract in the summary). The caller defers its batch until this entry resolves.
                if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint && log.Term >= currentTerm)
                {
                    EnqueueInheritedRecommitMarkers(recommit);
                    return InheritedDrainStatus.BlockedByInFlight;
                }

                expected = log.Id + 1;

                // Advancing over a prior-term Proposed entry commits it (Raft §5.4.2: the
                // current-term commit above it proves the prefix); record it for the durable
                // re-commit. Includes prior-term barrier no-ops — never delivered, but the durable
                // frontier must still pass them.
                if (log.Type is RaftLogType.Proposed or RaftLogType.ProposedCheckpoint)
                    (recommit ??= []).Add(log);

                // Apply committed entries and inherited Proposed entries (prior term only).
                // Skip current-term Proposed entries — they are in-flight proposals.
                // Promotion-barrier no-ops (including a prior term's, from a promotion that died
                // before committing its barrier) are consensus-internal and never delivered.
                bool deliver = (log.Type == RaftLogType.Committed ||
                               (log.Type == RaftLogType.Proposed && log.Term < currentTerm))
                               && log.LogType != RaftSystemConfig.LeadershipBarrierLogType;

                // Exactly-once: only deliver entries past the applied frontier (the cursor advances below).
                if (deliver && log.Id > lastAppliedIndex)
                {
                    try
                    {
                        bool ok;
                        if (host.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
                            ok = await host.InvokeSystemReplicationReceived(host.PartitionId, log).ConfigureAwait(false);
                        else
                            ok = await host.InvokeReplicationReceived(host.PartitionId, log).ConfigureAwait(false);

                        if (!ok)
                            host.InvokeReplicationError(host.PartitionId, log);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Consumer threw during inherited-entry apply of log {LogId}: {Message}\n{Stacktrace}",
                            host.LocalEndpoint, host.PartitionId, nodeState, log.Id, ex.Message, ex.StackTrace);
                        host.InvokeReplicationError(host.PartitionId, log);
                    }
                }

                if (log.Id > lastAppliedIndex)
                    lastAppliedIndex = log.Id;
            }

            long next = lastAppliedIndex + 1;
            if (next <= from)   // guard: no progress (e.g. all entries were checkpoints or wrong term)
                break;
            from = next;
        }

        EnqueueInheritedRecommitMarkers(recommit);

        // The loop can also exit without reaching upToIndex (an empty batch: the whole tail of the
        // range is absent). A missing tail above the floor is a hole exactly like an interior gap.
        if (expected <= upToIndex && !skipGaps)
        {
            long floor = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
            if (expected > 0 && expected > floor)
            {
                logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Inherited-entry drain missing the range tail: expected through {UpToIndex}, present through {Expected} (floor {Floor}).",
                    host.LocalEndpoint, host.PartitionId, nodeState, upToIndex, expected - 1, floor);
                return InheritedDrainStatus.Hole;
            }
        }

        CompleteReadIndexApplyWaiters();
        return InheritedDrainStatus.Covered;
    }

    /// <summary>
    /// Durably commits inherited prior-term entries the drain advanced over: writes their commit
    /// markers via <see cref="IRaftWalFacade.EnqueueCommit"/> so the on-disk log converges with the
    /// in-memory decision that they are committed. Without this, the entries stay Proposed on disk:
    /// a leader crash re-loses the applied projection they back, and — worse — the backfill read
    /// filters them out, so followers missing the range are shipped anchored batches that silently
    /// skip it and the partition wedges (see the note at the collection site). Lazy like all commit
    /// markers on the single-fsync path: the enqueue is not awaited for durability, and a
    /// backpressure rejection only defers the repair to the next drain — delivery already happened,
    /// so failing the drain over it would be strictly worse. Clears the list so the multiple drain
    /// exit paths cannot double-enqueue.
    /// </summary>
    private void EnqueueInheritedRecommitMarkers(List<RaftLog>? inherited)
    {
        if (inherited is null || inherited.Count == 0)
            return;

        try
        {
            WALWriteOperation operation = wal.EnqueueCommit(inherited);

            Scheduling.RaftPendingWalOperation pending = RentPendingWalOp();
            pending.IsInheritedRecommit = true;
            pendingWalOperations[operation.OperationId] = pending;

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Durably re-committing {Count} inherited prior-term entries ({First}..{Last})",
                    host.LocalEndpoint, host.PartitionId, nodeState, inherited.Count, inherited[0].Id, inherited[^1].Id);
        }
        catch (Exception ex)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Could not enqueue durable re-commit of {Count} inherited entries ({Message}) — the on-disk range stays Proposed and unbackfillable until the next promotion retries.",
                host.LocalEndpoint, host.PartitionId, nodeState, inherited.Count, ex.Message);
        }

        inherited.Clear();
    }

    /// <summary>
    /// Result of <see cref="DrainInheritedAppliesAsync"/>. The three outcomes demand different
    /// caller responses, which is why this is not a bool: <see cref="Hole"/> is retryable (a write
    /// may still be queued behind the read) and disqualifying if it persists, while
    /// <see cref="BlockedByInFlight"/> is neither — retrying cannot resolve an in-flight proposal
    /// (its own completion does), and treating it as disqualifying would step the leader down on
    /// ordinary pipelined load.
    /// </summary>
    private enum InheritedDrainStatus
    {
        /// <summary>The cursor covers the requested range; the caller's batch can be applied.</summary>
        Covered,

        /// <summary>An id in the range is absent above the snapshot floor — a genuine WAL hole or a
        /// write still queued in the WAL scheduler; indistinguishable from the read side.</summary>
        Hole,

        /// <summary>The drain reached a current-term Proposed entry: a pipelined proposal still in
        /// flight. The cursor was NOT advanced over it; the caller must defer its batch via
        /// <see cref="DeferLeaderApplies"/> until the in-flight entry resolves.</summary>
        BlockedByInFlight,
    }

    /// <summary>
    /// Parks a leader batch (committed or rolled-back entries) whose completion arrived while an
    /// earlier current-term proposal was still unresolved below it. Flushed in id order by
    /// <see cref="FlushDeferredLeaderAppliesAsync"/> once the applied cursor reaches the batch.
    /// Copies the list: proposals and their pending-operation envelopes are pooled, so retaining
    /// <c>proposal.Logs</c> itself would alias a buffer that gets recycled. The <see cref="RaftLog"/>
    /// instances are safe to retain (not pooled), and <see cref="RaftWriteAhead.EnqueueCommit"/> /
    /// <c>EnqueueRollback</c> already stamped their final types, so a later flush delivers (or
    /// advances over) them correctly.
    /// </summary>
    private void DeferLeaderApplies(long minLogIndex, List<RaftLog> logs)
    {
        if (deferredLeaderAppliesTerm != currentTerm)
        {
            deferredLeaderApplies.Clear();
            deferredLeaderAppliesTerm = currentTerm;
        }

        deferredLeaderApplies[minLogIndex] = new List<RaftLog>(logs);

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] Deferring apply of batch starting at {MinLogIndex}: an earlier proposal below it is still in flight (applied cursor {LastApplied}).",
                host.LocalEndpoint, host.PartitionId, nodeState, minLogIndex, lastAppliedIndex);
    }

    /// <summary>
    /// Delivers deferred out-of-order leader batches that have become contiguous with the applied
    /// cursor, in id order. Called wherever the leader path advances the cursor (commit and
    /// rollback completions): the batch just applied may have been the in-flight blocker that
    /// earlier out-of-order completions deferred behind. Per-log exactly-once is preserved by the
    /// cursor guard inside <see cref="ApplyLogToConsumerAsync"/>. Clears the buffer wholesale when
    /// the term has moved on — the WAL-based drains own delivery after a step-down, and a stale
    /// rolled-back range could since have been re-proposed at the same ids.
    /// </summary>
    private async ValueTask FlushDeferredLeaderAppliesAsync()
    {
        if (deferredLeaderApplies.Count == 0)
            return;

        if (deferredLeaderAppliesTerm != currentTerm)
        {
            deferredLeaderApplies.Clear();
            return;
        }

        while (deferredLeaderApplies.Count > 0)
        {
            KeyValuePair<long, List<RaftLog>> next = deferredLeaderApplies.First();
            if (next.Key > lastAppliedIndex + 1)
                break;

            deferredLeaderApplies.Remove(next.Key);

            foreach (RaftLog log in next.Value)
                await ApplyLogToConsumerAsync(log).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Resets <see cref="lastProposalAt"/> to <see cref="HLCTimestamp.Zero"/>.  Test-only;
    /// used to assert that the quiesce guard correctly blocks when no proposal history exists.
    /// </summary>
    public void ClearLastProposalAtForTesting()
    {
        lastProposalAt = HLCTimestamp.Zero;
        lastProposalAtTicks = 0;
    }

    private RaftNode? SelectStepDownTarget()
    {
        RaftNode? selected = null;
        long selectedCommitIndex = long.MinValue;

        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                continue;

            long commitIndex = lastCommitIndexes.GetValueOrDefault(
                node.Endpoint,
                startCommitIndexes.GetValueOrDefault(node.Endpoint, 0));

            if (selected is null ||
                commitIndex > selectedCommitIndex ||
                (commitIndex == selectedCommitIndex &&
                 string.CompareOrdinal(node.Endpoint, selected.Endpoint) < 0))
            {
                selected = node;
                selectedCommitIndex = commitIndex;
            }
        }

        return selected;
    }

    public async Task ReceiveStepDownNoticeAsync(StepDownNoticeRequest request)
    {
        if (currentTerm > request.Term)
            return;

        // Membership fence: only a committed roster member can have been a leader, so a step-down
        // notice from a non-member must not be able to clear our leader and force an election.
        if (!host.IsMember(request.Endpoint))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring StepDownNotice from non-member {Endpoint} Term={Term}", host.LocalEndpoint, host.PartitionId, nodeState, request.Endpoint, request.Term);
            return;
        }

        if (!string.IsNullOrEmpty(host.Leader) && host.Leader != request.Endpoint)
            return;

        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, request.Time);

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        currentTerm = Math.Max(currentTerm, request.Term);
        votingStartedAt = HLCTimestamp.Zero;
        votingStartedTicks = 0;
        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        localCommittedIndex = -1;
        activeProposals.Clear();
        lastHeartbeat = HLCTimestamp.Zero;
        lastHeartbeatTicks = 0;

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);
        await StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
    }

    public async Task ReceiveTransferLeadershipAsync(TransferLeadershipRequest request)
    {
        if (request.TargetEndpoint != host.LocalEndpoint)
            return;

        if (currentTerm > request.Term)
            return;

        // Membership fence: only the current leader (necessarily a roster member) may hand us
        // leadership; a non-member must not be able to trigger a disruptive election.
        if (!host.IsMember(request.Endpoint))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring TransferLeadership from non-member {Endpoint} Term={Term}", host.LocalEndpoint, host.PartitionId, nodeState, request.Endpoint, request.Term);
            return;
        }

        if (!string.IsNullOrEmpty(host.Leader) && host.Leader != request.Endpoint)
            return;

        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, request.Time);

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        currentTerm = Math.Max(currentTerm, request.Term);
        votingStartedAt = HLCTimestamp.Zero;
        votingStartedTicks = 0;
        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        localCommittedIndex = -1;
        FailAllActiveProposalWaiters();
        activeProposals.Clear();
        lastHeartbeat = HLCTimestamp.Zero;
        lastHeartbeatTicks = 0;

        await StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
    }

    public async Task ForceLeaderForTestingAsync(ulong? replyCorrelationId)
    {
        if (nodeState == RaftNodeState.Leader && host.Leader == host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        localCommittedIndex = -1;
        votes.Clear();
        activeProposals.Clear();

        long nowTicks = host.GetMonotonicTimestamp();

        nodeState = RaftNodeState.Candidate;
        host.Leader = "";
        votingStartedAt = currentTime;
        votingStartedTicks = nowTicks;
        lastHeartbeat = currentTime;
        lastHeartbeatTicks = nowTicks;
        currentTerm++;

        IncreaseVotes(host.LocalEndpoint, currentTerm);

        // B2b: durably record the new term and our self-vote before we solicit votes or become leader, so
        // a crash mid-election cannot restart at a stale term or let us vote for someone else this term.
        await wal.PersistHardStateAsync(currentTerm, host.LocalEndpoint).ConfigureAwait(false);

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

        if (host.Nodes.Count == 0)
        {
            // published == false means a promotion barrier is pending; with no peers the barrier
            // commits locally via the WAL scheduler (self-quorum) and CompleteLeaderCommit fires
            // both the publish and the LeaderChanged notification shortly after.
            bool published = await BecomeLeaderAsync().ConfigureAwait(false);
            if (published)
                await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint).ConfigureAwait(false);
            await SendHeartbeat(true).ConfigureAwait(false);

            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, 0L));
            return;
        }

        await RequestVotesAsync(currentTime, currentTerm).ConfigureAwait(false);
        CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Pending, 0L));
    }

    private long GetKnownRemoteMaxLogId(string endpoint) =>
        Math.Max(
            lastCommitIndexes.GetValueOrDefault(endpoint, -1),
            startCommitIndexes.GetValueOrDefault(endpoint, -1));

    /// <summary>
    /// Raft §5.4.1 log-freshness comparison: returns <see langword="true"/> when the candidate's
    /// <c>(lastLogTerm, lastLogIndex)</c> is <b>strictly less up-to-date</b> than our local
    /// <c>(localLastLogTerm, localMaxId)</c> — i.e. a strictly higher last-log term wins, and only on
    /// an equal last-log term does the higher index win. This is the check that stops a candidate whose
    /// larger index hides an older last term from being elected over a more current voter.
    /// <para><b>Wire compatibility (B5 rollout):</b> a <paramref name="remoteLastLogTerm"/> ≤ 0 means the
    /// peer predates the last-log-term field (sends 0) or genuinely has an empty log; in both cases we
    /// cannot trust the term and fall back to index-only comparison, exactly matching the pre-B5 ordering
    /// so a mixed-version cluster never mis-orders. An empty-log candidate (index 0) loses that fallback
    /// against any non-empty voter anyway.</para>
    /// </summary>
    /// <summary>
    /// B3: local elapsed time since a monotonic anchor. Returns <see cref="TimeSpan.MaxValue"/> when the
    /// anchor is unset (0) so an "unset" anchor never satisfies a "&lt; timeout" freshness guard — matching
    /// the historical <c>lastHeartbeat != HLCTimestamp.Zero &amp;&amp; …</c> shape. Callers still gate on the
    /// explicit <c>anchorTicks != 0</c> where the old code gated on <c>!= Zero</c>, for symmetry.
    /// </summary>
    private static TimeSpan MonotonicElapsed(long anchorTicks, long nowTicks) =>
        anchorTicks == 0 ? TimeSpan.MaxValue : Stopwatch.GetElapsedTime(anchorTicks, nowTicks);

    private static bool CandidateLogIsBehind(long remoteLastLogTerm, long remoteMaxLogId, long localLastLogTerm, long localMaxId)
    {
        if (remoteLastLogTerm <= 0)
            return remoteMaxLogId < localMaxId; // legacy peer / empty candidate log → index-only

        if (remoteLastLogTerm != localLastLogTerm)
            return remoteLastLogTerm < localLastLogTerm;

        return remoteMaxLogId < localMaxId;
    }

    /// <summary>
    /// The (index, term) log position this node advertises — and compares against — for Raft §5.4.1
    /// election freshness. Uses the WAL's contiguous-presence frontier, NOT the raw max id: the
    /// unanchored live-propose broadcast can write a lone high entry over a gap on a behind
    /// follower, and a raw-max comparison would let that node win an election while missing an
    /// arbitrary committed range — it would then serve as leader with an incomplete consumer
    /// projection (the §5.4.1 proof assumes contiguous logs). Falls back to the raw max id and
    /// last-entry term when the facade does not track presence (test stubs), preserving the legacy
    /// ordering there.
    /// </summary>
    private async ValueTask<(long MaxLogId, long LastLogTerm)> GetFreshnessLogPositionAsync()
    {
        long presentId = wal.GetPresentIndex();
        if (presentId >= 0)
            return (presentId, wal.GetPresentTerm());

        return (
            await wal.GetMaxLogAsync().ConfigureAwait(false),
            await wal.GetCurrentTermAsync().ConfigureAwait(false)
        );
    }

    /// <summary>
    /// Returns the last commit index for <paramref name="endpoint"/>:
    /// • If endpoint equals <see cref="IRaftHost.LocalEndpoint"/> (i.e., this is the leader asking about itself),
    ///   returns <see cref="localCommittedIndex"/> — the leader's own durable commit frontier.
    /// • Otherwise returns the last index reported by that follower via <c>CompleteAppendLogs</c>,
    ///   or -1 when no acknowledgement has been received yet.
    /// Must be called on the executor thread (reads private state machine fields).
    /// </summary>
    /// <summary>
    /// Returns the follower's last committed index, or <c>long.MinValue</c> when the follower
    /// has never sent a <c>CompleteAppendLogs</c> for this partition (key absent from
    /// <see cref="lastCommitIndexes"/>).
    /// <para>
    /// The <c>long.MinValue</c> sentinel lets callers distinguish "not a participant" from
    /// "participant with no committed entries yet (−1)".  <see cref="RaftPartition"/> maps it
    /// to −1 for the non-nullable API and to <c>null</c> for the nullable API.
    /// </para>
    /// </summary>
    internal long GetFollowerCommittedIndex(string endpoint)
    {
        if (endpoint == host.LocalEndpoint)
            return localCommittedIndex;
        if (lastCommitIndexes.TryGetValue(endpoint, out long idx))
            return idx;
        return long.MinValue; // sentinel: never heard from on this partition
    }

    private async Task StartElectionAsync(HLCTimestamp currentTime, bool ignoreRecentVoteCooldown)
    {
        // Two gates: the roster role (a cluster Learner/Leaving node never campaigns anywhere)
        // and the per-partition voter check (under replica placement a roster Voter may be only
        // a Learner/Removing replica of THIS range — campaigning would inflate the range's
        // quorum with a vote the committed replica set does not grant it).
        if (host.LocalRole != ClusterMemberRole.Voter || !host.IsVoter(host.LocalEndpoint))
        {
            logger.LogDebugSuppressingElection(host.LocalEndpoint, host.PartitionId, nodeState, host.LocalRole);
            return;
        }

        // Startup-join safety: an empty Nodes set means "single-node cluster" ONLY once discovery has
        // actually run. A seed-joining node starts with empty Nodes until the first UpdateNodes loads
        // its peers; self-electing in that window makes it win P0 leadership as a single-node quorum
        // and the existing cluster then follows it — but the joiner has an empty log (no partition
        // map), so the join deadlocks. Suppress the election until discovery reports (peers ⇒ normal
        // quorum election; genuinely none ⇒ legitimate single-node self-election). Does not affect a
        // real single-node cluster: its first UpdateNodes sets InitialNodesDiscovered with Nodes still
        // empty, so the very next tick elects.
        if (host.Nodes.Count == 0 && !host.InitialNodesDiscovered)
        {
            logger.LogDebugSuppressingElection(host.LocalEndpoint, host.PartitionId, nodeState, host.LocalRole);
            return;
        }

        long nowTicks = host.GetMonotonicTimestamp();

        if (!ignoreRecentVoteCooldown)
        {
            // B3: the recent-vote cooldown is a local elapsed interval → monotonic.
            if (lastVotationTicks != 0 && (MonotonicElapsed(lastVotationTicks, nowTicks) < (electionTimeout * 2)))
                return;

            string expectedLeader = expectedLeaders.GetValueOrDefault(currentTerm, "");
            if (!string.IsNullOrEmpty(expectedLeader))
            {
                // NOTE (B3 residual): GetLastNodeActivity returns an HLC written locally on the last
                // AppendLogs from this peer. The "heard from the leader recently" decision below is still an
                // HLC subtraction and remains mildly skew-sensitive — the peer-activity store migration to
                // monotonic ticks was deliberately deferred (contained B3 scope). On suppression we refresh
                // BOTH the HLC anchor and its monotonic shadow so the monotonic follower election gate
                // honours the back-off; the residual only affects whether we take this branch at all.
                HLCTimestamp lastKnownHeartbeat = host.GetLastNodeActivity(expectedLeader, host.PartitionId);

                if (lastKnownHeartbeat != HLCTimestamp.Zero && ((currentTime - lastKnownHeartbeat) < electionTimeout))
                {
                    lastHeartbeat = lastKnownHeartbeat;
                    lastHeartbeatTicks = nowTicks;
                    return;
                }
            }
        }

        // No global "am I outdated?" pre-election veto here (removed): candidate eligibility is decided
        // per-voter by the RequestVote log-freshness predicate in VoteAsync. The old veto compared our
        // WAL max against the maximum ever recorded in startCommitIndexes — a dictionary that is never
        // pruned, so a peer that once advertised a higher (possibly uncommitted) tail and then
        // failed/left permanently suppressed every election even when the survivors held a valid quorum.

        // A real election is starting: discard any open pre-vote round so a stale
        // pre-grant set for an old hypothetical term can't bleed into this one.
        ResetPreVoteRound();

        nodeState = RaftNodeState.Candidate;
        host.Leader = "";
        expectedLeaders.Clear();
        votingStartedAt = currentTime;
        votingStartedTicks = nowTicks;

        await host.InvokeLeaderChanged(host.PartitionId, "");

        currentTerm++;

        IncreaseVotes(host.LocalEndpoint, currentTerm);

        // B2b: durably record the new term and our self-vote before soliciting votes (see the same call
        // in ForceLeaderForTestingAsync for rationale).
        await wal.PersistHardStateAsync(currentTerm, host.LocalEndpoint).ConfigureAwait(false);

        double delayMs = lastHeartbeatTicks != 0
            ? MonotonicElapsed(lastHeartbeatTicks, nowTicks).TotalMilliseconds
            : 0;

        TagList electionTags = new() { { "partition_id", host.PartitionId } };
        KommanderMetrics.ElectionsStartedTotal.Add(1, electionTags);
        KommanderMetrics.ElectionDelayMs.Record(delayMs, electionTags);

        logger.LogWarnVotedToBecomeLeader(host.LocalEndpoint, host.PartitionId, nodeState, delayMs, currentTerm);

        if (host.Nodes.Count == 0)
        {
            nextIndex.Clear();
            matchIndex.Clear();
            regressedFrontiers.Clear();
            // published == false: barrier pending, self-quorum commit publishes shortly after
            // (see CompleteLeaderCommit), which also fires the LeaderChanged notification.
            bool published = await BecomeLeaderAsync().ConfigureAwait(false);
            if (published)
                await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint).ConfigureAwait(false);
            await SendHeartbeat(true).ConfigureAwait(false);
            return;
        }

        await RequestVotesAsync(currentTime, currentTerm).ConfigureAwait(false);
    }

    /// <summary>
    /// The pre-election (Raft §9.6) that gates a real election. Before bumping the term and
    /// becoming a Candidate, a follower whose leader went silent first runs a side-effect-free
    /// probe: it asks peers whether they *would* vote for it at <c>currentTerm + 1</c> given its
    /// current log, WITHOUT changing its own term/state. Only a pre-vote quorum (tallied in
    /// <see cref="ReceivedVoteAsync"/>) promotes to <see cref="StartElectionAsync"/>. This is what
    /// stops a stale or partitioned node from repeatedly inflating its term and disrupting a healthy
    /// leader — the livelock this whole change targets.
    /// </summary>
    private async Task StartPreVoteAsync(HLCTimestamp currentTime)
    {
        // Mirrors StartElectionAsync: roster role plus per-partition voter check — a node that
        // is not a Voter replica of this range must not campaign for it.
        if (host.LocalRole != ClusterMemberRole.Voter || !host.IsVoter(host.LocalEndpoint))
        {
            logger.LogDebugSuppressingPreVote(host.LocalEndpoint, host.PartitionId, nodeState, host.LocalRole);
            return;
        }

        long nowTicks = host.GetMonotonicTimestamp();

        // Same "should I even try?" guards as a real election. These guards do NOT touch any Raft
        // consensus state (currentTerm / votes / expectedLeaders / nodeState) — that is the whole
        // point of pre-vote. The one local write below (lastHeartbeat) is a back-off bookkeeping
        // refresh on the "leader still fresh" path, mirroring StartElectionAsync, not a consensus
        // mutation: it just records that we observed the leader so we don't immediately re-trigger.
        // B3: the recent-vote cooldown is a local elapsed interval → monotonic.
        if (lastVotationTicks != 0 && (MonotonicElapsed(lastVotationTicks, nowTicks) < (electionTimeout * 2)))
            return;

        string expectedLeader = expectedLeaders.GetValueOrDefault(currentTerm, "");
        if (!string.IsNullOrEmpty(expectedLeader))
        {
            // B3 residual (same as StartElectionAsync): the "heard from leader recently" test is still an
            // HLC subtraction off the HLC peer-activity store; on back-off we refresh the monotonic shadow.
            HLCTimestamp lastKnownHeartbeat = host.GetLastNodeActivity(expectedLeader, host.PartitionId);

            if (lastKnownHeartbeat != HLCTimestamp.Zero && ((currentTime - lastKnownHeartbeat) < electionTimeout))
            {
                // Intentional: back off and remember we saw the leader. Not a consensus mutation.
                lastHeartbeat = lastKnownHeartbeat;
                lastHeartbeatTicks = nowTicks;
                return;
            }
        }

        // No global "am I outdated?" pre-election veto here (removed): a pre-vote is side-effect-free by
        // design, so a genuinely-behind node can safely probe — its peers deny the pre-vote via the
        // per-voter log check in VoteAsync and it never reaches quorum. The old veto instead consulted
        // the never-pruned startCommitIndexes max, which let a departed peer's stale tail suppress every
        // pre-vote forever.

        // No peers to probe: there is nothing a pre-vote can tell us, so go straight to a real election.
        if (host.Nodes.Count == 0)
        {
            await StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
            return;
        }

        // Open a fresh pre-vote round for the hypothetical next term and seed our own pre-grant.
        electionPhase = RaftElectionPhase.PreVote;
        preVoteTerm = currentTerm + 1;
        preVotes.Clear();
        preVotes.Add(host.LocalEndpoint);

        logger.LogInfoStartingPreVoteRound(host.LocalEndpoint, host.PartitionId, nodeState, preVoteTerm);

        await RequestVotesAsync(currentTime, preVoteTerm, preVote: true).ConfigureAwait(false);
    }

    /// <summary>
    /// Requests votes from the other known nodes in the cluster. Shared by both the real election
    /// (<paramref name="preVote"/> = false) and the side-effect-free pre-vote probe
    /// (<paramref name="preVote"/> = true, Raft §9.6). The only difference on the wire is the
    /// <see cref="RequestVotesRequest.PreVote"/> flag and the <paramref name="term"/> used (the
    /// real <see cref="currentTerm"/> for an election, the hypothetical <c>currentTerm + 1</c> for a probe).
    /// </summary>
    /// <param name="timestamp"></param>
    /// <param name="term">Term to advertise: <see cref="currentTerm"/> for a real election, the hypothetical next term for a pre-vote.</param>
    /// <param name="preVote">When true the outbound request is marked as a pre-vote probe.</param>
    /// <exception cref="RaftException"></exception>
    private async Task RequestVotesAsync(HLCTimestamp timestamp, long term, bool preVote = false)
    {
        IReadOnlyList<RaftNode> nodes = host.Nodes;

        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to vote", host.LocalEndpoint, host.PartitionId, nodeState);
            return;
        }

        (long currentMaxLog, long currentLastLogTerm) = await GetFreshnessLogPositionAsync().ConfigureAwait(false);

        RequestVotesRequest request = new(host.PartitionId, term, currentMaxLog, currentLastLogTerm, timestamp, host.LocalEndpoint, preVote);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            logger.LogInfoAskedForVotes(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, term);

            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.RequestVotes, node, request));
        }
    }

    /// <summary>
    /// Sends a heartbeat message to follower nodes to indicate that the leader node in the partition is still alive.
    /// </summary>
    /// <param name="force"></param>
    /// <exception cref="RaftException"></exception>
    private async Task SendHeartbeat(bool force)
    {
        if (!force && heartbeatsSuspendedForTesting)
            return;
        if (!force && quiesced)
            return;

        IReadOnlyList<RaftNode> nodes = host.Nodes;

        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send hearthbeat", host.LocalEndpoint, host.PartitionId, nodeState);
            return;
        }

        HLCTimestamp prevHeartbeat = lastHeartbeat;
        lastHeartbeat = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();
        lastHeartbeatTicks = nowTicks;

        // "Live replication is quiet": no proposal has been issued for at least one heartbeat
        // interval (or we have never proposed as this leader). While writes are flowing, a follower
        // that trails by a few entries is simply mid-flight on the live-propose broadcast and will
        // converge on its own, so the small-gap backfill below stays disabled to avoid redundant WAL
        // reads. Once writes pause, that live path can no longer heal a residual tail gap — a follower
        // that missed the final committed entry (e.g. it was briefly unreachable at commit time) would
        // otherwise stay permanently behind, because empty heartbeats carry no entries and the
        // threshold-gated backfill never fires for a sub-threshold gap.
        bool liveReplicationQuiet = lastProposalAtTicks == 0
            || MonotonicElapsed(lastProposalAtTicks, nowTicks) >= host.Configuration.HeartbeatInterval;

        if (nodeState != RaftNodeState.Leader && nodeState != RaftNodeState.Candidate)
            return;

        TagList heartbeatTags = new() { { "partition_id", host.PartitionId } };
        KommanderMetrics.HeartbeatsSentTotal.Add(1, heartbeatTags);

        if (prevHeartbeat != HLCTimestamp.Zero)
            KommanderMetrics.HeartbeatDelayMs.Record(
                (lastHeartbeat - prevHeartbeat).TotalMilliseconds, heartbeatTags);

        // Shared across this round's followers: lagging peers are usually anchored at the same index,
        // so the range is read from the WAL (and Protobuf-encoded) once instead of once per follower.
        // Allocated lazily — a healthy round backfills nobody.
        BackfillRoundBatches? backfillRound = null;

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            if (host.PartitionId != RaftSystemConfig.SystemPartition && !force)
            {
                HLCTimestamp lastHearthBeatToNode = host.GetLastNodeHearthbeat(node.Endpoint, host.PartitionId);

                if (lastHearthBeatToNode != HLCTimestamp.Zero && ((lastHeartbeat - lastHearthBeatToNode) <= host.Configuration.RecentHeartbeat))
                    continue;
            }

            host.UpdateLastHeartbeat(node.Endpoint, host.PartitionId, lastHeartbeat);

            // Backfill: ship up to MaxBackfillEntriesPerRound committed entries instead of an empty
            // heartbeat so the follower converges without waiting for new writes.
            // TrySendBackfillBatchAsync handles nextIndex selection and the Log Matching anchors.
            //
            // Two triggers:
            //   * gap > BackfillThreshold — an actively-behind follower (join catch-up, long
            //     partition) is streamed forward regardless of write activity.
            //   * gap >= 1 && liveReplicationQuiet && a live commit exists above liveCommitFloor —
            //     once writes pause, even a single missed tail entry must be re-shipped explicitly;
            //     the live-propose broadcast is done and empty heartbeats can never deliver it. The
            //     liveCommitFloor guard confines this to entries committed during this term: a leader
            //     does not push merely-restored committed state to a follower until a new write occurs
            //     (that is the highest-WAL election-preference contract). Gating on quiet also keeps
            //     steady-state writes free of the per-heartbeat WAL read a healthy in-flight follower
            //     would otherwise incur.
            // localCommittedIndex is in-memory and always reflects only durably committed entries.
            long followerGap = lastCommitIndexes.TryGetValue(node.Endpoint, out long followerMaxLog)
                ? localCommittedIndex - followerMaxLog
                : 0;
            bool idleTailGap = followerGap > 0 && liveReplicationQuiet && localCommittedIndex > liveCommitFloor;

            // Crash-restart re-supply (paced): CompleteAppendLogsAsync recorded that this peer reported a
            // committed frontier below its recorded matchIndex (lost lazy markers on restart). The repair
            // runs here, once per heartbeat, rather than inline on every ack — the inline form livelocked
            // the cluster under load. Anchor at the recorded frontier (nextIndex tracks the monotonic
            // matchIndex and still points ABOVE the regressed range, so it would skip exactly what
            // regressed). The note is cleared whether or not a batch went out: if the peer is still
            // behind, its next ack re-records it; if the WAL read came back empty (compacted past the
            // frontier), the snapshot fallback below takes over.
            bool regressed = regressedFrontiers.TryGetValue(node.Endpoint, out long regressedFrontier);
            if (regressed)
                regressedFrontiers.Remove(node.Endpoint);

            bool willBackfill = nodeState == RaftNodeState.Leader
                && localCommittedIndex >= 0
                && (followerGap > host.Configuration.BackfillThreshold || idleTailGap || regressed);

            // DIAGNOSTIC (see FINDINGS.md #3/#5): records every input to the decision above, so a
            // run in which replicas stop advancing can be read for *why* the leader sent nothing
            // rather than inferred from its silence. `followerMaxLog` is the interesting one — it
            // is the leader's belief about the peer, and every trigger here is derived from it.
            LogBackfillDecision(node.Endpoint, willBackfill, followerMaxLog, followerGap,
                                idleTailGap, regressed, liveReplicationQuiet);

            if (willBackfill)
            {
                long anchorFrom = regressed ? regressedFrontier : followerMaxLog;
                backfillRound ??= new();
                if (await TrySendBackfillBatchAsync(node, anchorFrom, lastHeartbeat, anchorToFollowerFrontier: regressed, round: backfillRound).ConfigureAwait(false))
                    continue;

                // Empty batch: the leader has compacted past followerMaxLog+1.
                // If a StateMachineTransfer is registered and no snapshot is already in flight
                // for this follower, kick off an async snapshot transfer. The in-flight guard
                // prevents duplicate transfers; the postToExecutor callback will advance
                // lastCommitIndexes[endpoint] once the follower confirms installation.
                long lastCheckpoint = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
                bool p0System = host.PartitionId == RaftSystemConfig.SystemPartition && host.SystemStateTransfer is not null;
                if (lastCheckpoint > 0 && (host.StateMachineTransfer is not null || p0System))
                {
                    // LastIncludedTerm = the term of the entry at the checkpoint index (may be -1 if
                    // compacted away, in which case the receiver falls back to its own matching rules).
                    // LeaderTerm = this leader's currentTerm so the follower can apply leader-RPC term rules.
                    long lastIncludedTerm = await wal.GetAnyTermAtAsync(lastCheckpoint).ConfigureAwait(false);
                    snapshotSender.TrySend(node, lastCheckpoint, currentTerm, lastIncludedTerm);
                }
            }

            AppendLogToNode(node, lastHeartbeat, null);
        }
    }

    /// <summary>
    /// True when at least one peer is known — or not yet known — to hold less than this leader's
    /// committed frontier. Gates quiescence.
    ///
    /// <para><b>Why this gate is load-bearing.</b> Quiescing stops <see cref="SendHeartbeat"/>, and
    /// <see cref="SendHeartbeat"/> hosts the <em>only</em> catch-up path (both the
    /// <c>BackfillThreshold</c> stream and the idle-tail-gap re-ship). A leader that quiesces while a
    /// follower or learner is still behind therefore strands that peer permanently: empty heartbeats
    /// stop, no propose broadcast is coming on an idle partition, and nothing else will ever ship the
    /// missing entries. The partition goes silent with every executor idle and never converges — the
    /// exact signature of the hang this guard fixes. Only quiesce once every peer has demonstrably
    /// reached our frontier.</para>
    ///
    /// <para>A peer with no recorded progress counts as lagging: absence of evidence is not evidence of
    /// convergence, and treating it as caught-up is what would strand a freshly-joined learner. The
    /// <c>localCommittedIndex &lt;= 0</c> short-circuit keeps a genuinely empty partition (elected but
    /// never written) quiescible, which is the common idle case quiescence exists to serve.</para>
    /// </summary>
    private bool HasLaggingPeer()
    {
        if (localCommittedIndex <= 0)
            return false;

        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                continue;

            if (!lastCommitIndexes.TryGetValue(node.Endpoint, out long peerCommittedIndex)
                || peerCommittedIndex < localCommittedIndex)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Broadcasts a quiesce-flagged empty AppendLogs to all peers, signalling them to switch from
    /// the heartbeat timer to SWIM-based election gating.  Called once when the leader decides
    /// to suppress per-partition heartbeats for an idle partition.
    /// </summary>
    private void SendQuiesceMarker(HLCTimestamp timestamp)
    {
        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            AppendLogToNode(node, timestamp, null, quiesce: true);
        }
    }

    /// <summary>
    /// After the partition startup a handshake is sent to the other nodes to
    /// verify if we have the most recent logs and the node id is unique
    /// </summary>
    /// <param name="remoteNodeId"></param>
    /// <param name="endpoint"></param>
    /// <param name="remoteMaxLogId"></param>
    public void ReceiveHandshake(int remoteNodeId, string endpoint, long remoteMaxLogId)
    {
        // Membership fence: handshakes are best-effort (droppable, re-sent) and a joiner is a
        // committed Learner before its partitions start, so a non-member's handshake can be safely
        // ignored. Checked before the NodeId-collision exit so an unadmitted node with a duplicated
        // NodeId cannot kill a cluster member's process, and before startCommitIndexes so a
        // non-member never pollutes step-down target selection.
        if (!host.IsMember(endpoint))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring Handshake from non-member {Endpoint} NodeId={NodeId}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, remoteNodeId);
            return;
        }

        if (host.LocalNodeId == remoteNodeId)
        {
            logger.LogCritSameNodeId(host.LocalEndpoint, host.PartitionId, nodeState, host.LocalNodeId, remoteNodeId);

            Environment.Exit(1);
            return;
        }

        logger.LogInfoReceivedHandshake(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, remoteNodeId, remoteMaxLogId);

        startCommitIndexes[endpoint] = remoteMaxLogId;
    }

    /// <summary>
    /// Discards every piece of per-follower replication progress recorded for
    /// <paramref name="endpoint"/>. Invoked when the committed roster (re)admits the member:
    /// retained progress predates the (re)admission and may describe a log the member no longer
    /// holds (an evicted node typically rejoins with reset state). Stale progress is worse than
    /// none — <see cref="HasLaggingPeer"/> would read the member as caught-up, keep the partition
    /// quiesced, and starve it of the heartbeats that are its only catch-up path. After the reset
    /// the member counts as lagging, so a quiesced leader re-arms heartbeats immediately (the
    /// un-quiesce also re-enters the manager's hot set via the quiesce callback) and backfill
    /// re-anchors from the frontier the follower actually reports.
    /// No-op on non-leaders: followers/candidates rebuild this state on their next election win.
    /// </summary>
    public void ResetFollowerProgress(string endpoint)
    {
        if (nodeState != RaftNodeState.Leader || string.IsNullOrEmpty(endpoint) || endpoint == host.LocalEndpoint)
            return;

        bool hadProgress = lastCommitIndexes.Remove(endpoint);
        nextIndex.Remove(endpoint);
        matchIndex.Remove(endpoint);
        regressedFrontiers.Remove(endpoint);
        startCommitIndexes.Remove(endpoint);

        if ((hadProgress || quiesced) && logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "[{LocalEndpoint}/{PartitionId}/{State}] Reset replication progress for (re)admitted member {Endpoint} (hadProgress={HadProgress}, wasQuiesced={WasQuiesced})",
                host.LocalEndpoint, host.PartitionId, nodeState, endpoint, hadProgress, quiesced);

        // Waking here (rather than waiting for the next safety sweep to notice the lagging peer)
        // bounds the member's starvation window by the heartbeat interval instead of the sweep period.
        if (quiesced)
            SetQuiesced(false);
    }

    /// <summary>
    /// Sends a handshake to every node available in the cluster to verify if we have the most recent logs.
    /// </summary>
    /// <exception cref="RaftException"></exception>
    private async Task SendHandshakeAsync()
    {
        IReadOnlyList<RaftNode> nodes = host.Nodes;
        
        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to send handshake", host.LocalEndpoint, host.PartitionId, nodeState);
            return;
        }
        
        long localMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);
        
        HandshakeRequest request = new(host.LocalNodeId, host.PartitionId, localMaxId, host.LocalEndpoint);
        
        int number = 0;
        
        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            logger.LogDebugSendingHandshake(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, ++number);
            
            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Handshake, node, request));
        }
    }

    /// <summary>
    /// When another node requests our vote, we verify that the term is valid and the commitIndex is
    /// higher than ours to ensure we don't elect outdated nodes as leaders.
    ///
    /// When <paramref name="preVote"/> is true this answers a side-effect-free pre-election probe
    /// (Raft §9.6): we evaluate the §3 grant predicate and, on grant, reply with a
    /// <see cref="VoteRequest"/> carrying <c>PreVote=true</c> — but we must NOT mutate any real
    /// state (<see cref="currentTerm"/>, <see cref="votes"/>, <see cref="expectedLeaders"/>,
    /// <see cref="lastVotation"/>, <see cref="lastHeartbeat"/>, <see cref="nodeState"/>). This is
    /// what lets a stale/partitioned node probe its electability without disrupting a healthy leader.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="voteTerm"></param>
    /// <param name="remoteMaxLogId">The candidate's last log index.</param>
    /// <param name="remoteLastLogTerm">
    /// The candidate's last log term, compared lexicographically before <paramref name="remoteMaxLogId"/>
    /// (Raft §5.4.1). <c>0</c> from a peer predating this field or an empty candidate log; the freshness
    /// check falls back to index-only comparison in that case (see <see cref="CandidateLogIsBehind"/>).
    /// </param>
    /// <param name="timestamp"></param>
    /// <param name="preVote">When true, evaluate as a pure pre-vote probe and never persist state.</param>
    /// <remarks><paramref name="remoteLastLogTerm"/> is placed last with a default of <c>0</c> so callers
    /// that predate the §5.4.1 freshness key (and older tests) compile unchanged and fall back to
    /// index-only comparison; the transport dispatch path always supplies the real value.</remarks>
    public async Task VoteAsync(RaftNode node, long voteTerm, long remoteMaxLogId, HLCTimestamp timestamp, bool preVote = false, long remoteLastLogTerm = 0)
    {
        if (preVote)
        {
            // Side-effect-free pre-vote (Raft §9.6). NOTHING below this branch's `return`
            // may mutate state: we only read term/log/leader-freshness and, on grant, reply.

            if (!host.IsVoter(node.Endpoint))
            {
                logger.LogDebugDenyingPreVoteNotVoter(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
                return;
            }

            // A live leader never helps a challenger unseat it.
            if (nodeState == RaftNodeState.Leader)
            {
                logger.LogDebugDenyingPreVoteWeAreLeader(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);

                // A committed member probing for votes is direct evidence it cannot see this
                // leader. The common benign cause is a follower restart under quiescence: its
                // in-memory quiesce flag and leader knowledge died with the process, and a
                // quiesced leader sends no heartbeats to re-teach it — so the member loops
                // pre-vote rounds (denied here) while its partitions never assemble. Waking
                // re-arms heartbeats; the next interval re-establishes leadership for the
                // member and the partition re-quiesces once every peer has converged again.
                // (Quiescence bookkeeping is scheduling state, not Raft §3 vote state, so this
                // does not violate the pre-vote side-effect-free contract.)
                if (quiesced)
                    SetQuiesced(false);

                return;
            }

            // Deny if we ourselves would not start an election right now: a pre-vote grant must be
            // consistent with our own willingness to campaign, so this mirrors the Follower cases of
            // the CheckPartitionLeadershipAsync election trigger. Both decisions rely only on LOCAL
            // signals — the private `lastHeartbeat` field (refreshed on every accepted AppendLogs from
            // our leader) and the SWIM failure detector.
            //
            // It deliberately does NOT consult host.GetLastNodeActivity: that table is written only on
            // the leader side (CompleteAppendLogsAsync, when a leader receives a follower's ack). A
            // follower never populates the expected leader's entry, so the lookup was always Zero and
            // this freshness gate never fired — a follower with a perfectly fresh heartbeat still
            // granted a challenger's pre-vote, defeating pre-vote for the asymmetric-partition case it
            // exists to handle.
            // The candidate being the expected leader itself is proof the leadership is vacated:
            // a live leader never solicits votes, so this is a leader that restarted (or stepped
            // down) and lost its in-memory state. The freshness deny below must not apply — under
            // quiescence it consults SWIM, and the restarted process IS Alive as a node, so every
            // quiesced follower would deny the ex-leader its own vacated leadership forever while
            // their own election trigger stays calm for the same reason: a permanent leaderless
            // livelock with nobody heartbeating the restarted node. Fall through to the term/log
            // checks instead so the ex-leader can win a normal election (or lose it to a fresher
            // peer, which equally re-establishes a leader).
            string preVoteExpectedLeader = expectedLeaders.GetValueOrDefault(currentTerm, "");
            if (!string.IsNullOrEmpty(preVoteExpectedLeader) && preVoteExpectedLeader != node.Endpoint)
            {
                if (quiesced && host.Configuration.EnableQuiescence)
                {
                    // Quiesced: the leader suppresses heartbeats by design, so `lastHeartbeat` goes
                    // stale and is not a valid freshness signal. Defer to SWIM exactly as the quiesced
                    // Follower election case does — while the expected leader is Alive, don't help a
                    // challenger unseat it.
                    if (host.GetNodeLiveness(preVoteExpectedLeader) == MemberLivenessState.Alive)
                    {
                        logger.LogDebugDenyingPreVoteLeaderFresh(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, preVoteExpectedLeader);
                        return;
                    }
                }
                else if (lastHeartbeatTicks != 0 && (MonotonicElapsed(lastHeartbeatTicks, host.GetMonotonicTimestamp()) < electionTimeout))
                {
                    // Not quiesced: a recent heartbeat from our leader means it is still live to us.
                    // B3: measured as local elapsed time since we last heard from the leader (monotonic),
                    // NOT `incomingRequest.timestamp - lastHeartbeat` — that subtracted a remote HLC from a
                    // local one and inherited the challenger's clock skew, which could make a stale
                    // heartbeat look fresh (or vice-versa). "How long we've been without a heartbeat" is a
                    // purely local quantity.
                    logger.LogDebugDenyingPreVoteLeaderFresh(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, preVoteExpectedLeader);
                    return;
                }
            }

            // The hypothetical term must not be stale.
            if (voteTerm < currentTerm)
            {
                logger.LogDebugDenyingPreVoteStaleTerm(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, currentTerm);
                return;
            }

            (long preVoteLocalMaxId, long preVoteLocalLastTerm) = await GetFreshnessLogPositionAsync().ConfigureAwait(false);

            // The candidate's log must be at least as up-to-date, compared lexicographically by
            // (lastLogTerm, lastLogIndex) per Raft §5.4.1 — NOT index alone, which would let a higher
            // index hide a stale last term. Note this denies only when the candidate is *strictly*
            // behind: a pre-vote probes electability, so an equal log is grantable.
            if (CandidateLogIsBehind(remoteLastLogTerm, remoteMaxLogId, preVoteLocalLastTerm, preVoteLocalMaxId))
            {
                logger.LogDebugDenyingPreVoteOutdatedLog(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, remoteMaxLogId, preVoteLocalMaxId);
                return;
            }

            logger.LogDebugGrantingPreVote(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);

            VoteRequest preGrant = new(host.PartitionId, voteTerm, preVoteLocalMaxId, preVoteLocalLastTerm, timestamp, host.LocalEndpoint, preVote: true);
            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Vote, node, preGrant));
            return;
        }

        if (!host.IsVoter(node.Endpoint))
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugDenyingVoteNotVoter(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (votes.ContainsKey(voteTerm))
        {
            logger.LogInfoAlreadyVotedInTerm(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (nodeState != RaftNodeState.Follower && voteTerm == currentTerm)
        {
            logger.LogInfoCandidateOrLeaderSameTerm(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        if (currentTerm > voteTerm)
        {
            logger.LogInfoVoteOnPreviousTerm(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);
            return;
        }

        // Raft §5.1: a RequestVote carrying a term higher than ours makes us adopt that term
        // REGARDLESS of our current state. The adoption is what arms the `currentTerm > leaderTerm`
        // fence in AppendLogsCoreAsync against a deposed leader still replicating at its old term.
        // Gating adoption on `nodeState != Follower` (the original B2a scope) left a hole: a FOLLOWER
        // that granted a higher-term vote kept its in-memory term at the old value and went on ACKing
        // the deposed leader's appends — handing it a phantom quorum that kept committing acknowledged
        // writes the new leader then overwrote (observed as a Jepsen linearizability violation; the
        // grant path already persisted the new term to hard state, so a restart fenced correctly while
        // the live node did not). Only the leader/candidate step-down bookkeeping stays gated on state;
        // a follower keeps its (now old-term) leader knowledge until the new term's real leader
        // announces itself via AppendLogs. The vote target is left to the grant path below, which may
        // still deny on log-freshness — the term adoption happens either way.
        if (voteTerm > currentTerm)
        {
            bool stepDown = nodeState != RaftNodeState.Follower;

            if (stepDown)
            {
                logger.LogInfoSteppingDownOnHigherVoteTerm(
                    host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, currentTerm);

                // Mirrors the step-down in the AppendLogs path, except no leader is adopted (a vote
                // request does not identify a leader).
                nodeState = RaftNodeState.Follower;
                host.Leader = "";
                lastCommitIndexes.Clear();
                nextIndex.Clear();
                matchIndex.Clear();
                regressedFrontiers.Clear();
                localCommittedIndex = -1;
                FailAllActiveProposalWaiters();
                activeProposals.Clear();
            }

            currentTerm = voteTerm;

            // An open pre-vote round targets `oldTerm + 1`, which the adopted term makes stale — and a
            // follower CAN have one open (pre-vote runs in Follower state), so this must not be gated
            // on the step-down. Without it a completing round could promote to a disruptive election
            // against the very candidate we may be about to vote for. Mirrors the reset in the
            // AppendLogs leader-adoption path.
            ResetPreVoteRound();

            if (stepDown)
                await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

            // B2b: the higher term is adopted here even if we go on to DENY this candidate below (on
            // log-freshness), so persist it now with no vote yet — otherwise a crash after step-down but
            // before any log write would regress the term on restart. A grant below overwrites votedFor.
            await wal.PersistHardStateAsync(currentTerm, null).ConfigureAwait(false);
        }

        string expectedLeader = expectedLeaders.GetValueOrDefault(voteTerm, "");
        
        if (!string.IsNullOrEmpty(expectedLeader) && expectedLeader != node.Endpoint)
        {
            logger.LogInfoAlreadyVotedForOther(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, expectedLeader);
            return;
        }
        
        (long localMaxId, long localLastLogTerm) = await GetFreshnessLogPositionAsync().ConfigureAwait(false);

        if (CandidateLogIsBehind(remoteLastLogTerm, remoteMaxLogId, localLastLogTerm, localMaxId))
        {
            // Reject a real vote for a candidate whose log is behind ours, compared lexicographically
            // by (lastLogTerm, lastLogIndex) per Raft §5.4.1 — a higher index no longer overrides a
            // stale last term. We do NOT bump our own term here: with PreVote (§9.6) in place a stale
            // candidate can no longer reach this real-vote path with an inflated term, so the old
            // `currentTerm++` heuristic that forced us to be elected is no longer needed and only
            // risked spurious term churn.
            logger.LogInfoVoteOutdatedLog(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, remoteMaxLogId, localMaxId);
            return;
        }
        
        lastHeartbeat = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);
        lastVotation = lastHeartbeat;

        // B3: granting a vote counts as local activity — anchor both duration shadows to now so the
        // follower election gate and the recent-vote cooldown measure from this moment.
        long grantTicks = host.GetMonotonicTimestamp();
        lastHeartbeatTicks = grantTicks;
        lastVotationTicks = grantTicks;

        expectedLeaders[voteTerm] = node.Endpoint;

        // B2b: durably record who we voted for in this term BEFORE replying, so a crash right after the
        // reply cannot let us grant a different candidate in the same term after restart (the double-vote
        // that would let two leaders be elected for one term). The term persisted is voteTerm — the term we
        // are voting in, which is >= currentTerm here.
        await wal.PersistHardStateAsync(voteTerm, node.Endpoint).ConfigureAwait(false);

        logger.LogInfoSendingVote(host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm);

        VoteRequest request = new(host.PartitionId, voteTerm, localMaxId, localLastLogTerm, timestamp, host.LocalEndpoint);

        host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Vote, node, request));
    }

    /// <summary>
    /// Processes a vote received in the Raft consensus protocol.
    ///
    /// When <paramref name="preVote"/> is true this tallies a side-effect-free pre-grant (Raft §9.6)
    /// against the currently-open pre-vote round. A node running a pre-vote round is still a Follower,
    /// so this branch sits before the normal "didn't ask for it" Follower early-return. Reaching a
    /// pre-vote quorum promotes to a real election exactly once; no real vote/commit bookkeeping is
    /// touched on the pre-vote path.
    /// </summary>
    /// <param name="endpoint">The identifier of the remote node sending the vote.</param>
    /// <param name="voteTerm">The term associated with the received vote.</param>
    /// <param name="remoteMaxLogId">The highest log ID from the remote node.</param>
    /// <param name="preVote">When true, tally as a pre-vote grant for the open pre-vote round.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task ReceivedVoteAsync(string endpoint, long voteTerm, long remoteMaxLogId, bool preVote = false)
    {
        // Symmetric guard: discard grants from any endpoint that is not a committed voter.
        // Normal operation is safe without this (candidates only solicit host.Nodes, which is
        // voters-only), but an unsolicited or stale grant from a non-roster node should not
        // be tallied toward quorum.
        if (!host.IsVoter(endpoint))
        {
            logger.LogDebugIgnoringVoteGrantNotVoter(host.LocalEndpoint, host.PartitionId, nodeState, preVote ? "pre-" : "", endpoint, voteTerm);
            return;
        }

        if (preVote)
        {
            // Tally a pre-grant. Placed before the Follower early-return because a node running a
            // pre-vote round is still a Follower. Touches only pre-vote state until quorum promotes.
            if (electionPhase != RaftElectionPhase.PreVote || voteTerm != preVoteTerm)
            {
                logger.LogDebugIgnoringPreVoteGrantNoRound(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm, electionPhase, preVoteTerm);
                return;
            }

            preVotes.Add(endpoint);
            // Quorum is computed over voters only; learners in host.Nodes must not inflate the denominator.
            int preVoterTotal = host.Nodes.Count(n => host.IsVoter(n.Endpoint)) + 1; // +1 for self
            int preVoteQuorum = Math.Max(2, (preVoterTotal / 2) + 1);

            logger.LogInfoReceivedPreVote(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm, preVotes.Count, preVoteQuorum, preVoterTotal);

            if (preVotes.Count < preVoteQuorum)
                return;

            // Pre-vote quorum reached: promote to a real election (which bumps the term and casts
            // the self-vote). StartElectionAsync resets the round itself once it commits to candidacy,
            // but it can also bail out early (e.g. a role/suppression guard) *before* that reset, so
            // we reset again here unconditionally to guarantee the round can't be tallied twice.
            HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
            await StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
            ResetPreVoteRound();
            return;
        }

        if (nodeState == RaftNodeState.Follower)
        {
            logger.LogInfoReceivedUnsolicitedVote(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }

        if (voteTerm < currentTerm)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} on previous term Term={Term}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }
        
        if (nodeState == RaftNodeState.Leader)
        {
            // lastCommitIndexes is deliberately not written here — see the note at the quorum
            // seeding below. A vote reports a log id, not a committed frontier.
            startCommitIndexes[endpoint] = remoteMaxLogId;

            logger.LogInfoReceivedVoteAlreadyLeader(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }
        
        // Compare like against like: the granting voter now reports its contiguous-presence
        // position, so the local side of this guard must use the same metric — a raw max id here
        // could mask a hole in our own log and accept leadership we should not claim.
        (long maxLogResponse, _) = await GetFreshnessLogPositionAsync().ConfigureAwait(false);

        if (maxLogResponse < remoteMaxLogId)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} but remote node is on a higher RemoteCommitId={CommitId} Local={LocalCommitId}. Ignoring...", 
                host.LocalEndpoint, 
                host.PartitionId, 
                nodeState, 
                endpoint, 
                remoteMaxLogId, 
                maxLogResponse
            );
            return;
        }

        int numberVotes = IncreaseVotes(endpoint, voteTerm);
        // Quorum is computed over voters only; learners in host.Nodes must not inflate the denominator.
        int voterTotal = host.Nodes.Count(n => host.IsVoter(n.Endpoint)) + 1; // +1 for self
        int quorum = Math.Max(2, (voterTotal / 2) + 1);

        // lastCommitIndexes is NOT seeded from the vote. It means "the committed frontier this
        // follower last reported about itself", and a vote carries the voter's highest *log id* —
        // which sits at or above that frontier, because a log holds entries not yet applied.
        //
        // Recording the higher number here would poison the map with a value the peer never
        // reported. CompleteAppendLogsAsync now records Success self-reports last-writer-wins, so
        // the peer's first ack would overwrite a vote seed anyway — but a log id is simply not a
        // frontier report, and writing one here would make the leader believe, for the window
        // until that first ack, that the peer is current when it may be arbitrarily far behind
        // (SendHeartbeat computes followerGap from this map, so a seeded over-estimate suppresses
        // exactly the backfill that would repair the peer).
        //
        // Leaving the key absent is the conservative choice and self-heals in one round: the
        // backfill decision treats an unknown peer as having no gap, the peer's first ack records
        // where it actually is, and the next heartbeat catches it up from there. Seeding 0 would
        // *not* be conservative — nextIndex is optimistic (leaderMaxLog + 1) and therefore above
        // localCommittedIndex, so TrySendBackfillBatchAsync falls back to followerMaxLog + 1 and a
        // zero seed would re-ship every follower's log from index 1 on every election.
        //
        // startCommitIndexes keeps the vote's value: it records where a peer's log started this
        // term, which is what a log id is.
        startCommitIndexes[endpoint] = remoteMaxLogId;

        logger.LogInfoReceivedVote(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm, numberVotes, quorum, voterTotal, remoteMaxLogId, maxLogResponse);

        if (numberVotes < quorum)
            return;
        
        // Here quorum was achieved and we can mark ourselves as leader in the partition.
        // Seed per-follower replication progress. nextIndex is optimistic (leaderMaxLog + 1);
        // it will be corrected by LogMismatch replies if any peer is behind.
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        foreach (RaftNode peer in host.Nodes)
        {
            nextIndex[peer.Endpoint] = maxLogResponse + 1;
            matchIndex[peer.Endpoint] = 0;
        }

        bool leadershipPublished = await BecomeLeaderAsync().ConfigureAwait(false);

        double electionElapsedMs = MonotonicElapsed(votingStartedTicks, host.GetMonotonicTimestamp()).TotalMilliseconds;
        logger.LogInfoReceivedVoteProclaimedLeader(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, electionElapsedMs, voteTerm, numberVotes, quorum, host.Nodes.Count + 1, remoteMaxLogId, maxLogResponse);

        // With a promotion barrier pending, leadership is published (and LeaderChanged fired) by
        // CompleteLeaderCommit once the barrier no-op commits; the heartbeat below still goes out
        // immediately so followers adopt this term and rival elections stay suppressed.
        if (leadershipPublished)
            await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint);

        await SendHeartbeat(true).ConfigureAwait(false);
    }

    /// <summary>
    /// Appends logs to the Write-Ahead Log and updates the state of the node based on the leader's term.
    /// This method usually runs on follower nodes.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="leaderTerm"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    /// <returns></returns>
    public Task AppendLogsAsync(string endpoint, long term, HLCTimestamp timestamp, List<RaftLog>? logs, long prevLogIndex = 0, long prevLogTerm = 0, ulong? replyCorrelationId = null, bool quiesce = false) =>
        AppendLogsCoreAsync(endpoint, term, timestamp, logs, prevLogIndex, prevLogTerm, replyCorrelationId, quiesce);

    private async Task AppendLogsCoreAsync(
        string endpoint,
        long leaderTerm,
        HLCTimestamp timestamp,
        List<RaftLog>? logs,
        long prevLogIndex = 0,
        long prevLogTerm = 0,
        ulong? replyCorrelationId = null,
        bool quiesce = false
    )
    {
        if (currentTerm > leaderTerm)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received logs from a leader {Endpoint} with old ReceivedTerm={Term} CurrentTerm={CurrentTerm}. Ignoring...", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, leaderTerm, currentTerm);

            // The rejection carries THIS node's currentTerm (not an echo of the sender's stale term):
            // it is the only channel through which a deposed leader can learn a higher term exists.
            // A node whose term was bumped by a failed election rejects every AppendLogs here — before
            // the WAL is ever touched — so if this reply echoed the stale term, the sender would keep
            // shipping batches forever ("send=True" every round) while this node's committed frontier
            // stays frozen, and pre-vote (correctly) prevents the higher term from propagating by
            // election against a healthy leader. Raft §5.1 closes the loop on the receiving side:
            // CompleteAppendLogsAsync steps down and adopts any response term above its own.
            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs, 
                new(endpoint), 
                new CompleteAppendLogsRequest(host.PartitionId, currentTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LeaderInOldTerm, -1)
            ));
            
            return;
        }

        // Membership fence: a valid AppendEntries authoritatively identifies a term's leader, but only a
        // committed roster member can legitimately be one. Without this, any endpoint reachable through
        // the transport (e.g. registered in the transport map before joining membership) could be adopted
        // as leader merely by sending logs with a fresh term, churning an established partition's
        // leadership. Skipped for the already-accepted leader so a roster snapshot that briefly lags a
        // role change cannot make a follower reject its real leader.
        if (host.Leader != endpoint && !host.IsMember(endpoint))
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Ignoring AppendLogs from non-member {Endpoint} Term={Term}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, leaderTerm);

            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs,
                new(endpoint),
                new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogsFromAnotherLeader, -1)
            ));

            return;
        }

        // leaderTerm >= currentTerm is guaranteed here (the currentTerm > leaderTerm case returned
        // above). A valid AppendEntries authoritatively identifies the single leader of leaderTerm,
        // so adopt it regardless of whom we voted for this term. Granting a vote to a candidate does
        // not make it the leader: under a vote split a different candidate can win the term with
        // another quorum, so gating leader acceptance on our vote record (expectedLeaders) would
        // make this follower reject the real leader forever and wedge the partition. expectedLeaders
        // must constrain voting only, never leader acceptance.
        if (host.Leader != endpoint || currentTerm != leaderTerm || nodeState != RaftNodeState.Follower)
        {
            logger.LogInfoLeaderIsNow(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, leaderTerm);

            nodeState = RaftNodeState.Follower;
            host.Leader = endpoint;
            currentTerm = leaderTerm;
            lastCommitIndexes.Clear();
            nextIndex.Clear();
            matchIndex.Clear();
            regressedFrontiers.Clear();
            localCommittedIndex = -1;
            FailAllActiveProposalWaiters();
            activeProposals.Clear();
            expectedLeaders[leaderTerm] = endpoint;   // overwrite any stale vote target with the real leader
            ResetPreVoteRound();                       // break the pre-vote livelock on adoption

            await host.InvokeLeaderChanged(host.PartitionId, endpoint);

            // B2b: adopting a new leader advances our term. Persist it (recording the leader as the term's
            // vote target, matching expectedLeaders) so that a bare heartbeat carrying no log entries can't
            // leave the advanced term un-durable and let it regress on restart.
            await wal.PersistHardStateAsync(leaderTerm, endpoint).ConfigureAwait(false);
        }

        lastHeartbeat = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);
        // B3: a received AppendLogs (heartbeat or real batch) is the primary "we heard from the leader"
        // signal. Anchor the monotonic shadow to local now so the follower election gate measures the
        // silence interval on the local clock — this is the exact site whose HLC subtraction used to
        // freeze the timeout for the length of a leader's clock skew.
        lastHeartbeatTicks = host.GetMonotonicTimestamp();
        // A quiesce-flagged message tells us to stop expecting heartbeats and gate elections
        // on SWIM liveness instead.  Any non-quiesce AppendLogs (real logs or normal heartbeat)
        // wakes us back up by clearing the flag.
        SetQuiesced(quiesce);

        // Log Matching Property check: the follower must hold an entry at prevLogIndex whose
        // term equals prevLogTerm before it can safely append the incoming batch.
        //
        // Mismatch classification:
        //   * localTermAtPrev < 0 — hole: no entry exists at prevLogIndex. Holes arise because the
        //     live-propose path ships prevLogIndex=0 and skips contiguity, so an out-of-order batch
        //     can leave a gap below prevLogIndex on the follower. The repair truncates the orphaned
        //     tail above the gap so the leader heals it in one forward backfill pass instead of
        //     walking nextIndex down one slot at a time. This is safe by construction: a hole at
        //     prevLogIndex proves the committed prefix ends below it, so the truncated tail is
        //     necessarily uncommitted.
        //   * localTermAtPrev >= 0 && localTermAtPrev != prevLogTerm — genuine term divergence: an
        //     entry exists but belongs to a different term. The existing backtrack path is used
        //     unchanged; the leader decrements nextIndex and retries with an earlier anchor.
        //
        // GetAnyTermAtAsync is used (not GetRangeAsync) so that a Proposed-but-uncommitted entry at
        // prevLogIndex is matched correctly; GetRangeAsync filters uncommitted entries.
        if (prevLogIndex > 0 && logs is not null && logs.Count > 0)
        {
            long localMaxLog = await wal.GetMaxLogAsync().ConfigureAwait(false);

            if (prevLogIndex > localMaxLog)
            {
                // Follower is simply behind the leader's append point (prevLogIndex is ahead of our
                // tail). Backfill backtracks nextIndex and catches it up — benign and noisy under
                // high write concurrency, so this stays at Debug. Genuine divergence (a term mismatch
                // at an existing entry) is the Warning below.
                logger.LogDebugLogMatchingFollowerBehind(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, prevLogIndex, localMaxLog);

                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                ));
                return;
            }

            long localTermAtPrev = await wal.GetAnyTermAtAsync(prevLogIndex).ConfigureAwait(false);

            // Classify the anchor by TERM MATCH first, before the hole test. A shared value — including
            // -1 == -1 — is a match and falls through to append. The -1 == -1 case is a snapshot boundary: the
            // follower's entry at prevLogIndex is a CommittedCheckpoint whose term is unknown after compaction
            // (LastIncludedTerm can be -1), and the leader anchors on its own equally-compacted boundary, so the
            // snapshot-covered prefix already agrees. Testing the hole (localTermAtPrev < 0) BEFORE the match
            // would misread that -1 boundary term as a hole and truncate the just-shipped anchored backfill,
            // which the leader re-ships and the follower re-truncates forever — a live-lock that strands the
            // follower exactly one entry below the boundary (and its consumer's applied prefix with it).
            if (localTermAtPrev != prevLogTerm)
            {
                if (localTermAtPrev < 0)
                {
                    // Stale-read guard: the backend reads race the WAL write queue, so an entry whose
                    // physical append is still queued reads as absent (-1) — a FALSE hole. The presence
                    // frontier advances at enqueue time and is contiguous, so it covering prevLogIndex
                    // proves an entry exists there; truncating on that stale read would enqueue the
                    // delete BEHIND the pending append and discard a possibly-committed entry. Report
                    // "behind" instead and let the leader retry against the durable log. Unreachable
                    // under strict per-partition write FIFO (the prevLogIndex > localMaxLog pre-check
                    // fires first) — kept as defense in depth because the truncate is irreversible.
                    long presentIndexAtAnchor = wal.GetPresentIndex();
                    if (presentIndexAtAnchor >= prevLogIndex)
                    {
                        if (logger.IsEnabled(LogLevel.Debug))
                            logger.LogDebug("[{LocalEndpoint}/{PartitionId}/{State}] False-hole read at prevLogIndex={PrevLogIndex} (presence frontier {PresentIndex} covers it; append still queued) — deferring to backfill retry instead of truncating.",
                                host.LocalEndpoint, host.PartitionId, nodeState, prevLogIndex, presentIndexAtAnchor);
                        host.EnqueueResponse(endpoint, new(
                            RaftResponderRequestType.CompleteAppendLogs,
                            new(endpoint),
                            new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                        ));
                        return;
                    }

                    // Hole: no entry exists at prevLogIndex even though prevLogIndex <= localMaxLog, so the
                    // follower's log has an internal gap. This proves the follower's truly committed prefix ends
                    // below prevLogIndex: the leader commits contiguously, so no entry above an unfilled gap can
                    // have been quorum-committed — any entry sitting above the gap is an orphan delivered out of
                    // order by the unanchored live-propose broadcast. Truncating that orphaned tail (everything
                    // after prevLogIndex-1) can therefore never discard committed data, regardless of what the
                    // in-memory commitIndex reports (it can transiently overshoot the gap when a misordered
                    // Committed delivery lands above it). Reporting the post-truncation max lets the leader heal
                    // the gap in one forward backfill pass instead of walking nextIndex down one slot at a time.
                    long newMax = await wal.TruncateLogsAfterAsync(prevLogIndex - 1).ConfigureAwait(false);
                    logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Log-hole repair from {Endpoint}: prevLogIndex={PrevLogIndex} truncated to newMax={NewMax}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, prevLogIndex, newMax);
                    host.EnqueueResponse(endpoint, new(
                        RaftResponderRequestType.CompleteAppendLogs,
                        new(endpoint),
                        new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, newMax)
                    ));
                    return;
                }

                // Genuine term divergence: entry exists at prevLogIndex but belongs to a
                // different term. Leader backtracks nextIndex and retries with an earlier anchor.
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Log Matching rejection from {Endpoint}: prevLogIndex={PrevLogIndex} localTerm={LocalTerm} != prevLogTerm={PrevLogTerm}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, prevLogIndex, localTermAtPrev, prevLogTerm);
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                ));
                return;
            }

            // Anchor-contiguity check (AppendEntries semantics: entries[] immediately follows
            // prevLogIndex). A matching anchor proves the shared prefix through prevLogIndex —
            // nothing more. A batch whose first entry sits ABOVE prevLogIndex+1 would be written
            // over a gap the anchor never vouched for; accepting it strands this follower's commit
            // frontier below the gap while its log grows (the Jepsen one-stuck-entry wedge) with
            // no signal anywhere. Only the backfill path sends anchored batches and it sends them
            // contiguous by construction, so this firing means the sender's read skipped entries
            // it believes committed but holds uncommitted — reject loudly and let it repair.
            // The unanchored live-propose broadcast (prevLogIndex == 0) is exempt by the enclosing
            // guard: out-of-order lone-high deliveries are its documented, frontier-buffered shape.
            long firstIncomingId = long.MaxValue;
            foreach (RaftLog incoming in logs)
            {
                if (incoming.Id < firstIncomingId)
                    firstIncomingId = incoming.Id;
            }

            if (firstIncomingId != prevLogIndex + 1)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Non-contiguous anchored batch from {Endpoint}: prevLogIndex={PrevLogIndex} but first entry is {FirstId} — rejecting.",
                    host.LocalEndpoint, host.PartitionId, nodeState, endpoint, prevLogIndex, firstIncomingId);
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, localMaxLog)
                ));
                return;
            }
        }

        if (logs is not null && logs.Count > 0)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugReceivedLogs(
                    host.LocalEndpoint,
                    host.PartitionId,
                    nodeState,
                    endpoint,
                    leaderTerm,
                    timestamp,
                    string.Join(',', logs.Select(x => x.Id.ToString()))
                );

            WALWriteOperation? operation;

            try
            {
                operation = wal.EnqueueProposeOrCommit(logs, timestamp, endpoint, leaderTerm);
            }
            catch (WAL.IO.BackpressureExceededException ex)
            {
                // The WAL queue for this partition is full, so these entries were not
                // accepted. Answer the leader instead of letting the exception escape to
                // the executor's catch-all: an unanswered append is indistinguishable from
                // a dropped message, so the leader re-sends on its next tick with no idea
                // the follower is saturated — and each escaped exception also costs a
                // logged stack trace, which is I/O this node is already short of.
                //
                // Reporting the local max lets the leader anchor its next attempt without
                // walking nextIndex backwards, exactly as the Log Matching rejections above
                // do. nextIndex is deliberately not advanced by this status on the leader,
                // so the retry rides the normal heartbeat/backfill cadence rather than
                // spinning against a queue that never gets a chance to drain.
                long saturatedMax = await wal.GetMaxLogAsync().ConfigureAwait(false);

                LogWalSaturated(endpoint, ex.CurrentDepth, saturatedMax);

                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.FollowerWalSaturated, saturatedMax)
                ));
                return;
            }

            if (operation is not null)
            {
                Scheduling.RaftPendingWalOperation pendingAppend = RentPendingWalOp();
                pendingAppend.ReplyCorrelationId = replyCorrelationId;
                pendingAppend.Logs = logs;
                pendingAppend.Endpoint = endpoint;
                pendingAppend.Timestamp = timestamp;
                pendingWalOperations[operation.OperationId] = pendingAppend;
                return;
            }

            /*(RaftOperationStatus Status, long Index) response = await wal.ProposeOrCommit(logs).ConfigureAwait(false);
            
            if (response.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't append logs from leader {Endpoint} with Term={Term} Status={Status} Logs={Logs}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, leaderTerm, response.Status, logs.Count);
                
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, response.Status, -1)
                ));
                return;
            }
            
            foreach (HLCTimestamp logTimestamp in logs.Select(x => x.Time).Distinct())
            {
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs, 
                    new(endpoint), 
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, logTimestamp, host.LocalEndpoint, RaftOperationStatus.Success, response.Index)
                ));    
            }*/
            
            return;
        }
        
        // On the single-fsync fast path a heartbeat ack carries the follower's TRUE committed frontier
        // (not the legacy -1). This is the "leader's leaderCommit on reconnect" feedback channel: a
        // follower whose commit frontier regressed on restart (lazy markers lost, then reconstructed
        // conservatively) advertises the lower value so the leader can re-supply the still-committed tail.
        long reportedCommittedIndex = host.Configuration.WalSingleFsyncCommit ? wal.GetCommitIndex() : -1;

        host.EnqueueResponse(endpoint, new(
            RaftResponderRequestType.CompleteAppendLogs,
            new(endpoint),
            new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.Success, reportedCommittedIndex)
        ));

        CompleteReply(replyCorrelationId, RaftResponseStatic.NoneResponse);
    }

    /// <summary>
    /// Reports that this follower rejected a replicated batch because its WAL queue is full,
    /// at most once per second per partition and carrying the count suppressed since the last
    /// line.
    /// </summary>
    /// <remarks>
    /// Throttled deliberately. Saturation rejects on every inbound append, so a per-occurrence
    /// log turns one slow disk into a second, larger source of disk pressure — the amplification
    /// is not hypothetical: a run that logged each rejection with a stack trace produced 238k
    /// entries and a 251 MB log on a single node. Aggregating loses nothing that matters here,
    /// because the useful facts are that the partition is saturated and roughly how hard, not
    /// the identity of any individual rejected batch.
    /// </remarks>
    private void LogWalSaturated(string endpoint, int depth, long localMaxLog)
    {
        long now = Stopwatch.GetTimestamp();

        if (lastWalSaturatedLogTicks != 0 && (now - lastWalSaturatedLogTicks) < Stopwatch.Frequency)
        {
            suppressedWalSaturatedLogs++;
            return;
        }

        lastWalSaturatedLogTicks = now;

        logger.LogWarning(
            "[{LocalEndpoint}/{PartitionId}/{State}] WAL saturated, rejecting append from {Endpoint}: depth={Depth} localMaxLog={LocalMaxLog} suppressedSinceLastLine={Suppressed}",
            host.LocalEndpoint,
            host.PartitionId,
            nodeState,
            endpoint,
            depth,
            localMaxLog,
            suppressedWalSaturatedLogs
        );

        suppressedWalSaturatedLogs = 0;
    }

    /// <summary>
    /// Reports a failed AppendLogs acknowledgement, collapsing consecutive acks carrying the
    /// same status into one line per second with the count suppressed since the last.
    /// </summary>
    /// <remarks>
    /// The leader mirror of <see cref="LogWalSaturated"/>, and it exists for the same reason: a
    /// follower that cannot accept a batch cannot accept the next one either, so the failure
    /// arrives once per attempt and the attempts are frequent. Keyed on the status so a new
    /// kind of failure during a storm is still surfaced immediately.
    /// </remarks>
    private void LogFailedAppendAck(RaftOperationStatus status, string endpoint, HLCTimestamp timestamp, long committedIndex)
    {
        long now = Stopwatch.GetTimestamp();

        if (lastLoggedAckStatus == status && (now - lastFailedAckLogTicks) < Stopwatch.Frequency)
        {
            suppressedFailedAckLogs++;
            return;
        }

        logger.LogWarning(
            "[{LocalEndpoint}/{PartitionId}/{State}] Got {Status} from {Endpoint} Timestamp={Timestamp} CommittedIndex={CommittedIndex} suppressedSinceLastLine={Suppressed}",
            host.LocalEndpoint,
            host.PartitionId,
            nodeState,
            status,
            endpoint,
            timestamp,
            committedIndex,
            suppressedFailedAckLogs
        );

        lastLoggedAckStatus   = status;
        lastFailedAckLogTicks = now;
        suppressedFailedAckLogs = 0;
    }

    /// <summary>
    /// DIAGNOSTIC. Records the inputs to one peer's backfill decision in a heartbeat round.
    /// </summary>
    /// <remarks>
    /// Temporary. A leader that sends nothing looks identical in the logs to a leader with nothing
    /// to send, and telling those apart is the whole question when replicas stop advancing. Every
    /// trigger here derives from <paramref name="followerMaxLog"/> — the leader's belief about the
    /// peer — so that value is what the trace exists to expose. Remove once answered.
    /// </remarks>
    private void LogBackfillDecision(string endpoint, bool willBackfill, long followerMaxLog,
                                     long followerGap, bool idleTailGap, bool regressed, bool liveQuiet)
    {
        long now = Stopwatch.GetTimestamp();

        if (lastBackfillTraceTicks != 0 && (now - lastBackfillTraceTicks) < Stopwatch.Frequency)
        {
            suppressedBackfillTraces++;
            return;
        }

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation(
                "[{LocalEndpoint}/{PartitionId}/{State}] DIAG backfill-decision peer={Endpoint} send={Send} followerMaxLog={FollowerMaxLog} localCommitted={LocalCommitted} gap={Gap} threshold={Threshold} idleTailGap={IdleTailGap} regressed={Regressed} liveQuiet={LiveQuiet} liveCommitFloor={LiveCommitFloor} suppressedSinceLastLine={Suppressed}",
                host.LocalEndpoint, host.PartitionId, nodeState, endpoint, willBackfill,
                followerMaxLog, localCommittedIndex, followerGap, host.Configuration.BackfillThreshold,
                idleTailGap, regressed, liveQuiet, liveCommitFloor, suppressedBackfillTraces);
        }

        lastBackfillTraceTicks   = now;
        suppressedBackfillTraces = 0;
    }

    /// <summary>
    /// Replicates logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public Task ReplicateLogsAsync(List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId)
    {
        (RaftOperationStatus status, HLCTimestamp ticketId) = ReplicateLogs(logs, autoCommit, replyCorrelationId);

        if (status != RaftOperationStatus.Pending)
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, status, ticketId));

        return Task.CompletedTask;
    }

    public (RaftOperationStatus, HLCTimestamp ticketId) ReplicateLogs(
        List<RaftLog>? logs,
        bool autoCommit,
        ulong? replyCorrelationId = null
    )
    {
        if (logs is null || logs.Count == 0)
            return (RaftOperationStatus.Success, HLCTimestamp.Zero);

        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);

        HLCTimestamp currentTime = host.HybridLogicalClock.SendOrLocalEvent(host.LocalNodeId);
        lastProposalAt = currentTime;
        lastProposalAtTicks = host.GetMonotonicTimestamp(); // B3: quiesce-after measured on monotonic clock
        SetQuiesced(false); // un-quiesce on new proposal: resume normal heartbeating

        // Try to clear and reuse settled proposals. Only worth scanning once a few have accumulated;
        // the periodic Leader tick (CheckPartitionLeadershipAsync) runs the same drain unconditionally
        // so an idle leader that stops proposing still releases its settled proposals rather than
        // retaining their log payloads until the next leadership change.
        if (activeProposals.Count > 5)
            PruneSettledProposals(currentTime);

        // No peers: a single-node leader is its own quorum. Rather than rejecting the proposal,
        // we enqueue it to the local WAL exactly like the multi-node path; CompleteLeaderPropose
        // then drives the commit immediately (quorum = self), so JoinCluster/IsInitialized can
        // complete on a single-node cluster. The self-only commit shortcut lives entirely behind
        // the Nodes.Count == 0 guard in CompleteLeaderPropose, so multi-node safety is unaffected.

        // Snapshot Type and Time before mutation so we can restore atomically if the WAL scheduler
        // rejects the operation (e.g. BackpressureExceededException). Uses one pooled (Type, Time) buffer
        // rather than logs.ToArray() + two Array.ConvertAll copies; logs is an indexable List<RaftLog>, so
        // it is read and restored by index. Every entry is snapshotted first and only then mutated — the
        // same all-reads-before-any-writes order as the previous code — so the rollback is identical even
        // if the same entry appears twice. The buffer holds only value types (no references to clear), and
        // rent/return stay in this method via try/finally, never escaping onto a field.
        int logCount = logs.Count;
        (RaftLogType Type, HLCTimestamp Time)[] snapshot =
            ArrayPool<(RaftLogType, HLCTimestamp)>.Shared.Rent(logCount);
        try
        {
            for (int i = 0; i < logCount; i++)
                snapshot[i] = (logs[i].Type, logs[i].Time);

            for (int i = 0; i < logCount; i++)
            {
                logs[i].Type = RaftLogType.Proposed;
                logs[i].Time = currentTime;
            }

            WALWriteOperation operation;
            try
            {
                operation = wal.EnqueuePropose(currentTerm, logs, currentTime, autoCommit);
            }
            catch
            {
                for (int i = 0; i < logCount; i++)
                {
                    logs[i].Type = snapshot[i].Type;
                    logs[i].Time = snapshot[i].Time;
                }
                throw;
            }

            Scheduling.RaftPendingWalOperation pendingPropose = RentPendingWalOp();
            pendingPropose.ReplyCorrelationId = replyCorrelationId;
            pendingPropose.TicketId = currentTime;
            pendingPropose.Logs = logs;
            pendingPropose.AutoCommit = autoCommit;
            pendingWalOperations[operation.OperationId] = pendingPropose;

            return (RaftOperationStatus.Pending, currentTime);
        }
        finally
        {
            ArrayPool<(RaftLogType, HLCTimestamp)>.Shared.Return(snapshot);
        }

        // Append proposal logs to the Write-Ahead Log
        /*(RaftOperationStatus Status, long) proposeResponse = await wal.Propose(currentTerm, logs).ConfigureAwait(false);
        
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", host.LocalEndpoint, host.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = RaftProposalQuorumPool.Rent(logs, autoCommit, currentTime); // new(logs, autoCommit, currentTime);
        
        // Mark itself as completed
        proposalQuorum.MarkNodeCompleted(host.LocalEndpoint);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            
            AppendLogToNode(node, currentTime, logs);
        }

        if (!activeProposals.TryAdd(currentTime, proposalQuorum))
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugProposedLogs(host.LocalEndpoint, host.PartitionId, nodeState, currentTime, string.Join(',', logs.Select(x => x.Id.ToString())));

        return (RaftOperationStatus.Success, currentTime);*/
    }

    /// <summary>
    /// Releases settled proposals from <see cref="activeProposals"/>, returning each to the pool so its
    /// log payloads (which retain the proposed <c>KeyValueEntry</c>/byte[] data) become collectible.
    /// <para>
    /// A proposal is settled once it has reached quorum and has aged past the retention window. The
    /// window is a client-idempotency grace period: a committed proposal is kept briefly so a retried
    /// <see cref="CommitLogs"/> or a late <see cref="CheckTicketCompletion"/> poll still observes the
    /// settled result instead of <c>ProposalNotFound</c>. In-flight awaiters are unaffected — they hold
    /// the waiter task independently of the dictionary entry — so eviction only affects later lookups
    /// by ticket id, which the grace period covers.
    /// </para>
    /// <para>
    /// Historically this ran only at the head of <see cref="ReplicateLogs"/>, so a leader that stopped
    /// proposing never swept its last batch — the entries (and their payloads) were retained until the
    /// next leadership change, and their non-zero count also blocked quiescence
    /// (<see cref="CheckPartitionLeadershipAsync"/> requires <c>activeProposals.Count == 0</c>). It is
    /// now also driven from the periodic Leader tick so an idle leader converges to an empty map.
    /// </para>
    /// </summary>
    private void PruneSettledProposals(HLCTimestamp currentTime)
    {
        TimeSpan range = TimeSpan.FromSeconds(30);

        settledProposalScratch.Clear();

        foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> proposal in activeProposals)
        {
            if (proposal.Value.HasQuorum() && currentTime - proposal.Value.StartTimestamp > range)
                settledProposalScratch.Add(proposal.Key);
        }

        foreach (HLCTimestamp key in settledProposalScratch)
        {
            if (activeProposals.Remove(key, out RaftProposalQuorum? settled))
                RaftProposalQuorumPool.Return(settled);
        }

        settledProposalScratch.Clear();
    }

    /// <summary>
    /// Puts together a plan to replicate logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async Task ReplicateLogsBatchAsync(IReadOnlyList<(List<RaftLog>? Logs, bool AutoCommit, ulong? ReplyCorrelationId)> messages)
    {
        // Determine which autoCommit values to dispatch, in first-seen order among messages that carry
        // logs — a batch whose messages all have null/empty logs dispatches nothing. Only the KEYS drive
        // the dispatch loop below (each message keeps its own log list), so no per-key log aggregation
        // is needed on this per-batch hot path.
        bool sawFirstKey = false;
        bool sawSecondKey = false;
        bool firstKey = false;

        foreach ((List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId) message in messages)
        {
            if (message.logs is null || message.logs.Count == 0)
                continue;

            if (!sawFirstKey)
            {
                sawFirstKey = true;
                firstKey = message.autoCommit;
            }
            else if (message.autoCommit != firstKey)
                sawSecondKey = true;
        }

        for (int keyIndex = 0; keyIndex < 2; keyIndex++)
        {
            if (keyIndex == 0 && !sawFirstKey)
                break;

            if (keyIndex == 1 && !sawSecondKey)
                break;

            bool key = keyIndex == 0 ? firstKey : !firstKey;

            foreach ((List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId) item in messages)
            {
                if (item.autoCommit == key)
                    await ReplicateLogsAsync(item.logs, item.autoCommit, item.replyCorrelationId).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Replicates the checkpoint to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <returns></returns>
    public Task ReplicateCheckpointAsync(ulong? replyCorrelationId)
    {
        (RaftOperationStatus status, HLCTimestamp ticketId) = ReplicateCheckpoint(replyCorrelationId);

        if (status != RaftOperationStatus.Pending)
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, status, ticketId));

        return Task.CompletedTask;
    }

    private (RaftOperationStatus status, HLCTimestamp ticketId) ReplicateCheckpoint(
        ulong? replyCorrelationId = null
    )
    {
        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, HLCTimestamp.Zero);
        
        foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> proposal in activeProposals)
        {
            if (!proposal.Value.HasQuorum())
                return (RaftOperationStatus.ActiveProposal, HLCTimestamp.Zero);
        }
        
        // No peers: a single-node leader is its own quorum. We enqueue the checkpoint to the local
        // WAL exactly like the multi-node path and let CompleteLeaderPropose drive the commit
        // immediately (quorum = self), behind its Nodes.Count == 0 guard. Without this a single-node
        // cluster could never checkpoint, and since compaction is checkpoint-driven its WAL would
        // grow without bound. Multi-node safety is unaffected: with peers, the same proposal still
        // requires a real quorum.

        // We need a proper HLC sequence to determine a consistent order of the logs
        HLCTimestamp currentTime = host.HybridLogicalClock.SendOrLocalEvent(host.LocalNodeId);

        // P0 checkpoints carry the current system-configuration snapshot (roster + partition
        // map) so a restart that replays from this checkpoint reconstructs them even after
        // compaction removed the original config delta entries below it. The payload is inert
        // on the live path (CommittedCheckpoint entries are never delivered to consumers) and
        // replicates to followers like any other entry.
        byte[]? systemPayload = host.PartitionId == RaftSystemConfig.SystemPartition
            ? host.GetSystemCheckpointPayload()
            : null;

        List<RaftLog> checkpointLogs = [new()
        {
            Id = 0,
            Term = currentTerm,
            Type = RaftLogType.ProposedCheckpoint,
            Time = currentTime,
            LogType = systemPayload is null ? "" : RaftSystemConfig.CheckpointLogType,
            LogData = systemPayload ?? []
        }];

        WALWriteOperation operation = wal.EnqueuePropose(currentTerm, checkpointLogs, currentTime, true);
        Scheduling.RaftPendingWalOperation pendingCheckpoint = RentPendingWalOp();
        pendingCheckpoint.ReplyCorrelationId = replyCorrelationId;
        pendingCheckpoint.TicketId = currentTime;
        pendingCheckpoint.Logs = checkpointLogs;
        pendingCheckpoint.AutoCommit = true;
        pendingWalOperations[operation.OperationId] = pendingCheckpoint;

        return (RaftOperationStatus.Pending, currentTime);
        
        // Append proposal logs to the Write-Ahead Log
        /*(RaftOperationStatus Status, long) proposeResponse = await wal.Propose(context.Self, currentTerm, checkpointLogs).ConfigureAwait(false);
        
        if (proposeResponse.Status != RaftOperationStatus.Success)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't save proposed logs to local persistence", host.LocalEndpoint, host.PartitionId, nodeState);
            
            return (RaftOperationStatus.Errored, HLCTimestamp.Zero);
        }

        RaftProposalQuorum proposalQuorum = RaftProposalQuorumPool.Rent(checkpointLogs, true, currentTime);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");
            
            proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            
            AppendLogToNode(node, currentTime, checkpointLogs);
        }

        activeProposals.TryAdd(currentTime, proposalQuorum);
        
        logger.LogInfoProposedCheckpointLogs(
            host.LocalEndpoint, 
            host.PartitionId, 
            nodeState, 
            currentTime, 
            checkpointLogs.Count
        );

        return (RaftOperationStatus.Success, currentTime);*/
    }

    /// <summary>
    /// Marks proposals as committed
    /// </summary>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    public Task CommitLogsAsync(HLCTimestamp ticketId, ulong? replyCorrelationId)
    {
        (RaftOperationStatus status, long commitIndex) = CommitLogs(ticketId, replyCorrelationId);

        if (status != RaftOperationStatus.Pending)
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, status, commitIndex));

        return Task.CompletedTask;
    }

    /// <summary>
    /// Idempotent: if the proposal is already <see cref="RaftProposalState.Committed"/>, returns
    /// <see cref="RaftOperationStatus.Success"/> with the committed index immediately (no second WAL
    /// write).  If already <see cref="RaftProposalState.RolledBack"/>, returns
    /// <see cref="RaftOperationStatus.Errored"/> — the settled outcome wins and a commit is not
    /// applied.  The first terminal transition (Committed <em>or</em> RolledBack) is the one that
    /// persists; any subsequent opposite request reflects that settled state.
    /// </summary>
    private (RaftOperationStatus, long commitIndex) CommitLogs(
        HLCTimestamp ticketId,
        ulong? replyCorrelationId = null
    )
    {
        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, 0);

        if (!activeProposals.TryGetValue(ticketId, out RaftProposalQuorum? proposal))
            return (RaftOperationStatus.ProposalNotFound, 0);

        if (proposal.State == RaftProposalState.Committed)
            return (RaftOperationStatus.Success, proposal.LastLogIndex);

        if (proposal.State == RaftProposalState.RolledBack)
            return (RaftOperationStatus.Errored, 0);

        if (!proposal.HasQuorum())
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to commit proposal {Timestamp} without quorum...", host.LocalEndpoint, host.PartitionId, nodeState, ticketId);

            return (RaftOperationStatus.Errored, 0);
        }

        if (proposal.State != RaftProposalState.Completed)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to commit proposal {Timestamp} in state {State}...", host.LocalEndpoint, host.PartitionId, nodeState, ticketId, proposal.State);

            return (RaftOperationStatus.Errored, 0);
        }

        WALWriteOperation operation = wal.EnqueueCommit(proposal.Logs);
        Scheduling.RaftPendingWalOperation pendingCommit = RentPendingWalOp();
        pendingCommit.ReplyCorrelationId = replyCorrelationId;
        pendingCommit.Proposal = proposal;
        pendingCommit.TicketId = ticketId;
        pendingWalOperations[operation.OperationId] = pendingCommit;

        return (RaftOperationStatus.Pending, operation.LogIndex);
    }
    
    /// <summary>
    /// Marks proposals as rolled back
    /// </summary>
    /// <param name="ticketId"></param>
    /// <returns></returns>
    public Task RollbackLogsAsync(HLCTimestamp ticketId, ulong? replyCorrelationId)
    {
        (RaftOperationStatus status, long commitIndex) = RollbackLogs(ticketId, replyCorrelationId);

        if (status != RaftOperationStatus.Pending)
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, status, commitIndex));

        return Task.CompletedTask;
    }

    /// <summary>
    /// Idempotent: if the proposal is already <see cref="RaftProposalState.RolledBack"/>, returns
    /// <see cref="RaftOperationStatus.Success"/> immediately (no second WAL write).  If already
    /// <see cref="RaftProposalState.Committed"/>, returns <see cref="RaftOperationStatus.Errored"/>
    /// — the settled outcome wins and a rollback is not applied.  The first terminal transition
    /// (Committed <em>or</em> RolledBack) is the one that persists; any subsequent opposite request
    /// reflects that settled state.
    /// </summary>
    private (RaftOperationStatus, long commitIndex) RollbackLogs(
        HLCTimestamp ticketId,
        ulong? replyCorrelationId = null
    )
    {
        if (nodeState != RaftNodeState.Leader)
            return (RaftOperationStatus.NodeIsNotLeader, 0);

        if (!activeProposals.TryGetValue(ticketId, out RaftProposalQuorum? proposal))
            return (RaftOperationStatus.ProposalNotFound, 0);

        if (proposal.State == RaftProposalState.RolledBack)
            return (RaftOperationStatus.Success, 0);

        if (proposal.State == RaftProposalState.Committed)
            return (RaftOperationStatus.Errored, 0);

        if (!proposal.HasQuorum())
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to rollback proposal {Timestamp} without quorum...", host.LocalEndpoint, host.PartitionId, nodeState, ticketId);

            return (RaftOperationStatus.Errored, 0);
        }

        if (proposal.State != RaftProposalState.Completed)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Trying to rollback proposal {Timestamp} in state {State}...", host.LocalEndpoint, host.PartitionId, nodeState, ticketId, proposal.State);

            return (RaftOperationStatus.Errored, 0);
        }

        WALWriteOperation operation = wal.EnqueueRollback(proposal.Logs);
        Scheduling.RaftPendingWalOperation pendingRollback = RentPendingWalOp();
        pendingRollback.ReplyCorrelationId = replyCorrelationId;
        pendingRollback.Proposal = proposal;
        pendingRollback.TicketId = ticketId;
        pendingWalOperations[operation.OperationId] = pendingRollback;

        return (RaftOperationStatus.Pending, operation.LogIndex);
    }

    /// <summary>
    /// Increases the number of votes for a given term.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="term"></param>
    /// <returns></returns>
    private int IncreaseVotes(string endpoint, long term)
    {
        if (votes.TryGetValue(term, out HashSet<string>? votesPerEndpoint))
            votesPerEndpoint.Add(endpoint);
        else
            votes[term] = [endpoint];

        return votes[term].Count;
    }
    
    /// <summary>
    /// Appends logs to a specific node in the cluster.
    /// <paramref name="prevLogIndex"/> and <paramref name="prevLogTerm"/> are the Log Matching
    /// anchors: the id and term of the entry immediately preceding the first entry in
    /// <paramref name="logs"/>.  Both default to 0 (no anchor check) for heartbeats and live
    /// proposals where the leader knows the follower is in sync; they are set for backfill batches
    /// so a divergent follower can reject with <see cref="RaftOperationStatus.LogMismatch"/> and
    /// enable leader-side backtracking via <see cref="nextIndex"/>.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="timestamp"></param>
    /// <param name="logs"></param>
    /// <param name="prevLogIndex">Id of the entry immediately before the first entry in <paramref name="logs"/>; 0 skips the check.</param>
    /// <param name="prevLogTerm">Term of the entry at <paramref name="prevLogIndex"/>; 0 when index is 0.</param>
    private void AppendLogToNode(
        RaftNode node,
        HLCTimestamp timestamp,
        List<RaftLog>? logs,
        long prevLogIndex = 0,
        long prevLogTerm = 0,
        bool quiesce = false,
        AppendLogsGrpcLogCache? grpcLogCache = null)
    {
        AppendLogsRequest request;

        if (logs is null || logs.Count == 0)
            request = new(host.PartitionId, currentTerm, timestamp, host.LocalEndpoint) { Quiesce = quiesce };
        else
        {
            request = new(host.PartitionId, currentTerm, timestamp, host.LocalEndpoint, logs, prevLogIndex, prevLogTerm)
            {
                Quiesce = quiesce,
                GrpcLogCache = grpcLogCache,
            };

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "[{LocalEndpoint}/{PartitionId}/{State}] Enqueued entries for {Endpoint} {Timestamp} PrevLogIndex={PrevLogIndex} Logs={Logs}",
                    host.LocalEndpoint,
                    host.PartitionId,
                    nodeState,
                    node.Endpoint,
                    timestamp,
                    prevLogIndex,
                    string.Join(',', logs.Select(x => x.Id.ToString()))
                );
        }

        host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.AppendLogs, node, request));
    }

    /// <summary>
    /// Reads a bounded committed range for <paramref name="node"/> from the WAL and ships it via
    /// <see cref="AppendLogToNode"/> with the correct Log Matching anchors.
    /// Returns <see langword="true"/> when at least one entry was sent; <see langword="false"/> when
    /// the WAL read returns empty (compaction floor reached — caller decides whether to fall back to
    /// a snapshot transfer).
    /// </summary>
    /// <param name="node">The peer to send the batch to.</param>
    /// <param name="followerMaxLog">The highest committed index the leader believes the follower holds;
    /// used as the fallback start when <see cref="nextIndex"/> has not been backtracked below it.</param>
    /// <param name="timestamp">HLC timestamp to stamp the outbound request.</param>
    /// <param name="anchorToFollowerFrontier">
    /// Ignores <see cref="nextIndex"/> and starts the batch at <paramref name="followerMaxLog"/> + 1.
    /// Required by the fast-path re-supply of a <b>regressed</b> follower: <see cref="nextIndex"/> is
    /// derived from the monotonic <see cref="matchIndex"/> and so still points above the frontier the
    /// follower just reported, which is precisely the range that must be re-shipped.
    /// </param>
    /// <param name="round">
    /// Optional per-round memo (see <see cref="BackfillRoundBatches"/>). When supplied, a range already
    /// read in this heartbeat round is reused instead of being re-read and re-decoded from the WAL for
    /// each follower anchored at the same index — the common shape of a multi-follower catch-up. Pass
    /// <see langword="null"/> from one-off call sites, where there is nothing to share with.
    /// </param>
    private async Task<bool> TrySendBackfillBatchAsync(
        RaftNode node,
        long followerMaxLog,
        HLCTimestamp timestamp,
        bool anchorToFollowerFrontier = false,
        BackfillRoundBatches? round = null)
    {
        // Saturation backoff. This peer refused a batch because its WAL queue was full, and it
        // needs an interval in which to drain before another one arrives. Checked here rather
        // than at the call sites because this is the single choke point every entry-carrying
        // batch passes through — the heartbeat round, the ack fast-path re-supply, and the
        // forced heartbeat that follows every leadership publication all funnel into it. That
        // last one matters: the observed storm was driven by election churn, not by a timer, so
        // a throttle attached to the heartbeat interval would not have caught it.
        //
        // Returning false is the same answer this method gives when the WAL read comes back
        // empty: nothing was sent. Callers already treat that as "no batch this round" and try
        // again later, which is exactly the desired behaviour.
        if (backfillPausedUntilTicks.TryGetValue(node.Endpoint, out long pausedUntil))
        {
            if (host.GetMonotonicTimestamp() < pausedUntil)
                return false;

            backfillPausedUntilTicks.Remove(node.Endpoint);
        }

        long from = !anchorToFollowerFrontier && nextIndex.TryGetValue(node.Endpoint, out long ni) && ni <= localCommittedIndex
            ? ni
            : followerMaxLog + 1;

        long prevIdx = from - 1;

        if (round is not null && round.TryGet(from, out BackfillRoundBatches.Batch? cached))
        {
            if (cached!.Logs.Count == 0)
                return false;

            logger.LogDebugBackfilling(host.LocalEndpoint, host.PartitionId, nodeState, cached.Logs.Count, node.Endpoint, from, prevIdx, localCommittedIndex);

            AppendLogToNode(node, timestamp, cached.Logs, prevIdx, cached.PrevTerm, grpcLogCache: cached.GrpcLogCache);
            return true;
        }

        List<RaftLog> backfill = await wal.GetRangeAsync(from, host.Configuration.MaxBackfillEntriesPerRound).ConfigureAwait(false);

        if (backfill.Count == 0)
        {
            // Memoize the empty result as well: every follower anchored here would otherwise repeat the
            // same read before falling through to the snapshot path.
            round?.Add(from, backfill, 0);
            return false;
        }

        // Anchor-contiguity guard: an anchored batch asserts, via (prevIdx, prevTerm), that its
        // entries IMMEDIATELY follow the anchor. GetRangeAsync filters uncommitted entries, so a
        // Proposed run starting exactly at `from` (e.g. an inherited range whose commit markers
        // were lost and not yet re-committed) yields a batch whose first id is ABOVE `from` —
        // shipping it anchored at from-1 would land it over the follower's gap, advance nothing,
        // and repeat forever with no error anywhere (the observed Jepsen wedge). Refuse to ship:
        // returning false routes the heartbeat path to its snapshot fallback, and the log line
        // makes the state visible instead of silent. The inherited-tail re-commit in
        // DrainInheritedAppliesAsync repairs the underlying range, so this firing at all means
        // that repair has not landed (or a new gap source exists) — never suppress the log.
        if (backfill[0].Id != from)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Backfill read for {Endpoint} anchored at {From} returned committed entries starting at {FirstId} — uncommitted entries below block re-supply; refusing non-contiguous batch.",
                host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, from, backfill[0].Id);
            round?.Add(from, [], 0);
            return false;
        }

        long prevTerm = prevIdx > 0 ? await wal.GetAnyTermAtAsync(prevIdx).ConfigureAwait(false) : 0;

        BackfillRoundBatches.Batch? shared = round?.Add(from, backfill, prevTerm);

        logger.LogDebugBackfilling(host.LocalEndpoint, host.PartitionId, nodeState, backfill.Count, node.Endpoint, from, prevIdx, localCommittedIndex);

        AppendLogToNode(node, timestamp, backfill, prevIdx, prevTerm, grpcLogCache: shared?.GrpcLogCache);
        return true;
    }

    /// <summary>
    /// Called when a follower has acknowledged (or rejected) an AppendLogs request.
    /// On <see cref="RaftOperationStatus.Success"/> advances <see cref="matchIndex"/> and
    /// <see cref="nextIndex"/> for the peer and immediately ships the next bounded backfill
    /// batch if the follower is still behind, so convergence does not wait a full heartbeat
    /// interval per batch.
    /// On <see cref="RaftOperationStatus.LogMismatch"/> backtracks <see cref="nextIndex"/> using
    /// <c>max(1, min(nextIndex-1, followerMax+1))</c>, which always steps back
    /// at least one position even when the follower's max equals the anchor we sent.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="timestamp"></param>
    /// <param name="status"></param>
    /// <param name="committedIndex"></param>
    /// <param name="responseTerm">
    /// The term the acknowledging follower stamped on its reply. A follower ACK is only meaningful to
    /// the node that is currently the leader of that term; a delayed ACK from an earlier term must not
    /// repopulate progress/backfill/startCommitIndexes state after a step-down or term change. A value
    /// &lt; 0 means "not set" (legacy / in-process / test callers) and bypasses the fence, mirroring
    /// <see cref="CompleteWalOperationAsync"/>.
    /// </param>
    /// <summary>
    /// True when at least one peer is a voter. Replaces LINQ <c>Any</c> with a plain loop on
    /// paths that run per propose/commit — the capturing lambda allocated a closure per call.
    /// </summary>
    private bool HasVoterPeer()
    {
        IReadOnlyList<RaftNode> nodes = host.Nodes;
        for (int i = 0; i < nodes.Count; i++)
        {
            if (host.IsVoter(nodes[i].Endpoint))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Ordinal linear lookup of a peer by endpoint. Used on per-ack paths instead of LINQ
    /// <c>FirstOrDefault</c>, whose capturing lambda allocates a closure per call.
    /// </summary>
    private static RaftNode? FindNodeByEndpoint(IReadOnlyList<RaftNode> nodes, string endpoint)
    {
        for (int i = 0; i < nodes.Count; i++)
        {
            if (string.Equals(nodes[i].Endpoint, endpoint, StringComparison.Ordinal))
                return nodes[i];
        }

        return null;
    }

    public async ValueTask CompleteAppendLogsAsync(string endpoint, HLCTimestamp timestamp, RaftOperationStatus status, long committedIndex, long responseTerm = -1)
    {
        // ── Raft §5.1: a response stamped with a HIGHER term deposes us ─────────────────────────
        // Terms only enter a node through elections, so a higher response term proves a newer term
        // exists — this leader (or candidate) is stale and must step down and adopt it BEFORE the
        // fence below, which would discard the ack as "not my term" and learn nothing.
        //
        // This is the only repair channel for a per-partition term wedge (the Jepsen frozen-frontier
        // stall): a follower whose term was bumped by a failed election rejects every AppendLogs with
        // LeaderInOldTerm carrying its higher term, while pre-vote (correctly) keeps it from winning
        // an election against our still-healthy quorum — so without this step-down the leader ships
        // backfill forever, the follower rejects it forever, and that partition's replica on the
        // bumped node never commits another entry. Stepping down lets the next election converge the
        // term (either we re-win at a higher term and re-ship, or the bumped node's log wins).
        // Mirrors the higher-voteTerm adoption in VoteAsync: bookkeeping is gated on state, the term
        // adoption is not, and the adopted term is persisted with no vote so a crash cannot regress it.
        // Membership-fenced like inbound AppendLogs: only a committed roster member can depose a
        // leader, so an endpoint outside membership cannot churn leadership with a fabricated term.
        if (responseTerm > currentTerm && host.IsMember(endpoint))
        {
            bool stepDown = nodeState != RaftNodeState.Follower;

            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Stepping down on higher-term append ack from {Endpoint}: responseTerm={ResponseTerm} currentTerm={CurrentTerm} Status={Status}",
                host.LocalEndpoint, host.PartitionId, nodeState, endpoint, responseTerm, currentTerm, status);

            if (stepDown)
            {
                nodeState = RaftNodeState.Follower;
                host.Leader = "";
                lastCommitIndexes.Clear();
                nextIndex.Clear();
                matchIndex.Clear();
                regressedFrontiers.Clear();
                localCommittedIndex = -1;
                FailAllActiveProposalWaiters();
                activeProposals.Clear();
            }

            currentTerm = responseTerm;
            ResetPreVoteRound();

            if (stepDown)
                await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);

            await wal.PersistHardStateAsync(currentTerm, null).ConfigureAwait(false);
            return;
        }

        // ── Leader + term fence ──────────────────────────────────────────────────
        // Reject a stale ACK BEFORE any mutation (HLC receive, node activity, commit/backfill cursors,
        // matchIndex/nextIndex, startCommitIndexes). Without this, a delayed old-term ACK could make an
        // outdated follower look caught-up — e.g. appear eligible for a leadership transfer — or perturb
        // a later term's catch-up. responseTerm < 0 preserves the previous behaviour for callers that do
        // not stamp a term.
        if (responseTerm >= 0 && (nodeState != RaftNodeState.Leader || responseTerm != currentTerm))
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Ignoring stale CompleteAppendLogs from {Endpoint}: responseTerm={ResponseTerm} currentTerm={CurrentTerm}",
                host.LocalEndpoint, host.PartitionId, nodeState, endpoint, responseTerm, currentTerm);
            KommanderMetrics.StaleCompletionsTotal.Add(1,
                new KeyValuePair<string, object?>("reason", "append_ack_term_mismatch"));
            return;
        }

        HLCTimestamp currentTime = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);

        if (endpoint != host.LocalEndpoint)
            host.UpdateLastNodeActivity(endpoint, host.PartitionId, currentTime);
        
        // LogMismatch: the follower's log diverges at the prevLogIndex we sent.
        // committedIndex carries the follower's local max log at the time of rejection.
        // Backtrack formula: max(1, min(nextIndex[peer]-1, committedIndex+1)).
        // Taking min ensures we step back at least one position even when the follower's
        // max equals the anchor we just tried, preventing a livelock on repeated rejection
        // at the same anchor point.
        if (status == RaftOperationStatus.LogMismatch)
        {
            long currentNext  = nextIndex.GetValueOrDefault(endpoint, committedIndex + 2);
            long backtracked  = Math.Max(1, Math.Min(currentNext - 1, committedIndex + 1));
            nextIndex[endpoint] = backtracked;

            logger.LogDebugBacktrackingNextIndex(
                host.LocalEndpoint,
                host.PartitionId,
                nodeState,
                endpoint,
                currentNext,
                backtracked,
                committedIndex
            );

            return;
        }

        if (committedIndex > 0)
        {
            if (startCommitIndexes.TryGetValue(endpoint, out long currentIndex))
            {
                if (committedIndex > currentIndex)
                    startCommitIndexes[endpoint] = committedIndex;
            }
            else
                startCommitIndexes[endpoint] = committedIndex;

            logger.LogTraceSuccessfullyCompletedLogs(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, timestamp, committedIndex, (currentTime - timestamp).TotalMilliseconds);
        }

        if (status != RaftOperationStatus.Success)
        {
            // A saturated follower is the one rejection that must change the leader's behaviour
            // rather than just its logs. Every other status here is a condition the next batch
            // might resolve; this one is the follower saying it has no room, so re-sending
            // immediately is what keeps it from ever having room. Pause entry-carrying backfill
            // to this peer for a window and let its queue drain.
            if (status == RaftOperationStatus.FollowerWalSaturated)
                backfillPausedUntilTicks[endpoint] =
                    host.GetMonotonicTimestamp()
                    + (long)(host.Configuration.FollowerSaturationBackoff.TotalSeconds * Stopwatch.Frequency);

            LogFailedAppendAck(status, endpoint, timestamp, committedIndex);

            return;
        }

        // Record the follower's self-reported commit frontier — SUCCESS acks only. The
        // committedIndex field is overloaded: on Success it carries the follower's gap-aware
        // committed frontier (or -1: "nothing committed" on the single-fsync path, "no report"
        // from a legacy-path heartbeat ack), while rejection acks reuse the same field for the
        // follower's raw max log id (the LogMismatch backtrack anchor, the saturation report).
        // A raw max log sits arbitrarily far ABOVE the committed frontier whenever the log has
        // an uncommitted or non-contiguous tail — precisely the state of a follower whose
        // frontier stalled behind a lost commit marker while the unanchored live-propose
        // broadcast keeps growing its log. Folding a rejection's value into this map therefore
        // pinned an over-estimate that no later truthful (lower) report could correct, so
        // SendHeartbeat computed followerGap ≈ 0 and never backfilled the peer: its commit
        // frontier stalled forever while its log grew (the Jepsen stranded-replica findings).
        //
        // The update is last-writer-wins, not monotonic, for the same reason. The follower is
        // the only authority on its own frontier, and a genuine regression (crash-restart that
        // lost lazy commit markers) must be able to LOWER the record so the gap becomes visible
        // to the heartbeat backfill gate again. A reordered stale ack can transiently lower it
        // too — that costs one redundant, idempotent backfill batch and self-corrects on the
        // peer's next ack, whereas refusing lower reports cost a permanently stranded replica.
        // -1 is recorded only as an initial seed: a fresh follower must still enter the map so
        // SendHeartbeat's TryGetValue lag check sees it at all, but a legacy heartbeat ack's
        // "no report" must not erase a real frontier already recorded from an append ack.
        if (committedIndex >= 0 || !lastCommitIndexes.ContainsKey(endpoint))
            lastCommitIndexes[endpoint] = committedIndex;

        // Same-term success acks double as leadership proof: they feed the read-index confirmation
        // round and the check-quorum recency window. Only term-stamped acks count — an unstamped
        // (-1) ack passed the term fence above by default and could belong to an earlier stint of
        // this node's leadership.
        if (responseTerm >= 0 && endpoint != host.LocalEndpoint && nodeState == RaftNodeState.Leader)
        {
            lastVoterAckTicks[endpoint] = host.GetMonotonicTimestamp();
            await RegisterReadIndexAckAsync(endpoint).ConfigureAwait(false);
        }

        // Success: advance matchIndex and nextIndex for this peer so the backfill loop
        // knows the follower has caught up to at least committedIndex. matchIndex stays monotonic
        // (a stale in-flight ack must not drag a peer's recorded progress backwards), so the prior
        // value is captured first — it is the only evidence of a genuine frontier regression, which
        // the fast-path re-supply below keys on.
        // newMatchIndex mirrors matchIndex[endpoint] locally — this method previously re-read the
        // dictionary up to four more times below (a string hash + compare each) for a value fully
        // determined right here on the highest-frequency inbound message a leader handles.
        bool hadMatchIndex = matchIndex.TryGetValue(endpoint, out long priorMatchIndex);
        long newMatchIndex = priorMatchIndex;
        if (!hadMatchIndex || committedIndex > priorMatchIndex)
        {
            newMatchIndex = committedIndex;
            matchIndex[endpoint] = committedIndex;
        }
        nextIndex[endpoint] = newMatchIndex + 1;

        // Immediately ship the next bounded batch only while an active catch-up is in progress,
        // so a multi-batch backfill converges without stalling a full heartbeat per batch. This
        // must honour the same BackfillThreshold gate as the heartbeat path: a follower lagging by
        // ≤ threshold is intentionally not actively backfilled (small lag rides on normal
        // replication), and eagerly catching it up here would, e.g., make a barely-behind node look
        // fresh enough to receive a leadership transfer it should not.
        if (nodeState == RaftNodeState.Leader
            && localCommittedIndex - newMatchIndex > host.Configuration.BackfillThreshold)
        {
            RaftNode? behindNode = FindNodeByEndpoint(host.Nodes, endpoint);
            if (behindNode is not null)
                await TrySendBackfillBatchAsync(behindNode, committedIndex, host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId)).ConfigureAwait(false);
        }

        // Fast-path far-behind re-supply: a follower whose reported frontier trails the leader by more
        // than BackfillThreshold is streamed forward here on its own ack (anchored normally via
        // nextIndex), so a multi-batch catch-up converges without stalling a heartbeat per batch. This
        // mirrors the eager catch-up above but keys on the reported committed frontier rather than
        // matchIndex; confined to the fast path (flag off ⇒ a heartbeat reports -1 ⇒ never fires).
        if (host.Configuration.WalSingleFsyncCommit
            && nodeState == RaftNodeState.Leader
            && committedIndex >= 0
            && localCommittedIndex - committedIndex > host.Configuration.BackfillThreshold)
        {
            RaftNode? behindNode2 = FindNodeByEndpoint(host.Nodes, endpoint);
            if (behindNode2 is not null)
                await TrySendBackfillBatchAsync(behindNode2, committedIndex, host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId)).ConfigureAwait(false);
        }

        // Commit-frontier REGRESSION detection (crash-restart signature) — detection only; the repair
        // is done by SendHeartbeat, not here. A follower that restarted after losing its lazy commit
        // markers reports a frontier BELOW what the leader's monotonic matchIndex already recorded.
        //
        // This must NOT re-ship inline. An earlier version did, and it livelocked the cluster under load:
        // the anchored re-supply issues a WAL read plus an AppendLogs on the hot per-ack path, and when
        // it fires on a peer that is NOT genuinely crash-restarted — a reordered ack during ordinary
        // catch-up can transiently satisfy the "below matchIndex" test — the anchored batch fights the
        // in-flight forward catch-up (log-hole truncate → re-replicate) and starves the executor enough
        // to stall elections. So detection is split from action:
        //   * Here (cheap, per ack): if the ack looks like a genuine regression, RECORD the reported
        //     frontier. If instead the ack shows the peer at/above its recorded match (normal progress),
        //     CLEAR any pending note — a transient reordering self-heals before the next heartbeat and
        //     never triggers a re-supply.
        //   * In SendHeartbeat (paced, once per interval): act on any note still standing.
        // The "was caught up" clause (priorMatchIndex within BackfillThreshold of localCommittedIndex)
        // excludes a still-climbing joining/far-behind follower, whose low acks are catch-up, not
        // regression — those are handled by the threshold paths above.
        if (host.Configuration.WalSingleFsyncCommit && nodeState == RaftNodeState.Leader && committedIndex >= 0)
        {
            bool frontierRegressed = hadMatchIndex
                && committedIndex < priorMatchIndex
                && priorMatchIndex >= localCommittedIndex - host.Configuration.BackfillThreshold;

            if (frontierRegressed)
                regressedFrontiers[endpoint] = committedIndex;
            else if (committedIndex >= newMatchIndex)
                regressedFrontiers.Remove(endpoint);
        }

        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return;

        if (proposal.State != RaftProposalState.Incomplete)
            return;

        proposal.MarkNodeCompleted(endpoint);

        if (!proposal.HasQuorum())
        {
            logger.LogInfoProposalPartiallyCompletedAt(host.LocalEndpoint, host.PartitionId, nodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);
            return;
        }

        logger.LogInfoProposalCompletedAt(host.LocalEndpoint, host.PartitionId, nodeState, timestamp, (currentTime - proposal.StartTimestamp).TotalMilliseconds);

        proposal.SetState(RaftProposalState.Completed);

        // Observability (off in production): report the acknowledgements that carried this proposal to commit
        // quorum — the local leader (a voter, implicitly durable) plus every registered voter that acked.
        // Learner acks never appear here (learners are not registered in the quorum). A live quorum-discipline
        // checker uses these to verify each commit had a voter majority and no learner was counted.
        if (host.CommitAckObservationEnabled)
        {
            long committedId = proposal.LastLogIndex;
            int votersTotal = host.Nodes.Count(n => host.IsVoter(n.Endpoint)) + 1; // +1 for the local leader
            List<RaftCommitAckObservation> acks =
            [
                new(host.PartitionId, committedId, currentTerm, host.LocalEndpoint, host.IsVoter(host.LocalEndpoint), votersTotal),
            ];
            foreach (string acker in proposal.CompletedEndpoints())
                acks.Add(new RaftCommitAckObservation(host.PartitionId, committedId, currentTerm, acker, host.IsVoter(acker), votersTotal));
            host.ObserveCommitAcks(acks);
        }

        if (!proposal.AutoCommit)
        {
            logger.LogInfoProposalNoAutoCommit(host.LocalEndpoint, host.PartitionId, nodeState, timestamp);
            // Manual two-phase: the public ReplicateLogs(autoCommit:false) caller awaits the
            // propose phase, which succeeds here on propose-quorum-durable (the explicit commit
            // comes later via CommitLogs, whose result returns through the reply-correlation path,
            // not this waiter). CheckTicketCompletion historically reported {AutoCommit:false,
            // Completed} as Committed, so complete the waiter the same way — otherwise the caller
            // blocks until the 10 s timeout. CompleteLeaderCommit/Rollback fire TrySetResult again
            // later; both are idempotent no-ops once this has run.
            proposal.CompleteWaiter(RaftProposalTicketState.Committed, proposal.LastLogIndex);
            return;
        }

        // Single-fsync fast path: release the client ticket on propose-quorum-durable,
        // ahead of the commit fsync below. No-op unless WalSingleFsyncCommit is on.
        TryReleaseTicketOnQuorumDurable(proposal);

        WALWriteOperation operation = wal.EnqueueCommit(proposal.Logs);
        Scheduling.RaftPendingWalOperation pendingAutoCommit = RentPendingWalOp();
        pendingAutoCommit.Proposal = proposal;
        pendingAutoCommit.TicketId = timestamp;
        pendingWalOperations[operation.OperationId] = pendingAutoCommit;
    }

    public async Task CompleteWalOperationAsync(RaftWalCompletion? completion)
    {
        if (completion is null)
            return;

        // ── Partition fence ────────────────────────────────────────────────────
        // A completion for a different partition must never drive our state machine.
        // This can happen during the transition period if a completion is mis-routed.
        if (completion.PartitionId != host.PartitionId)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion for partition {CompletionPartition} delivered to partition {HostPartition}; discarding stale completion.",
                host.LocalEndpoint, host.PartitionId, nodeState,
                completion.PartitionId, host.PartitionId);
            KommanderMetrics.StaleCompletionsTotal.Add(1,
                new KeyValuePair<string, object?>("reason", "partition_mismatch"));
            return;
        }

        // ── Term fence ─────────────────────────────────────────────────────────
        // A completion submitted when the node was in an earlier term must not
        // advance state after a leadership or followership change.  Term -1 means
        // "not set" (legacy / test paths) and bypasses the fence.
        if (completion.Term >= 0 && completion.Term != currentTerm)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion for term {CompletionTerm} delivered in term {CurrentTerm}; discarding stale completion (op {OperationId}).",
                host.LocalEndpoint, host.PartitionId, nodeState,
                completion.Term, currentTerm, completion.OperationId);
            if (pendingWalOperations.Remove(completion.OperationId, out Scheduling.RaftPendingWalOperation? stalePending))
                ReturnPendingWalOp(stalePending);
            KommanderMetrics.StaleCompletionsTotal.Add(1,
                new KeyValuePair<string, object?>("reason", "term_mismatch"));
            return;
        }

        // ── Log-range validation ───────────────────────────────────────────────
        if (completion.MinLogIndex >= 0 && completion.MaxLogIndex >= 0 && completion.MinLogIndex > completion.MaxLogIndex)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion op {OperationId} has inverted log range [{Min},{Max}]; discarding.",
                host.LocalEndpoint, host.PartitionId, nodeState,
                completion.OperationId, completion.MinLogIndex, completion.MaxLogIndex);
            return;
        }

        // ── Pending-operation fence ────────────────────────────────────────────
        // Use the envelope OperationId (authoritative) as the lookup key.
        // All operation types that carry per-operation data in pending (leader and
        // follower paths) require the pending entry: a completion for an operation
        // that was never registered — or was already processed — must not drive
        // further state transitions; that would create orphaned proposals and
        // mis-routed client replies.  Only Compaction is fire-and-forget.
        bool found = pendingWalOperations.Remove(completion.OperationId, out RaftPendingWalOperation? pending);

        if (!found && completion.OperationType is
            WALWriteOperationType.LeaderPropose or
            WALWriteOperationType.LeaderCommit or
            WALWriteOperationType.LeaderRollback or
            WALWriteOperationType.FollowerAppend)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion op {OperationId} ({Type}) is not in pendingWalOperations; discarding unknown/superseded completion.",
                host.LocalEndpoint, host.PartitionId, nodeState,
                completion.OperationId, completion.OperationType);
            return;
        }

        // ── Min-log cross-check against pending entry ──────────────────────────
        if (pending?.Logs is { Count: > 0 } pendingLogs && completion.MinLogIndex >= 0)
        {
            // Indexed loop, not Enumerable.Min: this runs on every WAL completion (propose,
            // commit, rollback, follower append) and the LINQ path boxes the list enumerator.
            long actualMin = pendingLogs[0].Id;
            for (int i = 1; i < pendingLogs.Count; i++)
            {
                if (pendingLogs[i].Id < actualMin)
                    actualMin = pendingLogs[i].Id;
            }

            if (actualMin != completion.MinLogIndex)
            {
                logger.LogWarning(
                    "[{LocalEndpoint}/{PartitionId}/{State}] WAL completion op {OperationId} min-log-index mismatch: envelope {EnvelopeMin} vs actual {ActualMin}; discarding.",
                    host.LocalEndpoint, host.PartitionId, nodeState,
                    completion.OperationId, completion.MinLogIndex, actualMin);
                return;
            }
        }

        switch (completion.OperationType)
        {
            case WALWriteOperationType.LeaderPropose:
                await CompleteLeaderPropose(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.LeaderCommit:
                await CompleteLeaderCommit(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.LeaderRollback:
                await CompleteLeaderRollback(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.FollowerAppend:
                await CompleteFollowerAppend(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.Compaction:
            default:
                CompleteReply(pending?.ReplyCorrelationId, RaftResponseStatic.NoneResponse);
                break;
        }

        // Return the drained metadata object to the pool. Only reached on the main completion path
        // (rare error early-returns above simply let their entry be collected); the entry was already
        // removed from the dictionary at the fence above, and each op completes once, so there is no
        // double-return. The Complete* handlers have finished reading `pending` by here.
        if (found && pending is not null)
            ReturnPendingWalOp(pending);
    }

    /// <summary>
    /// Completes a leader propose WAL write by broadcasting the proposed entries to all peers.
    /// <para>
    /// The live-replication broadcast deliberately carries no Log Matching anchors. A follower that
    /// is transiently behind (e.g. a node still catching up during a concurrent join) would reject
    /// an anchored live proposal with <see cref="RaftOperationStatus.LogMismatch"/>, but the
    /// live-proposal quorum path has no recovery for a rejected proposal — it simply never reaches
    /// quorum and times out (<c>ProposalTimeout</c>), and under load this livelocks. Log Matching is
    /// therefore enforced only on the backfill path, which has <c>nextIndex</c> backtracking to
    /// recover; the leader never ships a non-contiguous live batch, so contiguity holds by
    /// construction on this path.
    /// </para>
    /// </summary>
    private async Task CompleteLeaderPropose(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;
        List<RaftLog> logs = pending?.Logs ?? [];
        bool autoCommit = pending?.AutoCommit ?? false;

        if (completion.Status != RaftOperationStatus.Success)
        {
            // The promotion-barrier no-op failed to even persist locally: the barrier can never
            // commit, so the unpublished leadership must be abandoned rather than held open.
            if (leadershipBarrierTicket != HLCTimestamp.Zero && ticketId == leadershipBarrierTicket && nodeState == RaftNodeState.Leader)
                await RevertUnpublishedPromotionAsync($"barrier propose failed ({completion.Status})").ConfigureAwait(false);

            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, completion.Status, ticketId));
            return;
        }

        RaftProposalQuorum proposalQuorum = RaftProposalQuorumPool.Rent(logs, autoCommit, ticketId);

        // Register the local leader as a voter participant and mark it completed immediately.
        // Must be done via AddExpectedNodeCompletion so MarkNodeCompleted (which now only
        // updates existing keys) correctly counts the self-vote in the quorum denominator.
        proposalQuorum.AddExpectedNodeCompletion(host.LocalEndpoint);
        proposalQuorum.MarkNodeCompleted(host.LocalEndpoint);

        AppendLogsGrpcLogCache? grpcLogCache = logs.Count > 0 ? new() : null;

        // Recorded while the loop already visits every peer, so the single-voter check below
        // does not re-scan host.Nodes with a closure-allocating LINQ Any on every propose.
        bool hasVoterPeer = false;

        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            // Learners receive log entries for catch-up but must not count toward quorum.
            // Only add voters to the quorum set; AppendLogToNode is called for all nodes.
            if (host.IsVoter(node.Endpoint))
            {
                hasVoterPeer = true;
                proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
            }
            AppendLogToNode(node, ticketId, logs, grpcLogCache: grpcLogCache);
        }

        if (!activeProposals.TryAdd(ticketId, proposalQuorum))
        {
            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Errored, HLCTimestamp.Zero));
            return;
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugProposedLogs(host.LocalEndpoint, host.PartitionId, nodeState, ticketId, string.Join(',', logs.Select(x => x.Id.ToString())));

        // Single-voter leader (no voter peers): the self-completion above already satisfies
        // quorum, but no voter ack will arrive to drive CompleteAppendLogsAsync. Drive the
        // Completed → (auto)commit transition here. Guarded to voter-only peers so learner-only
        // peers (which never ack for quorum) don't silently prevent single-voter commit.
        if (!hasVoterPeer)
        {
            proposalQuorum.SetState(RaftProposalState.Completed);

            if (autoCommit)
            {
                // A single-voter leader is its own quorum, so the propose fsync that just
                // completed already made the entry quorum-durable: the fast path applies here too.
                TryReleaseTicketOnQuorumDurable(proposalQuorum);

                WALWriteOperation commitOperation = wal.EnqueueCommit(proposalQuorum.Logs);
                Scheduling.RaftPendingWalOperation pendingFollowUpCommit = RentPendingWalOp();
                pendingFollowUpCommit.Proposal = proposalQuorum;
                pendingFollowUpCommit.TicketId = ticketId;
                pendingWalOperations[commitOperation.OperationId] = pendingFollowUpCommit;
            }
        }

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, ticketId));
    }

    /// <summary>
    /// Commits the proposal locally and broadcasts the committed entries to all peers.
    /// <para>
    /// The commit broadcast deliberately carries no Log Matching anchors: it re-ships ids that
    /// were already validated and accepted by each follower during the (anchored) propose, so a
    /// per-commit anchor adds no safety. It would, however, force a WAL term read
    /// (<c>GetAnyTermAtAsync</c>) on this hot completion path before fan-out, which stalls commit
    /// propagation under load and caused proposal timeouts. Divergence is detected and repaired on
    /// the propose and backfill paths, which remain anchored.
    /// </para>
    /// <para>
    /// Also applies every committed consumer entry to the local consumer state machine via
    /// <see cref="ApplyLogToConsumerAsync"/>. Followers receive these via
    /// <see cref="CompleteFollowerAppend"/>; the leader must apply them through the same path so
    /// its consumer projection stays consistent. This covers entries inherited from a prior term
    /// that have no local proposal waiter on this node and would otherwise be silently absent
    /// from the leader's consumer state.
    /// </para>
    /// <para>
    /// Applies are strictly in log order, exactly like the follower path. Pipelined proposals
    /// reach quorum in network order, so this completion can arrive while an earlier proposal is
    /// still in flight below it; the batch is then deferred (<see cref="DeferLeaderApplies"/>)
    /// rather than applied, because applying it would advance the cursor over the in-flight entry
    /// and permanently suppress its later delivery. The blocker's own completion flushes deferred
    /// batches in order via <see cref="FlushDeferredLeaderAppliesAsync"/>.
    /// </para>
    /// </summary>
    private async Task CompleteLeaderCommit(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        // Inherited-tail re-commit (see EnqueueInheritedRecommitMarkers): no proposal ticket, no
        // client waiter, no commit broadcast — the markers are lazy durability for entries already
        // committed and applied. Success needs nothing further; failure only means the on-disk
        // range stays Proposed (and therefore unbackfillable) until a later drain retries, which
        // is worth a log line but must not run the ordinary null-proposal failure handling below
        // (that path can revert an armed promotion).
        if (pending is { IsInheritedRecommit: true })
        {
            if (completion.Status != RaftOperationStatus.Success)
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Durable re-commit of inherited entries failed ({Status}) — the range stays Proposed on disk and cannot be backfilled until a later drain retries.",
                    host.LocalEndpoint, host.PartitionId, nodeState, completion.Status);
            return;
        }

        RaftProposalQuorum? proposal = pending?.Proposal;
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;

        if (completion.Status != RaftOperationStatus.Success || proposal is null)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't commit proposal {Timestamp}", host.LocalEndpoint, host.PartitionId, nodeState, ticketId);

            // A failed commit of the promotion-barrier no-op means this leader can never prove its
            // consumer projection complete: revert instead of holding unpublished leadership open.
            if (leadershipBarrierTicket != HLCTimestamp.Zero && ticketId == leadershipBarrierTicket && nodeState == RaftNodeState.Leader)
                await RevertUnpublishedPromotionAsync($"barrier commit failed ({completion.Status})").ConfigureAwait(false);

            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, completion.Status, 0));
            return;
        }

        proposal.SetState(RaftProposalState.Committed);
        // Unblock event-driven waiters on the public write path. If TryReleaseTicketOnQuorumDurable
        // already fired on the fast path (WalSingleFsyncCommit + autoCommit), TrySetResult is a no-op.
        proposal.CompleteWaiter(RaftProposalTicketState.Committed, completion.MaxLogIndex);
        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);

        if (completion.MaxLogIndex > localCommittedIndex)
            localCommittedIndex = completion.MaxLogIndex;

        AppendLogsGrpcLogCache? grpcLogCache = proposal.Logs.Count > 0 ? new() : null;

        // Send committed entries to ALL peers (voters + learners). proposal.Nodes only tracks
        // quorum voters; learners were excluded from quorum but still need log delivery so their
        // WAL stays in sync. host.Nodes already excludes self, so no self-skip is needed here.
        foreach (RaftNode node in host.Nodes)
            AppendLogToNode(node, ticketId, proposal.Logs, grpcLogCache: grpcLogCache);

        // Apply any inherited Proposed entries (from a prior term) that sit between the
        // last-applied cursor and this commit batch. These are entries proposed by the
        // previous leader, held as Proposed in our WAL, and now committed by quorum.
        // They have no local proposal waiter and were never delivered via CompleteFollowerAppend,
        // so the leader's consumer would silently miss them without this drain.
        long inheritedEnd = completion.MinLogIndex - 1;
        InheritedDrainStatus drainStatus = InheritedDrainStatus.Covered;
        if (inheritedEnd >= 0 && inheritedEnd > lastAppliedIndex)
        {
            // Reads race the WAL write queue on the leader too: with pipelined proposals, an
            // earlier entry (or its commit marker) can still sit in the write scheduler when this
            // completion's drain reads the backend, and the absent id is indistinguishable from a
            // real hole. Retry until the writes land and the drain covers the range, bounded by
            // the barrier timeout — stepping down on a transient read gap would churn leadership
            // under ordinary pipelined load. The loop exits as soon as the range is covered (or
            // the drain hits a current-term in-flight proposal, which retrying cannot resolve),
            // so the common case adds no latency.
            // Same bounds as the promotion drain: a sole voter only needs its own write queue to
            // drain, so its bound is short; with voter peers the full barrier timeout is worth
            // spending before stepping down.
            bool drainHasVoterPeers = HasVoterPeer();
            TimeSpan inheritedDrainBound = drainHasVoterPeers ? host.Configuration.LeadershipBarrierTimeout : TimeSpan.FromMilliseconds(250);
            long drainStartTicks = Stopwatch.GetTimestamp();

            while ((drainStatus =
                       await DrainInheritedAppliesAsync(lastAppliedIndex + 1, inheritedEnd).ConfigureAwait(false)) == InheritedDrainStatus.Hole)
            {
                if (Stopwatch.GetElapsedTime(drainStartTicks) > inheritedDrainBound)
                {
                    // Same sole-voter escape as the promotion drain: with no voter peer to defer
                    // to, stepping down just re-elects this node into the same gap forever. The
                    // gap is unrecoverable either way — keep serving and say so loudly.
                    if (!drainHasVoterPeers)
                    {
                        logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Inherited-entry drain incomplete with no voter peers to defer to — proceeding as sole voter; entries in the gap are unrecoverable.",
                            host.LocalEndpoint, host.PartitionId, nodeState);

                        // Deliver everything this survivor DOES hold past the gap, so only the
                        // genuinely absent entries are lost rather than the whole suffix. The
                        // skip-gaps drain still stops at a current-term in-flight proposal
                        // (self-quorum resolves those), so it reports Covered or BlockedByInFlight.
                        drainStatus = await DrainInheritedAppliesAsync(lastAppliedIndex + 1, inheritedEnd, skipGaps: true).ConfigureAwait(false);
                    }

                    break;
                }

                await Task.Delay(2).ConfigureAwait(false);
            }
        }

        // Apply committed consumer entries to the local state machine. Mirrors the apply loop
        // in CompleteFollowerAppend so the leader's consumer projection stays in sync — including
        // its in-order discipline:
        //   * Covered — the cursor is contiguous up to this batch: apply it, then flush any
        //     out-of-order batches that deferred behind an entry this batch just resolved.
        //   * BlockedByInFlight — an earlier pipelined proposal is still awaiting quorum below
        //     this batch. Applying now would advance the cursor over that entry and its own
        //     completion would then be suppressed by the exactly-once guard, silently skipping a
        //     committed, client-acknowledged write on the leader alone (the Jepsen hole). Defer
        //     this batch; the blocker's completion flushes it in order.
        //   * Hole — delivering would advance the cursor over withheld entries and orphan them
        //     permanently: skip; this leader steps down below and the next leader (or a later
        //     drain) delivers everything in order.
        switch (drainStatus)
        {
            case InheritedDrainStatus.Covered:
                foreach (RaftLog log in proposal.Logs)
                    await ApplyLogToConsumerAsync(log).ConfigureAwait(false);

                await FlushDeferredLeaderAppliesAsync().ConfigureAwait(false);
                break;

            case InheritedDrainStatus.BlockedByInFlight:
                DeferLeaderApplies(completion.MinLogIndex, proposal.Logs);
                break;
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugCommittedLogs(
                host.LocalEndpoint,
                host.PartitionId,
                nodeState,
                ticketId,
                string.Join(',', proposal.Logs.Select(x => x.Id.ToString())),
                (currentTime - proposal.StartTimestamp).TotalMilliseconds
            );

        wal.NotifyCommitted();

        // Promotion barrier: this commit's inherited drain (above) has applied every prior-term
        // entry below the barrier no-op, so the consumer projection is now provably complete —
        // publish leadership. Fenced on state and term so a stale barrier completion from a
        // superseded promotion can never publish. An incomplete inherited drain (a WAL hole in the
        // inherited range) disproves projection completeness: publishing anyway would serve an
        // arbitrary missing committed range for the whole tenure (a leader is never backfilled),
        // so revert and let a node with a contiguous log win the next term.
        if (leadershipBarrierTicket != HLCTimestamp.Zero && ticketId == leadershipBarrierTicket)
        {
            leadershipBarrierTicket = HLCTimestamp.Zero;

            if (nodeState == RaftNodeState.Leader && currentTerm == leadershipBarrierTerm)
            {
                // BlockedByInFlight cannot legitimately happen here (the barrier is the first
                // proposal of the term, so nothing current-term sits below it), but if it ever
                // does the projection is just as unproven as with a hole: revert either way.
                if (drainStatus != InheritedDrainStatus.Covered)
                {
                    await RevertUnpublishedPromotionAsync("inherited-entry drain could not cover the pre-barrier range").ConfigureAwait(false);
                }
                else
                {
                    host.Leader = host.LocalEndpoint;

                    if (logger.IsEnabled(LogLevel.Information))
                        logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Promotion barrier committed at {Ticket}; leadership published",
                            host.LocalEndpoint, host.PartitionId, nodeState, ticketId);

                    await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint).ConfigureAwait(false);
                }
            }
        }
        else if (drainStatus == InheritedDrainStatus.Hole && nodeState == RaftNodeState.Leader)
        {
            // A hole below an ORDINARY commit is equally disqualifying: this leader's consumer
            // projection cannot cover the committed range and it is never backfilled, so every
            // grant it serves is minted from incomplete state. Ignoring the incomplete drain here
            // (while the batch apply advanced the cursor) is what silently orphaned the whole
            // inherited range. Step down; the entries this commit made durable are quorum-safe and
            // the next leader delivers them in order. BlockedByInFlight deliberately does NOT step
            // down: it is routine pipelining, and its batch was deferred above, not orphaned.
            logger.LogError("[{LocalEndpoint}/{PartitionId}/{State}] Inherited-entry drain incomplete on a leader commit — stepping down.",
                host.LocalEndpoint, host.PartitionId, nodeState);
            await RevertUnpublishedPromotionAsync("inherited-entry drain incomplete on a leader commit").ConfigureAwait(false);
        }

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, completion.MaxLogIndex));
    }

    /// <summary>
    /// Rolls back the proposal locally and broadcasts the rolled-back entries to all peers.
    /// Like <see cref="CompleteLeaderCommit"/>, this delivery carries no Log Matching anchors:
    /// it targets ids the follower already saw during the anchored propose, and adding a WAL term
    /// read on this completion path would stall propagation. LMP remains enforced on propose/backfill.
    /// </summary>
    private async Task CompleteLeaderRollback(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        RaftProposalQuorum? proposal = pending?.Proposal;
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;

        // A rolled-back promotion barrier (however it got here — the barrier is auto-commit and
        // internal, but a rollback request by ticket id is possible) can never publish leadership.
        if (leadershipBarrierTicket != HLCTimestamp.Zero && ticketId == leadershipBarrierTicket && nodeState == RaftNodeState.Leader)
            await RevertUnpublishedPromotionAsync("barrier proposal rolled back").ConfigureAwait(false);

        if (completion.Status != RaftOperationStatus.Success || proposal is null)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't rollback proposal {Timestamp}", host.LocalEndpoint, host.PartitionId, nodeState, ticketId);
            CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, completion.Status, 0));
            return;
        }

        proposal.SetState(RaftProposalState.RolledBack);
        // Signal failure to any event-driven waiter so the public write path is unblocked
        // immediately rather than waiting for the proposal to expire from activeProposals.
        proposal.CompleteWaiter(RaftProposalTicketState.NotFound, -1);

        AppendLogsGrpcLogCache? grpcLogCache = proposal.Logs.Count > 0 ? new() : null;

        // Same as CompleteLeaderCommit: deliver rollback to all peers, not just quorum voters.
        foreach (RaftNode node in host.Nodes)
            AppendLogToNode(node, ticketId, proposal.Logs, grpcLogCache: grpcLogCache);

        // Resolve the rolled-back range for apply ordering. Rolled-back ids are advance-only for
        // the applied cursor (ApplyLogToConsumerAsync never delivers non-Committed types), and a
        // pipelined batch that committed out of order above this range may be parked in
        // deferredLeaderApplies waiting for it — without this, that batch would only flush when a
        // later commit's inherited drain happened to read the rollback markers back from the WAL.
        // Uses the same drain gate as CompleteLeaderCommit so the pre-first-id sentinel and
        // compacted prefixes are classified by the snapshot floor, not by naive contiguity, but
        // with a single attempt and no step-down: a rollback is not a serving decision, so on
        // Hole (a write still queued behind the read) we simply leave the range for a later
        // commit's retrying drain instead of stalling this completion.
        // Term-fenced by CompleteWalOperationAsync, so a stale tenure's rollback never runs this.
        if (nodeState == RaftNodeState.Leader && proposal.Logs.Count > 0 && completion.MinLogIndex >= 0)
        {
            long rolledBackInheritedEnd = completion.MinLogIndex - 1;
            InheritedDrainStatus rollbackDrainStatus = InheritedDrainStatus.Covered;
            if (rolledBackInheritedEnd >= 0 && rolledBackInheritedEnd > lastAppliedIndex)
                rollbackDrainStatus = await DrainInheritedAppliesAsync(lastAppliedIndex + 1, rolledBackInheritedEnd).ConfigureAwait(false);

            switch (rollbackDrainStatus)
            {
                case InheritedDrainStatus.Covered:
                    foreach (RaftLog log in proposal.Logs)
                        await ApplyLogToConsumerAsync(log).ConfigureAwait(false);

                    await FlushDeferredLeaderAppliesAsync().ConfigureAwait(false);
                    break;

                case InheritedDrainStatus.BlockedByInFlight:
                    DeferLeaderApplies(completion.MinLogIndex, proposal.Logs);
                    break;
            }
        }

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebugRolledbackLogs(
                host.LocalEndpoint,
                host.PartitionId,
                nodeState,
                ticketId,
                string.Join(',', proposal.Logs.Select(x => x.Id.ToString()))
            );

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, completion.MaxLogIndex));
    }

    /// <summary>
    /// Finalises a follower append after the WAL write completes. Dispatches each committed
    /// log entry to the appropriate callback: entries on P0 with <c>LogType == "_RaftSystem"</c>
    /// go to <c>InvokeSystemReplicationReceived</c> (system coordinator); all other entries —
    /// including non-system types on P0 — go to <c>InvokeReplicationReceived</c> (consumer).
    /// This type-based routing is what allows P0 to host consumer data alongside coordinator
    /// entries without any WAL format change.
    /// </summary>
    private async Task CompleteFollowerAppend(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        string endpoint = pending!.Endpoint ?? "";
        long leaderTerm = completion.Term;
        HLCTimestamp timestamp = pending.Timestamp;
        // Report the WAL's gap-aware commit frontier, NOT the raw batch max (completion.MaxLogIndex).
        // The unanchored live-propose path (prevLogIndex==0) can write a lone high entry over a gap on
        // a behind follower without an LMP check, leaving a hole. GetMaxLog (Keys.Max) would then
        // advertise that high id as the follower's progress, the leader's backfill gate would see the
        // follower as caught up (localCommittedIndex - reported == 0), and the missing prefix would
        // never be repaired — a stable non-contiguous log. GetCommitIndex stops at the hole, so the
        // leader keeps matchIndex/nextIndex behind it and backfills the prefix forward until the log
        // is contiguous. This drives only backfill/nextIndex bookkeeping (not quorum commit, which is
        // the propose-ticket path), and mirrors the gap-aware heartbeat-ack report at the fast path.
        long committedIndex = completion.Status == RaftOperationStatus.Success ? wal.GetCommitIndex() : -1;

        if (completion.Status == RaftOperationStatus.Success)
        {
            // Exactly-once, IN-ORDER apply, bounded by the WAL's gap-aware committed frontier
            // (committedIndex = GetCommitIndex). Contract: deliver every committed id exactly once, in order,
            // never over a hole. The unanchored live-propose broadcast ships prevLogIndex=0, so a behind
            // follower can persist a high committed entry before its prefix exists; the commit frontier buffers
            // that id over the gap (AdvanceCommitFrontier), so committedIndex stops below the hole and it is
            // withheld until backfill fills the prefix.
            //
            // Fast path (no WAL read): deliver the contiguous committed prefix straight from this batch. This
            // is the steady-state case — entries arrive in order and this delivers them without a scheduler
            // round-trip. Stops at the first id that is not exactly frontier+1, is beyond the committed
            // frontier, or is not yet committed (Proposed) — anything the batch cannot deliver in order.
            foreach (RaftLog log in pending.Logs ?? [])
            {
                if (log.Id != lastAppliedIndex + 1 || log.Id > committedIndex)
                    break;
                if (log.Type == RaftLogType.Committed)
                {
                    // Promotion-barrier no-ops are consensus-internal: skip delivery, advance cursor.
                    if (log.LogType != RaftSystemConfig.LeadershipBarrierLogType)
                    {
                        if (host.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
                        {
                            if (!await host.InvokeSystemReplicationReceived(host.PartitionId, log).ConfigureAwait(false))
                                host.InvokeReplicationError(host.PartitionId, log);
                        }
                        else if (!await host.InvokeReplicationReceived(host.PartitionId, log).ConfigureAwait(false))
                            host.InvokeReplicationError(host.PartitionId, log);
                    }
                }
                else if (log.Type != RaftLogType.CommittedCheckpoint)
                    break;                          // Proposed/other non-committed entry: not deliverable yet.
                lastAppliedIndex = log.Id;          // advance over delivered entries and skipped checkpoints
            }

            // Slow path (rare): the committed frontier is still ahead of the applied cursor — a hole just
            // filled, so entries buffered by earlier out-of-order batches (no longer in this batch) became
            // deliverable. Drain them from the WAL in order. A no-op when the fast path already caught up.
            if (committedIndex > lastAppliedIndex)
                await DrainCommittedAppliesAsync(committedIndex).ConfigureAwait(false);

            wal.NotifyCommitted();
        }

        if (!string.IsNullOrEmpty(endpoint))
        {
            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs,
                new(endpoint),
                new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, completion.Status, committedIndex)
            ));
        }

        CompleteReply(pending.ReplyCorrelationId, RaftResponseStatic.NoneResponse);
    }

    /// <summary>
    /// Returns the event-driven completion task for an active proposal so that callers can
    /// await it directly instead of polling <see cref="CheckTicketCompletion"/>.
    /// Returns <c>null</c> when the proposal is not found in <see cref="activeProposals"/>
    /// (already cleaned up or never registered), in which case the caller should fall back
    /// to a single <see cref="CheckTicketCompletion"/> poll.
    /// </summary>
    public Task<(RaftProposalTicketState, long)>? GetTicketWaiterTask(HLCTimestamp timestamp)
    {
        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return null;
        return proposal.GetWaiterTask();
    }

    /// <summary>
    /// Checks whether a proposal has been completed/committed or not.
    /// </summary>
    /// <param name="timestamp"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    public (RaftProposalTicketState state, long commitIndex) CheckTicketCompletion(HLCTimestamp timestamp)
    {
        if (!activeProposals.TryGetValue(timestamp, out RaftProposalQuorum? proposal))
            return (RaftProposalTicketState.NotFound, -1);

        if (proposal is { AutoCommit: false, State: RaftProposalState.Completed } or { AutoCommit: true, State: RaftProposalState.Committed } or { AutoCommit: false, State: RaftProposalState.Committed })
            return (RaftProposalTicketState.Committed, proposal.LastLogIndex);

        return (RaftProposalTicketState.Proposed, -1);
    }

    /// <summary>
    /// Single-fsync commit fast path (<see cref="RaftConfiguration.WalSingleFsyncCommit"/>).
    /// For an <c>autoCommit</c> proposal whose propose quorum is already durable, advances the
    /// leader's commit frontier and moves the proposal to <see cref="RaftProposalState.Committed"/>
    /// <b>before</b> the commit fsync is enqueued, so the client ticket (<see cref="CheckTicketCompletion"/>)
    /// is released on quorum-durable rather than on the leader's own second fsync. The per-entry
    /// <c>Committed</c> record is still written by the subsequent <c>EnqueueCommit</c>; only the
    /// acknowledgement point moves earlier.
    /// <para>
    /// Safe because propose-quorum-durable is the true Raft commit point — a quorum holds the entry
    /// on disk — so "acked ⇒ durable on a quorum" is preserved. The frontier value reused here
    /// (<see cref="RaftProposalQuorum.LastLogIndex"/>) is exactly what <see cref="CompleteLeaderCommit"/>
    /// would set from <c>completion.MaxLogIndex</c>, and a quorum acking this proposal implies it
    /// holds every lower-id entry too (followers append contiguously), so advancing the frontier
    /// here cannot skip an unreplicated predecessor. <see cref="CompleteLeaderCommit"/> still runs
    /// afterward and re-applies the same (idempotent) advance.
    /// </para>
    /// <para>No-op unless the flag is on or the proposal is not <c>autoCommit</c>; the explicit
    /// two-phase path is untouched.</para>
    /// </summary>
    /// <summary>
    /// Completes the event-driven waiters for all active proposals with a failure result
    /// so that any caller awaiting them via <c>WaitForQuorum</c> is unblocked immediately
    /// when leadership is lost. Must be called before <c>activeProposals.Clear()</c> on
    /// every leader→follower transition so the proposal objects are still reachable.
    /// </summary>
    private void FailAllActiveProposalWaiters()
    {
        // Every call site is a leadership-loss (or failed-candidacy) transition, so any pending
        // promotion barrier is dead with the proposals: clear it here so no later completion for
        // its ticket can publish leadership for a term this node no longer leads. The publish path
        // is additionally fenced on nodeState/term, so this is defense in depth, not the only guard.
        //
        // Read-index waiters die with leadership for the same reason: a confirmation must never
        // survive the term it was requested in.
        FailAllReadIndexWaiters();

        leadershipBarrierTicket = HLCTimestamp.Zero;
        leadershipBarrierTerm = -1;
        leadershipBarrierArmedTicks = 0;

        foreach (RaftProposalQuorum proposal in activeProposals.Values)
            proposal.CompleteWaiter(RaftProposalTicketState.NotFound, -1);
    }

    private void TryReleaseTicketOnQuorumDurable(RaftProposalQuorum proposal)
    {
        if (!host.Configuration.WalSingleFsyncCommit || !proposal.AutoCommit)
            return;

        if (proposal.LastLogIndex > localCommittedIndex)
            localCommittedIndex = proposal.LastLogIndex;

        proposal.SetState(RaftProposalState.Committed);
        // Unblock any caller awaiting event-driven completion; CompleteLeaderCommit will
        // also fire TrySetResult, but TrySetResult is idempotent so the duplicate is safe.
        proposal.CompleteWaiter(RaftProposalTicketState.Committed, proposal.LastLogIndex);
    }

    /// <summary>
    /// Bookkeeping for one read-index quorum round: the commit frontier captured at round start,
    /// the same-term acks collected so far, and the reply correlations awaiting the result.
    /// Only ever touched on the partition executor thread.
    /// </summary>
    private sealed class ReadIndexRound
    {
        public long Term { get; init; }
        public long ReadIndex { get; init; }
        public long StartedTicks { get; init; }
        public HashSet<string> Acks { get; } = [];
        public List<ulong> Waiters { get; } = [];
    }

    /// <summary>
    /// Read-index leadership confirmation (Raft dissertation §6.4). Proves this node is still the
    /// leader with a same-term quorum ack round, then completes the reply once the local applied
    /// frontier covers the commit index captured at confirmation time. A local read served after a
    /// successful confirmation is linearizable; without it, a minority-partitioned leader that has
    /// not yet heard of its own deposition serves stale state as an authoritative success (writes
    /// already fail on such a node because replication fails — this closes the read half).
    /// <para>Fails immediately when this node is not the <b>published</b> leader: while the
    /// promotion barrier is armed, <c>nodeState</c> is already Leader but <c>host.Leader</c> is
    /// not, and a confirmation must not leak through before the barrier publishes.</para>
    /// <para>Concurrent callers coalesce into the in-flight round; a confirmation completed within
    /// the last heartbeat interval is reused outright. Expiry
    /// (<see cref="RaftConfiguration.LeadershipConfirmationTimeout"/>) is enforced from the leader
    /// tick; leadership loss fails all waiters via <see cref="FailAllReadIndexWaiters"/>.</para>
    /// </summary>
    public async Task ConfirmLeadershipAsync(ulong? replyCorrelationId)
    {
        if (nodeState != RaftNodeState.Leader || host.Leader != host.LocalEndpoint)
        {
            CompleteReply(replyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            return;
        }

        // No observer: nothing to confirm for. (The public path always registers a reply.)
        if (replyCorrelationId is null)
            return;

        long nowTicks = host.GetMonotonicTimestamp();

        // Fast path: a confirmation completed within the last heartbeat interval is exactly as
        // fresh as the acks it counted — pre-vote leader stickiness keeps any rival from winning
        // an election quorum inside that window. The applied-frontier wait still runs against the
        // CURRENT commit frontier (strictly stronger than the round's capture), so a write this
        // leader committed after the confirmation is never missed by a later read.
        if (lastLeadershipConfirmedTerm == currentTerm
            && lastLeadershipConfirmedTicks != 0
            && MonotonicElapsed(lastLeadershipConfirmedTicks, nowTicks) < host.Configuration.HeartbeatInterval)
        {
            CompleteOrParkReadIndexWaiter(replyCorrelationId.Value, localCommittedIndex, nowTicks);
            return;
        }

        if (readIndexRound is not null)
        {
            // Coalesce: joining an in-flight round is only linearizable while the round's captured
            // read index still covers the commit frontier — otherwise a write that completed after
            // round start (but before this read began) could be missed. Late arrivals chain into
            // the next round instead.
            if (readIndexRound.ReadIndex >= localCommittedIndex)
                readIndexRound.Waiters.Add(replyCorrelationId.Value);
            else
                readIndexPendingWaiters.Add((replyCorrelationId.Value, nowTicks));
            return;
        }

        await StartReadIndexRoundAsync(replyCorrelationId.Value, nowTicks).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a read-index round: captures <c>(currentTerm, localCommittedIndex)</c> and fires a
    /// forced heartbeat so every peer produces a same-term ack. A quiesced leader is woken first —
    /// the ack round itself is the leadership proof, so quiescence needs no safety carve-out, only
    /// this wake-up; the tick then keeps heartbeating (and thus retrying the round) until the
    /// waiters confirm or expire.
    /// </summary>
    private async Task StartReadIndexRoundAsync(ulong waiterCorrelationId, long startedTicks)
    {
        ReadIndexRound round = new()
        {
            Term = currentTerm,
            ReadIndex = localCommittedIndex,
            StartedTicks = startedTicks,
        };
        round.Waiters.Add(waiterCorrelationId);
        readIndexRound = round;

        // Single-voter cluster: the leader alone is the quorum — confirm without any ack round.
        if (TryConfirmReadIndexQuorum())
            return;

        if (quiesced)
            SetQuiesced(false);

        await SendHeartbeat(true).ConfigureAwait(false);
    }

    /// <summary>
    /// Confirms the in-flight round if its same-term acks (plus the leader itself) reach a
    /// majority of voters. Voter status is re-evaluated at count time so an endpoint demoted
    /// mid-round cannot carry the quorum. On confirmation every waiter moves to the
    /// applied-frontier wait keyed on the round's captured read index.
    /// </summary>
    private bool TryConfirmReadIndexQuorum()
    {
        ReadIndexRound? round = readIndexRound;
        if (round is null)
            return false;

        int votersTotal = 1;  // the local leader
        int acked = 0;
        foreach (RaftNode node in host.Nodes)
        {
            if (!host.IsVoter(node.Endpoint))
                continue;
            votersTotal++;
            if (round.Acks.Contains(node.Endpoint))
                acked++;
        }

        if (acked + 1 < votersTotal / 2 + 1)
            return false;

        readIndexRound = null;
        lastLeadershipConfirmedTicks = host.GetMonotonicTimestamp();
        lastLeadershipConfirmedTerm = round.Term;

        foreach (ulong waiter in round.Waiters)
            CompleteOrParkReadIndexWaiter(waiter, round.ReadIndex, round.StartedTicks);

        return true;
    }

    /// <summary>
    /// Second half of the read-index contract: the reply completes only once
    /// <see cref="lastAppliedIndex"/> covers the captured commit index, so a local read issued
    /// after the confirmation observes every entry committed at capture time.
    /// </summary>
    private void CompleteOrParkReadIndexWaiter(ulong correlationId, long requiredIndex, long startedTicks)
    {
        if (lastAppliedIndex >= requiredIndex)
        {
            CompleteReply(correlationId, new(RaftResponseType.None, RaftOperationStatus.Success, requiredIndex));
            return;
        }

        readIndexApplyWaiters.Add((correlationId, requiredIndex, startedTicks));
    }

    /// <summary>
    /// Non-leader half of <c>IRaft.ConfirmLocalApplicationAsync</c>: parks the caller until
    /// <see cref="lastAppliedIndex"/> covers <paramref name="requiredIndex"/> — a commit index the
    /// partition leader confirmed with a same-term quorum ack round <b>after the caller's request
    /// began</b>. The leadership proof already happened remotely, so no node-state guard applies
    /// here; this method only supplies the local applied-frontier wait, reusing
    /// <see cref="readIndexApplyWaiters"/> (completed wherever the applied frontier advances,
    /// expired from the tick in every node state, failed on leadership transitions). A non-success
    /// reply means "not confirmed" — the caller must skip or defer its destructive action, never
    /// treat it as "confirmed enough".
    /// </summary>
    public void WaitLocalApplication(long requiredIndex, ulong? replyCorrelationId)
    {
        if (replyCorrelationId is null)
            return;

        CompleteOrParkReadIndexWaiter(replyCorrelationId.Value, requiredIndex, host.GetMonotonicTimestamp());
    }

    /// <summary>
    /// Feeds a same-term successful append/heartbeat ack into the in-flight read-index round.
    /// Any such ack proves the peer still recognises this leader at <see cref="currentTerm"/>
    /// no earlier than round start (rounds only accumulate acks while open), so heartbeat and
    /// live-replication acks both count. Confirming may chain-start the next round for waiters
    /// that arrived after the commit frontier moved.
    /// </summary>
    private async ValueTask RegisterReadIndexAckAsync(string endpoint)
    {
        ReadIndexRound? round = readIndexRound;
        if (round is null || !host.IsVoter(endpoint))
            return;

        round.Acks.Add(endpoint);

        if (TryConfirmReadIndexQuorum())
            await StartPendingReadIndexRoundAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Starts a new round for the callers that could not join the previous one (the commit
    /// frontier had moved past its capture). Their timeout budget is anchored at the earliest
    /// caller's arrival, so chaining cannot extend a caller's wait past the configured timeout.
    /// </summary>
    private async ValueTask StartPendingReadIndexRoundAsync()
    {
        if (readIndexPendingWaiters.Count == 0 || nodeState != RaftNodeState.Leader)
            return;

        ReadIndexRound round = new()
        {
            Term = currentTerm,
            ReadIndex = localCommittedIndex,
            StartedTicks = readIndexPendingWaiters[0].StartedTicks,
        };
        foreach ((ulong correlationId, _) in readIndexPendingWaiters)
            round.Waiters.Add(correlationId);
        readIndexPendingWaiters.Clear();
        readIndexRound = round;

        if (TryConfirmReadIndexQuorum())
            return;

        if (quiesced)
            SetQuiesced(false);

        await SendHeartbeat(true).ConfigureAwait(false);
    }

    /// <summary>
    /// Completes any quorum-confirmed readers whose required commit index the applied frontier
    /// now covers. Called wherever <see cref="lastAppliedIndex"/> advances; O(1) when no reader
    /// is parked, which is the steady state.
    /// </summary>
    private void CompleteReadIndexApplyWaiters()
    {
        if (readIndexApplyWaiters.Count == 0)
            return;

        for (int i = readIndexApplyWaiters.Count - 1; i >= 0; i--)
        {
            (ulong correlationId, long requiredIndex, _) = readIndexApplyWaiters[i];
            if (lastAppliedIndex < requiredIndex)
                continue;

            CompleteReply(correlationId, new(RaftResponseType.None, RaftOperationStatus.Success, requiredIndex));
            readIndexApplyWaiters.RemoveAt(i);
        }
    }

    /// <summary>
    /// Enforces <see cref="RaftConfiguration.LeadershipConfirmationTimeout"/> from the leader
    /// tick: an expired round fails all its waiters (a minority-partitioned leader reaches this —
    /// it can never collect the acks), pending waiters that outwaited the timeout fail
    /// individually, and applied-frontier waiters are bounded too so a wedged consumer cannot
    /// park readers forever. An expired round chain-starts the next one so queued callers still
    /// get their own attempt.
    /// </summary>
    private async ValueTask ExpireReadIndexWaitersAsync(long nowTicks)
    {
        TimeSpan timeout = host.Configuration.LeadershipConfirmationTimeout;

        if (readIndexRound is not null && MonotonicElapsed(readIndexRound.StartedTicks, nowTicks) >= timeout)
        {
            ReadIndexRound round = readIndexRound;
            readIndexRound = null;

            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Read-index round expired without quorum ({Waiters} waiter(s), {Acks} ack(s)). Term={CurrentTerm}",
                host.LocalEndpoint, host.PartitionId, nodeState, round.Waiters.Count, round.Acks.Count, currentTerm);

            foreach (ulong waiter in round.Waiters)
                CompleteReply(waiter, new(RaftResponseType.None, RaftOperationStatus.ProposalTimeout, 0L));

            await StartPendingReadIndexRoundAsync().ConfigureAwait(false);
        }

        for (int i = readIndexPendingWaiters.Count - 1; i >= 0; i--)
        {
            if (MonotonicElapsed(readIndexPendingWaiters[i].StartedTicks, nowTicks) < timeout)
                continue;

            CompleteReply(readIndexPendingWaiters[i].CorrelationId, new(RaftResponseType.None, RaftOperationStatus.ProposalTimeout, 0L));
            readIndexPendingWaiters.RemoveAt(i);
        }

        for (int i = readIndexApplyWaiters.Count - 1; i >= 0; i--)
        {
            if (MonotonicElapsed(readIndexApplyWaiters[i].StartedTicks, nowTicks) < timeout)
                continue;

            CompleteReply(readIndexApplyWaiters[i].CorrelationId, new(RaftResponseType.None, RaftOperationStatus.ProposalTimeout, 0L));
            readIndexApplyWaiters.RemoveAt(i);
        }
    }

    /// <summary>
    /// Fails every read-index waiter (in-flight round, chained, and applied-frontier) and resets
    /// the confirmation fast path and check-quorum bookkeeping. Must run on every leadership-loss
    /// transition — a confirmation must never survive the term it was requested in. Invoked from
    /// <see cref="FailAllActiveProposalWaiters"/>, which every demotion path already calls.
    /// </summary>
    private void FailAllReadIndexWaiters()
    {
        lastLeadershipConfirmedTicks = 0;
        lastLeadershipConfirmedTerm = -1;
        lastVoterAckTicks.Clear();
        lastQuorumContactTicks = 0;

        if (readIndexRound is not null)
        {
            foreach (ulong waiter in readIndexRound.Waiters)
                CompleteReply(waiter, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            readIndexRound = null;
        }

        if (readIndexPendingWaiters.Count > 0)
        {
            foreach ((ulong correlationId, _) in readIndexPendingWaiters)
                CompleteReply(correlationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            readIndexPendingWaiters.Clear();
        }

        if (readIndexApplyWaiters.Count > 0)
        {
            foreach ((ulong correlationId, _, _) in readIndexApplyWaiters)
                CompleteReply(correlationId, new(RaftResponseType.None, RaftOperationStatus.NodeIsNotLeader, 0L));
            readIndexApplyWaiters.Clear();
        }
    }

    /// <summary>
    /// Resets read-index and check-quorum bookkeeping at the start of a leadership stint: any
    /// remembered confirmation or ack recency belongs to a previous stint and must not be reused,
    /// and the check-quorum grace window starts now (a fresh leader has heard from a quorum by
    /// definition — its election — but those grants are not append acks).
    /// </summary>
    private void ResetLeadershipConfirmationState(long nowTicks)
    {
        lastLeadershipConfirmedTicks = 0;
        lastLeadershipConfirmedTerm = -1;
        lastVoterAckTicks.Clear();
        lastQuorumContactTicks = nowTicks;
    }

    /// <summary>
    /// Check-quorum step-down: this leader has not heard same-term acks from a majority of voters
    /// for the configured window, so it is almost certainly isolated and possibly already deposed.
    /// Mirrors the bookkeeping of <see cref="StepDownAsync"/> but sends no step-down notice — the
    /// peers are unreachable by hypothesis, and the majority side elects on its own timeout.
    /// Setting <c>lastHeartbeatTicks</c> here means this node waits a full election timeout before
    /// campaigning, giving a majority-side leader time to adopt it as a follower first.
    /// </summary>
    private async Task StepDownOnQuorumLossAsync()
    {
        logger.LogWarning(
            "[{LocalEndpoint}/{PartitionId}/{State}] Check-quorum: no majority of voter acks within {Window} — stepping down. Term={CurrentTerm}",
            host.LocalEndpoint, host.PartitionId, nodeState,
            host.Configuration.HeartbeatInterval * host.Configuration.CheckQuorumIntervalMultiplier, currentTerm);

        HLCTimestamp currentTime = host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId);
        long nowTicks = host.GetMonotonicTimestamp();

        nodeState = RaftNodeState.Follower;
        host.Leader = "";
        lastHeartbeat = currentTime;
        lastVotation = currentTime;
        lastHeartbeatTicks = nowTicks;
        lastVotationTicks = nowTicks;
        expectedLeaders.Clear();
        lastCommitIndexes.Clear();
        nextIndex.Clear();
        matchIndex.Clear();
        regressedFrontiers.Clear();
        localCommittedIndex = -1;
        FailAllActiveProposalWaiters();
        activeProposals.Clear();
        lastProposalAt = HLCTimestamp.Zero;
        lastProposalAtTicks = 0;
        SetQuiesced(false);

        await host.InvokeLeaderChanged(host.PartitionId, "").ConfigureAwait(false);
    }

    // ReadExactAsync body lives in StreamUtils; forwarded here for call-site compatibility.
    private static ValueTask<int> ReadExactAsync(Stream stream, byte[] buffer, int count, CancellationToken ct) =>
        StreamUtils.ReadExactAsync(stream, buffer, count, ct);

    /// <summary>
    /// Follower-side snapshot install on the single-writer executor path (Raft "Rule 7").
    ///
    /// <para>Runs the recoverable ordering: (1) validate the leader term and, on a higher term, take the
    /// same durable step-down as other leader RPCs; reject a stale leader without importing; (2) short-
    /// circuit as an idempotent success when a matching boundary is already installed at or below the
    /// index; (3) invoke the application import; (4) install the durable WAL boundary
    /// (<see cref="IRaftWalFacade.InstallSnapshotBoundaryAsync"/>) which retains the suffix on a matching
    /// boundary term and truncates it on conflict; (5) reconstruct the apply cursor; (6) acknowledge.</para>
    ///
    /// <para>Import runs on the executor thread so the whole install is serialized against every other
    /// partition operation. If import succeeds but the WAL write fails, the sender receives failure and
    /// retries the same snapshot; the repeated import must be idempotent for
    /// <c>(partition, SnapshotIndex, LastIncludedTerm)</c> per the transfer contract. Uses
    /// <see cref="CancellationToken.None"/> for the import so the caller cannot dispose the staged buffer
    /// while this method is still reading it.</para>
    /// </summary>
    public async Task<RaftResponse> InstallSnapshotAsync(SnapshotInstallRequest request)
    {
        string leaderEndpoint = request.LeaderEndpoint ?? "";
        long leaderTerm = request.LeaderTerm;
        long snapshotIndex = request.SnapshotIndex;

        // Idempotency (Rule 7.4): short-circuit ONLY when an installed snapshot BOUNDARY already covers this
        // index with a compatible identity — never merely because ordinary log entries reach the index. A
        // lagging follower can hold proposed/committed suffix entries through snapshotIndex while its
        // application state still needs the import; keying idempotency on the raw WAL max would acknowledge
        // installation without importing, and would let a stale or conflicting sender succeed off an unrelated
        // high id (bypassing the term/leader validation below). The installed checkpoint boundary is the
        // authoritative "already applied" signal. Return success early — before any term adoption/step-down —
        // so a redundant re-install never disrupts a caught-up node.
        long installedBoundary = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
        if (installedBoundary >= snapshotIndex)
        {
            // Confirm identity compatibility before treating this as a no-op. A newer installed boundary
            // (installedBoundary > snapshotIndex) supersedes the request. Otherwise the stored boundary term
            // at the index must match LastIncludedTerm; a -1 (compacted/unknown) term on either side is
            // treated as compatible (mirrors the log-matching boundary rule). A genuine term conflict is not
            // the same snapshot and falls through to full validation + a fresh install.
            long boundaryTermAtIndex = await wal.GetAnyTermAtAsync(snapshotIndex).ConfigureAwait(false);
            bool compatible = installedBoundary > snapshotIndex
                || boundaryTermAtIndex < 0
                || request.LastIncludedTerm < 0
                || boundaryTermAtIndex == request.LastIncludedTerm;
            if (compatible)
            {
                if (snapshotIndex > lastAppliedIndex)
                    lastAppliedIndex = snapshotIndex;
                wal.SeedCommitFrontierFromSnapshot(snapshotIndex, Math.Max(boundaryTermAtIndex, 0));
                return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Success, snapshotIndex);
            }
        }

        bool legacy = leaderTerm <= 0 || string.IsNullOrEmpty(leaderEndpoint);
        if (legacy && !host.Configuration.AllowLegacySnapshotSenders)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: legacy sender (LeaderTerm={LeaderTerm}, LeaderEndpoint='{Endpoint}') and AllowLegacySnapshotSenders is off.",
                host.LocalEndpoint, host.PartitionId, nodeState, leaderTerm, leaderEndpoint);
            return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
        }

        // The term of the entry the checkpoint boundary is stamped with. For a legacy sender we have no
        // authoritative last-included term, so fall back to our local current term (old behaviour).
        long boundaryTerm = legacy ? currentTerm : request.LastIncludedTerm;

        if (!legacy)
        {
            // Rule 7.1 — reject a stale leader without importing (mirror AppendLogsCoreAsync).
            if (currentTerm > leaderTerm)
            {
                logger.LogWarning(
                    "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot from stale leader {Endpoint}: LeaderTerm={LeaderTerm} < CurrentTerm={CurrentTerm}. Rejecting.",
                    host.LocalEndpoint, host.PartitionId, nodeState, leaderEndpoint, leaderTerm, currentTerm);
                return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
            }

            // Membership fence — mirror AppendLogsCoreAsync: a snapshot is a leader RPC, and only a
            // committed roster member can legitimately be a leader. Skipped for the already-accepted
            // leader so a briefly-lagging roster snapshot cannot reject the real leader.
            if (host.Leader != leaderEndpoint && !host.IsMember(leaderEndpoint))
            {
                logger.LogWarning(
                    "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: sender {Endpoint} is not a committed cluster member.",
                    host.LocalEndpoint, host.PartitionId, nodeState, leaderEndpoint);
                return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
            }

            // Rule 7.3 — election safety: at equal term there is exactly one leader. If we have already
            // adopted a different leader for this term, a snapshot from another endpoint is inconsistent.
            // (A legitimate new leader always arrives with a higher term, which passes this check.)
            if (currentTerm == leaderTerm && !string.IsNullOrEmpty(host.Leader) && host.Leader != leaderEndpoint)
            {
                logger.LogWarning(
                    "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: sender {Sender} conflicts with accepted leader {Leader} for term {Term}.",
                    host.LocalEndpoint, host.PartitionId, nodeState, leaderEndpoint, host.Leader, leaderTerm);
                return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
            }

            // Rule 7.2 — adopt the leader / durable step-down on a valid term, identical to the
            // AppendEntries path. A snapshot is a leader RPC, so it authoritatively identifies the term's
            // leader regardless of our vote record (expectedLeaders constrains voting only).
            if (host.Leader != leaderEndpoint || currentTerm != leaderTerm || nodeState != RaftNodeState.Follower)
            {
                logger.LogInfoLeaderIsNow(host.LocalEndpoint, host.PartitionId, nodeState, leaderEndpoint, leaderTerm);

                nodeState = RaftNodeState.Follower;
                host.Leader = leaderEndpoint;
                currentTerm = leaderTerm;
                lastCommitIndexes.Clear();
                nextIndex.Clear();
                matchIndex.Clear();
                regressedFrontiers.Clear();
                localCommittedIndex = -1;
                FailAllActiveProposalWaiters();
                activeProposals.Clear();
                expectedLeaders[leaderTerm] = leaderEndpoint;
                ResetPreVoteRound();

                await host.InvokeLeaderChanged(host.PartitionId, leaderEndpoint);
                await wal.PersistHardStateAsync(leaderTerm, leaderEndpoint).ConfigureAwait(false);
            }
        }

        // Ordering step 2 — invoke the application import. Must precede the durable WAL boundary so a
        // crash between them leaves recoverable state (import is idempotent; the boundary is not yet
        // durable so the sender retries the whole snapshot).
        try
        {
            if (request.Kind == SnapshotKind.SystemState)
            {
                IRaftSystemStateTransfer? systemTransfer = host.SystemStateTransfer;
                if (systemTransfer is null)
                {
                    logger.LogWarning(
                        "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: no IRaftSystemStateTransfer registered.",
                        host.LocalEndpoint, host.PartitionId, nodeState);
                    return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
                }

                await systemTransfer.ImportPartitionState(host.PartitionId, request.Snapshot, CancellationToken.None).ConfigureAwait(false);
            }
            else
            {
                IRaftStateMachineTransfer? rangeTransfer = host.StateMachineTransfer;
                if (rangeTransfer is null)
                {
                    logger.LogWarning(
                        "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: no IRaftStateMachineTransfer registered.",
                        host.LocalEndpoint, host.PartitionId, nodeState);
                    return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
                }

                await rangeTransfer.ImportRange(host.PartitionId, request.Snapshot, CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot import failed for index {Index}: {Message}",
                host.LocalEndpoint, host.PartitionId, nodeState, snapshotIndex, ex.Message);
            return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
        }

        // Ordering step 3 + Rule 7.5/7.6 — install the durable checkpoint boundary. The backend retains
        // the suffix above the index when its stored term matches boundaryTerm and truncates it on
        // conflict, atomically.
        (RaftOperationStatus boundaryStatus, bool suffixTruncated) =
            await wal.InstallSnapshotBoundaryAsync(snapshotIndex, boundaryTerm).ConfigureAwait(false);
        if (boundaryStatus != RaftOperationStatus.Success)
        {
            logger.LogError(
                "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot WAL boundary failed for index {Index}: {Status}. Import succeeded; sender will retry.",
                host.LocalEndpoint, host.PartitionId, nodeState, snapshotIndex, boundaryStatus);
            return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
        }

        // Ordering step 4 + Rule 7.7 — reconstruct the apply cursor from the installed boundary so a later
        // promotion does not re-deliver the imported prefix (mirrors CompleteRestoreAsync's cursor seed), and
        // advance the in-memory commit frontier to the boundary so GetCommitIndex reflects the compacted prefix
        // as committed (otherwise post-snapshot consumer delivery and backfill reporting stall below it).
        if (snapshotIndex > lastAppliedIndex)
            lastAppliedIndex = snapshotIndex;
        wal.SeedCommitFrontierFromSnapshot(snapshotIndex, Math.Max(boundaryTerm, 0));

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInfoReceiveInstallSnapshot(host.LocalEndpoint, host.PartitionId, snapshotIndex);

        return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Success, snapshotIndex);
    }

    /// <summary>
    /// Advances <c>lastCommitIndexes</c> for <paramref name="endpoint"/> after the background
    /// snapshot task confirmed successful installation. Called on the executor thread via the
    /// <c>postToExecutor</c> callback; delegates ownership update to <see cref="snapshotSender"/>.
    /// </summary>
    public void CompleteSnapshotInstalled(string endpoint, long snapshotIndex) =>
        snapshotSender.CompleteSnapshotInstalled(endpoint, snapshotIndex);

}
