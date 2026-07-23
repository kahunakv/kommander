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
    private readonly Dictionary<long, string> expectedLeaders = [];
    private readonly Dictionary<HLCTimestamp, RaftProposalQuorum> activeProposals = [];
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

    private RaftNodeState nodeState = RaftNodeState.Follower;

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
    /// Monotonic tick of the last regressed-frontier re-supply sent to each peer, used to rate-limit
    /// that path to at most one batch per <see cref="RaftConfiguration.HeartbeatInterval"/> per peer.
    ///
    /// <para>Without this, every ack already in flight when the regression is first observed triggers
    /// its own re-supply of the same range — the follower cannot have processed the first batch yet, so
    /// it is still reporting the low frontier — and a burst of acks re-ships the identical entries
    /// several times. Harmless (the re-shipped <c>Committed</c> copies are idempotent upserts) but pure
    /// waste on the WAL read path and the wire. Rate-limiting by time rather than suppressing a repeat
    /// of the same anchor keeps the retry: if a re-supply is lost, the next heartbeat re-issues it.</para>
    ///
    /// <para>Entries are never explicitly evicted. The value is only ever compared against "now", so a
    /// stale entry from a prior term can at worst delay one re-supply by a single heartbeat interval.</para>
    /// </summary>
    private readonly Dictionary<string, long> lastRegressionResupplyTicks = [];

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

    public RaftNodeState NodeState => nodeState;
    public long CurrentTerm => currentTerm;

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

        switch (nodeState)
        {
            // if node is leader just send hearthbeats every Configuration.HeartbeatInterval
            case RaftNodeState.Leader:
            {
                if (quiesced)
                    return;

                // B3: heartbeat cadence measured on the monotonic clock — a heartbeat received from a
                // skewed peer must not inflate the interval and suppress our own heartbeats.
                if (currentTime != HLCTimestamp.Zero && (MonotonicElapsed(lastHeartbeatTicks, nowTicks) >= host.Configuration.HeartbeatInterval))
                {
                    // When quiescence is on and the partition has been idle longer than QuiesceAfter,
                    // send a quiesce marker to followers and stop heartbeating.  Followers switch to
                    // SWIM-based election gating once they receive the marker.
                    if (host.Configuration.EnableQuiescence
                        && !quiesced
                        && activeProposals.Count == 0
                        && lastProposalAtTicks != 0
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

        long localMaxLogId = await wal.GetMaxLogAsync().ConfigureAwait(false);
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
    /// <see cref="IRaftPartitionHost.Leader"/> until AFTER a full drain of committed
    /// WAL entries (see <see cref="DrainCommittedAppliesAsync"/>).
    ///
    /// <para>This enforces the ordering invariant: by the time a caller sees
    /// <c>host.Leader == host.LocalEndpoint</c> (the gate for <c>AmILeader</c>),
    /// the consumer state machine has already applied every committed entry up to
    /// the promotion commit frontier.  Inherited entries from a prior term that are
    /// committed after promotion are delivered via <see cref="CompleteLeaderCommit"/>
    /// through the same apply path.</para>
    ///
    /// <para><b>Atomicity:</b> the promotion is all-or-nothing. If the drain throws
    /// (WAL read backpressure, scheduler shutdown, etc.), the node is reverted to
    /// <see cref="RaftNodeState.Follower"/> so it is never left in a half-promoted
    /// state where <c>nodeState == Leader</c> but <c>host.Leader</c> is unset and
    /// heartbeats have not started.  The exception is re-thrown; the cluster
    /// self-heals by electing a replacement in the next term.</para>
    ///
    /// <para><b>Latency note:</b> the drain holds the partition executor for the full
    /// duration of the backlog (one <c>ReadScheduler</c> round-trip per 512-entry batch
    /// plus one consumer callback per committed entry).  <c>SendHeartbeat</c> runs only
    /// after this method returns, so a node with a large unapplied backlog will delay
    /// its first heartbeat and risk triggering a further election round.  No explicit
    /// bound on the drain is enforced today; a future improvement could cap the drain
    /// and resume it in background once leadership is established.</para>
    /// </summary>
    private async Task<HLCTimestamp> BecomeLeaderAsync()
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
        SetQuiesced(false);

        try
        {
            // Drain all committed entries up to the promotion frontier. By the time this
            // returns every InvokeReplicationReceived call has completed, so the consumer
            // projection is current before the partition is advertised as the serving leader.
            // Consumer exceptions are caught inside ApplyLogToConsumerAsync and do not
            // propagate here; only WAL-level errors (backpressure, shutdown) can reach this
            // catch block.
            await DrainCommittedAppliesAsync(commitFrontier).ConfigureAwait(false);
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

        // Publish leader status only after the drain. host.Leader is the gate for
        // AmILeader, so no external observer can see leader == self while the drain
        // is in progress.
        host.Leader = host.LocalEndpoint;
        return ts;
    }

    /// <summary>
    /// Delivers every committed WAL entry from <c>lastAppliedIndex + 1</c> through
    /// <paramref name="upToIndex"/> (inclusive) to the consumer via
    /// <see cref="ApplyLogToConsumerAsync"/>.  Reads the WAL in bounded batches to
    /// avoid loading the full tail into memory.
    ///
    /// <para>A no-op when <see cref="lastAppliedIndex"/> already covers
    /// <paramref name="upToIndex"/> or the WAL is empty for this range.</para>
    /// </summary>
    private async Task DrainCommittedAppliesAsync(long upToIndex)
    {
        if (upToIndex < 0 || lastAppliedIndex >= upToIndex)
            return;

        const int BatchSize = 512;
        long from = lastAppliedIndex + 1;

        while (from <= upToIndex)
        {
            List<RaftLog> batch = await wal.GetRangeAsync(from, BatchSize).ConfigureAwait(false);
            if (batch.Count == 0)
                break;

            foreach (RaftLog log in batch)
            {
                if (log.Id > upToIndex)
                    return;
                await ApplyLogToConsumerAsync(log).ConfigureAwait(false);
            }

            long next = lastAppliedIndex + 1;
            if (next <= from)   // guard: lastAppliedIndex did not advance past 'from' — would loop forever
                break;
            from = next;
        }
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
        if (log.Type == RaftLogType.Committed)
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
    /// </summary>
    private async Task DrainInheritedAppliesAsync(long from, long upToIndex)
    {
        const int BatchSize = 512;

        while (from <= upToIndex)
        {
            List<RaftLog> batch = await wal.GetRangeAllTypesAsync(from, BatchSize).ConfigureAwait(false);
            if (batch.Count == 0)
                break;

            foreach (RaftLog log in batch)
            {
                if (log.Id > upToIndex)
                    return;

                // Apply committed entries and inherited Proposed entries (prior term only).
                // Skip current-term Proposed entries — they are in-flight proposals.
                bool deliver = log.Type == RaftLogType.Committed ||
                               (log.Type == RaftLogType.Proposed && log.Term < currentTerm);

                if (deliver)
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
            await BecomeLeaderAsync().ConfigureAwait(false);
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
        if (host.LocalRole != ClusterMemberRole.Voter)
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
            await BecomeLeaderAsync().ConfigureAwait(false);
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
        if (host.LocalRole != ClusterMemberRole.Voter)
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

        long currentMaxLog = await wal.GetMaxLogAsync().ConfigureAwait(false);
        long currentLastLogTerm = await wal.GetCurrentTermAsync().ConfigureAwait(false);

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
            if (nodeState == RaftNodeState.Leader
                && localCommittedIndex >= 0
                && (followerGap > host.Configuration.BackfillThreshold || idleTailGap))
            {
                if (await TrySendBackfillBatchAsync(node, followerMaxLog, lastHeartbeat).ConfigureAwait(false))
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
                    snapshotSender.TrySend(node, lastCheckpoint);
                }
            }

            AppendLogToNode(node, lastHeartbeat, null);
        }
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
            string preVoteExpectedLeader = expectedLeaders.GetValueOrDefault(currentTerm, "");
            if (!string.IsNullOrEmpty(preVoteExpectedLeader))
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

            long preVoteLocalMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);
            long preVoteLocalLastTerm = await wal.GetCurrentTermAsync().ConfigureAwait(false);

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

        // Raft §5.1: a RequestVote carrying a term higher than ours proves our leadership/candidacy is
        // stale. Adopt the new term and step down to Follower BEFORE evaluating the grant, so an old
        // leader is fenced promptly instead of continuing to heartbeat at its old term. The previous
        // guard above only rejected a non-follower for the *same* term (voteTerm == currentTerm), so a
        // higher-term vote used to fall through and be granted while the node stayed a stale Leader —
        // the bug this fixes. Mirrors the step-down in the AppendLogs path, except no leader is adopted
        // (a vote request does not identify a leader) and the vote target is left to the grant path
        // below, which may still deny on log-freshness — the term adoption is unconditional either way.
        // NOTE: durably persisting the adopted term before replying is deferred to B2b (hard state);
        // this is the in-memory half. Follower-side term adoption on a higher-term vote is likewise
        // deferred to B2b — here we only fix the Leader/Candidate fencing bug.
        if (voteTerm > currentTerm && nodeState != RaftNodeState.Follower)
        {
            logger.LogInfoSteppingDownOnHigherVoteTerm(
                host.LocalEndpoint, host.PartitionId, nodeState, node.Endpoint, voteTerm, currentTerm);

            nodeState = RaftNodeState.Follower;
            host.Leader = "";
            currentTerm = voteTerm;
            lastCommitIndexes.Clear();
            nextIndex.Clear();
            matchIndex.Clear();
            localCommittedIndex = -1;
            FailAllActiveProposalWaiters();
            activeProposals.Clear();
            ResetPreVoteRound();

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
        
        long localMaxId = await wal.GetMaxLogAsync().ConfigureAwait(false);
        long localLastLogTerm = await wal.GetCurrentTermAsync().ConfigureAwait(false);

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
            lastCommitIndexes[endpoint] = remoteMaxLogId;
            startCommitIndexes[endpoint] = remoteMaxLogId;
            
            logger.LogInfoReceivedVoteAlreadyLeader(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm);
            return;
        }
        
        long maxLogResponse = await wal.GetMaxLogAsync().ConfigureAwait(false);

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

        lastCommitIndexes[endpoint] = remoteMaxLogId;
        startCommitIndexes[endpoint] = remoteMaxLogId;

        logger.LogInfoReceivedVote(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, voteTerm, numberVotes, quorum, voterTotal, remoteMaxLogId, maxLogResponse);

        if (numberVotes < quorum)
            return;
        
        // Here quorum was achieved and we can mark ourselves as leader in the partition.
        // Seed per-follower replication progress. nextIndex is optimistic (leaderMaxLog + 1);
        // it will be corrected by LogMismatch replies if any peer is behind.
        nextIndex.Clear();
        matchIndex.Clear();
        foreach (RaftNode peer in host.Nodes)
        {
            nextIndex[peer.Endpoint] = maxLogResponse + 1;
            matchIndex[peer.Endpoint] = 0;
        }

        await BecomeLeaderAsync().ConfigureAwait(false);

        double electionElapsedMs = MonotonicElapsed(votingStartedTicks, host.GetMonotonicTimestamp()).TotalMilliseconds;
        logger.LogInfoReceivedVoteProclaimedLeader(host.LocalEndpoint, host.PartitionId, nodeState, endpoint, electionElapsedMs, voteTerm, numberVotes, quorum, host.Nodes.Count + 1, remoteMaxLogId, maxLogResponse);

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
            
            host.EnqueueResponse(endpoint, new(
                RaftResponderRequestType.CompleteAppendLogs, 
                new(endpoint), 
                new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LeaderInOldTerm, -1)
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

            if (localTermAtPrev < 0)
            {
                // Hole: no entry exists at prevLogIndex even though prevLogIndex <= localMaxLog,
                // so the follower's log has an internal gap. This proves the follower's truly
                // committed prefix ends below prevLogIndex: the leader commits contiguously, so no
                // entry above an unfilled gap can have been quorum-committed — any entry sitting
                // above the gap is an orphan delivered out of order by the unanchored live-propose
                // broadcast. Truncating that orphaned tail (everything after prevLogIndex-1) can
                // therefore never discard committed data, regardless of what the in-memory
                // commitIndex reports (it can transiently overshoot the gap when a misordered
                // Committed delivery lands above it). Reporting the post-truncation max lets the
                // leader heal the gap in one forward backfill pass instead of walking nextIndex
                // down one slot at a time.
                long newMax = await wal.TruncateLogsAfterAsync(prevLogIndex - 1).ConfigureAwait(false);
                logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Log-hole repair from {Endpoint}: prevLogIndex={PrevLogIndex} truncated to newMax={NewMax}", host.LocalEndpoint, host.PartitionId, nodeState, endpoint, prevLogIndex, newMax);
                host.EnqueueResponse(endpoint, new(
                    RaftResponderRequestType.CompleteAppendLogs,
                    new(endpoint),
                    new CompleteAppendLogsRequest(host.PartitionId, leaderTerm, timestamp, host.LocalEndpoint, RaftOperationStatus.LogMismatch, newMax)
                ));
                return;
            }

            if (localTermAtPrev != prevLogTerm)
            {
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

            WALWriteOperation? operation = wal.EnqueueProposeOrCommit(logs, timestamp, endpoint, leaderTerm);

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

        // Try to clear and reuse expired proposals
        if (activeProposals.Count > 5)
        {
            TimeSpan range = TimeSpan.FromSeconds(30);
            Dictionary<HLCTimestamp, RaftProposalQuorum> tickets = new();            

            foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> proposal in activeProposals)
            {
                if (proposal.Value.HasQuorum() && currentTime - proposal.Value.StartTimestamp > range)
                    tickets.Add(proposal.Key, proposal.Value);
            }

            if (tickets.Count > 0)
            {
                foreach (KeyValuePair<HLCTimestamp, RaftProposalQuorum> kv in tickets)
                {
                    RaftProposalQuorumPool.Return(kv.Value);
                    activeProposals.Remove(kv.Key);
                }                               
            }
        }

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
    /// Puts together a plan to replicate logs to other nodes in the cluster when the node is the leader.
    /// </summary>
    /// <param name="logs"></param>
    /// <param name="autoCommit"></param>
    /// <returns></returns>
    /// <exception cref="RaftException"></exception>
    public async Task ReplicateLogsBatchAsync(IReadOnlyList<(List<RaftLog>? Logs, bool AutoCommit, ulong? ReplyCorrelationId)> messages)
    {
        // Keyed by the two possible autoCommit values, so at most two entries — sized exactly to the
        // bool key space. SmallDictionary avoids the hashed-dictionary bucket/entry allocation on this
        // per-batch hot path; capacity can never be exceeded because a bool has two values.
        SmallDictionary<bool, List<RaftLog>> logsPlan = new(2);
        
        foreach ((List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId) message in messages)
        {
            if (logsPlan.TryGetValue(message.autoCommit, out List<RaftLog>? logs))
            {
                if (message.logs is not null && message.logs.Count > 0)
                    logs.AddRange(message.logs);
            }
            else
            {
                if (message.logs is not null && message.logs.Count > 0)
                {
                    logs = [];
                    logs.AddRange(message.logs);
                    logsPlan.Add(message.autoCommit, logs);
                }
            }
        }

        foreach (KeyValuePair<bool, List<RaftLog>> kv in logsPlan)
        {
            foreach ((List<RaftLog>? logs, bool autoCommit, ulong? replyCorrelationId) item in messages)
            {
                if (item.autoCommit == kv.Key)
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

        List<RaftLog> checkpointLogs = [new()
        {
            Id = 0,
            Term = currentTerm,
            Type = RaftLogType.ProposedCheckpoint,
            Time = currentTime,
            LogType = "",
            LogData = []
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
    private async Task<bool> TrySendBackfillBatchAsync(RaftNode node, long followerMaxLog, HLCTimestamp timestamp, bool anchorToFollowerFrontier = false)
    {
        long from = !anchorToFollowerFrontier && nextIndex.TryGetValue(node.Endpoint, out long ni) && ni <= localCommittedIndex
            ? ni
            : followerMaxLog + 1;

        List<RaftLog> backfill = await wal.GetRangeAsync(from, host.Configuration.MaxBackfillEntriesPerRound).ConfigureAwait(false);

        if (backfill.Count == 0)
            return false;

        long prevIdx  = from - 1;
        long prevTerm = prevIdx > 0 ? await wal.GetAnyTermAtAsync(prevIdx).ConfigureAwait(false) : 0;

        logger.LogDebugBackfilling(host.LocalEndpoint, host.PartitionId, nodeState, backfill.Count, node.Endpoint, from, prevIdx, localCommittedIndex);

        AppendLogToNode(node, timestamp, backfill, prevIdx, prevTerm);
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
    public async ValueTask CompleteAppendLogsAsync(string endpoint, HLCTimestamp timestamp, RaftOperationStatus status, long committedIndex, long responseTerm = -1)
    {
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
        
        // Always register the follower's committed index so the backfill loop can detect lag
        // via TryGetValue. A fresh follower with no log entries reports -1; without this
        // the TryGetValue check in SendHeartbeat would fail and backfill would never fire.
        if (!lastCommitIndexes.TryGetValue(endpoint, out long currentIndex) || committedIndex > currentIndex)
            lastCommitIndexes[endpoint] = committedIndex;

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
            if (startCommitIndexes.TryGetValue(endpoint, out currentIndex))
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
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] Got {Status} from {Endpoint} Timestamp={Timestamp} CommittedIndex={CommittedIndex}",
                host.LocalEndpoint,
                host.PartitionId,
                nodeState,
                status,
                endpoint,
                timestamp,
                committedIndex
            );

            return;
        }

        // Success: advance matchIndex and nextIndex for this peer so the backfill loop
        // knows the follower has caught up to at least committedIndex. matchIndex stays monotonic
        // (a stale in-flight ack must not drag a peer's recorded progress backwards), so the prior
        // value is captured first — it is the only evidence of a genuine frontier regression, which
        // the fast-path re-supply below keys on.
        bool hadMatchIndex = matchIndex.TryGetValue(endpoint, out long priorMatchIndex);
        if (!hadMatchIndex || committedIndex > priorMatchIndex)
            matchIndex[endpoint] = committedIndex;
        nextIndex[endpoint] = matchIndex[endpoint] + 1;

        // Immediately ship the next bounded batch only while an active catch-up is in progress,
        // so a multi-batch backfill converges without stalling a full heartbeat per batch. This
        // must honour the same BackfillThreshold gate as the heartbeat path: a follower lagging by
        // ≤ threshold is intentionally not actively backfilled (small lag rides on normal
        // replication), and eagerly catching it up here would, e.g., make a barely-behind node look
        // fresh enough to receive a leadership transfer it should not.
        if (nodeState == RaftNodeState.Leader
            && localCommittedIndex - matchIndex[endpoint] > host.Configuration.BackfillThreshold)
        {
            RaftNode? behindNode = host.Nodes.FirstOrDefault(n => n.Endpoint == endpoint);
            if (behindNode is not null)
                await TrySendBackfillBatchAsync(behindNode, committedIndex, host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId)).ConfigureAwait(false);
        }

        // Fast-path commit-frontier re-supply: a follower that restarted after losing its lazy commit
        // markers reports a committed frontier BELOW what the leader's (monotonic) matchIndex already
        // recorded — the regression signature. matchIndex never moves backward, so the trigger above
        // cannot see it; detect it explicitly and re-ship the still-committed tail from the follower's
        // reported frontier. Idempotent: those entries are present on the follower as Proposed, and the
        // re-shipped Committed copies upsert and apply them. Confined to the fast path (flag off ⇒ a
        // heartbeat reports -1 ⇒ this never fires), so legacy replication is unchanged.
        //
        // BackfillThreshold must NOT gate this. That threshold encodes "a small gap rides on normal
        // replication", which is true for a follower that is merely behind but false for a regressed
        // one: the entries it is missing are already-committed history, so no future propose broadcast
        // carries them, and the idle-tail trigger in SendHeartbeat cannot help either — its
        // liveCommitFloor guard deliberately refuses to push merely-restored committed state. A
        // sub-threshold regression would therefore never be repaired until unrelated writes happened to
        // push the gap past the threshold, leaving the node serving a truncated view of its own
        // acknowledged writes for an unbounded time on an idle cluster. A regression is unambiguous
        // (a commit frontier cannot legitimately move backwards) and rare, so it is repaired at any size.
        //
        // The anchor matters too: nextIndex[peer] tracks the monotonic matchIndex and therefore still
        // points ABOVE the regressed frontier, so the batch must be anchored to what the follower just
        // reported instead — otherwise the re-supply would skip exactly the range that regressed.
        if (host.Configuration.WalSingleFsyncCommit
            && nodeState == RaftNodeState.Leader
            && committedIndex >= 0
            && localCommittedIndex > committedIndex)
        {
            bool frontierRegressed = hadMatchIndex && committedIndex < priorMatchIndex;

            // Rate-limit the regression path: acks already in flight when the regression is first seen
            // all still report the low frontier, and each would otherwise re-ship the same range.
            if (frontierRegressed)
            {
                long nowTicks = host.GetMonotonicTimestamp();
                if (lastRegressionResupplyTicks.TryGetValue(endpoint, out long sentAtTicks)
                    && MonotonicElapsed(sentAtTicks, nowTicks) < host.Configuration.HeartbeatInterval)
                    frontierRegressed = false;
                else
                    lastRegressionResupplyTicks[endpoint] = nowTicks;
            }

            if (frontierRegressed || localCommittedIndex - committedIndex > host.Configuration.BackfillThreshold)
            {
                RaftNode? regressedNode = host.Nodes.FirstOrDefault(n => n.Endpoint == endpoint);
                if (regressedNode is not null)
                    await TrySendBackfillBatchAsync(regressedNode, committedIndex, host.HybridLogicalClock.TrySendOrLocalEvent(host.LocalNodeId), anchorToFollowerFrontier: frontierRegressed).ConfigureAwait(false);
            }
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
            long actualMin = pendingLogs.Min(l => l.Id);
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
                CompleteLeaderPropose(completion, pending);
                break;

            case WALWriteOperationType.LeaderCommit:
                await CompleteLeaderCommit(completion, pending).ConfigureAwait(false);
                break;

            case WALWriteOperationType.LeaderRollback:
                CompleteLeaderRollback(completion, pending);
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
    private void CompleteLeaderPropose(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;
        List<RaftLog> logs = pending?.Logs ?? [];
        bool autoCommit = pending?.AutoCommit ?? false;

        if (completion.Status != RaftOperationStatus.Success)
        {
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

        foreach (RaftNode node in host.Nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            // Learners receive log entries for catch-up but must not count toward quorum.
            // Only add voters to the quorum set; AppendLogToNode is called for all nodes.
            if (host.IsVoter(node.Endpoint))
                proposalQuorum.AddExpectedNodeCompletion(node.Endpoint);
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
        if (!host.Nodes.Any(n => host.IsVoter(n.Endpoint)))
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
    /// </summary>
    private async Task CompleteLeaderCommit(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        RaftProposalQuorum? proposal = pending?.Proposal;
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;

        if (completion.Status != RaftOperationStatus.Success || proposal is null)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Couldn't commit proposal {Timestamp}", host.LocalEndpoint, host.PartitionId, nodeState, ticketId);
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
        if (inheritedEnd >= 0 && inheritedEnd > lastAppliedIndex)
            await DrainInheritedAppliesAsync(lastAppliedIndex + 1, inheritedEnd).ConfigureAwait(false);

        // Apply committed consumer entries to the local state machine. Mirrors the apply loop
        // in CompleteFollowerAppend so the leader's consumer projection stays in sync.
        foreach (RaftLog log in proposal.Logs)
            await ApplyLogToConsumerAsync(log).ConfigureAwait(false);

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

        CompleteReply(pending?.ReplyCorrelationId, new(RaftResponseType.None, RaftOperationStatus.Success, completion.MaxLogIndex));
    }

    /// <summary>
    /// Rolls back the proposal locally and broadcasts the rolled-back entries to all peers.
    /// Like <see cref="CompleteLeaderCommit"/>, this delivery carries no Log Matching anchors:
    /// it targets ids the follower already saw during the anchored propose, and adding a WAL term
    /// read on this completion path would stall propagation. LMP remains enforced on propose/backfill.
    /// </summary>
    private void CompleteLeaderRollback(RaftWalCompletion completion, RaftPendingWalOperation? pending)
    {
        RaftProposalQuorum? proposal = pending?.Proposal;
        HLCTimestamp ticketId = pending?.TicketId ?? HLCTimestamp.Zero;

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
            foreach (RaftLog log in pending.Logs ?? [])
            {
                if (log.Type != RaftLogType.Committed)
                    continue;

                if (host.PartitionId == RaftSystemConfig.SystemPartition && log.LogType == RaftSystemConfig.RaftLogType)
                {
                    if (!await host.InvokeSystemReplicationReceived(host.PartitionId, log).ConfigureAwait(false))
                        host.InvokeReplicationError(host.PartitionId, log);
                }
                else
                {
                    if (!await host.InvokeReplicationReceived(host.PartitionId, log).ConfigureAwait(false))
                        host.InvokeReplicationError(host.PartitionId, log);
                }

                // Track per-entry, not via the gap-aware committedIndex above: committedIndex
                // is computed before this loop and is used only for backfill reporting to the
                // leader. Using it here would cap lastAppliedIndex below entries we actually
                // applied if a gap ever occurred (committedIndex stops at the hole; log.Id does
                // not). Under the contiguous-prefix invariant the two values agree; this
                // per-entry form is safer if the invariant were ever relaxed.
                if (log.Id > lastAppliedIndex)
                    lastAppliedIndex = log.Id;
            }

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

    // ReadExactAsync body lives in StreamUtils; forwarded here for call-site compatibility.
    private static ValueTask<int> ReadExactAsync(Stream stream, byte[] buffer, int count, CancellationToken ct) =>
        StreamUtils.ReadExactAsync(stream, buffer, count, ct);

    /// <summary>
    /// Advances <c>lastCommitIndexes</c> for <paramref name="endpoint"/> after the background
    /// snapshot task confirmed successful installation. Called on the executor thread via the
    /// <c>postToExecutor</c> callback; delegates ownership update to <see cref="snapshotSender"/>.
    /// </summary>
    public void CompleteSnapshotInstalled(string endpoint, long snapshotIndex) =>
        snapshotSender.CompleteSnapshotInstalled(endpoint, snapshotIndex);

}
