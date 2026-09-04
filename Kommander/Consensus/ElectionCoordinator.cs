using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Gossip;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// Everything about becoming, or refusing to become, the leader of one partition: the pre-vote
/// probe (Raft §9.6), the real election, the vote-granting side of RequestVote (§5.2, §5.4.1), and
/// the tallies and vote records behind both.
///
/// <para><b>What stays outside.</b> Winning an election is not the same as taking office: this type
/// signals the win through the injected <c>becomeLeaderAsync</c> callback and the core performs the
/// promotion, whose ordering (drain applies, arm the barrier, publish leadership) is a separate
/// invariant. The same applies to the step-down bookkeeping on a higher term — this type decides
/// that a step-down is required, the core decides what a step-down consists of.</para>
///
/// <para><b>Correctness anchors</b> (all of them fixed Jepsen findings; preserve them exactly):
/// a RequestVote carrying a higher term is adopted <em>unconditionally</em>, including by a
/// follower, or a follower keeps ACKing a deposed leader at the old term and hands it a phantom
/// quorum; the step-down bookkeeping alone stays gated on node state. Pre-vote is strictly
/// side-effect-free with one deliberate exception — un-quiescing, which is scheduling state, not
/// §3 vote state. A candidate whose log is behind is denied by
/// <c>(lastLogTerm, lastLogIndex)</c> lexicographic comparison, never by index alone. And the vote
/// record is persisted to hard state <em>before</em> the grant is sent, so a crash cannot produce a
/// double vote in one term.</para>
///
/// <para><b>Concurrency.</b> Invoked only on the partition executor thread; holds no locks by
/// design.</para>
/// </summary>
internal sealed class ElectionCoordinator
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ReplicationTracker tracker;
    private readonly ILogger<IRaft> logger;

    /// <summary>Takes office after a won election. See the class remarks for why promotion is not
    /// performed here.</summary>
    private readonly Func<Task<bool>> becomeLeaderAsync;

    /// <summary>Fails and drops in-flight proposals when a higher term forces a step-down.</summary>
    private readonly Action failAllActiveProposalWaiters;

    /// <summary>Forces a heartbeat immediately after taking office so followers adopt the new term
    /// and rival elections stay suppressed.</summary>
    private readonly Func<bool, Task> sendHeartbeat;

    public ElectionCoordinator(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ReplicationTracker tracker,
        ILogger<IRaft> logger,
        Func<Task<bool>> becomeLeaderAsync,
        Action failAllActiveProposalWaiters,
        Func<bool, Task> sendHeartbeat)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.tracker = tracker;
        this.logger = logger;
        this.becomeLeaderAsync = becomeLeaderAsync;
        this.failAllActiveProposalWaiters = failAllActiveProposalWaiters;
        this.sendHeartbeat = sendHeartbeat;

        // Mix a STABLE, per-node identity into the seed so nodes in the same partition don't draw an
        // identical election-timeout sequence. A `seed ^ partitionId` gave every node in a partition the
        // same sequence, so after a symmetric split vote they'd keep choosing identical retry timeouts and
        // fire simultaneously forever — defeating the randomization meant to break the tie. Folding in
        // host.LocalNodeId gives each node its own reproducible sequence (deterministic given the node's
        // identity, so seeded runs stay repeatable per node). Only applies when a seed is configured; the
        // production default (null) already uses per-node Random.Shared.
        random = host.Configuration.ElectionTimeoutSeed is int seed
            ? new Random(DeriveElectionSeed(seed, host.PartitionId, host.LocalNodeId))
            : Random.Shared;

        RandomizeElectionTimeout();
    }

    /// <summary>
    /// Re-draws the election timeout from the full configured range rather than capping an
    /// incremented value. Incremental backoff converges competing nodes to EndElectionTimeout after
    /// one or two failed elections, so they fire at the same instant every time — a persistent
    /// split-vote livelock. Called at construction and on every failed candidacy.
    /// </summary>
    public void RandomizeElectionTimeout() =>
        coreState.ElectionTimeout = TimeSpan.FromMilliseconds(
            random.Next(host.Configuration.StartElectionTimeout, host.Configuration.EndElectionTimeout));

    /// <summary>The endpoint this node has already accepted or voted for in <paramref name="term"/>,
    /// or an empty string when it has committed to nobody yet.</summary>
    public string GetExpectedLeader(long term) => expectedLeaders.GetValueOrDefault(term, "");

    /// <summary>Records the endpoint this node has accepted or voted for in <paramref name="term"/>.
    /// This is the record the "already voted for someone else this term" guard reads.</summary>
    public void RecordExpectedLeader(long term, string endpoint) => expectedLeaders[term] = endpoint;

    /// <summary>Forgets every vote target. Used by transitions that invalidate the whole record.</summary>
    public void ClearExpectedLeaders() => expectedLeaders.Clear();

    /// <summary>Discards the real-election tally.</summary>
    public void ClearVotes() => votes.Clear();

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

    /// <summary>
    /// Clears all bookkeeping for the current pre-vote round. Called when a candidacy is
    /// abandoned (Candidate→Follower) and when a real election begins, so a stale pre-vote
    /// round for an old term can never leak into and falsely promote a later term.
    /// Side-effect-free with respect to real Raft state (term/votes/leader are untouched).
    /// </summary>
    public void ResetPreVoteRound()
    {
        preVotes.Clear();
        preVoteTerm = -1;
        electionPhase = RaftElectionPhase.None;
    }

    /// <summary>
    /// Raft §5.4.1 log freshness; see <see cref="ElectionFreshness"/> for the rule, for the
    /// symmetric missing-term fallback, and for why it lives in its own type. Kept as a local
    /// forwarder so the two call sites below read the same as before.
    /// </summary>
    private static bool CandidateLogIsBehind(long remoteLastLogTerm, long remoteMaxLogId, long localLastLogTerm, long localMaxId) =>
        ElectionFreshness.CandidateLogIsBehind(remoteLastLogTerm, remoteMaxLogId, localLastLogTerm, localMaxId);

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
    /// Records the §5.4.1 log position a peer advertised on a vote-path message. Called for every
    /// RequestVotes probe and every grant received, so the map stays current even while this node
    /// denies the peer (a denial teaches the denier nothing otherwise).
    /// </summary>
    private void RecordPeerFreshness(string endpoint, long position)
    {
        if (position < 0)
            return;

        peerFreshness[endpoint] = (position, host.GetMonotonicTimestamp());
    }

    /// <summary>
    /// True when some committed voter is (a) known — from a recent vote-path advertisement — to
    /// hold a log position strictly above <paramref name="localPosition"/>, and (b) currently
    /// Alive per the failure detector. Used by the candidacy deference and by the promotion-gate
    /// escalations: while such a voter exists, refusing/yielding lets it win the term and repair
    /// this node by backfill, so destructive self-repair (tail truncation, gap-skipping drains)
    /// must wait. Both the TTL and the liveness gate exist so a departed or dead peer's stale
    /// advertisement can never hold liveness hostage.
    /// </summary>
    public bool KnowsFresherAliveVoter(long localPosition)
    {
        if (localPosition < 0 || peerFreshness.Count == 0)
            return false;

        long nowTicks = host.GetMonotonicTimestamp();

        foreach (RaftNode node in host.Nodes)
        {
            if (!host.IsVoter(node.Endpoint))
                continue;

            if (!peerFreshness.TryGetValue(node.Endpoint, out (long Position, long ObservedTicks) seen))
                continue;

            if (seen.Position <= localPosition)
                continue;

            if (RaftMonotonic.Elapsed(seen.ObservedTicks, nowTicks) > PeerFreshnessTtl)
                continue;

            if (host.GetNodeLiveness(node.Endpoint) == MemberLivenessState.Alive)
                return true;
        }

        return false;
    }

    /// <summary>
    /// The candidacy-deference gate: a node holding entries above an unfilled WAL gap knows it is
    /// missing a range some peer may hold; if a fresher live voter is known, campaigning is worse
    /// than waiting — this node would either lose, or win and refuse at the promotion gates, and
    /// each such refused term appends a new-term barrier no-op that RAISES this node's advertised
    /// last-log term above the complete peer's, locking it out of §5.4.1 forever (the Jepsen
    /// majority-hole wedge: four gapped voters out-elected the one complete node for the rest of
    /// the run). Yielding a bounded number of rounds quiets the election churn — and the
    /// vote-grant cooldowns it keeps refreshing — so the complete peer's own pre-vote can reach
    /// quorum. Bounded so a fresher peer that never campaigns cannot suppress this node forever.
    /// </summary>
    private bool ShouldDeferCandidacy()
    {
        if (!wal.HasPresenceGap() || !KnowsFresherAliveVoter(wal.GetPresentIndex()))
        {
            candidacyDeferrals = 0;
            return false;
        }

        if (candidacyDeferrals >= MaxCandidacyDeferrals)
            return false;

        candidacyDeferrals++;

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("[{LocalEndpoint}/{PartitionId}/{State}] Deferring candidacy ({Attempt}/{MaxAttempts}): this log has a hole (contiguous through {PresentId}) and a fresher live voter is known — yielding the term so it can win and backfill.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, candidacyDeferrals, MaxCandidacyDeferrals, wal.GetPresentIndex());

        return true;
    }

    public async Task StartElectionAsync(HLCTimestamp currentTime, bool ignoreRecentVoteCooldown)
    {
        // Two gates: the roster role (a cluster Learner/Leaving node never campaigns anywhere)
        // and the per-partition voter check (under replica placement a roster Voter may be only
        // a Learner/Removing replica of THIS range — campaigning would inflate the range's
        // quorum with a vote the committed replica set does not grant it).
        if (host.LocalRole != ClusterMemberRole.Voter || !host.IsVoter(host.LocalEndpoint))
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugSuppressingElection(host.LocalEndpoint, host.PartitionId, coreState.NodeState, host.LocalRole, host.IsVoter(host.LocalEndpoint));
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
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugSuppressingElection(host.LocalEndpoint, host.PartitionId, coreState.NodeState, host.LocalRole, host.IsVoter(host.LocalEndpoint));
            return;
        }

        long nowTicks = host.GetMonotonicTimestamp();

        if (!ignoreRecentVoteCooldown)
        {
            // B3: the recent-vote cooldown is a local elapsed interval → monotonic.
            if (coreState.LastVotationTicks != 0 && (RaftMonotonic.Elapsed(coreState.LastVotationTicks, nowTicks) < (coreState.ElectionTimeout * 2)))
                return;

            string expectedLeader = expectedLeaders.GetValueOrDefault(coreState.CurrentTerm, "");
            if (!string.IsNullOrEmpty(expectedLeader))
            {
                // NOTE (B3 residual): GetLastNodeActivity returns an HLC written locally on the last
                // AppendLogs from this peer. The "heard from the leader recently" decision below is still an
                // HLC subtraction and remains mildly skew-sensitive — the peer-activity store migration to
                // monotonic ticks was deliberately deferred (contained B3 scope). On suppression we refresh
                // BOTH the HLC anchor and its monotonic shadow so the monotonic follower election gate
                // honours the back-off; the residual only affects whether we take this branch at all.
                HLCTimestamp lastKnownHeartbeat = host.GetLastNodeActivity(expectedLeader, host.PartitionId);

                if (lastKnownHeartbeat != HLCTimestamp.Zero && ((currentTime - lastKnownHeartbeat) < coreState.ElectionTimeout))
                {
                    coreState.LastHeartbeat = lastKnownHeartbeat;
                    coreState.LastHeartbeatTicks = nowTicks;
                    return;
                }
            }

            // A gapped log with a fresher live voter known: yield the round (bounded) instead of
            // winning a term this node would refuse at the promotion gates. Skipped when a
            // pre-vote quorum promoted us here (peers already judged us electable).
            if (ShouldDeferCandidacy())
                return;
        }

        // No global "am I outdated?" pre-election veto here (removed): candidate eligibility is decided
        // per-voter by the RequestVote log-freshness predicate in VoteAsync. The old veto compared our
        // WAL max against the maximum ever recorded in startCommitIndexes — a dictionary that is never
        // pruned, so a peer that once advertised a higher (possibly uncommitted) tail and then
        // failed/left permanently suppressed every election even when the survivors held a valid quorum.

        // A real election is starting: discard any open pre-vote round so a stale
        // pre-grant set for an old hypothetical term can't bleed into this one.
        ResetPreVoteRound();

        coreState.NodeState = RaftNodeState.Candidate;
        host.Leader = "";
        expectedLeaders.Clear();
        coreState.VotingStartedAt = currentTime;
        coreState.VotingStartedTicks = nowTicks;

        await host.InvokeLeaderChanged(host.PartitionId, "");

        // B2b: durably record the new term and our self-vote BEFORE adopting either in memory and before
        // soliciting votes (see the same call in ForceLeaderForTestingAsync for rationale). Persisted
        // first so a rejected write leaves nothing to undo. On a full disk (field incident 2026-09-01)
        // the write fails on every attempt; a term bumped in memory without a durable self-vote could
        // let this node vote a second time in that term after a crash. The attempt is abandoned
        // instead: the node drops back to Follower with its term unchanged, and the election timer
        // fires again on its own cadence until the WAL accepts the write.
        long candidateTerm = coreState.CurrentTerm + 1;

        if (!await wal.PersistHardStateAsync(candidateTerm, host.LocalEndpoint).ConfigureAwait(false))
        {
            logger.LogWarnHardStateNotPersisted(
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, candidateTerm, host.LocalEndpoint,
                "election start", "Election abandoned; it is retried on the next election timeout.");

            coreState.NodeState = RaftNodeState.Follower;
            return;
        }

        coreState.CurrentTerm = candidateTerm;

        IncreaseVotes(host.LocalEndpoint, coreState.CurrentTerm);

        double delayMs = coreState.LastHeartbeatTicks != 0
            ? RaftMonotonic.Elapsed(coreState.LastHeartbeatTicks, nowTicks).TotalMilliseconds
            : 0;

        TagList electionTags = new() { { "partition_id", host.PartitionId } };
        KommanderMetrics.ElectionsStartedTotal.Add(1, electionTags);
        KommanderMetrics.ElectionDelayMs.Record(delayMs, electionTags);

        logger.LogWarnVotedToBecomeLeader(host.LocalEndpoint, host.PartitionId, coreState.NodeState, delayMs, coreState.CurrentTerm);

        // Self-quorum when no committed voter peer exists — not merely when no peer exists at
        // all. Quorum is a majority of the committed voter set, so a sole voter whose only peers
        // are transitional replicas (Learner/Removing) is its own majority; soliciting them
        // instead deadlocks the range, because their grants are (correctly) discarded by the
        // tally and no countable vote can ever arrive. This mirrors the commit path
        // (CompleteLeaderPropose drives single-voter commit when no voter peer is registered).
        // The heartbeat below fans out to ALL peers, so a transitional replica starts receiving
        // appends and its catch-up proceeds under the new leadership.
        if (CountVoterPeers() == 0)
        {
            tracker.ClearProgressKeepingCommitFrontiers();
            // published == false: barrier pending, self-quorum commit publishes shortly after
            // (see CompleteLeaderCommit), which also fires the LeaderChanged notification.
            bool published = await becomeLeaderAsync().ConfigureAwait(false);
            if (published)
                await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint).ConfigureAwait(false);
            await sendHeartbeat(true).ConfigureAwait(false);
            return;
        }

        await RequestVotesAsync(currentTime, coreState.CurrentTerm).ConfigureAwait(false);
    }

    /// <summary>
    /// The pre-election (Raft §9.6) that gates a real election. Before bumping the term and
    /// becoming a Candidate, a follower whose leader went silent first runs a side-effect-free
    /// probe: it asks peers whether they *would* vote for it at <c>CurrentTerm + 1</c> given its
    /// current log, WITHOUT changing its own term/state. Only a pre-vote quorum (tallied in
    /// <see cref="ReceivedVoteAsync"/>) promotes to <see cref="StartElectionAsync"/>. This is what
    /// stops a stale or partitioned node from repeatedly inflating its term and disrupting a healthy
    /// leader — the livelock this whole change targets.
    /// </summary>
    public async Task StartPreVoteAsync(HLCTimestamp currentTime)
    {
        // Mirrors StartElectionAsync: roster role plus per-partition voter check — a node that
        // is not a Voter replica of this range must not campaign for it.
        if (host.LocalRole != ClusterMemberRole.Voter || !host.IsVoter(host.LocalEndpoint))
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugSuppressingPreVote(host.LocalEndpoint, host.PartitionId, coreState.NodeState, host.LocalRole, host.IsVoter(host.LocalEndpoint));
            return;
        }

        long nowTicks = host.GetMonotonicTimestamp();

        // Same "should I even try?" guards as a real election. These guards do NOT touch any Raft
        // consensus state (coreState.CurrentTerm / votes / expectedLeaders / coreState.NodeState) — that is the whole
        // point of pre-vote. The one local write below (coreState.LastHeartbeat) is a back-off bookkeeping
        // refresh on the "leader still fresh" path, mirroring StartElectionAsync, not a consensus
        // mutation: it just records that we observed the leader so we don't immediately re-trigger.
        // B3: the recent-vote cooldown is a local elapsed interval → monotonic.
        if (coreState.LastVotationTicks != 0 && (RaftMonotonic.Elapsed(coreState.LastVotationTicks, nowTicks) < (coreState.ElectionTimeout * 2)))
            return;

        string expectedLeader = expectedLeaders.GetValueOrDefault(coreState.CurrentTerm, "");
        if (!string.IsNullOrEmpty(expectedLeader))
        {
            // B3 residual (same as StartElectionAsync): the "heard from leader recently" test is still an
            // HLC subtraction off the HLC peer-activity store; on back-off we refresh the monotonic shadow.
            HLCTimestamp lastKnownHeartbeat = host.GetLastNodeActivity(expectedLeader, host.PartitionId);

            if (lastKnownHeartbeat != HLCTimestamp.Zero && ((currentTime - lastKnownHeartbeat) < coreState.ElectionTimeout))
            {
                // Intentional: back off and remember we saw the leader. Not a consensus mutation.
                coreState.LastHeartbeat = lastKnownHeartbeat;
                coreState.LastHeartbeatTicks = nowTicks;
                return;
            }
        }

        // A gapped log with a fresher live voter known: yield the round (bounded) instead of
        // probing for a term this node would refuse at the promotion gates. Unlike the removed
        // global veto this is TTL- and liveness-gated and bounded, so it cannot suppress forever.
        if (ShouldDeferCandidacy())
            return;

        // No global "am I outdated?" pre-election veto here (removed): a pre-vote is side-effect-free by
        // design, so a genuinely-behind node can safely probe — its peers deny the pre-vote via the
        // per-voter log check in VoteAsync and it never reaches quorum. The old veto instead consulted
        // the never-pruned startCommitIndexes max, which let a departed peer's stale tail suppress every
        // pre-vote forever.

        // No committed voter peer to probe: only voters can grant a countable pre-vote (the
        // tally discards everything else), so with none present — a single-node range, or a
        // sole voter whose only peers are transitional Learner/Removing replicas — the probe
        // can never reach quorum. Go straight to a real election, where the same voter-only
        // arithmetic makes this node its own majority.
        if (CountVoterPeers() == 0)
        {
            await StartElectionAsync(currentTime, ignoreRecentVoteCooldown: true).ConfigureAwait(false);
            return;
        }

        // Open a fresh pre-vote round for the hypothetical next term and seed our own pre-grant.
        electionPhase = RaftElectionPhase.PreVote;
        preVoteTerm = coreState.CurrentTerm + 1;
        preVotes.Clear();
        preVotes.Add(host.LocalEndpoint);

        logger.LogInfoStartingPreVoteRound(host.LocalEndpoint, host.PartitionId, coreState.NodeState, preVoteTerm);

        await RequestVotesAsync(currentTime, preVoteTerm, preVote: true).ConfigureAwait(false);
    }

    /// <summary>
    /// Requests votes from the other known nodes in the cluster. Shared by both the real election
    /// (<paramref name="preVote"/> = false) and the side-effect-free pre-vote probe
    /// (<paramref name="preVote"/> = true, Raft §9.6). The only difference on the wire is the
    /// <see cref="RequestVotesRequest.PreVote"/> flag and the <paramref name="term"/> used (the
    /// real <see cref="RaftPartitionCoreState.CurrentTerm"/> for an election, the hypothetical <c>CurrentTerm + 1</c> for a probe).
    /// </summary>
    /// <param name="timestamp"></param>
    /// <param name="term">Term to advertise: <see cref="RaftPartitionCoreState.CurrentTerm"/> for a real election, the hypothetical next term for a pre-vote.</param>
    /// <param name="preVote">When true the outbound request is marked as a pre-vote probe.</param>
    /// <exception cref="RaftException"></exception>
    public async Task RequestVotesAsync(HLCTimestamp timestamp, long term, bool preVote = false)
    {
        IReadOnlyList<RaftNode> nodes = host.Nodes;

        if (nodes.Count == 0)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] No other nodes availables to vote", host.LocalEndpoint, host.PartitionId, coreState.NodeState);
            return;
        }

        (long currentMaxLog, long currentLastLogTerm) = await GetFreshnessLogPositionAsync().ConfigureAwait(false);

        RequestVotesRequest request = new(host.PartitionId, term, currentMaxLog, currentLastLogTerm, timestamp, host.LocalEndpoint, preVote);

        foreach (RaftNode node in nodes)
        {
            if (node.Endpoint == host.LocalEndpoint)
                throw new RaftException("Corrupted nodes");

            // Only committed voters can grant a countable (pre-)vote — the tally discards every
            // other grant — so transitional Learner/Removing replicas are not solicited at all.
            if (!host.IsVoter(node.Endpoint))
                continue;

            logger.LogInfoAskedForVotes(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, term);

            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.RequestVotes, node, request));
        }
    }

    /// <summary>
    /// Number of peers in <c>host.Nodes</c> that are committed voters of this range. Transitional
    /// replicas (Learner/Removing) are replication peers but never part of the quorum
    /// denominator. Plain loop — this runs on the election path, where a LINQ predicate would
    /// allocate a closure per call.
    /// </summary>
    internal int CountVoterPeers()
    {
        int count = 0;

        IReadOnlyList<RaftNode> nodes = host.Nodes;
        for (int i = 0; i < nodes.Count; i++)
        {
            if (host.IsVoter(nodes[i].Endpoint))
                count++;
        }

        return count;
    }

    /// <summary>
    /// When another node requests our vote, we verify that the term is valid and the commitIndex is
    /// higher than ours to ensure we don't elect outdated nodes as leaders.
    ///
    /// When <paramref name="preVote"/> is true this answers a side-effect-free pre-election probe
    /// (Raft §9.6): we evaluate the §3 grant predicate and, on grant, reply with a
    /// <see cref="VoteRequest"/> carrying <c>PreVote=true</c> — but we must NOT mutate any real
    /// state (<see cref="RaftPartitionCoreState.CurrentTerm"/>, <see cref="votes"/>, <see cref="expectedLeaders"/>,
    /// <see cref="lastVotation"/>, <see cref="lastHeartbeat"/>, <see cref="RaftPartitionCoreState.NodeState"/>). This is
    /// what lets a stale/partitioned node probe its electability without disrupting a healthy leader.
    /// </summary>
    /// <param name="node"></param>
    /// <param name="voteTerm"></param>
    /// <param name="remoteMaxLogId">The candidate's last log index.</param>
    /// <param name="remoteLastLogTerm">
    /// The candidate's last log term, compared lexicographically before <paramref name="remoteMaxLogId"/>
    /// (Raft §5.4.1). <c>0</c> from a peer predating this field or an empty candidate log; the freshness
    /// check falls back to index-only comparison when EITHER side lacks a usable term — the local side
    /// included (see <see cref="CandidateLogIsBehind"/>).
    /// </param>
    /// <param name="timestamp"></param>
    /// <param name="preVote">When true, evaluate as a pure pre-vote probe and never persist state.</param>
    /// <remarks><paramref name="remoteLastLogTerm"/> is placed last with a default of <c>0</c> so callers
    /// that predate the §5.4.1 freshness key (and older tests) compile unchanged and fall back to
    /// index-only comparison; the transport dispatch path always supplies the real value.</remarks>
    public async Task VoteAsync(RaftNode node, long voteTerm, long remoteMaxLogId, HLCTimestamp timestamp, bool preVote = false, long remoteLastLogTerm = 0)
    {
        // Every vote-path message advertises the sender's §5.4.1 position — record it even when
        // the request is denied below, so the deference/escalation evidence stays current. (A
        // denial otherwise teaches this node nothing, and the majority-hole wedge is exactly the
        // state where the fresher peer's probes keep being denied.)
        RecordPeerFreshness(node.Endpoint, remoteMaxLogId);

        if (preVote)
        {
            // Side-effect-free pre-vote (Raft §9.6). NOTHING below this branch's `return`
            // may mutate state: we only read term/log/leader-freshness and, on grant, reply.

            if (!host.IsVoter(node.Endpoint))
            {
                logger.LogDebugDenyingPreVoteNotVoter(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm);
                return;
            }

            // A live leader never helps a challenger unseat it.
            if (coreState.NodeState == RaftNodeState.Leader)
            {
                logger.LogDebugDenyingPreVoteWeAreLeader(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm);

                // A committed member probing for votes is direct evidence it cannot see this
                // leader. The common benign cause is a follower restart under quiescence: its
                // in-memory quiesce flag and leader knowledge died with the process, and a
                // quiesced leader sends no heartbeats to re-teach it — so the member loops
                // pre-vote rounds (denied here) while its partitions never assemble. Waking
                // re-arms heartbeats; the next interval re-establishes leadership for the
                // member and the partition re-quiesces once every peer has converged again.
                // (Quiescence bookkeeping is scheduling state, not Raft §3 vote state, so this
                // does not violate the pre-vote side-effect-free contract.)
                if (coreState.Quiesced)
                    coreState.SetQuiesced(false);

                return;
            }

            // Deny if we ourselves would not start an election right now: a pre-vote grant must be
            // consistent with our own willingness to campaign, so this mirrors the Follower cases of
            // the CheckPartitionLeadershipAsync election trigger. Both decisions rely only on LOCAL
            // signals — the private `coreState.LastHeartbeat` field (refreshed on every accepted AppendLogs from
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
            string preVoteExpectedLeader = expectedLeaders.GetValueOrDefault(coreState.CurrentTerm, "");
            if (!string.IsNullOrEmpty(preVoteExpectedLeader) && preVoteExpectedLeader != node.Endpoint)
            {
                if (coreState.Quiesced && host.Configuration.EnableQuiescence)
                {
                    // Quiesced: the leader suppresses heartbeats by design, so `coreState.LastHeartbeat` goes
                    // stale and is not a valid freshness signal. Defer to SWIM exactly as the quiesced
                    // Follower election case does — while the expected leader is Alive, don't help a
                    // challenger unseat it.
                    if (host.GetNodeLiveness(preVoteExpectedLeader) == MemberLivenessState.Alive)
                    {
                        logger.LogDebugDenyingPreVoteLeaderFresh(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm, preVoteExpectedLeader);
                        return;
                    }
                }
                else if (coreState.LastHeartbeatTicks != 0 && (RaftMonotonic.Elapsed(coreState.LastHeartbeatTicks, host.GetMonotonicTimestamp()) < coreState.ElectionTimeout))
                {
                    // Not quiesced: a recent heartbeat from our leader means it is still live to us.
                    // B3: measured as local elapsed time since we last heard from the leader (monotonic),
                    // NOT `incomingRequest.timestamp - coreState.LastHeartbeat` — that subtracted a remote HLC from a
                    // local one and inherited the challenger's clock skew, which could make a stale
                    // heartbeat look fresh (or vice-versa). "How long we've been without a heartbeat" is a
                    // purely local quantity.
                    logger.LogDebugDenyingPreVoteLeaderFresh(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm, preVoteExpectedLeader);
                    return;
                }
            }

            // The hypothetical term must not be stale.
            if (voteTerm < coreState.CurrentTerm)
            {
                logger.LogDebugDenyingPreVoteStaleTerm(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm, coreState.CurrentTerm);
                return;
            }

            (long preVoteLocalMaxId, long preVoteLocalLastTerm) = await GetFreshnessLogPositionAsync().ConfigureAwait(false);

            // The candidate's log must be at least as up-to-date, compared lexicographically by
            // (lastLogTerm, lastLogIndex) per Raft §5.4.1 — NOT index alone, which would let a higher
            // index hide a stale last term. Note this denies only when the candidate is *strictly*
            // behind: a pre-vote probes electability, so an equal log is grantable.
            if (CandidateLogIsBehind(remoteLastLogTerm, remoteMaxLogId, preVoteLocalLastTerm, preVoteLocalMaxId))
            {
                logger.LogDebugDenyingPreVoteOutdatedLog(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm, remoteMaxLogId, preVoteLocalMaxId);
                return;
            }

            logger.LogDebugGrantingPreVote(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm);

            VoteRequest preGrant = new(host.PartitionId, voteTerm, preVoteLocalMaxId, preVoteLocalLastTerm, timestamp, host.LocalEndpoint, preVote: true);
            host.EnqueueResponse(node.Endpoint, new(RaftResponderRequestType.Vote, node, preGrant));
            return;
        }

        if (!host.IsVoter(node.Endpoint))
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebugDenyingVoteNotVoter(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm);
            return;
        }

        if (votes.ContainsKey(voteTerm))
        {
            logger.LogInfoAlreadyVotedInTerm(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm);
            return;
        }

        if (coreState.NodeState != RaftNodeState.Follower && voteTerm == coreState.CurrentTerm)
        {
            logger.LogInfoCandidateOrLeaderSameTerm(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm);
            return;
        }

        if (coreState.CurrentTerm > voteTerm)
        {
            logger.LogInfoVoteOnPreviousTerm(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm);
            return;
        }

        // Raft §5.1: a RequestVote carrying a term higher than ours makes us adopt that term
        // REGARDLESS of our current state. The adoption is what arms the `coreState.CurrentTerm > leaderTerm`
        // fence in AppendLogsCoreAsync against a deposed leader still replicating at its old term.
        // Gating adoption on `coreState.NodeState != Follower` (the original B2a scope) left a hole: a FOLLOWER
        // that granted a higher-term vote kept its in-memory term at the old value and went on ACKing
        // the deposed leader's appends — handing it a phantom quorum that kept committing acknowledged
        // writes the new leader then overwrote (observed as a Jepsen linearizability violation; the
        // grant path already persisted the new term to hard state, so a restart fenced correctly while
        // the live node did not). Only the leader/candidate step-down bookkeeping stays gated on state;
        // a follower keeps its (now old-term) leader knowledge until the new term's real leader
        // announces itself via AppendLogs. The vote target is left to the grant path below, which may
        // still deny on log-freshness — the term adoption happens either way.
        if (voteTerm > coreState.CurrentTerm)
        {
            bool stepDown = coreState.NodeState != RaftNodeState.Follower;

            if (stepDown)
            {
                logger.LogInfoSteppingDownOnHigherVoteTerm(
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm, coreState.CurrentTerm);

                // Mirrors the step-down in the AppendLogs path, except no leader is adopted (a vote
                // request does not identify a leader).
                coreState.NodeState = RaftNodeState.Follower;
                host.Leader = "";
                tracker.ClearAll();
                coreState.ResetLocalCommittedIndexOnDemotion();
                failAllActiveProposalWaiters();
            }

            coreState.CurrentTerm = voteTerm;

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
            // A rejected write is tolerated: the term stays adopted in memory, and a crash may regress it,
            // which is safe because no vote was recorded in it (a grant below persists on its own).
            if (!await wal.PersistHardStateAsync(coreState.CurrentTerm, null).ConfigureAwait(false))
            {
                logger.LogWarnHardStateNotPersisted(
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, coreState.CurrentTerm, null,
                    "higher-term adoption on a vote request", "The term is adopted in memory only.");
            }
        }

        string expectedLeader = expectedLeaders.GetValueOrDefault(voteTerm, "");
        
        if (!string.IsNullOrEmpty(expectedLeader) && expectedLeader != node.Endpoint)
        {
            logger.LogInfoAlreadyVotedForOther(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, expectedLeader);
            return;
        }
        
        (long localMaxId, long localLastLogTerm) = await GetFreshnessLogPositionAsync().ConfigureAwait(false);

        if (CandidateLogIsBehind(remoteLastLogTerm, remoteMaxLogId, localLastLogTerm, localMaxId))
        {
            // Reject a real vote for a candidate whose log is behind ours, compared lexicographically
            // by (lastLogTerm, lastLogIndex) per Raft §5.4.1 — a higher index no longer overrides a
            // stale last term. We do NOT bump our own term here: with PreVote (§9.6) in place a stale
            // candidate can no longer reach this real-vote path with an inflated term, so the old
            // `coreState.CurrentTerm++` heuristic that forced us to be elected is no longer needed and only
            // risked spurious term churn.
            logger.LogInfoVoteOutdatedLog(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, remoteMaxLogId, localMaxId);
            return;
        }
        
        // B2b: durably record who we voted for in this term BEFORE replying, so a crash right after the
        // reply cannot let us grant a different candidate in the same term after restart (the double-vote
        // that would let two leaders be elected for one term). The term persisted is voteTerm — the term we
        // are voting in, which is >= coreState.CurrentTerm here. Persisted before any in-memory grant
        // bookkeeping so a rejected write leaves nothing to undo: the vote is simply withheld, because a
        // vote the node cannot remember across a crash is exactly the double-vote hazard above. The
        // candidate asks again on its next round, and the grant succeeds once the WAL accepts writes.
        if (!await wal.PersistHardStateAsync(voteTerm, node.Endpoint).ConfigureAwait(false))
        {
            logger.LogWarnHardStateNotPersisted(
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, voteTerm, node.Endpoint,
                "vote grant", "Vote withheld; the candidate may ask again.");
            return;
        }

        coreState.LastHeartbeat = host.HybridLogicalClock.ReceiveEvent(host.LocalNodeId, timestamp);
        coreState.LastVotation = coreState.LastHeartbeat;

        // B3: granting a vote counts as local activity — anchor both duration shadows to now so the
        // follower election gate and the recent-vote cooldown measure from this moment.
        long grantTicks = host.GetMonotonicTimestamp();
        coreState.LastHeartbeatTicks = grantTicks;
        coreState.LastVotationTicks = grantTicks;

        expectedLeaders[voteTerm] = node.Endpoint;

        logger.LogInfoSendingVote(host.LocalEndpoint, host.PartitionId, coreState.NodeState, node.Endpoint, voteTerm);

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
            logger.LogDebugIgnoringVoteGrantNotVoter(host.LocalEndpoint, host.PartitionId, coreState.NodeState, preVote ? "pre-" : "", endpoint, voteTerm);
            return;
        }

        // A grant advertises the granter's §5.4.1 position — record it BEFORE any of the guards
        // below can discard the message (in particular the fresher-granter guard: a voter whose
        // position exceeds ours is exactly the evidence the deference/escalation logic needs).
        RecordPeerFreshness(endpoint, remoteMaxLogId);

        if (preVote)
        {
            // Tally a pre-grant. Placed before the Follower early-return because a node running a
            // pre-vote round is still a Follower. Touches only pre-vote state until quorum promotes.
            if (electionPhase != RaftElectionPhase.PreVote || voteTerm != preVoteTerm)
            {
                logger.LogDebugIgnoringPreVoteGrantNoRound(host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, voteTerm, electionPhase, preVoteTerm);
                return;
            }

            preVotes.Add(endpoint);
            // Quorum is computed over voters only; learners in host.Nodes must not inflate the
            // denominator. No artificial floor: a sole committed voter's majority is 1, and
            // flooring at 2 made a 1-voter range with a transitional peer unelectable.
            int preVoterTotal = CountVoterPeers() + 1; // +1 for self
            int preVoteQuorum = (preVoterTotal / 2) + 1;

            logger.LogInfoReceivedPreVote(host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, voteTerm, preVotes.Count, preVoteQuorum, preVoterTotal);

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

        if (coreState.NodeState == RaftNodeState.Follower)
        {
            logger.LogInfoReceivedUnsolicitedVote(host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, voteTerm);
            return;
        }

        if (voteTerm < coreState.CurrentTerm)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Received vote from {Endpoint} on previous term Term={Term}. Ignoring...", host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, voteTerm);
            return;
        }
        
        if (coreState.NodeState == RaftNodeState.Leader)
        {
            // lastCommitIndexes is deliberately not written here — see the note at the quorum
            // seeding below. A vote reports a log id, not a committed frontier.
            tracker.SetStartCommitIndex(endpoint, remoteMaxLogId);

            logger.LogInfoReceivedVoteAlreadyLeader(host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, voteTerm);
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
                coreState.NodeState, 
                endpoint, 
                remoteMaxLogId, 
                maxLogResponse
            );
            return;
        }

        int numberVotes = IncreaseVotes(endpoint, voteTerm);
        // Quorum is computed over voters only; learners in host.Nodes must not inflate the
        // denominator. No artificial floor: a sole committed voter's majority is 1, and
        // flooring at 2 made a 1-voter range with a transitional peer unelectable.
        int voterTotal = CountVoterPeers() + 1; // +1 for self
        int quorum = (voterTotal / 2) + 1;

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
        // coreState.LocalCommittedIndex, so TrySendBackfillBatchAsync falls back to followerMaxLog + 1 and a
        // zero seed would re-ship every follower's log from index 1 on every election.
        //
        // startCommitIndexes keeps the vote's value: it records where a peer's log started this
        // term, which is what a log id is.
        tracker.SetStartCommitIndex(endpoint, remoteMaxLogId);

        logger.LogInfoReceivedVote(host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, voteTerm, numberVotes, quorum, voterTotal, remoteMaxLogId, maxLogResponse);

        if (numberVotes < quorum)
            return;

        // A quorum built from stale dispatches must not promote: under executor backlog the vote
        // that completes quorum can be processed many seconds after the election started (a
        // 10,001 ms ReceiveVote dispatch was observed in the Jepsen nightlies), and by then the
        // cluster has moved — the promotion drain targets a frontier the stale round never saw,
        // spins for its whole bound inside the dispatch, and is recorded as a drain failure when
        // the log is actually fine. Abandon the round instead: hand the term back cheaply and let
        // the election timer start a fresh one. This is a staleness check, not a log-integrity
        // check, so it deliberately does not touch the promotion-gate refusal counters.
        if (coreState.VotingStartedTicks != 0
            && RaftMonotonic.Elapsed(coreState.VotingStartedTicks, host.GetMonotonicTimestamp()) > coreState.ElectionTimeout * 2)
        {
            logger.LogWarning("[{LocalEndpoint}/{PartitionId}/{State}] Quorum for Term={Term} completed {ElapsedMs}ms after the election started — abandoning the stale promotion; the election timer will start a fresh round.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, voteTerm,
                RaftMonotonic.Elapsed(coreState.VotingStartedTicks, host.GetMonotonicTimestamp()).TotalMilliseconds);

            coreState.NodeState = RaftNodeState.Follower;
            coreState.LocalCommittedIndex = -1;
            return;
        }

        // Here quorum was achieved and we can mark ourselves as leader in the partition.
        // Seed per-follower replication progress. nextIndex is optimistic (leaderMaxLog + 1);
        // it will be corrected by LogMismatch replies if any peer is behind.
        tracker.ClearProgressKeepingCommitFrontiers();
        foreach (RaftNode peer in host.Nodes)
        {
            tracker.SeedOptimisticProgress(peer.Endpoint, maxLogResponse);
        }

        bool leadershipPublished = await becomeLeaderAsync().ConfigureAwait(false);

        double electionElapsedMs = RaftMonotonic.Elapsed(coreState.VotingStartedTicks, host.GetMonotonicTimestamp()).TotalMilliseconds;
        logger.LogInfoReceivedVoteProclaimedLeader(host.LocalEndpoint, host.PartitionId, coreState.NodeState, endpoint, electionElapsedMs, voteTerm, numberVotes, quorum, host.Nodes.Count + 1, remoteMaxLogId, maxLogResponse);

        // With a promotion barrier pending, leadership is published (and LeaderChanged fired) by
        // CompleteLeaderCommit once the barrier no-op commits; the heartbeat below still goes out
        // immediately so followers adopt this term and rival elections stay suppressed.
        if (leadershipPublished)
            await host.InvokeLeaderChanged(host.PartitionId, host.LocalEndpoint);

        await sendHeartbeat(true).ConfigureAwait(false);
    }

    /// <summary>
    /// Increases the number of votes for a given term.
    /// </summary>
    /// <param name="endpoint"></param>
    /// <param name="term"></param>
    /// <returns></returns>
    public int IncreaseVotes(string endpoint, long term)
    {
        if (votes.TryGetValue(term, out HashSet<string>? votesPerEndpoint))
            votesPerEndpoint.Add(endpoint);
        else
            votes[term] = [endpoint];

        return votes[term].Count;
    }

    private readonly Dictionary<long, HashSet<string>> votes = [];

    /// <summary>
    /// Endpoints (including self) that pre-granted for <see cref="preVoteTerm"/>.
    /// Pre-vote-only and side-effect-free; separate from the real-election <see cref="votes"/> tally.
    /// </summary>
    private readonly HashSet<string> preVotes = [];

    /// <summary>
    /// Gates whether an incoming <c>Vote(PreVote=true)</c> reply is tallied as a pre-grant.
    /// Pre-vote-only bookkeeping: it is never persisted and answering a pre-vote never mutates it.
    /// </summary>
    private RaftElectionPhase electionPhase = RaftElectionPhase.None;

    /// <summary>
    /// The hypothetical <c>CurrentTerm + 1</c> the currently-open pre-vote round is for.
    /// <c>-1</c> when no round is open. Pre-vote-only and side-effect-free: the real
    /// <see cref="RaftPartitionCoreState.CurrentTerm"/> is only bumped once a pre-vote quorum promotes to a real election.
    /// </summary>
    private long preVoteTerm = -1;

    private readonly Dictionary<long, string> expectedLeaders = [];

    /// <summary>
    /// Last §5.4.1 log position each peer advertised on the vote paths (its RequestVotes probes
    /// and its grants), with the monotonic tick it was observed at. This is the evidence base for
    /// <see cref="KnowsFresherAliveVoter"/>: a node whose own log has a hole must not campaign —
    /// or escalate a promotion refusal into a tail truncation — while a live voter is known to
    /// hold a fresher contiguous log, because that voter can win the term (and then backfill the
    /// hole) with nothing lost. Kept separate from the replication tracker's
    /// <c>startCommitIndexes</c>, whose entries carry different semantics and drive backfill
    /// decisions. Entries are never trusted beyond <see cref="PeerFreshnessTtl"/> and never
    /// without a live SWIM state, so a departed peer's stale advertisement cannot suppress
    /// elections forever (the failure mode of the removed pre-election veto).
    /// </summary>
    private readonly Dictionary<string, (long Position, long ObservedTicks)> peerFreshness = [];

    private static readonly TimeSpan PeerFreshnessTtl = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Consecutive election-timer rounds this node has yielded because its own log has a hole and
    /// a fresher live voter is known (see <see cref="StartPreVoteAsync"/>). Bounded by
    /// <see cref="MaxCandidacyDeferrals"/> so a fresher peer that never campaigns cannot suppress
    /// this node's candidacy forever; reset whenever the deference condition stops holding.
    /// </summary>
    private int candidacyDeferrals;

    private const int MaxCandidacyDeferrals = 10;

    private readonly Random random;
}
