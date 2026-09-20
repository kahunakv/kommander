using Kommander.Data;
using Kommander.Logging;
using Kommander.Scheduling;
using Microsoft.Extensions.Logging;

namespace Kommander.Consensus;

/// <summary>
/// Follower-side snapshot install (Raft "Rule 7") — the receive counterpart to
/// <see cref="SnapshotSender"/>.
///
/// <para><b>Ordering is the whole design.</b> Import must precede the durable WAL boundary, so a
/// crash between them leaves recoverable state: the import is idempotent and the boundary is not
/// yet durable, so the sender simply retries the whole snapshot. Reversing the two would let a
/// durable boundary claim state the application never received.</para>
///
/// <para><b>Concurrency.</b> Runs on the partition executor thread, so the whole install is
/// serialized against every other partition operation. The import is passed
/// <see cref="CancellationToken.None"/> deliberately — the caller must not be able to dispose the
/// staged buffer while this is still reading it.</para>
///
/// <para><b>Test gate.</b> A registered <see cref="SnapshotInstallGate"/> suspends the install
/// between the numbered steps so a test can observe or act while a snapshot is half-installed. It
/// never reorders anything and is bounded by
/// <see cref="RaftConfiguration.LeadershipBarrierTimeout"/>; on expiry the install fails and the
/// sender retries. With no gate registered each phase costs one null check.</para>
/// </summary>
internal sealed class SnapshotInstaller
{
    private readonly IRaftPartitionHost host;
    private readonly IRaftWalFacade wal;
    private readonly RaftPartitionCoreState coreState;
    private readonly ILogger<IRaft> logger;

    /// <summary>
    /// Adopts a leader and takes the durable step-down that goes with it. Injected rather than
    /// implemented here: the transition spans election bookkeeping, proposal waiters and
    /// replication progress — state this type has no business owning — so the core stays the
    /// single place a leadership adoption is expressed.
    /// </summary>
    private readonly Func<string, long, Task> adoptLeaderAsync;

    /// <summary>
    /// The installed test gate, or <see langword="null"/> in every ordinary run. Written from the
    /// caller's thread by <c>IRaft.SetSnapshotInstallGateForTesting</c> and read on the executor
    /// turn, so it is published and read with <see cref="Volatile"/>; with none installed the
    /// install costs one null check per phase.
    /// </summary>
    private SnapshotInstallGate? installGate;

    public SnapshotInstaller(
        IRaftPartitionHost host,
        IRaftWalFacade wal,
        RaftPartitionCoreState coreState,
        ILogger<IRaft> logger,
        Func<string, long, Task> adoptLeaderAsync)
    {
        this.host = host;
        this.wal = wal;
        this.coreState = coreState;
        this.logger = logger;
        this.adoptLeaderAsync = adoptLeaderAsync;
    }

    /// <summary>
    /// Installs (or clears, with <see langword="null"/>) the install gate for this partition. A
    /// second installation replaces the first; the replaced gate is simply never awaited again.
    /// </summary>
    public void SetInstallGateForTesting(SnapshotInstallGate? gate) => Volatile.Write(ref installGate, gate);

    /// <summary>
    /// Clears <paramref name="gate"/> only if it is still the installed one, so a stale handle
    /// cannot remove a registration that already replaced it.
    /// </summary>
    public void ClearInstallGateForTesting(SnapshotInstallGate gate) =>
        Interlocked.CompareExchange(ref installGate, null, gate);

    /// <summary>
    /// Suspends the install at <paramref name="phase"/> when a gate is registered for it.
    ///
    /// <para>Awaited <b>inline on the executor turn</b>, unlike the reply hold: freezing the
    /// operation mid-way is the entire point. It is bounded by
    /// <see cref="RaftConfiguration.LeadershipBarrierTimeout"/>. On expiry — or if the gate throws
    /// — the install fails with <see cref="RaftOperationStatus.Errored"/>, which the sender retries
    /// exactly as it retries any other install failure. A test gate must never wedge a
    /// partition.</para>
    ///
    /// <para>Returns <see langword="true"/> when the install may continue.</para>
    /// </summary>
    private async ValueTask<bool> PassGateAsync(
        SnapshotInstallPhase phase,
        SnapshotInstallRequest request,
        long boundaryTerm)
    {
        SnapshotInstallGate? gate = Volatile.Read(ref installGate);
        if (gate is null || gate.Phase != phase)
            return true;

        long localMaxLogId = await wal.GetMaxLogAsync().ConfigureAwait(false);

        SnapshotInstallSignal signal = new(
            host.PartitionId,
            request.SnapshotIndex,
            boundaryTerm,
            request.Kind,
            phase,
            localMaxLogId,
            coreState.LastAppliedIndex);

        try
        {
            await gate.Gate(signal).AsTask().WaitAsync(host.Configuration.LeadershipBarrierTimeout).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            logger.LogError(
                "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot test gate at {Phase} did not release within {Timeout}ms for index {Index}; failing the install so the sender retries.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, phase, host.Configuration.LeadershipBarrierTimeout.TotalMilliseconds, request.SnapshotIndex);
            return false;
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot test gate at {Phase} threw for index {Index}: {Message}. Failing the install so the sender retries.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, phase, request.SnapshotIndex, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Follower-side snapshot install on the single-writer executor path (Raft "Rule 7").
    ///
    /// <para>Runs the recoverable ordering: (1) validate the leader term and, on a higher term, take the
    /// same durable step-down as other leader RPCs; reject a stale leader without importing; (2) short-
    /// circuit as an idempotent success when a matching boundary is already installed at or below the
    /// index; (3) invoke the application import; (4) install the durable WAL boundary
    /// (<see cref="IRaftWalFacade.InstallSnapshotBoundaryAsync"/>) which retains the suffix on a matching
    /// boundary term and truncates it on conflict; (5) reconstruct the apply cursor; (6) acknowledge
    /// with a typed outcome (<see cref="SnapshotInstallOutcome"/>): the idempotent short-circuit in (2)
    /// answers <see cref="SnapshotInstallOutcome.SkippedAlreadyCovered"/>, a completed install answers
    /// <see cref="SnapshotInstallOutcome.Installed"/>, and every refusal is a non-success status the
    /// partition maps to <see cref="SnapshotInstallOutcome.Rejected"/>. The sender logs "seeded" only
    /// for an import; a bare success bit let it write that line for a skip too.</para>
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
        //
        // The boundary must also be CONTIGUOUSLY HELD: a CommittedCheckpoint row can be broadcast onto a
        // behind follower over a replication gap (the unanchored live path), and a checkpoint that certifies
        // entries this node never held is exactly the state a snapshot install exists to repair. Every WAL
        // backend withholds the persisted last-checkpoint id in that case (the prefix is verified at land
        // time), so this guard is a second fence on the same rule: when the facade tracks presence, the
        // contiguous presence frontier must reach the index too, or the install proceeds. A facade that does
        // not track presence (-1) leaves the decision to the boundary alone.
        long installedBoundary = await wal.GetLastCheckpointAsync().ConfigureAwait(false);
        long presentIndex = wal.GetPresentIndex();
        if (installedBoundary >= snapshotIndex && (presentIndex < 0 || presentIndex >= snapshotIndex))
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
                if (snapshotIndex > coreState.LastAppliedIndex)
                    coreState.LastAppliedIndex = snapshotIndex;
                wal.SeedCommitFrontierFromSnapshot(snapshotIndex, Math.Max(boundaryTermAtIndex, 0));

                // Say so: the sender reads this outcome, and an operator reading this node's log
                // must be able to tell a skip from an import when a follower's state is in doubt.
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInfoReceiveInstallSnapshotSkipped(host.LocalEndpoint, host.PartitionId, snapshotIndex, installedBoundary);

                return new RaftResponse(SnapshotInstallOutcome.SkippedAlreadyCovered, snapshotIndex);
            }
        }
        else if (installedBoundary >= snapshotIndex)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot at index {Index}: the last checkpoint {Boundary} covers the index but the contiguous presence frontier {Present} does not (a checkpoint row over a replication gap); installing instead of skipping.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, snapshotIndex, installedBoundary, presentIndex);
        }

        bool legacy = leaderTerm <= 0 || string.IsNullOrEmpty(leaderEndpoint);
        if (legacy && !host.Configuration.AllowLegacySnapshotSenders)
        {
            logger.LogWarning(
                "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: legacy sender (LeaderTerm={LeaderTerm}, LeaderEndpoint='{Endpoint}') and AllowLegacySnapshotSenders is off.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, leaderTerm, leaderEndpoint);
            return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
        }

        // The term of the entry the checkpoint boundary is stamped with. For a legacy sender we have no
        // authoritative last-included term, so fall back to our local current term (old behaviour).
        long boundaryTerm = legacy ? coreState.CurrentTerm : request.LastIncludedTerm;

        if (!legacy)
        {
            // Rule 7.1 — reject a stale leader without importing (mirror AppendLogsCoreAsync).
            if (coreState.CurrentTerm > leaderTerm)
            {
                logger.LogWarning(
                    "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot from stale leader {Endpoint}: LeaderTerm={LeaderTerm} < CurrentTerm={CurrentTerm}. Rejecting.",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, leaderEndpoint, leaderTerm, coreState.CurrentTerm);
                return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
            }

            // Membership fence — mirror AppendLogsCoreAsync: a snapshot is a leader RPC, and only a
            // committed roster member can legitimately be a leader. Skipped for the already-accepted
            // leader so a briefly-lagging roster snapshot cannot reject the real leader.
            if (host.Leader != leaderEndpoint && !host.IsMember(leaderEndpoint))
            {
                logger.LogWarning(
                    "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: sender {Endpoint} is not a committed cluster member.",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, leaderEndpoint);
                return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
            }

            // Rule 7.3 — election safety: at equal term there is exactly one leader. If we have already
            // adopted a different leader for this term, a snapshot from another endpoint is inconsistent.
            // (A legitimate new leader always arrives with a higher term, which passes this check.)
            if (coreState.CurrentTerm == leaderTerm && !string.IsNullOrEmpty(host.Leader) && host.Leader != leaderEndpoint)
            {
                logger.LogWarning(
                    "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: sender {Sender} conflicts with accepted leader {Leader} for term {Term}.",
                    host.LocalEndpoint, host.PartitionId, coreState.NodeState, leaderEndpoint, host.Leader, leaderTerm);
                return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
            }

            // Rule 7.2 — adopt the leader / durable step-down on a valid term, identical to the
            // AppendEntries path. A snapshot is a leader RPC, so it authoritatively identifies the term's
            // leader regardless of our vote record (expectedLeaders constrains voting only).
            if (host.Leader != leaderEndpoint || coreState.CurrentTerm != leaderTerm || coreState.NodeState != RaftNodeState.Follower)
            {
                await adoptLeaderAsync(leaderEndpoint, leaderTerm).ConfigureAwait(false);
            }
        }

        if (!await PassGateAsync(SnapshotInstallPhase.BeforeImport, request, boundaryTerm).ConfigureAwait(false))
            return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);

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
                        host.LocalEndpoint, host.PartitionId, coreState.NodeState);
                    return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
                }

                await systemTransfer.ImportPartitionState(host.PartitionId, request.Snapshot, CancellationToken.None).ConfigureAwait(false);
            }
            else if (request.Kind == SnapshotKind.PartitionState)
            {
                IRaftPartitionStateTransfer? partitionTransfer = host.PartitionStateTransfer;
                if (partitionTransfer is null)
                {
                    logger.LogWarning(
                        "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: no IRaftPartitionStateTransfer registered.",
                        host.LocalEndpoint, host.PartitionId, coreState.NodeState);
                    return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
                }

                await partitionTransfer.ImportPartitionState(host.PartitionId, request.Snapshot, CancellationToken.None).ConfigureAwait(false);
            }
            else
            {
                IRaftStateMachineTransfer? rangeTransfer = host.StateMachineTransfer;
                if (rangeTransfer is null)
                {
                    logger.LogWarning(
                        "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot rejected: no IRaftStateMachineTransfer registered.",
                        host.LocalEndpoint, host.PartitionId, coreState.NodeState);
                    return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
                }

                await rangeTransfer.ImportRange(host.PartitionId, request.Snapshot, CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot import failed for index {Index}: {Message}",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, snapshotIndex, ex.Message);
            return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
        }

        if (!await PassGateAsync(SnapshotInstallPhase.AfterImport, request, boundaryTerm).ConfigureAwait(false))
            return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);

        // Ordering step 3 + Rule 7.5/7.6 — install the durable checkpoint boundary. The backend retains
        // the suffix above the index when its stored term matches boundaryTerm and truncates it on
        // conflict, atomically.
        (RaftOperationStatus boundaryStatus, bool suffixTruncated) =
            await wal.InstallSnapshotBoundaryAsync(snapshotIndex, boundaryTerm).ConfigureAwait(false);
        if (boundaryStatus != RaftOperationStatus.Success)
        {
            logger.LogError(
                "[{LocalEndpoint}/{PartitionId}/{State}] InstallSnapshot WAL boundary failed for index {Index}: {Status}. Import succeeded; sender will retry.",
                host.LocalEndpoint, host.PartitionId, coreState.NodeState, snapshotIndex, boundaryStatus);
            return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);
        }

        if (!await PassGateAsync(SnapshotInstallPhase.AfterBoundary, request, boundaryTerm).ConfigureAwait(false))
            return new RaftResponse(RaftResponseType.None, RaftOperationStatus.Errored, -1);

        // Ordering step 4 + Rule 7.7 — reconstruct the apply cursor from the installed boundary so a later
        // promotion does not re-deliver the imported prefix (mirrors CompleteRestoreAsync's cursor seed), and
        // advance the in-memory commit frontier to the boundary so GetCommitIndex reflects the compacted prefix
        // as committed (otherwise post-snapshot consumer delivery and backfill reporting stall below it).
        if (snapshotIndex > coreState.LastAppliedIndex)
            coreState.LastAppliedIndex = snapshotIndex;
        wal.SeedCommitFrontierFromSnapshot(snapshotIndex, Math.Max(boundaryTerm, 0), suffixTruncated);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInfoReceiveInstallSnapshot(host.LocalEndpoint, host.PartitionId, snapshotIndex);

        return new RaftResponse(SnapshotInstallOutcome.Installed, snapshotIndex);
    }
}
