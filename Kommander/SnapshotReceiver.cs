
using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Logging;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Owns the in-progress snapshot-receive session buffers on behalf of <see cref="RaftManager"/>.
/// Incoming chunks are accumulated per <see cref="SnapshotRequest.SessionId"/> in a
/// <see cref="MemoryStream"/> until <see cref="SnapshotRequest.IsLast"/> triggers import.
/// All mutable state is guarded by <c>_pendingSnapshotsLock</c>; the class is safe to
/// call concurrently from gRPC dispatch threads.
/// </summary>
internal sealed class SnapshotReceiver
{
    private readonly ConcurrentDictionary<string, MemoryStream> _pendingSnapshots = new();
    private readonly object _pendingSnapshotsLock = new();

    private readonly Func<bool> isDisposed;
    private readonly Func<IRaftSystemStateTransfer?> getSystemTransfer;
    private readonly Func<IRaftStateMachineTransfer?> getRangeTransfer;
    private readonly IWAL walAdapter;
    private readonly ILogger<IRaft> logger;
    private readonly string localEndpoint;

    internal SnapshotReceiver(
        Func<bool> isDisposed,
        Func<IRaftSystemStateTransfer?> getSystemTransfer,
        Func<IRaftStateMachineTransfer?> getRangeTransfer,
        IWAL walAdapter,
        ILogger<IRaft> logger,
        string localEndpoint)
    {
        this.isDisposed = isDisposed;
        this.getSystemTransfer = getSystemTransfer;
        this.getRangeTransfer = getRangeTransfer;
        this.walAdapter = walAdapter;
        this.logger = logger;
        this.localEndpoint = localEndpoint;
    }

    internal async Task<SnapshotResponse> ReceiveInstallSnapshot(
        SnapshotRequest request,
        CancellationToken cancellationToken = default)
    {
        if (isDisposed())
            return new SnapshotResponse(false);

        bool isSystemState = request.Kind == SnapshotKind.SystemState;
        IRaftSystemStateTransfer? systemTransfer = null;
        IRaftStateMachineTransfer? rangeTransfer = null;
        if (isSystemState)
        {
            systemTransfer = getSystemTransfer();
            if (systemTransfer is null)
                return new SnapshotResponse(false);
        }
        else
        {
            rangeTransfer = getRangeTransfer();
            if (rangeTransfer is null)
                return new SnapshotResponse(false);
        }

        cancellationToken.ThrowIfCancellationRequested();

        long currentMax = walAdapter.GetMaxLog(request.PartitionId);
        if (currentMax >= request.SnapshotIndex)
            return new SnapshotResponse(true);

        MemoryStream buf;
        lock (_pendingSnapshotsLock)
        {
            if (isDisposed())
                return new SnapshotResponse(false);

            buf = _pendingSnapshots.GetOrAdd(request.SessionId, _ => new MemoryStream());
            buf.Write(request.Data.Span);

            if (!request.IsLast)
                return new SnapshotResponse(true);

            _pendingSnapshots.TryRemove(request.SessionId, out _);
            buf.Position = 0;
        }

        try
        {
            if (isSystemState)
                await systemTransfer!.ImportPartitionState(request.PartitionId, buf, cancellationToken).ConfigureAwait(false);
            else
                await rangeTransfer!.ImportRange(request.PartitionId, buf, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            string importMethod = isSystemState ? "ImportPartitionState" : "ImportRange";
            logger.LogError(
                "[{Endpoint}] ReceiveInstallSnapshot: {Method} partition={PartitionId} index={Index} failed: {Message}",
                localEndpoint, importMethod, request.PartitionId, request.SnapshotIndex, ex.Message);
            return new SnapshotResponse(false);
        }
        finally
        {
            await buf.DisposeAsync().ConfigureAwait(false);
        }

        RaftLog checkpointLog = new()
        {
            Id = request.SnapshotIndex,
            Type = RaftLogType.CommittedCheckpoint,
            Term = walAdapter.GetCurrentTerm(request.PartitionId),
        };

        walAdapter.Write([(request.PartitionId, [checkpointLog])]);

        logger.LogInfoReceiveInstallSnapshot(localEndpoint, request.PartitionId, request.SnapshotIndex);

        return new SnapshotResponse(true);
    }

    /// <summary>Returns the count of active receive sessions. For test assertions only.</summary>
    internal int PendingSessionCount => _pendingSnapshots.Count;

    /// <summary>
    /// Drains and disposes all in-progress receive buffers. Called during
    /// <see cref="RaftManager.Dispose"/> after the timer is stopped and before
    /// partition queues are drained.
    /// </summary>
    internal void DisposePendingSnapshots()
    {
        List<MemoryStream> snapshots = [];

        lock (_pendingSnapshotsLock)
        {
            foreach (KeyValuePair<string, MemoryStream> pending in _pendingSnapshots)
            {
                if (_pendingSnapshots.TryRemove(pending.Key, out MemoryStream? snapshot))
                    snapshots.Add(snapshot);
            }
        }

        foreach (MemoryStream snapshot in snapshots)
            snapshot.Dispose();
    }
}
