
using System.Collections.Concurrent;
using Kommander.Data;
using Kommander.Logging;
using Kommander.Scheduling;
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kommander;

/// <summary>
/// Owns the in-flight snapshot-send guard table (<c>pendingSnapshotEndpoints</c>) for
/// <see cref="RaftPartitionStateMachine"/> and encapsulates the full chunked-send loop.
/// <see cref="TrySend"/> is the entry point called on the executor thread; it fires
/// <see cref="TrySendSnapshotAsync"/> as a detached background <see cref="Task"/> and
/// guarantees at most one concurrent transfer per follower endpoint.
/// All background work runs off the executor thread — no executor locks are held during I/O.
/// </summary>
internal sealed class SnapshotSender
{
    private readonly ConcurrentDictionary<string, byte> pendingSnapshotEndpoints = new();

    private readonly IRaftPartitionHost host;
    private readonly ILogger<IRaft> logger;
    private readonly Func<RaftNodeState> getNodeState;
    private readonly Func<Action<RaftRequest>?> getPostToExecutor;
    private readonly Action<string, long> onSnapshotInstalled;

    internal SnapshotSender(
        IRaftPartitionHost host,
        ILogger<IRaft> logger,
        Func<RaftNodeState> getNodeState,
        Func<Action<RaftRequest>?> getPostToExecutor,
        Action<string, long> onSnapshotInstalled)
    {
        this.host = host;
        this.logger = logger;
        this.getNodeState = getNodeState;
        this.getPostToExecutor = getPostToExecutor;
        this.onSnapshotInstalled = onSnapshotInstalled;
    }

    /// <summary>
    /// Called on the executor thread each heartbeat cycle. Fires a background snapshot
    /// transfer to <paramref name="node"/> if no transfer is already in progress for that
    /// endpoint (guarded by <c>pendingSnapshotEndpoints.TryAdd</c>). The entry is
    /// removed in the <c>finally</c> block of <see cref="TrySendSnapshotAsync"/> so the
    /// next heartbeat can retry on failure.
    /// </summary>
    internal void TrySend(RaftNode node, long snapshotIndex)
    {
        if (pendingSnapshotEndpoints.TryAdd(node.Endpoint, 0))
        {
            logger.LogInfoStartingSnapshotTransfer(
                host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex);
            _ = TrySendSnapshotAsync(node, snapshotIndex);
        }
    }

    /// <summary>
    /// Advances the <c>lastCommitIndexes</c> entry for the follower after the background
    /// snapshot task confirmed successful installation. Always called on the executor thread
    /// via the <c>postToExecutor</c> callback, preserving the single-owner invariant.
    /// </summary>
    internal void CompleteSnapshotInstalled(string endpoint, long snapshotIndex) =>
        onSnapshotInstalled(endpoint, snapshotIndex);

    private async Task TrySendSnapshotAsync(RaftNode node, long snapshotIndex)
    {
        const int chunkSize = 3 * 1024 * 1024;

        try
        {
            bool useSystemState = host.PartitionId == RaftSystemConfig.SystemPartition
                                  && host.SystemStateTransfer is not null;

            Stream snapshot;
            SnapshotKind kind;
            if (useSystemState)
            {
                kind = SnapshotKind.SystemState;
                try
                {
                    snapshot = await host.SystemStateTransfer!
                        .ExportPartitionState(host.PartitionId, snapshotIndex, CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        "[{LocalEndpoint}/{PartitionId}/{State}] TrySendSnapshotAsync: ExportPartitionState failed for {Endpoint}: {Message}",
                        host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, ex.Message);
                    return;
                }
            }
            else
            {
                IRaftStateMachineTransfer? transfer = host.StateMachineTransfer;
                if (transfer is null)
                    return;

                kind = SnapshotKind.Range;
                RaftSplitPlan plan = new() { TargetPartitionId = host.PartitionId };
                try
                {
                    snapshot = await transfer.ExportRange(plan, snapshotIndex, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        "[{LocalEndpoint}/{PartitionId}/{State}] TrySendSnapshotAsync: ExportRange failed for {Endpoint}: {Message}",
                        host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, ex.Message);
                    return;
                }
            }

            string sessionId = Guid.NewGuid().ToString("N");
            byte[] buffer = new byte[chunkSize];
            int chunkIndex = 0;
            bool success = false;

            await using (snapshot.ConfigureAwait(false))
            {
                while (true)
                {
                    int bytesRead = await StreamUtils.ReadExactAsync(snapshot, buffer, chunkSize, CancellationToken.None).ConfigureAwait(false);
                    bool isLast = bytesRead < chunkSize;

                    SnapshotRequest chunk = new()
                    {
                        SessionId = sessionId,
                        PartitionId = host.PartitionId,
                        SnapshotIndex = snapshotIndex,
                        FollowerEndpoint = node.Endpoint,
                        ChunkIndex = chunkIndex,
                        IsLast = isLast,
                        Data = buffer[..bytesRead],
                        Kind = kind,
                    };

                    SnapshotResponse response = await host.SendInstallSnapshotAsync(node, chunk, CancellationToken.None).ConfigureAwait(false);
                    if (!response.Success)
                    {
                        logger.LogWarning(
                            "[{LocalEndpoint}/{PartitionId}/{State}] Snapshot chunk {ChunkIndex} to {Endpoint} was rejected",
                            host.LocalEndpoint, host.PartitionId, getNodeState(), chunkIndex, node.Endpoint);
                        return;
                    }

                    if (isLast)
                    {
                        success = true;
                        break;
                    }

                    chunkIndex++;
                }
            }

            if (success)
            {
                logger.LogInfoSnapshotInstalled(host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, snapshotIndex, chunkIndex + 1);

                getPostToExecutor()?.Invoke(new RaftRequest(
                    RaftRequestType.SnapshotInstalled,
                    commitIndex: snapshotIndex,
                    endpoint: node.Endpoint));
            }
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[{LocalEndpoint}/{PartitionId}/{State}] TrySendSnapshotAsync: unhandled error for {Endpoint}: {Message}",
                host.LocalEndpoint, host.PartitionId, getNodeState(), node.Endpoint, ex.Message);
        }
        finally
        {
            pendingSnapshotEndpoints.TryRemove(node.Endpoint, out _);
        }
    }
}
