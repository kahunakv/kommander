
using Kommander.Data;
using Kommander.System;

namespace Kommander;

/// <summary>
/// Owns all application-facing event delegates for <see cref="RaftManager"/> and
/// provides the internal <c>Invoke*</c> dispatch helpers. <see cref="RaftManager"/>
/// re-exposes each event as an accessor-backed façade so callers see no API change.
/// Delegate-chain subscribe/unsubscribe is inherently thread-safe. <c>Invoke*</c>
/// methods may be called from any thread; handlers must be re-entrant.
/// </summary>
internal sealed class RaftEventNotifier
{
    internal event Action<int>? OnRestoreStarted;
    internal event Action<int>? OnRestoreFinished;
    internal event Action<int>? OnSystemRestoreFinished;
    internal event Action<int, RaftLog>? OnReplicationError;
    internal event Func<int, RaftLog, Task<bool>>? OnLogRestored;
    internal event Func<int, RaftLog, Task<bool>>? OnSystemLogRestored;
    internal event Func<int, RaftLog, Task<bool>>? OnReplicationReceived;
    internal event Func<int, RaftLog, Task<bool>>? OnSystemReplicationReceived;
    internal event Func<int, string, Task<bool>>? OnLeaderChanged;
    internal event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged;
    internal event Action<ClusterMembership>? OnMembershipChanged;

    internal void InvokeRestoreStarted(int partitionId)
    {
        Action<int>? callback = OnRestoreStarted;
        callback?.Invoke(partitionId);
    }

    internal void InvokeRestoreFinished(int partitionId)
    {
        Action<int>? callback = OnRestoreFinished;
        callback?.Invoke(partitionId);
    }

    internal void InvokeSystemRestoreFinished(int partitionId)
    {
        Action<int>? callback = OnSystemRestoreFinished;
        callback?.Invoke(partitionId);
    }

    internal void InvokeReplicationError(int partitionId, RaftLog log)
    {
        Action<int, RaftLog>? callback = OnReplicationError;
        callback?.Invoke(partitionId, log);
    }

    internal async Task<bool> InvokeReplicationReceived(int partitionId, RaftLog log)
    {
        Func<int, RaftLog, Task<bool>>? callback = OnReplicationReceived;
        if (callback is null)
            return true;
        return await callback(partitionId, log);
    }

    internal async Task<bool> InvokeSystemReplicationReceived(int partitionId, RaftLog log)
    {
        Func<int, RaftLog, Task<bool>>? callback = OnSystemReplicationReceived;
        if (callback is null)
            return true;
        return await callback(partitionId, log);
    }

    internal async Task<bool> InvokeSystemLogRestored(int partitionId, RaftLog log)
    {
        Func<int, RaftLog, Task<bool>>? callback = OnSystemLogRestored;
        if (callback is null)
            return true;
        return await callback(partitionId, log).ConfigureAwait(false);
    }

    internal async Task<bool> InvokeLogRestored(int partitionId, RaftLog log)
    {
        Func<int, RaftLog, Task<bool>>? callback = OnLogRestored;
        if (callback is null)
            return true;
        return await callback(partitionId, log).ConfigureAwait(false);
    }

    internal async Task<bool> InvokeLeaderChanged(int partitionId, string node)
    {
        Func<int, string, Task<bool>>? callback = OnLeaderChanged;
        if (callback is null)
            return true;
        return await callback(partitionId, node).ConfigureAwait(false);
    }

    internal void InvokePartitionMapChanged(IReadOnlyList<RaftPartitionRange> ranges) =>
        OnPartitionMapChanged?.Invoke(ranges);

    internal void RaiseMembershipChanged(ClusterMembership membership) =>
        OnMembershipChanged?.Invoke(membership);
}
