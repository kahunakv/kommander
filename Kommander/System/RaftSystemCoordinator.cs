
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Kommander.System.Protos;
using Google.Protobuf;
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.System;

/// <summary>
/// Non-actor replacement for <c>RaftSystemActor</c>.  Owns a single-consumer
/// <see cref="Channel{T}"/> so all system-partition events are processed serially
/// without any Nixie dependency.
/// </summary>
internal sealed class RaftSystemCoordinator : IDisposable
{
    private const int MaxRetries = 10;

    private readonly Dictionary<string, string> systemConfiguration = new();

    private readonly RaftManager manager;

    private readonly ILogger<IRaft> logger;

    private string? leaderNode;

    private readonly Channel<RaftSystemRequest> _channel =
        Channel.CreateUnbounded<RaftSystemRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    // ── Test injection points ──────────────────────────────────────────────
    // Null in production. Tests set these to intercept replication and partition
    // activation without requiring a real Raft quorum or system partition.
    internal Func<string, byte[], bool, CancellationToken, Task<RaftReplicationResult>>? ReplicateOverride;
    internal Action<List<RaftPartitionRange>>? StartPartitionsOverride;

    /// <summary>
    /// Delay between replication retries. Defaults to 5 seconds in production;
    /// tests set this to <see cref="TimeSpan.Zero"/> for fast retry cycles.
    /// </summary>
    internal TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    // Queue of drain sentinels.  Each DrainAsync() call enqueues one TCS here
    // and sends a DrainSentinel request; the loop completes it in FIFO order.
    private readonly ConcurrentQueue<TaskCompletionSource> _drainQueue = new();

    // ── Split state ────────────────────────────────────────────────────────
    // Keyed by source partition id; holds everything needed to complete Phase 2.
    private sealed record SplitInProgress(
        int TargetPartitionId,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? Completion);

    private readonly Dictionary<int, SplitInProgress> _pendingSplits = new();

    // ── Merge state ────────────────────────────────────────────────────────
    // Keyed by source (Draining) partition id; holds the survivor id and caller TCS.
    private sealed record MergeInProgress(
        int SurvivorPartitionId,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? Completion);

    private readonly Dictionary<int, MergeInProgress> _pendingMerges = new();

    // Exposed so tests can await clean loop exit after Stop().
    internal Task LoopTask => _loop;

    public RaftSystemCoordinator(RaftManager manager, ILogger<IRaft> logger)
    {
        this.manager = manager;
        this.logger = logger;
        _loop = Task.Run(RunAsync);
    }

    /// <summary>
    /// Enqueues a request for serial processing. Safe to call from any thread.
    /// </summary>
    internal void Send(RaftSystemRequest request) =>
        _channel.Writer.TryWrite(request);

    /// <summary>
    /// Returns a <see cref="Task"/> that completes only after all requests
    /// currently in the channel have been processed by the background loop.
    /// Used by tests instead of a fixed <c>Task.Delay</c>.
    /// </summary>
    internal Task DrainAsync()
    {
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _drainQueue.Enqueue(tcs);
        _channel.Writer.TryWrite(new RaftSystemRequest(RaftSystemRequestType.DrainSentinel));
        return tcs.Task;
    }

    // ── Background loop ────────────────────────────────────────────────────

    private async Task RunAsync()
    {
        ChannelReader<RaftSystemRequest> reader = _channel.Reader;

        try
        {
            while (await reader.WaitToReadAsync(_cts.Token).ConfigureAwait(false))
            {
                while (reader.TryRead(out RaftSystemRequest? request))
                {
                    try
                    {
                        await Receive(request, _cts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(
                            "[RaftSystemCoordinator] Unhandled exception processing {Type}: {Message}\n{StackTrace}",
                            request.Type, ex.Message, ex.StackTrace);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown
        }
        catch (Exception ex)
        {
            logger.LogError(
                "[RaftSystemCoordinator] RunAsync faulted: {Message}\n{StackTrace}",
                ex.Message, ex.StackTrace);
        }

        // Drain any messages written before the channel was completed.
        // The token is already cancelled at this point so Receive will return
        // quickly without starting any new replication or split work.
        while (reader.TryRead(out RaftSystemRequest? remaining))
        {
            try
            {
                await Receive(remaining, _cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogError(
                    "[RaftSystemCoordinator] drain {Type}: {Message}",
                    remaining.Type, ex.Message);
            }
        }
    }

    // ── Message handler ────────────────────────────────────────────────────

    private async Task Receive(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return;
        switch (message.Type)
        {
            case RaftSystemRequestType.ConfigRestored:
            {
                if (message.LogData is null)
                {
                    logger.LogWarning("Restored message is null");
                    return;
                }

                RaftSystemMessage systemMessage = Unserialize(message.LogData);
                systemConfiguration[systemMessage.Key] = systemMessage.Value;
                logger.LogInformation("Restored system configuration: {Key}", systemMessage.Key);
            }
            break;

            case RaftSystemRequestType.ConfigReplicated:
            {
                if (message.LogData is null)
                {
                    logger.LogWarning("Replication message is null");
                    return;
                }

                RaftSystemMessage systemMessage = Unserialize(message.LogData);
                systemConfiguration[systemMessage.Key] = systemMessage.Value;
                logger.LogInformation("Replicated system configuration: {Key}", systemMessage.Key);

                InitializePartitions();
            }
            break;

            case RaftSystemRequestType.LeaderChanged:
                leaderNode = message.LeaderNode;

                if (string.IsNullOrEmpty(leaderNode))
                {
                    logger.LogInformation("Waiting for leader on system partition...");
                    return;
                }

                if (manager.LocalEndpoint == leaderNode)
                    await TrySetInitialPartitions(cancellationToken).ConfigureAwait(false);
                else
                    InitializePartitions();
                break;

            case RaftSystemRequestType.SplitPartition:
                await TrySplitPartition(message.PartitionId, message.SplitPlan, message.Completion, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.SplitPartitionCommit:
                await TrySplitPartitionCommit(message.PartitionId, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.MergePartition:
                await TryMergePartitions(message.MergePlan!, message.Completion, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.MergePartitionCommit:
                await TryMergePartitionCommit(message.PartitionId, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.RestoreCompleted:
                break;

            case RaftSystemRequestType.CreatePartition:
                await TryCreatePartition(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.RemovePartition:
                await TryRemovePartition(message, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.DrainSentinel:
                if (_drainQueue.TryDequeue(out TaskCompletionSource? tcs))
                    tcs.TrySetResult();
                break;

            default:
                throw new NotImplementedException($"Unhandled RaftSystemRequestType: {message.Type}");
        }
    }

    // ── Partition helpers ──────────────────────────────────────────────────

    private Task<RaftReplicationResult> Replicate(string type, byte[] data, bool autoCommit, CancellationToken ct) =>
        ReplicateOverride is { } fn
            ? fn(type, data, autoCommit, ct)
            : manager.ReplicateSystemLogs(type, data, autoCommit, ct);

    private void StartPartitions(List<RaftPartitionRange> ranges) =>
        (StartPartitionsOverride ?? manager.StartUserPartitions)(ranges);

    private void InitializePartitions()
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogWarning("InitializePartitions: Failed to get partitions from system configuration");
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("InitializePartitions: Failed to parse partition map: {Partitions}", partitions);
            return;
        }

        StartPartitions(map.Partitions);

        // On restart, any Removed tombstone may represent a crash before DeletePartitionWAL ran.
        // Re-attempt reclamation now; the call is idempotent on all three WAL adapters.
        foreach (RaftPartitionRange range in map.Partitions)
        {
            if (range.State == RaftPartitionState.Removed)
                manager.WalAdapter.DeletePartitionWAL(range.PartitionId);
        }

        // Crash recovery: if Phase 1 was committed but Phase 2 was not, the map will
        // contain Splitting pairs.  Re-enqueue Phase 2 so the leader can complete them.
        List<RaftPartitionRange> splitting = map.Partitions
            .Where(r => r.State == RaftPartitionState.Splitting)
            .ToList();

        foreach (RaftPartitionRange target in splitting.Where(r => r.Generation == 1))
        {
            // For HashRange splits: source.EndRange + 1 == target.StartRange after Phase 1 shrink.
            RaftPartitionRange? source = splitting.FirstOrDefault(r =>
                r.PartitionId != target.PartitionId &&
                r.EndRange + 1 == target.StartRange);

            if (source is not null && !_pendingSplits.ContainsKey(source.PartitionId))
            {
                _pendingSplits[source.PartitionId] = new SplitInProgress(target.PartitionId, null);
                Send(new RaftSystemRequest(RaftSystemRequestType.SplitPartitionCommit, source.PartitionId));
            }
        }

        // Crash recovery: if Phase 1 of a merge was committed but Phase 2 was not, the map
        // will contain a Draining partition with no _pendingMerges entry.  Pair it with its
        // adjacent Active survivor and re-enqueue Phase 2.
        foreach (RaftPartitionRange src in map.Partitions.Where(r => r.State == RaftPartitionState.Draining))
        {
            if (_pendingMerges.ContainsKey(src.PartitionId))
                continue;

            // For HashRange: find the adjacent Active partition (left or right neighbor).
            // For Unrouted: find any other Active Unrouted partition.
            // Both sides are checked; the first match wins — either is a valid merge target.
            //
            // DESIGN LIMITATION (Unrouted): with multiple Active Unrouted partitions,
            // FirstOrDefault picks whichever appears first in the persisted list — not
            // necessarily the partition that was the intended survivor before the crash.
            // The correct fix is to store the survivor's id on the Draining entry itself
            // (e.g., a MergeSurvivorId field on RaftPartitionRange) so crash recovery can
            // reconstruct the original plan exactly.  Until that field exists, Unrouted
            // crash recovery is best-effort: the chosen survivor is stable (list order is
            // deterministic within a map version) but may differ from the original intent.
            // HashRange is not affected — adjacency uniquely identifies the survivor.
            RaftPartitionRange? survivor = src.RoutingMode == RaftRoutingMode.HashRange
                ? map.Partitions.FirstOrDefault(r =>
                    r.State == RaftPartitionState.Active &&
                    r.RoutingMode == RaftRoutingMode.HashRange &&
                    (src.EndRange + 1 == r.StartRange || r.EndRange + 1 == src.StartRange))
                : map.Partitions.FirstOrDefault(r =>
                    r.State == RaftPartitionState.Active &&
                    r.RoutingMode == RaftRoutingMode.Unrouted &&
                    r.PartitionId != src.PartitionId);

            if (survivor is null)
            {
                logger.LogWarning(
                    "InitializePartitions: Draining partition {Id} has no adjacent Active survivor; merge cannot be resumed",
                    src.PartitionId);
                continue;
            }

            logger.LogInformation(
                "InitializePartitions: Re-enqueuing MergePartitionCommit for stuck Draining partition {Src} → survivor {Surv}",
                src.PartitionId, survivor.PartitionId);

            _pendingMerges[src.PartitionId] = new MergeInProgress(survivor.PartitionId, null);
            Send(new RaftSystemRequest(RaftSystemRequestType.MergePartitionCommit, src.PartitionId));
        }
    }

    private async Task TrySetInitialPartitions(CancellationToken cancellationToken)
    {
        List<RaftPartitionRange> initialRanges;

        if (systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            RaftPartitionMap? existingMap = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
            if (existingMap is not null)
            {
                StartPartitions(existingMap.Partitions);
                return;
            }
        }

        initialRanges = DivideIntoRanges(manager.Configuration.InitialPartitions);

        // MapVersion starts at 1; bumped on every subsequent mutation.
        RaftPartitionMap newMap = new() { MapVersion = 1, Partitions = initialRanges };

        RaftSystemMessage message = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(newMap)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TrySetInitialPartitions aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(message),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "Failed to replicate initial partitions {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.Success)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TrySetInitialPartitions delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                logger.LogError(
                    "Cannot continue without initial partitions {Status} {LogIndex}",
                    result.Status, result.LogIndex);
                Environment.Exit(1);
                return;
            }

            logger.LogInformation(
                "Successfully replicated initial partitions {Status} {LogIndex}",
                result.Status, result.LogIndex);
            break;
        }

        StartPartitions(initialRanges);
    }

    /// <summary>
    /// Phase 1 of the two-phase split: marks both source and new-target as
    /// <see cref="RaftPartitionState.Splitting"/>, replicates the map, then
    /// either auto-enqueues Phase 2 (when <see cref="RaftSplitPlan.AutoCommit"/>
    /// is true) or leaves the split pending for the caller to commit explicitly.
    /// </summary>
    private async Task TrySplitPartition(
        int partitionId,
        RaftSplitPlan? plan,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion,
        CancellationToken cancellationToken)
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TrySplitPartition: Failed to get partitions from system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TrySplitPartition: Failed to parse partition map {Partitions}", partitions);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        List<RaftPartitionRange> ranges = map.Partitions;

        RaftPartitionRange? partitionRange = ranges.FirstOrDefault(r => r.PartitionId == partitionId);
        if (partitionRange is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find partition range {Partition}", partitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Idempotency guard: if Phase 1 already committed for this source, reject the
        // duplicate rather than spawning a second overlapping split.
        if (partitionRange.State == RaftPartitionState.Splitting)
        {
            logger.LogWarning(
                "TrySplitPartition: Partition {Id} is already in Splitting state; rejecting duplicate request",
                partitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, partitionRange.Generation));
            return;
        }

        // Resolve target partition id: explicit plan value if non-zero, else auto-assign.
        RaftPartitionRange? maxPartition = ranges.MaxBy(r => r.PartitionId);
        if (maxPartition is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find next partition");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        int targetPartitionId = (plan?.TargetPartitionId > 0)
            ? plan.TargetPartitionId
            : maxPartition.PartitionId + 1;

        RaftRoutingMode targetRoutingMode = plan?.TargetRoutingMode ?? RaftRoutingMode.HashRange;

        // Capture the original end before shrinking.
        int originalEnd = partitionRange.EndRange;

        int splitBoundary;
        if (targetRoutingMode == RaftRoutingMode.HashRange)
        {
            splitBoundary = plan?.HashBoundary
                ?? (partitionRange.StartRange + (partitionRange.EndRange - partitionRange.StartRange) / 2);

            // Shrink the existing partition so it no longer owns [splitBoundary, originalEnd].
            partitionRange.EndRange = splitBoundary - 1;
        }
        else
        {
            // Unrouted split: source keeps its full range; new partition has no hash range.
            splitBoundary = 0;
        }

        // Phase 1: both partitions enter Splitting state.
        partitionRange.Generation++;
        partitionRange.State = RaftPartitionState.Splitting;

        RaftPartitionRange newRange = new()
        {
            PartitionId = targetPartitionId,
            StartRange  = targetRoutingMode == RaftRoutingMode.HashRange ? splitBoundary : 0,
            EndRange    = targetRoutingMode == RaftRoutingMode.HashRange ? originalEnd   : 0,
            Generation  = 1,
            State       = RaftPartitionState.Splitting,
            RoutingMode = targetRoutingMode,
        };

        ranges.Add(newRange);
        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TrySplitPartition aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(message),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TrySplitPartition: Phase 1 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TrySplitPartition delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInformation(
                "TrySplitPartition: Phase 1 committed for partition {Partition}", partitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        StartPartitions(ranges);

        _pendingSplits[partitionId] = new SplitInProgress(targetPartitionId, completion);

        if (plan?.AutoCommit == true)
            Send(new RaftSystemRequest(RaftSystemRequestType.SplitPartitionCommit, partitionId));
    }

    /// <summary>
    /// Phase 2 of the two-phase split: transitions both source and target from
    /// <see cref="RaftPartitionState.Splitting"/> to <see cref="RaftPartitionState.Active"/>.
    /// </summary>
    private async Task TrySplitPartitionCommit(int sourcePartitionId, CancellationToken cancellationToken)
    {
        if (!_pendingSplits.TryGetValue(sourcePartitionId, out SplitInProgress? split))
        {
            logger.LogError(
                "TrySplitPartitionCommit: No pending split for partition {Partition}", sourcePartitionId);
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TrySplitPartitionCommit: Failed to get partitions from system configuration");
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TrySplitPartitionCommit: Failed to parse partition map");
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? sourceRange = map.Partitions.FirstOrDefault(r => r.PartitionId == sourcePartitionId);
        if (sourceRange is null)
        {
            logger.LogError(
                "TrySplitPartitionCommit: Source partition {Id} not found", sourcePartitionId);
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? targetRange = map.Partitions.FirstOrDefault(r => r.PartitionId == split.TargetPartitionId);
        if (targetRange is null)
        {
            logger.LogError(
                "TrySplitPartitionCommit: Target partition {Id} not found", split.TargetPartitionId);
            split.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Phase 2: both partitions become Active.
        sourceRange.Generation++;
        sourceRange.State = RaftPartitionState.Active;
        targetRange.Generation++;
        targetRange.State = RaftPartitionState.Active;
        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TrySplitPartitionCommit aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(message),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TrySplitPartitionCommit: Phase 2 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TrySplitPartitionCommit delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                split.Completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInformation(
                "TrySplitPartitionCommit: Phase 2 committed for partition {Partition}", sourcePartitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        _pendingSplits.Remove(sourcePartitionId);
        StartPartitions(map.Partitions);
        split.Completion?.TrySetResult((RaftOperationStatus.Success, targetRange.Generation));
    }

    /// <summary>
    /// Phase 1 of the two-phase merge: marks the source partition as
    /// <see cref="RaftPartitionState.Draining"/> and replicates the updated map.
    /// Phase 2 is auto-enqueued via <see cref="RaftSystemRequestType.MergePartitionCommit"/>.
    /// </summary>
    private async Task TryMergePartitions(
        RaftMergePlan plan,
        TaskCompletionSource<(RaftOperationStatus Status, long Generation)>? completion,
        CancellationToken cancellationToken)
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryMergePartitions: No partition map in system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryMergePartitions: Failed to parse partition map");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? src = map.Partitions.FirstOrDefault(r => r.PartitionId == plan.SourcePartitionId);
        if (src is null)
        {
            logger.LogError("TryMergePartitions: Source partition {Id} not found", plan.SourcePartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? surv = map.Partitions.FirstOrDefault(r => r.PartitionId == plan.SurvivorPartitionId);
        if (surv is null)
        {
            logger.LogError("TryMergePartitions: Survivor partition {Id} not found", plan.SurvivorPartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        if (src.State == RaftPartitionState.Draining)
        {
            logger.LogWarning(
                "TryMergePartitions: Source partition {Id} is already Draining; rejecting duplicate merge request",
                plan.SourcePartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, src.Generation));
            return;
        }

        if (src.State != RaftPartitionState.Active)
        {
            logger.LogError(
                "TryMergePartitions: Source partition {Id} is not Active (State={State})",
                plan.SourcePartitionId, src.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, src.Generation));
            return;
        }

        if (surv.State != RaftPartitionState.Active)
        {
            logger.LogError(
                "TryMergePartitions: Survivor partition {Id} is not Active (State={State})",
                plan.SurvivorPartitionId, surv.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, surv.Generation));
            return;
        }

        // Adjacency validation.
        bool adjacent =
            (src.RoutingMode == RaftRoutingMode.Unrouted && surv.RoutingMode == RaftRoutingMode.Unrouted)
            || (src.RoutingMode == RaftRoutingMode.HashRange && surv.RoutingMode == RaftRoutingMode.HashRange
                && (src.EndRange + 1 == surv.StartRange || surv.EndRange + 1 == src.StartRange));

        if (!adjacent)
        {
            logger.LogError(
                "TryMergePartitions: Partitions {Src} and {Surv} are not adjacent",
                plan.SourcePartitionId, plan.SurvivorPartitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Phase 1: mark source as Draining.
        src.State = RaftPartitionState.Draining;
        src.Generation++;
        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryMergePartitions aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType, Serialize(message), true, cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryMergePartitions: Phase 1 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryMergePartitions delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8) continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInformation(
                "TryMergePartitions: Phase 1 committed — source {Src} is Draining",
                plan.SourcePartitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        StartPartitions(map.Partitions);

        _pendingMerges[plan.SourcePartitionId] = new MergeInProgress(plan.SurvivorPartitionId, completion);
        Send(new RaftSystemRequest(RaftSystemRequestType.MergePartitionCommit, plan.SourcePartitionId));
    }

    /// <summary>
    /// Phase 2 of the two-phase merge: absorbs the source range into the survivor,
    /// marks the source as <see cref="RaftPartitionState.Removed"/>, replicates,
    /// then stops the source partition and reclaims its WAL.
    /// </summary>
    private async Task TryMergePartitionCommit(int sourcePartitionId, CancellationToken cancellationToken)
    {
        if (!_pendingMerges.TryGetValue(sourcePartitionId, out MergeInProgress? merge))
        {
            logger.LogError(
                "TryMergePartitionCommit: No pending merge for source partition {Id}", sourcePartitionId);
            return;
        }

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryMergePartitionCommit: No partition map in system configuration");
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryMergePartitionCommit: Failed to parse partition map");
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? src = map.Partitions.FirstOrDefault(r => r.PartitionId == sourcePartitionId);
        if (src is null)
        {
            logger.LogError("TryMergePartitionCommit: Source partition {Id} not found", sourcePartitionId);
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? surv = map.Partitions.FirstOrDefault(r => r.PartitionId == merge.SurvivorPartitionId);
        if (surv is null)
        {
            logger.LogError(
                "TryMergePartitionCommit: Survivor partition {Id} not found", merge.SurvivorPartitionId);
            merge.Completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Guard against double-execution: if a previous Phase 2 already ran (e.g. crash
        // recovery re-enqueued MergePartitionCommit after Phase 2 had already committed),
        // the source will be Removed rather than Draining.  Re-running would corrupt
        // generations and re-expand the survivor range.
        if (src.State != RaftPartitionState.Draining)
        {
            logger.LogWarning(
                "TryMergePartitionCommit: Source partition {Id} is not Draining (State={State}); skipping duplicate Phase 2",
                sourcePartitionId, src.State);
            _pendingMerges.Remove(sourcePartitionId);
            merge.Completion?.TrySetResult((RaftOperationStatus.Success, surv.Generation));
            return;
        }

        // Phase 2: survivor absorbs source range; source is removed.
        if (surv.RoutingMode == RaftRoutingMode.HashRange)
        {
            surv.StartRange = Math.Min(surv.StartRange, src.StartRange);
            surv.EndRange   = Math.Max(surv.EndRange,   src.EndRange);
        }

        surv.Generation++;
        surv.State = RaftPartitionState.Active;

        src.State = RaftPartitionState.Removed;
        src.Generation++;

        map.MapVersion++;

        RaftSystemMessage message = new()
        {
            Key   = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryMergePartitionCommit aborted on shutdown");
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType, Serialize(message), true, cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryMergePartitionCommit: Phase 2 replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryMergePartitionCommit delay aborted on shutdown");
                        return;
                    }
                    if (i <= 8) continue;
                }

                merge.Completion?.TrySetResult((result.Status, 0));
                return;
            }

            logger.LogInformation(
                "TryMergePartitionCommit: Phase 2 committed — source {Src} removed, survivor {Surv} expanded",
                sourcePartitionId, merge.SurvivorPartitionId);
            break;
        }

        systemConfiguration[RaftSystemConfigKeys.Partitions] = message.Value;
        _pendingMerges.Remove(sourcePartitionId);

        // Stop and evict the source partition, then reclaim its WAL.
        if (manager.Partitions.TryGetValue(sourcePartitionId, out RaftPartition? livePartition))
        {
            await livePartition.DrainAsync(cancellationToken).ConfigureAwait(false);
            livePartition.Stop();
            manager.Partitions.TryRemove(sourcePartitionId, out _);
        }

        manager.WalAdapter.DeletePartitionWAL(sourcePartitionId);

        // Apply the updated map (source is Removed, so StartUserPartitions will skip it).
        StartPartitions(map.Partitions);

        merge.Completion?.TrySetResult((RaftOperationStatus.Success, surv.Generation));
    }

    private async Task TryCreatePartition(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryCreatePartition: No partition map in system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryCreatePartition: Failed to parse partition map");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Idempotency: if the partition already exists with Active state, return its current generation.
        RaftPartitionRange? existing = map.Partitions.FirstOrDefault(r => r.PartitionId == message.PartitionId);
        if (existing is not null)
        {
            if (existing.State == RaftPartitionState.Active)
            {
                logger.LogInformation(
                    "TryCreatePartition: Partition {Id} already active at generation {Gen}",
                    message.PartitionId, existing.Generation);
                completion?.TrySetResult((RaftOperationStatus.Success, existing.Generation));
                return;
            }

            logger.LogError(
                "TryCreatePartition: Partition {Id} exists with non-active state {State}",
                message.PartitionId, existing.State);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // For HashRange mode, bounds are required and must not overlap any existing HashRange entry.
        int newStart = 0, newEnd = 0;
        if (message.RoutingMode == RaftRoutingMode.HashRange)
        {
            if (message.HashRangeStart is null || message.HashRangeEnd is null)
            {
                logger.LogError("TryCreatePartition: HashRange mode requires start and end");
                completion?.TrySetResult((RaftOperationStatus.Errored, 0));
                return;
            }

            newStart = message.HashRangeStart.Value;
            newEnd = message.HashRangeEnd.Value;

            foreach (RaftPartitionRange range in map.Partitions)
            {
                if (range.RoutingMode != RaftRoutingMode.HashRange)
                    continue;
                if (newStart <= range.EndRange && newEnd >= range.StartRange)
                {
                    logger.LogError(
                        "TryCreatePartition: [{Start},{End}] overlaps partition {Id} [{RStart},{REnd}]",
                        newStart, newEnd, range.PartitionId, range.StartRange, range.EndRange);
                    completion?.TrySetResult((RaftOperationStatus.Errored, 0));
                    return;
                }
            }
        }

        map.Partitions.Add(new RaftPartitionRange
        {
            PartitionId = message.PartitionId,
            StartRange = newStart,
            EndRange = newEnd,
            Generation = 1,
            State = RaftPartitionState.Active,
            RoutingMode = message.RoutingMode
        });
        map.MapVersion++;

        RaftSystemMessage sysMessage = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryCreatePartition aborted on shutdown");
                completion?.TrySetCanceled(cancellationToken);
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(sysMessage),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryCreatePartition: Replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryCreatePartition delay aborted on shutdown");
                        completion?.TrySetCanceled(cancellationToken);
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            break;
        }

        // Keep local cache in sync so subsequent coordinator operations on this node see the new map.
        systemConfiguration[RaftSystemConfigKeys.Partitions] = sysMessage.Value;
        StartPartitions(map.Partitions);
        completion?.TrySetResult((RaftOperationStatus.Success, 1));
    }

    private async Task TryRemovePartition(RaftSystemRequest message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<(RaftOperationStatus, long)>? completion = message.Completion;
        int partitionId = message.PartitionId;

        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TryRemovePartition: No partition map in system configuration");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionMap? map = JsonSerializer.Deserialize<RaftPartitionMap>(partitions);
        if (map is null)
        {
            logger.LogError("TryRemovePartition: Failed to parse partition map");
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        RaftPartitionRange? entry = map.Partitions.FirstOrDefault(r => r.PartitionId == partitionId);
        if (entry is null)
        {
            logger.LogError("TryRemovePartition: Partition {Id} not found in partition map", partitionId);
            completion?.TrySetResult((RaftOperationStatus.Errored, 0));
            return;
        }

        // Idempotency: already removed by a prior call.
        if (entry.State == RaftPartitionState.Removed)
        {
            logger.LogInformation("TryRemovePartition: Partition {Id} already removed", partitionId);
            completion?.TrySetResult((RaftOperationStatus.Success, entry.Generation));
            // Re-attempt WAL reclamation in case a prior crash prevented it.
            manager.WalAdapter.DeletePartitionWAL(partitionId);
            return;
        }

        // Mark Removed, bump generation and map version, then replicate.
        entry.State = RaftPartitionState.Removed;
        entry.Generation++;
        map.MapVersion++;

        RaftSystemMessage sysMessage = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(map)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("[RaftSystemCoordinator] TryRemovePartition aborted on shutdown");
                completion?.TrySetCanceled(cancellationToken);
                return;
            }

            RaftReplicationResult result = await Replicate(
                RaftSystemConfig.RaftLogType,
                Serialize(sysMessage),
                true,
                cancellationToken
            ).ConfigureAwait(false);

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning(
                    "TryRemovePartition: Replication failed {Status} {LogIndex} Retry={Retry}",
                    result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    try { await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false); }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("[RaftSystemCoordinator] TryRemovePartition delay aborted on shutdown");
                        completion?.TrySetCanceled(cancellationToken);
                        return;
                    }
                    if (i <= 8)
                        continue;
                }

                completion?.TrySetResult((result.Status, 0));
                return;
            }

            break;
        }

        // Keep local cache in sync so subsequent coordinator operations on this node see the new map.
        systemConfiguration[RaftSystemConfigKeys.Partitions] = sysMessage.Value;

        // Tombstone is now committed. Drain and stop the local partition instance,
        // evict it from the manager dictionary, then reclaim its WAL storage.
        if (manager.Partitions.TryGetValue(partitionId, out RaftPartition? livePartition))
        {
            await livePartition.DrainAsync(cancellationToken).ConfigureAwait(false);
            livePartition.Stop();
            manager.Partitions.TryRemove(partitionId, out _);
        }

        manager.WalAdapter.DeletePartitionWAL(partitionId);

        logger.LogInformation(
            "TryRemovePartition: Partition {Id} removed and WAL reclaimed", partitionId);

        completion?.TrySetResult((RaftOperationStatus.Success, entry.Generation));
    }

    // ── Static helpers ─────────────────────────────────────────────────────

    private static List<RaftPartitionRange> DivideIntoRanges(int numberOfRanges)
    {
        int monotonicId = RaftSystemConfig.SystemPartition + 1;

        List<RaftPartitionRange> ranges = new(numberOfRanges);

        const long totalCount = (long)int.MaxValue + 1;
        long baseRangeSize = totalCount / numberOfRanges;
        long remainder = totalCount % numberOfRanges;

        long currentStart = 0;

        for (int i = 0; i < numberOfRanges; i++)
        {
            long currentRangeSize = baseRangeSize + (i < remainder ? 1 : 0);
            long currentEnd = currentStart + currentRangeSize - 1;

            ranges.Add(new()
            {
                PartitionId = monotonicId++,
                StartRange = (int)currentStart,
                EndRange = (int)currentEnd,
                Generation = 1,
                State = RaftPartitionState.Active,
                RoutingMode = RaftRoutingMode.HashRange
            });

            currentStart = currentEnd + 1;
        }

        return ranges;
    }

    private static byte[] Serialize(RaftSystemMessage message)
    {
        using MemoryStream memoryStream = new();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    private static RaftSystemMessage Unserialize(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return RaftSystemMessage.Parser.ParseFrom(memoryStream);
    }

    private int _stopped;

    // ── Lifecycle ──────────────────────────────────────────────────────────

    /// <summary>
    /// Signals the coordinator to stop: cancels in-flight async work immediately,
    /// then completes the channel so the background loop drains and exits.
    /// Idempotent — safe to call multiple times.
    /// </summary>
    internal void Stop()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
            return;

        // Cancel first so any in-flight ReplicateSystemLogs / Task.Delay returns promptly.
        _cts.Cancel();
        _channel.Writer.TryComplete();
    }

    public void Dispose()
    {
        Stop(); // cancels _cts and completes channel; loop exits promptly
        try { _loop.Wait(TimeSpan.FromSeconds(5)); } catch { /* ignore shutdown races */ }
        _cts.Dispose();
    }
}
