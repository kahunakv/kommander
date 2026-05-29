
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
                await TrySplitPartition(message.PartitionId, cancellationToken).ConfigureAwait(false);
                break;

            case RaftSystemRequestType.RestoreCompleted:
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

        List<RaftPartitionRange>? initialRanges = JsonSerializer.Deserialize<List<RaftPartitionRange>>(partitions);
        if (initialRanges is null)
        {
            logger.LogError("InitializePartitions: Failed to parse partition ranges: {Partitions}", partitions);
            return;
        }

        StartPartitions(initialRanges);
    }

    private async Task TrySetInitialPartitions(CancellationToken cancellationToken)
    {
        List<RaftPartitionRange>? initialRanges;

        if (systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            initialRanges = JsonSerializer.Deserialize<List<RaftPartitionRange>>(partitions);
            if (initialRanges is not null)
            {
                StartPartitions(initialRanges);
                return;
            }
        }

        initialRanges = DivideIntoRanges(manager.Configuration.InitialPartitions);

        RaftSystemMessage message = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(initialRanges)
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

    private async Task TrySplitPartition(int partitionId, CancellationToken cancellationToken)
    {
        if (!systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            logger.LogError("TrySplitPartition: Failed to get partitions from system configuration");
            return;
        }

        List<RaftPartitionRange>? initialRanges = JsonSerializer.Deserialize<List<RaftPartitionRange>>(partitions);
        if (initialRanges is null)
        {
            logger.LogError("TrySplitPartition: Failed to parse partition ranges {Partitions}", partitions);
            return;
        }

        RaftPartitionRange? partitionRange = initialRanges.FirstOrDefault(r => r.PartitionId == partitionId);
        if (partitionRange is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find partition range {Partition}", partitionId);
            return;
        }

        RaftPartitionRange? nextPartition = initialRanges.MaxBy(r => r.PartitionId);
        if (nextPartition is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find next partition");
            return;
        }

        int midPoint = partitionRange.StartRange + (partitionRange.EndRange - partitionRange.StartRange) / 2;

        // Capture the original end before shrinking.
        int originalEnd = partitionRange.EndRange;

        // Shrink the existing partition so it no longer owns [midPoint+1, originalEnd].
        // This must happen before serialization so the replicated map is non-overlapping.
        partitionRange.EndRange = midPoint;

        RaftPartitionRange newRange = new()
        {
            PartitionId = nextPartition.PartitionId + 1,
            StartRange = midPoint + 1,
            EndRange = originalEnd
        };

        initialRanges.Add(newRange);

        RaftSystemMessage message = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(initialRanges)
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
                    "Failed to replicate partitions {Status} {LogIndex} Retry={Retry}",
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

                return;
            }

            logger.LogInformation(
                "Successfully replicated new partitions {Status} {LogIndex}",
                result.Status, result.LogIndex);
            break;
        }

        StartPartitions(initialRanges);
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
                EndRange = (int)currentEnd
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
