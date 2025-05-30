
using Nixie;
using System.Text.Json;
using Kommander.System.Protos;
using Google.Protobuf;
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kommander.System;

/// <summary>
/// The RaftSystemActor is a background actor responsible for handling a system partition
/// It processes requests related to Raft events, such as configuration replication, leader changes, member changes,
/// partition splitting and restoration operations.
/// </summary>
/// <typeparam name="RaftSystemRequest">
/// The type of message that this actor handles.
/// </typeparam>
/// <seealso cref="IActor{RaftSystemRequest}"/>
public class RaftSystemActor : IActor<RaftSystemRequest>
{
    private const int MaxRetries = 10;
    
    private readonly Dictionary<string, string> systemConfiguration = new();
    
    private readonly RaftManager manager;

    private readonly ILogger<IRaft> logger;

    private string? leaderNode;

    public RaftSystemActor(
        IActorContext<RaftSystemActor, RaftSystemRequest> _,
        RaftManager manager,
        ILogger<IRaft> logger
    )
    {
        this.manager = manager;
        this.logger = logger;
    }

    /// <summary>
    /// Handles various request types pertaining to the Raft system, such as leader changes,
    /// configuration restoration or replication, and partition handling.
    /// </summary>
    /// <param name="message">The request message of type <see cref="RaftSystemRequest"/> containing details
    /// about the operation to perform, including its type, partition information, and leader node.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    /// <exception cref="NotImplementedException">Thrown if the provided message type is not implemented.</exception>
    public async Task Receive(RaftSystemRequest message)
    {
        switch (message.Type)
        {
            case RaftSystemRequestType.ConfigRestored:
            {
                if (message.LogData is null)
                {
                    logger.LogWarning("Restored message is null");
                    return;
                }

                RaftSystemMessage systemMessage = Unserializer(message.LogData);

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

                RaftSystemMessage systemMessage = Unserializer(message.LogData);

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
                    await TrySetInitialPartitions().ConfigureAwait(false);
                else
                    InitializePartitions();
                
                break;
            
            case RaftSystemRequestType.SplitPartition:
                await TrySplitPartition(message.PartitionId).ConfigureAwait(false);
                break;
            
            case RaftSystemRequestType.RestoreCompleted:
                break;
            
            default:
                throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Initializes the user partitions in the system by retrieving and deserializing
    /// partition configuration from the system configuration. If the configuration
    /// is invalid or deserialization fails, logs the corresponding errors.
    /// </summary>
    /// <exception cref="JsonException">Thrown when the partition configuration cannot be parsed.</exception>
    /// <exception cref="KeyNotFoundException">Thrown when the partition key is missing in the system configuration.</exception>
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
                
        // foreach (RaftPartitionRange range in initialRanges)
        //     Console.Error.WriteLine("{0} {1} {2}", range.PartitionId, range.StartRange, range.EndRange);

        manager.StartUserPartitions(initialRanges);
    }

    /// <summary>
    /// Attempts to set the initial partitions for the Raft system by dividing the configured number of partitions
    /// into ranges and replicating the system logs across the cluster.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    /// <exception cref="JsonException">Thrown if the serialization of the partitions fails.</exception>
    /// <exception cref="InvalidOperationException">Thrown if the replication of system logs does not succeed after max retry attempts.</exception>
    private async Task TrySetInitialPartitions()
    {
        List<RaftPartitionRange>? initialRanges;
        
        if (systemConfiguration.TryGetValue(RaftSystemConfigKeys.Partitions, out string? partitions))
        {
            initialRanges = JsonSerializer.Deserialize<List<RaftPartitionRange>>(partitions);

            if (initialRanges is not null)
            {
                manager.StartUserPartitions(initialRanges);
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
            RaftReplicationResult result = await manager.ReplicateSystemLogs(
                RaftSystemConfig.RaftLogType,
                Serialize(message),
                true,
                CancellationToken.None
            );

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning("Failed to replicate initial partitions {Status} {LogIndex} Retry={Retry}", result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.Success)
                {
                    await Task.Delay(5000);
                    if (i <= 8)
                        continue;
                }

                logger.LogError("Cannot continue without initial partitions {Status} {LogIndex}", result.Status, result.LogIndex);
                Environment.Exit(1);
                return;
            }

            logger.LogInformation("Succesfully replicated initial partitions {Status} {LogIndex}", result.Status, result.LogIndex);
            break;
        }

        manager.StartUserPartitions(initialRanges);
        
        // foreach (RaftPartitionRange range in initialRanges)
        //     Console.Error.WriteLine("{0} {1} {2}", range.PartitionId, range.StartRange, range.EndRange);
    }

    /// <summary>
    /// Attempts to split an existing partition into two new partitions by recalculating range boundaries.
    /// Updates the system configuration with the modified partition details.
    /// </summary>
    /// <param name="partitionId">The identifier of the partition to be split.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">Thrown if partition ranges cannot be retrieved or updated due to invalid configuration.</exception>
    private async Task TrySplitPartition(int partitionId)
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
        
        RaftPartitionRange? partitionRange = initialRanges.FirstOrDefault(range => range.PartitionId == partitionId);
        if (partitionRange is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find partition range {Partition}", partitionId);
            return;
        }
        
        RaftPartitionRange? nextPartition = initialRanges.MaxBy(range => range.PartitionId);
        if (nextPartition is null)
        {
            logger.LogError("TrySplitPartition: Couldn't find next partition");
            return;
        }
        
        int midPoint = partitionRange.StartRange + (partitionRange.EndRange - partitionRange.StartRange) / 2;
        
        RaftPartitionRange newRange = new()
        {
            PartitionId = nextPartition.PartitionId + 1,
            StartRange = midPoint + 1,
            EndRange = partitionRange.EndRange
        };
        
        initialRanges.Add(newRange);
        
        RaftSystemMessage message = new()
        {
            Key = RaftSystemConfigKeys.Partitions,
            Value = JsonSerializer.Serialize(initialRanges)
        };

        for (int i = 0; i < MaxRetries; i++)
        {
            RaftReplicationResult result = await manager.ReplicateSystemLogs(
                RaftSystemConfig.RaftLogType,
                Serialize(message),
                true,
                CancellationToken.None
            );

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogWarning("Failed to replicate partitions {Status} {LogIndex} Retry={Retry}", result.Status, result.LogIndex, i);

                if (result.Status != RaftOperationStatus.NodeIsNotLeader)
                {
                    await Task.Delay(5000);
                    if (i <= 8)
                        continue;
                }
                
                return;
            }

            logger.LogInformation("Succesfully replicated new partitions {Status} {LogIndex}", result.Status, result.LogIndex);
            break;
        }

        manager.StartUserPartitions(initialRanges);
    }

    /// <summary>
    /// Divides the range of integers from 0 to <see cref="int.MaxValue"/> into a specified number of
    /// partitions, ensuring each partition has a nearly equal range size with any remainder distributed
    /// among the initial partitions.
    /// </summary>
    /// <param name="numberOfRanges">The number of partitions to divide the range into. Must be greater than 0.</param>
    /// <returns>A list of <see cref="RaftPartitionRange"/> objects representing the partitioned ranges,
    /// each containing its corresponding partition ID, start range, and end range values.</returns>
    private static List<RaftPartitionRange> DivideIntoRanges(int numberOfRanges)
    {
        int monotonicId = RaftSystemConfig.SystemPartition + 1;
        
        List<RaftPartitionRange> ranges = new(numberOfRanges);
        
        // Total number of values from 0 to int.MaxValue inclusive is int.MaxValue + 1
        const long totalCount = (long)int.MaxValue + 1;
        long baseRangeSize = totalCount / numberOfRanges;
        long remainder = totalCount % numberOfRanges;
        
        long currentStart = 0;
        
        for (int i = 0; i < numberOfRanges; i++)
        {
            // Distribute any extra numbers among the first 'remainder' ranges.
            long currentRangeSize = baseRangeSize + (i < remainder ? 1 : 0);
            
            // Calculate the inclusive end value for this range.
            long currentEnd = currentStart + currentRangeSize - 1;
            
            // Casting back to int is safe because currentEnd will not exceed int.MaxValue.
            ranges.Add(new() { PartitionId = monotonicId++, StartRange = (int)currentStart, EndRange = (int)currentEnd });
            
            // Next range starts immediately after the current range's end.
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
    
    private static RaftSystemMessage Unserializer(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return RaftSystemMessage.Parser.ParseFrom(memoryStream);
    }
}
