
using Nixie;
using System.Text.Json;
using Kommander.System.Protos;
using Google.Protobuf;
using Kommander.Data;

namespace Kommander.System;

public class RaftSystemActor : IActor<RaftSystemRequest>
{
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

    public async Task Receive(RaftSystemRequest message)
    {
        await Task.CompletedTask;
        
        switch (message.Type)
        {
            case RaftSystemRequestType.ConfigRestored:
            {
                if (message.LogData is null)
                    return;

                RaftSystemMessage systemMessage = Unserializer(message.LogData);

                systemConfiguration[systemMessage.Key] = systemMessage.Value;
            }
            break;

            case RaftSystemRequestType.ConfigReplicated:
            {
                if (message.LogData is null)
                    return;

                RaftSystemMessage systemMessage = Unserializer(message.LogData);

                systemConfiguration[systemMessage.Key] = systemMessage.Value;

                InitializePartitions();
            }
            break;

            case RaftSystemRequestType.LeaderChanged:
                leaderNode = message.LeaderNode;
                
                if (manager.LocalEndpoint == leaderNode)
                    await TrySetInitialPartitions();
                else
                    InitializePartitions();
                
                break;
            
            case RaftSystemRequestType.RestoreCompleted:
                break;
        }
    }

    private void InitializePartitions()
    {
        if (!systemConfiguration.TryGetValue("partitions", out string? partitions))
        {
            logger.LogWarning("Failed to get partitions from system configuration");
            return;
        }
        
        List<RaftPartitionRange>? initialRanges = JsonSerializer.Deserialize<List<RaftPartitionRange>>(partitions);
        if (initialRanges is null)
        {
            logger.LogError("Failed to parse partition ranges: {Partitions}", partitions);
            return;
        }
                
        // foreach (RaftPartitionRange range in initialRanges)
        //     Console.Error.WriteLine("{0} {1} {2}", range.PartitionId, range.StartRange, range.EndRange);

        manager.StartUserPartitions(initialRanges);
    }

    private async Task TrySetInitialPartitions()
    {
        if (systemConfiguration.TryGetValue("partitions", out string? partitions))
        {
            JsonSerializer.Deserialize<List<RaftPartitionRange>>(partitions);
            return;
        }
        
        List<RaftPartitionRange> initialRanges = DivideIntoRanges(manager.Configuration.InitialPartitions);

        RaftSystemMessage message = new()
        {
            Key = "partitions",
            Value = JsonSerializer.Serialize(initialRanges)
        };

        for (int i = 0; i < 10; i++)
        {
            RaftReplicationResult result = await manager.ReplicateSystemLogs(
                RaftSystemConfig.RaftLogType,
                Serialize(message),
                true,
                CancellationToken.None
            );

            if (result.Status != RaftOperationStatus.Success)
            {
                logger.LogDebug("Failed to replicate initial partitions {Status} {LogIndex}", result.Status, result.LogIndex);
                
                await Task.Delay(5000);
                continue;
            }

            logger.LogInformation("Succesfully replicated initial partitions {Status} {LogIndex}", result.Status, result.LogIndex);
            break;
        }

        manager.StartUserPartitions(initialRanges);
        
        // foreach (RaftPartitionRange range in initialRanges)
        //     Console.Error.WriteLine("{0} {1} {2}", range.PartitionId, range.StartRange, range.EndRange);
    }

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
