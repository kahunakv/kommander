
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

                if (systemMessage.Key == "partitions")
                {
                    List<RaftPartitionRange>? partList = JsonSerializer.Deserialize<List<RaftPartitionRange>>(systemMessage.Value);
                    
                    foreach (RaftPartitionRange range in partList!)
                    {
                        Console.WriteLine("{0} {1} {2}", range.PartitionId, range.StartRange, range.EndRange);
                    }
                }
            }
            break;

            case RaftSystemRequestType.LeaderChanged:
                leaderNode = message.LeaderNode;
                
                if (manager.LocalEndpoint == leaderNode)
                    await TrySetInitialPartitions();
                
                break;
            
            case RaftSystemRequestType.RestoreCompleted:
                break;
        }
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

        RaftReplicationResult result = await manager.ReplicateSystemLogs(
            RaftSystemConfig.RaftLogType, 
            Serialize(message),
            true, 
            CancellationToken.None
        );

        if (result.Status != RaftOperationStatus.Success)
        {
            Console.WriteLine("Failed to replicate initial partitions {0} {1}", result.Status, result.LogIndex);
            return;
        }
        
        Console.WriteLine("Succesfully replicated initial partitions {0} {1}", result.Status, result.LogIndex);
        
        manager.StartUserPartitions(initialRanges);
    }

    private static List<RaftPartitionRange> DivideIntoRanges(int numberOfRanges)
    {
        int monotonicId = RaftSystemConfig.SystemPartition + 1;
        
        List<RaftPartitionRange> ranges = [];
        
        // total values is int.MaxValue (2147483647)
        const int total = int.MaxValue;
        
        // Use long for intermediate arithmetic to avoid overflow
        long baseRangeSize = (long)total / numberOfRanges;
        long remainder = (long)total % numberOfRanges;
        
        // previousEnd will mark the boundary of the previous range.
        long previousEnd = 0;
        
        for (int i = 0; i < numberOfRanges; i++)
        {
            // Distribute the remainder evenly among the first 'remainder' ranges.
            long currentRangeSize = baseRangeSize + (i < remainder ? 1 : 0);
            
            // Define the range as (previousEnd, previousEnd + currentRangeSize)
            // meaning that valid numbers in the range are > previousEnd and < previousEnd + currentRangeSize.
            long start = previousEnd;
            long end = start + currentRangeSize;
            
            // Casting back to int is safe because end will never exceed int.MaxValue.
            ranges.Add(new() { PartitionId = monotonicId++, StartRange = (int)start, EndRange = (int)end });
            
            previousEnd = end;
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

public class RaftPartitionRange
{
    public int PartitionId { get; set; }
    
    public int StartRange { get; set; }
    
    public int EndRange { get; set; }
}