
//using MessagePack;

namespace Kommander.Data;

//[MessagePackObject]
public sealed class RaftLog
{
    //[Key(0)]
    public ulong Id { get; set; }

    //[Key(1)]
    public RaftLogType Type { get; set; } = RaftLogType.Regular;

    //[Key(2)]
    public long Time { get; set; }

    //[Key(3)]
    public string? Message { get; set; }
}
