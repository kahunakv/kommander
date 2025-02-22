﻿
//using MessagePack;

namespace Kommander.Data;

//[MessagePackObject]
public sealed class RaftLog
{
    //[Key(0)]
    public long Id { get; set; }

    //[Key(1)]
    public RaftLogType Type { get; set; } = RaftLogType.Regular;
    
    public long Term { get; set; }

    //[Key(2)]
    public long Time { get; set; }

    //[Key(3)]
    public string? Message { get; set; }
}
