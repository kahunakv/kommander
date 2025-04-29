
using Kommander.Data;

namespace Kommander;

public static class RaftBatcherPool
{
    [ThreadStatic] 
    private static Stack<List<(int, List<RaftLog>)>>? _raftLogsPlan;
    
    public static List<(int, List<RaftLog>)> RentRaftLogsPlan(int capacity)
    {
        _raftLogsPlan ??= new();
        if (_raftLogsPlan.Count > 0)
        {
            List<(int, List<RaftLog>)> rented = _raftLogsPlan.Pop();            
            return rented;
        }
        
        return new(capacity);
    }

    public static void Return(List<(int, List<RaftLog>)> obj)
    {
        obj.Clear();
        _raftLogsPlan ??= new();
        _raftLogsPlan.Push(obj);
    }
}
