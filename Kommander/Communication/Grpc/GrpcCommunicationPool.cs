
using Kommander.Data;

namespace Kommander.Communication.Grpc;

public static class GrpcCommunicationPool
{
    [ThreadStatic] 
    private static Stack<GrpcAppendLogsRequest>? _appendLogsPool;
    
    [ThreadStatic] 
    private static Stack<GrpcCompleteAppendLogsRequest>? _completeAppendLogsPool;
    
    [ThreadStatic]
    private static Stack<List<BatchRequestsRequestItem>>? _batchRequestsItemPool;
    
    public static GrpcAppendLogsRequest RentAppendLogsRequest()
    {
        _appendLogsPool ??= new();
        if (_appendLogsPool.Count > 0)
        {
            GrpcAppendLogsRequest rented = _appendLogsPool.Pop();            
            return rented;
        }
        
        return new();
    }
    
    public static void Return(GrpcAppendLogsRequest obj)
    {
        obj.Logs.Clear();
        _appendLogsPool ??= new();
        _appendLogsPool.Push(obj);
    }
    
    public static GrpcCompleteAppendLogsRequest RentCompleteAppendLogsRequest()
    {
        _completeAppendLogsPool ??= new();
        if (_completeAppendLogsPool.Count > 0)
        {
            GrpcCompleteAppendLogsRequest rented = _completeAppendLogsPool.Pop();            
            return rented;
        }
        
        return new();
    }
    
    public static void Return(GrpcCompleteAppendLogsRequest obj)
    {        
        _completeAppendLogsPool ??= new();
        _completeAppendLogsPool.Push(obj);
    }
    
    public static List<BatchRequestsRequestItem> RentListBatchRequestsRequestItem(int capacity)
    {
        _batchRequestsItemPool ??= new();
        
        if (_batchRequestsItemPool.Count > 0)
        {
            List<BatchRequestsRequestItem> rented = _batchRequestsItemPool.Pop();            
            return rented;
        }
        
        return new(capacity);
    }
    
    public static void Return(List<BatchRequestsRequestItem> obj)
    {
        obj.Clear();
        _batchRequestsItemPool ??= new();
        _batchRequestsItemPool.Push(obj);
    }
}