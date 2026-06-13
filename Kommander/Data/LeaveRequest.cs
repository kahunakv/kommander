
namespace Kommander.Data;

/// <summary>
/// Sent by a departing node to the P0 leader to request removal from the committed roster.
/// </summary>
public sealed record LeaveRequest(string Endpoint, int NodeId);
