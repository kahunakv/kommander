
namespace Kommander.Data;

/// <summary>
/// Compact outbound-message envelope carried through the per-endpoint transport channel.
///
/// <para>A <c>readonly struct</c> holding just three fields — the message <see cref="Type"/>, the target
/// <see cref="Node"/>, and a single <c>object</c> payload reference — rather than the former sealed class
/// with a nullable field per payload kind (eight of them) plus the node, of which only one payload was ever
/// live. Because it is a value type it is stored inline in the dispatcher's <c>Channel</c> and drain
/// <c>List</c>, so no envelope object is allocated per outbound message; only the concrete payload DTO (which
/// must exist regardless) is on the heap.</para>
///
/// <para>The public shape is unchanged: the same per-payload constructors and the same per-payload nullable
/// accessors, so every producer (<c>new RaftResponderRequest(type, node, request)</c>) and consumer
/// (<c>msg.VoteRequest</c>, …) compiles and behaves as before. Each accessor is <c>_payload as T</c>, which
/// yields the payload only when its runtime type matches — equivalent to the old "only the live field is
/// non-null" contract, since each <see cref="RaftResponderRequestType"/> has a distinct payload class.</para>
/// </summary>
public readonly struct RaftResponderRequest
{
    private readonly object? _payload;

    public RaftResponderRequestType Type { get; }

    public RaftNode? Node { get; }

    public HandshakeRequest? HandshakeRequest => _payload as HandshakeRequest;

    public VoteRequest? VoteRequest => _payload as VoteRequest;

    public RequestVotesRequest? RequestVotesRequest => _payload as RequestVotesRequest;

    public StepDownNoticeRequest? StepDownNoticeRequest => _payload as StepDownNoticeRequest;

    public TransferLeadershipRequest? TransferLeadershipRequest => _payload as TransferLeadershipRequest;

    public TransferLeadershipSuggestionRequest? TransferLeadershipSuggestionRequest =>
        _payload as TransferLeadershipSuggestionRequest;

    public AppendLogsRequest? AppendLogsRequest => _payload as AppendLogsRequest;

    public CompleteAppendLogsRequest? CompleteAppendLogsRequest => _payload as CompleteAppendLogsRequest;

    private RaftResponderRequest(RaftResponderRequestType type, RaftNode node, object payload)
    {
        Type = type;
        Node = node;
        _payload = payload;
    }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, HandshakeRequest request)
        : this(type, node, (object)request) { }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, VoteRequest request)
        : this(type, node, (object)request) { }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, RequestVotesRequest request)
        : this(type, node, (object)request) { }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, StepDownNoticeRequest request)
        : this(type, node, (object)request) { }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, TransferLeadershipRequest request)
        : this(type, node, (object)request) { }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, AppendLogsRequest request)
        : this(type, node, (object)request) { }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, CompleteAppendLogsRequest request)
        : this(type, node, (object)request) { }

    public RaftResponderRequest(RaftResponderRequestType type, RaftNode node, TransferLeadershipSuggestionRequest request)
        : this(type, node, (object)request) { }
}
