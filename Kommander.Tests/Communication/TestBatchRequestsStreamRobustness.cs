using System.Collections.Concurrent;
using Grpc.Core;
using Kommander.Communication.Grpc;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kommander.Tests.Communication;

/// <summary>
/// Pins the server-side robustness of <see cref="RaftService.BatchRequests"/> against a typed
/// batch item that carries no payload.
///
/// <para>
/// That wire shape was shipped for years by a sender-side mapping gap (the
/// TransferLeadershipSuggestion branch was missing from the gRPC item mapper), and the old
/// null-forgiving dereference turned each occurrence into a bare NullReferenceException with a
/// single stack frame and no peer identity — the Caraxes 30-second
/// "BatchRequests: NullReferenceException" finding. The handler must instead drop the item with
/// a log line that names the type and the peer, keep the stream alive, and keep processing the
/// remaining items.
/// </para>
/// </summary>
public sealed class TestBatchRequestsStreamRobustness
{
    /// <summary>
    /// The regression shape: one item per payload-bearing type, each with a null payload, plus a
    /// trailing ping. The handler must complete the stream, write its per-batch response, log a
    /// missing-payload line per dropped item, and log no NullReferenceException anywhere.
    /// </summary>
    [Fact]
    public async Task TypedItemWithoutPayload_IsDroppedWithoutNullReferenceException()
    {
        using Kommander.WAL.InMemoryWAL wal = new(Microsoft.Extensions.Logging.Abstractions.NullLogger<IRaft>.Instance);
        Kommander.Discovery.StaticDiscovery discovery = new([]);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9985, InitialPartitions = 1 };
        RaftManager manager = new(cfg, discovery, wal, comm, new HybridLogicalClock(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<IRaft>.Instance);

        CapturingLogger logger = new();
        RaftService service = new(manager, logger);

        GrpcBatchRequestsRequest batch = new();
        foreach (GrpcBatchRequestsRequestType type in Enum.GetValues<GrpcBatchRequestsRequestType>())
        {
            if (type == GrpcBatchRequestsRequestType.Ping)
                continue;

            // Type declared, payload deliberately absent — the defective wire shape.
            batch.Requests.Add(new GrpcBatchRequestsRequestItem { Type = type });
        }
        batch.Requests.Add(new GrpcBatchRequestsRequestItem { Type = GrpcBatchRequestsRequestType.Ping });

        FakeStreamReader reader = new([batch]);
        FakeStreamWriter writer = new();

        try
        {
            await service.BatchRequests(reader, writer, new FakeServerCallContext());

            // The stream survived and produced its one response frame for the batch.
            Assert.Single(writer.Responses);

            // No bare NullReferenceException anywhere — the undiagnosable failure this replaces.
            Assert.DoesNotContain(logger.Messages, m => m.Contains("NullReferenceException"));

            // Every dropped item is attributable: type and peer are named.
            Assert.Contains(logger.Messages,
                m => m.Contains("carried no payload") && m.Contains("TransferLeadershipSuggestion"));
            Assert.Contains(logger.Messages,
                m => m.Contains("carried no payload") && m.Contains("test-peer:1234"));
        }
        finally
        {
            manager.Dispose();
        }
    }

    /// <summary>
    /// A well-formed suggestion item must still be delivered after the hardening: the null guard
    /// must not swallow real payloads. Delivery is observed indirectly — no missing-payload line
    /// and no exception line for the item.
    /// </summary>
    [Fact]
    public async Task SuggestionWithPayload_IsProcessedWithoutErrors()
    {
        using Kommander.WAL.InMemoryWAL wal = new(Microsoft.Extensions.Logging.Abstractions.NullLogger<IRaft>.Instance);
        Kommander.Discovery.StaticDiscovery discovery = new([]);
        Kommander.Communication.Memory.InMemoryCommunication comm = new();
        RaftConfiguration cfg = new() { NodeId = 1, Host = "localhost", Port = 9984, InitialPartitions = 1 };
        RaftManager manager = new(cfg, discovery, wal, comm, new HybridLogicalClock(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<IRaft>.Instance);

        CapturingLogger logger = new();
        RaftService service = new(manager, logger);

        GrpcBatchRequestsRequest batch = new();
        batch.Requests.Add(new GrpcBatchRequestsRequestItem
        {
            Type = GrpcBatchRequestsRequestType.TransferLeadershipSuggestion,
            TransferLeadershipSuggestion = new GrpcTransferLeadershipSuggestionRequest
            {
                Partition = 0,
                Term = 1,
                TimeNode = 1,
                TimePhysical = 100,
                TimeCounter = 0,
                SuggestedBy = "p0-leader:9000",
                TargetEndpoint = "node-b:9002",
            },
        });

        FakeStreamReader reader = new([batch]);
        FakeStreamWriter writer = new();

        try
        {
            await service.BatchRequests(reader, writer, new FakeServerCallContext());

            Assert.Single(writer.Responses);
            Assert.DoesNotContain(logger.Messages, m => m.Contains("carried no payload"));
            Assert.DoesNotContain(logger.Messages, m => m.Contains("NullReferenceException"));
        }
        finally
        {
            manager.Dispose();
        }
    }

    // ── fakes ────────────────────────────────────────────────────────────────

    private sealed class FakeStreamReader : IAsyncStreamReader<GrpcBatchRequestsRequest>
    {
        private readonly Queue<GrpcBatchRequestsRequest> queue;

        public FakeStreamReader(IEnumerable<GrpcBatchRequestsRequest> messages) => queue = new(messages);

        public GrpcBatchRequestsRequest Current { get; private set; } = null!;

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            if (queue.Count == 0)
                return Task.FromResult(false);

            Current = queue.Dequeue();
            return Task.FromResult(true);
        }
    }

    private sealed class FakeStreamWriter : IServerStreamWriter<GrpcBatchRequestsResponse>
    {
        public List<GrpcBatchRequestsResponse> Responses { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(GrpcBatchRequestsResponse message)
        {
            Responses.Add(message);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Minimal ServerCallContext: node authentication is disabled by default, so the handler only
    /// reads <see cref="ServerCallContext.Peer"/> (for log attribution) from this fake.
    /// </summary>
    private sealed class FakeServerCallContext : ServerCallContext
    {
        protected override string MethodCore => "/Rafter/BatchRequests";
        protected override string HostCore => "localhost";
        protected override string PeerCore => "test-peer:1234";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore { get; } = [];
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore { get; } = [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore { get; } = new(null, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }

    private sealed class CapturingLogger : ILogger<IRaft>
    {
        public ConcurrentBag<string> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception));
    }
}
