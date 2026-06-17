using System.Text.Json;
using Google.Protobuf;
using Kommander.System;

namespace Kommander.Communication.Grpc;

/// <summary>
/// Caches UTF-8 roster JSON for gRPC gossip sends on one <see cref="GrpcCommunication"/>
/// instance. The roster changes only when <see cref="ClusterMembership.MembershipVersion"/>
/// advances, so steady-state fan-out reuses the same <see cref="ByteString"/> across peers
/// and rounds. Scoped per communication instance because <see cref="MembershipVersion"/> is
/// only unique within a cluster, not across independent clusters in the same process.
/// </summary>
internal sealed class GossipRosterJsonCache
{
    private readonly object _lock = new();

    private long _cachedVersion = -1;

    private ByteString _cachedUtf8 = ByteString.Empty;

    /// <summary>For tests: how many times roster JSON was serialized on this instance.</summary>
    internal int SerializeCount { get; private set; }

    internal ByteString GetUtf8(long membershipVersion, ClusterMembership roster)
    {
        lock (_lock)
        {
            if (membershipVersion == _cachedVersion)
                return _cachedUtf8;
        }

        ByteString utf8 = ByteString.CopyFromUtf8(JsonSerializer.Serialize(roster));

        lock (_lock)
        {
            if (membershipVersion == _cachedVersion)
                return _cachedUtf8;

            SerializeCount++;
            _cachedVersion = membershipVersion;
            _cachedUtf8 = utf8;
            return _cachedUtf8;
        }
    }
}
