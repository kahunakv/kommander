using Google.Protobuf;
using Kommander.Communication.Grpc;
using Kommander.System;

namespace Kommander.Tests.Communication;

public sealed class TestGossipRosterJsonCache
{
    [Fact]
    public void GetUtf8_SerializesOncePerMembershipVersion()
    {
        GossipRosterJsonCache cache = new();

        ClusterMembership roster = CreateRoster(version: 3, primaryEndpoint: "node-a:8001");

        ByteString first = cache.GetUtf8(3, roster);
        ByteString second = cache.GetUtf8(3, roster);

        Assert.Equal(first, second);
        Assert.Equal(1, cache.SerializeCount);

        ClusterMembership bumped = CreateRoster(version: 4, primaryEndpoint: "node-a:8001");

        cache.GetUtf8(4, bumped);

        Assert.Equal(2, cache.SerializeCount);
    }

    [Fact]
    public void IndependentInstances_DoNotShareEntriesAtSameMembershipVersion()
    {
        GossipRosterJsonCache clusterA = new();
        GossipRosterJsonCache clusterB = new();

        ClusterMembership rosterA = CreateRoster(version: 1, primaryEndpoint: "cluster-a:8001");
        ClusterMembership rosterB = CreateRoster(version: 1, primaryEndpoint: "cluster-b:9001");

        ByteString jsonA = clusterA.GetUtf8(1, rosterA);
        ByteString jsonB = clusterB.GetUtf8(1, rosterB);

        Assert.NotEqual(jsonA, jsonB);
        Assert.Equal(1, clusterA.SerializeCount);
        Assert.Equal(1, clusterB.SerializeCount);
        Assert.Contains("cluster-a:8001", jsonA.ToStringUtf8());
        Assert.Contains("cluster-b:9001", jsonB.ToStringUtf8());
    }

    private static ClusterMembership CreateRoster(long version, string primaryEndpoint)
    {
        return new()
        {
            MembershipVersion = version,
            Members =
            [
                new ClusterMember
                {
                    Endpoint = primaryEndpoint,
                    NodeId = 1,
                    Role = ClusterMemberRole.Voter
                },
                new ClusterMember
                {
                    Endpoint = "peer:8002",
                    NodeId = 2,
                    Role = ClusterMemberRole.Voter
                }
            ]
        };
    }
}
