
using Kommander.Communication.Rest;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Kommander.Tests.Communication;

/// <summary>
/// Guards the one weakness of prefix-scoped authentication: a Kommander REST endpoint mapped
/// outside the guarded prefix would be silently unauthenticated.
/// </summary>
/// <remarks>
/// <para>
/// Authentication is applied by middleware over <c>RestCommunicationExtensions.RaftRoutePrefix</c>
/// rather than to the whole pipeline, because <c>MapRestRaftRoutes</c> runs on the host's own
/// <c>WebApplication</c> — an application embedding Kommander alongside its own API would otherwise
/// find every one of its endpoints demanding a cluster credential.
/// </para>
/// <para>
/// The failure mode that scoping creates is invisible: a new route mapped at, say, <c>/v1/rafts/…</c>
/// or <c>/admin/…</c> would work perfectly and simply skip authentication. No test would fail and
/// nothing would be logged. This test enumerates what <c>MapRestRaftRoutes</c> actually registered
/// and asserts every route falls inside the guarded prefix, so that mistake becomes a build failure
/// rather than an open endpoint.
/// </para>
/// </remarks>
public sealed class TestRestRoutesAreAuthenticated
{
    [Fact]
    public void EveryMappedRaftRoute_LivesUnderTheAuthenticatedPrefix()
    {
        IReadOnlyList<string> routes = MapAndCollectRoutePatterns();

        Assert.NotEmpty(routes);

        foreach (string route in routes)
        {
            Assert.True(
                route.StartsWith(RestCommunicationExtensions.RaftRoutePrefix, StringComparison.Ordinal),
                $"Route '{route}' is mapped outside '{RestCommunicationExtensions.RaftRoutePrefix}' and would "
                + "therefore bypass the transport-authentication middleware. Move it under the prefix, or "
                + "make its exemption explicit and deliberate.");
        }
    }

    /// <summary>
    /// A sanity check on the check: if the route table ever comes back empty — a refactor that stops
    /// registering endpoints, or an enumeration API change — the assertion above would pass
    /// vacuously and guard nothing.
    /// </summary>
    [Fact]
    public void RouteEnumeration_SeesTheExpectedSurface()
    {
        IReadOnlyList<string> routes = MapAndCollectRoutePatterns();

        // A few well-known endpoints from across the surface: consensus, membership, and gossip.
        Assert.Contains("/v1/raft/append-logs", routes);
        Assert.Contains("/v1/raft/install-snapshot", routes);
        Assert.Contains("/v1/raft/gossip", routes);
        Assert.Contains("/v1/raft/vote", routes);

        // The real surface is well over a handful; a collapse to one or two would mean the
        // enumeration is no longer seeing what MapRestRaftRoutes registers.
        Assert.True(routes.Count >= 10, $"only {routes.Count} routes enumerated");
    }

    private static IReadOnlyList<string> MapAndCollectRoutePatterns()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();

        // IRaft must be *registered* for minimal APIs to infer it as a service parameter rather than
        // a request body — endpoint construction consults IServiceProviderIsService. The factory
        // throws because it is never meant to run: these tests enumerate the route table, they do not
        // invoke handlers.
        builder.Services.AddSingleton<IRaft>(
            _ => throw new NotSupportedException("Route enumeration must not invoke handlers."));

        WebApplication app = builder.Build();

        app.MapRestRaftRoutes();

        // Read the builder's own data sources rather than the EndpointDataSource service: minimal-API
        // routes land here as they are mapped, whereas the resolved service is only composed once the
        // application starts, and would enumerate empty here — passing the assertions vacuously.
        IEndpointRouteBuilder routeBuilder = app;

        return
        [
            .. routeBuilder.DataSources
                .SelectMany(source => source.Endpoints)
                .OfType<RouteEndpoint>()
                .Select(endpoint => endpoint.RoutePattern.RawText ?? string.Empty)
        ];
    }
}
