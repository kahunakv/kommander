using System.Collections.Concurrent;
using Kommander.Support.Parallelization;

namespace Kommander.Tests;

/// <summary>
/// The <c>ForEachAsync</c> parallelization helpers: every source overload must reach the body
/// for each item, the degree cap must actually serialize at 1 and permit overlap above 1, and
/// an empty source must complete without invoking the body.
/// </summary>
public sealed class TestForEachAsyncParallelization
{
    [Fact]
    public async Task ForEachAsync_EnumerableOverload_ProcessesEveryItem()
    {
        IEnumerable<int> source = Enumerable.Range(1, 10);
        ConcurrentQueue<int> processed = new();

        await source.ForEachAsync(2, item => { processed.Enqueue(item); return Task.CompletedTask; });

        Assert.Equal(10, processed.Count);
        Assert.True(processed.OrderBy(i => i).SequenceEqual(Enumerable.Range(1, 10)));
    }

    [Fact]
    public async Task ForEachAsync_ListArrayAndSetOverloads_ProcessEveryItem()
    {
        List<int> list = [1, 2, 3];
        int[] array = [4, 5];
        HashSet<string> set = ["a", "b", "c"];

        ConcurrentQueue<int> seen = new();
        ConcurrentQueue<string> strings = new();

        await list.ForEachAsync(1, i => { seen.Enqueue(i); return Task.CompletedTask; });
        await array.ForEachAsync(1, i => { seen.Enqueue(i); return Task.CompletedTask; });
        await set.ForEachAsync(1, s => { strings.Enqueue(s); return Task.CompletedTask; });

        Assert.Equal(5, seen.Count);
        Assert.Equal(3, strings.Count);
        Assert.Contains(1, seen);
        Assert.Contains(5, seen);
        Assert.Contains("a", strings);
        Assert.Contains("c", strings);
    }

    [Fact]
    public async Task ForEachAsync_DegreeOfOne_NeverRunsBodiesConcurrently()
    {
        // With MaxDegreeOfParallelism 1 the loop is strictly sequential, so the observed
        // concurrency must never exceed 1 no matter how the bodies yield.
        int inFlight = 0;
        int maxInFlight = 0;

        await Enumerable.Range(0, 25).ForEachAsync(1, async item =>
        {
            int current = Interlocked.Increment(ref inFlight);
            int previousMax;
            do
            {
                previousMax = Volatile.Read(ref maxInFlight);
                if (current <= previousMax)
                    break;
            } while (Interlocked.CompareExchange(ref maxInFlight, current, previousMax) != previousMax);

            await Task.Yield();

            Interlocked.Decrement(ref inFlight);
        });

        Assert.Equal(0, inFlight);
        Assert.Equal(1, maxInFlight);
    }

    [Fact]
    public async Task ForEachAsync_DegreeOfTwo_AllowsOverlappingBodies()
    {
        // If the helper ignored the degree, this body pair would deadlock under degree 1
        // (the first item waits for the second to release it). Completing at all — within a
        // generous bound — proves the cap permits at least two bodies in flight.
        TaskCompletionSource firstStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Task loop = new[] { "first", "second" }.ForEachAsync(2, async item =>
        {
            if (item == "first")
            {
                firstStarted.SetResult();
                await releaseFirst.Task;
            }
            else
            {
                await firstStarted.Task;
                releaseFirst.SetResult();
            }
        });

        await loop.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ForEachAsync_EmptySource_CompletesWithoutInvokingTheBody()
    {
        int invocations = 0;

        await Array.Empty<int>().ForEachAsync(4, _ => { Interlocked.Increment(ref invocations); return Task.CompletedTask; });

        Assert.Equal(0, invocations);
    }
}
