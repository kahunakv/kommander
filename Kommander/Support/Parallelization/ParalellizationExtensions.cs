
namespace Kommander.Support.Parallelization;

public static class ParalellizationExtensions
{
    public static async Task ForEachAsync<T>(this IEnumerable<T> enumerable, int maxDegreeOfParallelism, Func<T, Task> body)
    {
        await Parallel.ForEachAsync(enumerable, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, async (item, cancellationToken) =>
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            await body(item).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public static async Task ForEachAsync<T>(this IAsyncEnumerable<T> enumerable, int maxDegreeOfParallelism, Func<T, Task> body)
    {
        await Parallel.ForEachAsync(enumerable, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, async (item, cancellationToken) =>
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            await body(item).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public static async Task ForEachAsync<T>(this List<T> enumerable, int maxDegreeOfParallelism, Func<T, Task> body)
    {
        await Parallel.ForEachAsync(enumerable, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, async (item, cancellationToken) =>
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            await body(item).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public static async Task ForEachAsync<T>(this T[] enumerable, int maxDegreeOfParallelism, Func<T, Task> body)
    {
        await Parallel.ForEachAsync(enumerable, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, async (item, cancellationToken) =>
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            await body(item).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    public static async Task ForEachAsync<T>(this HashSet<T> enumerable, int maxDegreeOfParallelism, Func<T, Task> body)
    {
        await Parallel.ForEachAsync(enumerable, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, async (item, cancellationToken) =>
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            await body(item).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }
}