using Kommander.Support.Collections;

namespace Kommander.Tests;

/// <summary>
/// Edge behavior of the packed linear-scan <see cref="SmallDictionary{TKey,TValue}"/>: the
/// swap-remove keeps the live slice dense and moves values along with keys, capacity is a hard
/// bound, missing-key accesses have defined outcomes, and a freed slot is reusable.
/// </summary>
public sealed class TestSmallDictionaryEdges
{
    [Fact]
    public void Remove_MissingKey_IsANoOp()
    {
        SmallDictionary<string, int> dict = new(2) { { "a", 1 }, { "b", 2 } };

        dict.Remove("nope");

        Assert.Equal(2, dict.Count);
        Assert.True(dict.ContainsKey("a"));
        Assert.True(dict.ContainsKey("b"));
    }

    [Fact]
    public void Remove_SwapsLastEntryIntoTheHole_ValuesFollowTheirKeys()
    {
        // Keys and values live in separate arrays; the swap must move both or every lookup
        // after a removal would pair a key with another entry's value.
        SmallDictionary<string, int> dict = new(3) { { "a", 1 }, { "b", 2 }, { "c", 3 } };

        dict.Remove("a");

        Assert.Equal(2, dict.Count);
        Assert.Equal(2, dict["b"]);
        Assert.Equal(3, dict["c"]);
        Assert.False(dict.ContainsKey("a"));
    }

    [Fact]
    public void Remove_LastLiveEntry_LeavesAnEmptyDictionary()
    {
        SmallDictionary<string, int> dict = new(1) { { "only", 7 } };

        dict.Remove("only");

        Assert.Equal(0, dict.Count);
        Assert.False(dict.ContainsKey("only"));
    }

    [Fact]
    public void Add_BeyondCapacity_Throws()
    {
        SmallDictionary<string, int> dict = new(2) { { "a", 1 }, { "b", 2 } };

        Assert.Throws<ArgumentException>(() => dict.Add("c", 3));
        Assert.Equal(2, dict.Count);
    }

    [Fact]
    public void TryAdd_BeyondCapacity_Throws()
    {
        SmallDictionary<string, int> dict = new(1) { { "a", 1 } };

        Assert.Throws<ArgumentException>(() => dict.TryAdd("b", 2));
    }

    [Fact]
    public void Indexer_GetMissingKey_ThrowsKeyNotFound()
    {
        SmallDictionary<string, int> dict = new(1) { { "a", 1 } };

        KeyNotFoundException ex = Assert.Throws<KeyNotFoundException>(() => dict["missing"]);

        Assert.Contains("missing", ex.Message);
    }

    [Fact]
    public void Indexer_SetExistingKey_ReplacesTheValueWithoutGrowing()
    {
        SmallDictionary<string, int> dict = new(2) { { "a", 1 }, { "b", 2 } };

        dict["a"] = 100;

        Assert.Equal(2, dict.Count);
        Assert.Equal(100, dict["a"]);
        Assert.Equal(2, dict["b"]);
    }

    [Fact]
    public void Add_AfterRemove_ReusesTheFreedSlotWithinCapacity()
    {
        SmallDictionary<string, int> dict = new(2) { { "a", 1 }, { "b", 2 } };

        dict.Remove("a");
        dict.Add("c", 3);

        Assert.Equal(2, dict.Count);
        Assert.True(dict.TryGetValue("c", out int value));
        Assert.Equal(3, value);
        Assert.True(dict.ContainsKey("b"));
    }

    [Fact]
    public void NullKey_ArgumentNullOnEveryEntry()
    {
        SmallDictionary<string, int> dict = new(2) { { "a", 1 } };

        Assert.Throws<ArgumentNullException>(() => dict.Add(null!, 2));
        Assert.Throws<ArgumentNullException>(() => dict.TryAdd(null!, 2));
        Assert.Throws<ArgumentNullException>(() => dict.Remove(null!));
        Assert.Throws<ArgumentNullException>(() => _ = dict[null!]);
    }

    [Fact]
    public void Enumeration_AfterSwapRemove_YieldsExactlyTheLiveEntries()
    {
        SmallDictionary<string, int> dict = new(4) { { "a", 1 }, { "b", 2 }, { "c", 3 }, { "d", 4 } };

        dict.Remove("b");
        dict.Remove("d");

        List<KeyValuePair<string, int>> entries = [.. dict];

        Assert.Equal(2, entries.Count);
        Assert.Contains(new KeyValuePair<string, int>("a", 1), entries);
        Assert.Contains(new KeyValuePair<string, int>("c", 3), entries);
    }

    [Fact]
    public void ZeroCapacityDictionary_AcceptsNothing()
    {
        SmallDictionary<string, int> dict = new(0);

        Assert.Equal(0, dict.Count);
        Assert.Throws<ArgumentException>(() => dict.Add("a", 1));
    }
}
