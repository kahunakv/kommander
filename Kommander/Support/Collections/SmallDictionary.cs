
#pragma warning disable CA1051

using System.Collections;

namespace Kommander.Support.Collections;

/// <summary>
/// Small dictionary backed by linear-scan lookup over a packed key array. It isn't thread-safe.
/// </summary>
/// <remarks>
/// <para>Intended for tiny maps (a handful of entries, e.g. <c>RaftWriteAhead.plan</c> with three
/// <c>RaftLogAction</c> keys) where the constant factors of a hashed <see cref="Dictionary{TKey,TValue}"/>
/// — hashing, bucket/entry arrays, two allocations — cost more than a short linear scan. Lookup is O(n),
/// so this only wins while <c>n</c> stays small; benchmarks put the crossover against
/// <see cref="Dictionary{TKey,TValue}"/> at roughly 16 entries. Above that, prefer a real dictionary.</para>
///
/// <para><b>Layout.</b> Keys and values are held in separate arrays (structure-of-arrays) and packed
/// densely into <c>[0, Count)</c>. A lookup scans only the live, contiguous <c>keys</c> slice — cache-dense
/// and vectorizable by the JIT for primitive keys — without pulling values through cache. This is
/// deliberately not an array-of-<c>KeyValuePair</c>/bucket-struct layout, which would interleave values
/// (and formerly an occupancy flag) into every key comparison and roughly triple the bytes scanned.</para>
///
/// <para><b>Removal reorders.</b> <see cref="Remove"/> swaps the last live entry into the vacated slot to
/// keep the array dense (no tombstones, so scans never pay for holes). Enumeration order is therefore
/// insertion order only until the first removal; callers that need stable order after removals must not
/// rely on it.</para>
/// </remarks>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
public sealed class SmallDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
{
    // Cached once so the EqualityComparer<TKey>.Default property fetch isn't repeated per scanned element.
    private static readonly EqualityComparer<TKey> Comparer = EqualityComparer<TKey>.Default;

    private readonly TKey[] keys;

    private readonly TValue?[] values;

    public int Count { get; private set; }

    public SmallDictionary(int capacity)
    {
        keys = new TKey[capacity];
        values = new TValue?[capacity];
    }

    public void Add(TKey key, TValue value)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        if (IndexOf(key) >= 0)
            throw new ArgumentException("An item with the same key has already been added", nameof(key));

        int count = Count;
        if (count >= keys.Length)
            throw new ArgumentException("Not enough space in the dictionary");

        keys[count] = key;
        values[count] = value;
        Count = count + 1;
    }

    public void Remove(TKey key)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        int index = IndexOf(key);
        if (index < 0)
            return;

        // Swap-remove: pull the last live entry into the hole so [0, Count) stays dense.
        int last = Count - 1;
        keys[index] = keys[last];
        values[index] = values[last];
        keys[last] = default!;
        values[last] = default;
        Count = last;
    }

    /// <summary>
    /// Attempts to add the specified key and value to the dictionary.
    /// </summary>
    /// <param name="key">
    /// The key of the element to add. Must not be null.
    /// </param>
    /// <param name="value">
    /// The value of the element to add.
    /// </param>
    /// <returns>
    /// Returns true if the key and value were successfully added to the dictionary;
    /// otherwise, false if the key already exists in the dictionary.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when the key is null.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the dictionary has insufficient capacity to add the element.
    /// </exception>
    public bool TryAdd(TKey key, TValue value)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));

        if (IndexOf(key) >= 0)
            return false;

        int count = Count;
        if (count >= keys.Length)
            throw new ArgumentException("Not enough space in the dictionary");

        keys[count] = key;
        values[count] = value;
        Count = count + 1;
        return true;
    }

    // Find the index of the live entry with the given key, or -1 if not found.
    private int IndexOf(TKey key)
    {
        TKey[] localKeys = keys;
        int count = Count;
        for (int i = 0; i < count; i++)
        {
            if (Comparer.Equals(localKeys[i], key))
                return i;
        }

        return -1;
    }

    /// <summary>
    /// Gets or sets the value associated with the specified key in the dictionary.
    /// Throws a <see cref="KeyNotFoundException"/> if the key does not exist when getting a value.
    /// Replaces the existing value or adds a new key-value pair when setting a value.
    /// </summary>
    /// <param name="key">The key of the value to get or set.</param>
    /// <returns>The value associated with the specified key.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the key is null.</exception>
    /// <exception cref="KeyNotFoundException">Thrown when trying to get a value for a nonexistent key.</exception>
    /// <exception cref="ArgumentException">Thrown when trying to add a duplicate key.</exception>
    public TValue this[TKey key]
    {
        get
        {
            int index = IndexOf(key);
            if (index < 0)
                throw new KeyNotFoundException($"The given key '{key}' was not present in the dictionary.");

            return values[index]!;
        }
        set
        {
            int index = IndexOf(key);
            if (index >= 0)
            {
                // Update existing value
                values[index] = value;
            }
            else
            {
                // Add new key-value pair
                Add(key, value);
            }
        }
    }

    // Check if the dictionary contains a key
    public bool ContainsKey(TKey key)
    {
        return IndexOf(key) >= 0;
    }

    /// <summary>
    /// Attempts to retrieve the value associated with the specified key.
    /// </summary>
    /// <param name="key">
    /// The key of the value to retrieve. Must not be null.
    /// </param>
    /// <param name="value">
    /// When this method returns, contains the value associated with the specified key
    /// if the key is found; otherwise, the default value for the type of the value parameter.
    /// This parameter is passed uninitialized.
    /// </param>
    /// <returns>
    /// Returns true if the key is found in the dictionary; otherwise, false.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when the key is null.
    /// </exception>
    public bool TryGetValue(TKey key, out TValue value)
    {
        int index = IndexOf(key);
        if (index >= 0)
        {
            value = values[index]!;
            return true;
        }

        value = default!;
        return false;
    }

    /// <summary>
    /// Returns the number of live entries. Equivalent to <see cref="Count"/>; retained for callers that
    /// used the explicit method form.
    /// </summary>
    /// <returns>
    /// The number of key-value pairs currently stored.
    /// </returns>
    public int GetCount()
    {
        return Count;
    }

    // IEnumerable<KeyValuePair<TKey, TValue>> implementation
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        int count = Count;
        for (int i = 0; i < count; i++)
            yield return new(keys[i], values[i]!);
    }

    // IEnumerable implementation
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}

#pragma warning restore CA1051
