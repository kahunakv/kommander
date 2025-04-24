
#pragma warning disable CA1051

using System.Collections;

namespace Kommander.Support.Collections;

/// <summary>
/// Represents a single bucket in the SmallDictionary.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
public struct SmallBucket<TKey, TValue>
{
    public bool IsNotEmpty = false;
    
    public TKey Key;
    
    public TValue Value;

    public SmallBucket()
    {
        IsNotEmpty = false;
        Key = default!;
        Value = default!;
    }
}

/// <summary>
/// Small dictionary backend by an array of buckets. It isn't thread-safe.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
public sealed class SmallDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
{
    private readonly SmallBucket<TKey, TValue?>[] buckets;

    public int Count { get; private set; }
    
    public SmallDictionary(int capacity)
    {
        buckets = new SmallBucket<TKey, TValue?>[capacity];
    }

    public void Add(TKey key, TValue value)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));
        
        // Check if key already exists
        int existingIndex = FindBucketIndex(key);
        if (existingIndex >= 0)
            throw new ArgumentException("An item with the same key has already been added", nameof(key));
        
        for (int i = 0; i < buckets.Length; i++)
        {
            ref SmallBucket<TKey, TValue?> bucket = ref buckets[i];
            
            if (!bucket.IsNotEmpty)
            {
                bucket.IsNotEmpty = true;
                bucket.Key = key;
                bucket.Value = value;
                Count++;
                return;
            }
        }
        
        throw new ArgumentException("Not enough space in the dictionary");
    }
    
    public void Remove(TKey key)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));
        
        // Check if key already exists
        int existingIndex = FindBucketIndex(key);
        if (existingIndex >= 0)
        {
            buckets[existingIndex].IsNotEmpty = false;
            Count--;
        }
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

        // Check if key already exists
        int existingIndex = FindBucketIndex(key);
        if (existingIndex >= 0)
            return false;
        
        for (int i = 0; i < buckets.Length; i++)
        {
            ref SmallBucket<TKey, TValue?> bucket = ref buckets[i];
            
            if (!bucket.IsNotEmpty)
            {
                bucket.IsNotEmpty = true;
                bucket.Key = key;
                bucket.Value = value;
                return true;
            }
        }
        
        throw new ArgumentException("Not enough space in the dictionary");
    }
    
    // Find the index of the bucket with the given key, or -1 if not found
    private int FindBucketIndex(TKey key)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key));
            
        for (int i = 0; i < buckets.Length; i++)
        {
            if (buckets[i].IsNotEmpty && EqualityComparer<TKey>.Default.Equals(buckets[i].Key, key))
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
            int index = FindBucketIndex(key);
            if (index < 0)
                throw new KeyNotFoundException($"The given key '{key}' was not present in the dictionary.");
                
            return (TValue)buckets[index].Value!;
        }
        set
        {
            int index = FindBucketIndex(key);
            if (index >= 0)
            {
                // Update existing value
                buckets[index].Value = value;
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
        return FindBucketIndex(key) >= 0;
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
        int index = FindBucketIndex(key);
        if (index >= 0)
        {
            value = (TValue)buckets[index].Value!;
            return true;
        }
        
        value = default!;
        return false;
    }

    /// <summary>
    /// Calculates the number of non-empty buckets in the dictionary.
    /// </summary>
    /// <returns>
    /// The number of buckets that contain at least one element.
    /// </returns>
    public int GetCount()
    {
        int counter = 0;

        for (int i = 0; i < buckets.Length; i++)
        {
            if (buckets[i].IsNotEmpty)
                counter++;
        }

        return counter;
    }
    
    // IEnumerable<KeyValuePair<TKey, TValue>> implementation
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        for (int i = 0; i < buckets.Length; i++)
        {
            if (buckets[i].IsNotEmpty)
                yield return new(buckets[i].Key, (TValue)buckets[i].Value!);
        }
    }
    
    // IEnumerable implementation
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}

#pragma warning restore CA1051