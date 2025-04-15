
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
            if (!buckets[i].IsNotEmpty)
            {
                buckets[i].IsNotEmpty = true;
                buckets[i].Key = key;
                buckets[i].Value = value;
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
            if (!buckets[i].IsNotEmpty)
            {
                buckets[i].IsNotEmpty = true;
                buckets[i].Key = key;
                buckets[i].Value = value;
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
    
    // Indexer for getting and setting values
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
    
    // Try to get a value by key
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