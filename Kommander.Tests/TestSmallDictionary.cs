
using Kommander.Support.Collections;

namespace Kommander.Tests;

public class TestSmallDictionary
{
    [Fact]
    public void TestInitialization()
    {
        SmallDictionary<string, int> dict = new(3)
        {
            { "key1", 1 },
            { "key2", 2 },
            { "key3", 3 }
        };

        // Check if the keys and values are stored correctly
        Assert.Equal(1, dict["key1"]);
        Assert.Equal(2, dict["key2"]);
        Assert.Equal(3, dict["key3"]);
        
        // Check if an exception is thrown when adding a duplicate key
        Assert.Throws<ArgumentException>(() => dict.Add("key1", 4));
    }
    
    [Fact]
    public void TestInitialization2()
    {
        // ReSharper disable once UseObjectOrCollectionInitializer
        SmallDictionary<string, int> dict = new(3);
        
        dict["key1"] = 1;
        dict["key2"] = 2;
        dict["key3"] = 3;

        // Check if the keys and values are stored correctly
        Assert.Equal(1, dict["key1"]);
        Assert.Equal(2, dict["key2"]);
        Assert.Equal(3, dict["key3"]);
        
        // Check if an exception is thrown when adding a duplicate key
        Assert.Throws<ArgumentException>(() => dict.Add("key1", 4));
    }
    
    [Fact]
    public void TestInitialization3()
    {
        // ReSharper disable once UseObjectOrCollectionInitializer
        SmallDictionary<string, int> dict = new(3);
        
        dict.Add("key1", 1);
        dict.Add("key2", 2);
        dict.Add("key3", 3);

        // Check if the keys and values are stored correctly
        Assert.Equal(1, dict["key1"]);
        Assert.Equal(2, dict["key2"]);
        Assert.Equal(3, dict["key3"]);
        
        // Check if an exception is thrown when adding a duplicate key
        Assert.Throws<ArgumentException>(() => dict.Add("key1", 4));
    }
    
    [Fact]
    public void TestInitialization4()
    {
        // ReSharper disable once UseObjectOrCollectionInitializer
        SmallDictionary<string, int> dict = new(3);
        
        Assert.True(dict.TryAdd("key1", 1));
        Assert.True(dict.TryAdd("key2", 2));
        Assert.True(dict.TryAdd("key3", 3));
        
        Assert.False(dict.TryAdd("key1", 1));
        Assert.False(dict.TryAdd("key2", 2));
        Assert.False(dict.TryAdd("key3", 3));

        // Check if the keys and values are stored correctly
        Assert.Equal(1, dict["key1"]);
        Assert.Equal(2, dict["key2"]);
        Assert.Equal(3, dict["key3"]);
        
        Assert.True(dict.TryGetValue("key1", out int value));
        Assert.Equal(1, value);
        
        Assert.True(dict.TryGetValue("key2", out value));
        Assert.Equal(2, value);
        
        Assert.True(dict.TryGetValue("key3", out value));
        Assert.Equal(3, value);
        
        Assert.False(dict.TryGetValue("key4", out value));
        
        // Check if an exception is thrown when adding a duplicate key
        Assert.Throws<ArgumentException>(() => dict.Add("key1", 4));
    }
    
    [Fact]
    public void TestTraverse()
    {
        SmallDictionary<string, int> dict = new(3)
        {
            { "key1", 1 },
            { "key2", 2 },
            { "key3", 3 }
        };

        int counter = 0;
        
        foreach (KeyValuePair<string, int> kvp in dict)
        {
            Assert.True(dict.ContainsKey(kvp.Key));
            Assert.Equal(kvp.Value, dict[kvp.Key]);
            
            counter++;
        }
        
        Assert.Equal(3, counter);
    }
    
    [Fact]
    public void TestCount()
    {
        SmallDictionary<string, int> dict = new(3)
        {
            { "key1", 1 },
            { "key2", 2 },
            { "key3", 3 }
        };
        
        Assert.Equal(3, dict.Count);
        Assert.Equal(3, dict.GetCount());
    }
    
    [Fact]
    public void TestRemove()
    {
        SmallDictionary<string, int> dict = new(3)
        {
            { "key1", 1 },
            { "key2", 2 },
            { "key3", 3 }
        };

        dict.Remove("key1");
        
        Assert.False(dict.ContainsKey("key1"));
        Assert.True(dict.ContainsKey("key2"));
        Assert.True(dict.ContainsKey("key3"));
    }
}