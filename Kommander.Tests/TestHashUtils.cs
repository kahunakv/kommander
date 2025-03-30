
namespace Kommander.Tests.Hash;

public class TestHashUtils
{
    [Fact]
    public void TestConsistentHash()
    {
        long bucket = HashUtils.ConsistentHash("hello", 10);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.ConsistentHash("hello", 10);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.ConsistentHash("hello", 10);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.ConsistentHash("hello", 11);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.ConsistentHash("hello", 12);
        Assert.Equal(7, bucket);
    }
    
    [Fact]
    public void TestPrefixedHashNoIndexOf()
    {
        long bucket = HashUtils.PrefixedHash("hello", '-', 9);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.PrefixedHash("hello", '-', 10);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.PrefixedHash("hello", '-', 10);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.PrefixedHash("hello", '-', 10);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.PrefixedHash("hello", '-', 11);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.PrefixedHash("hello", '-', 12);
        Assert.Equal(7, bucket);
    }
    
    [Fact]
    public void TestPrefixedHashIndexOf()
    {
        long bucket = HashUtils.PrefixedHash("hello/foo", '/', 9);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.PrefixedHash("hello/bar", '/', 10);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.PrefixedHash("hello/bar", '/', 10);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.PrefixedHash("hello/nice", '/', 10);
        Assert.Equal(7, bucket);
        
        bucket = HashUtils.PrefixedHash("hello/nice", '/', 11);
        Assert.Equal(7, bucket);
    }
    
    [Fact]
    public void TestInversePrefixedHash()
    {
        long bucket = HashUtils.InversePrefixedHash("hello/foo/100", '/', 9);
        Assert.Equal(4, bucket);
        
        bucket = HashUtils.InversePrefixedHash("hello/foo/200", '/', 10);
        Assert.Equal(4, bucket);
        
        bucket = HashUtils.InversePrefixedHash("hello/foo/200", '/', 10);
        Assert.Equal(4, bucket);
        
        bucket = HashUtils.InversePrefixedHash("hello/foo/300", '/', 10);
        Assert.Equal(4, bucket);
        
        bucket = HashUtils.InversePrefixedHash("hello/foo/300", '/', 11);
        Assert.Equal(4, bucket);
    }
    
    [Fact]
    public void TestRandomConsistentHash()
    {
        for (int i = 0; i < 100; i++)
        {
            for (int b = 1; b < 20; b++)
            {
                long bucket = HashUtils.ConsistentHash(Guid.NewGuid().ToString(), b);
                Assert.True(bucket is >= 0 and < 20);
            }
        }
    }
}