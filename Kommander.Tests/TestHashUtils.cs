
namespace Kommander.Tests.Hash;

public class TestHashUtils
{
    [Fact]
    public void TestConsistentHash()
    {
        long bucket = HashUtils.ConsistentHash("hello", 10);
        Assert.Equal(1, bucket);
        
        bucket = HashUtils.ConsistentHash("hello", 10);
        Assert.Equal(1, bucket);
        
        bucket = HashUtils.ConsistentHash("hello", 10);
        Assert.Equal(1, bucket);
        
        bucket = HashUtils.ConsistentHash("hello", 11);
        Assert.Equal(1, bucket);
        
        bucket = HashUtils.ConsistentHash("hello", 12);
        Assert.Equal(1, bucket);
    }
    
    [Fact]
    public void TestPrefixedHashNoIndexOf()
    {
        long bucket = HashUtils.PrefixedHash("hello", '-', 10);
        Assert.Equal(9, bucket);
        
        bucket = HashUtils.PrefixedHash("hello", '-', 10);
        Assert.Equal(9, bucket);
        
        bucket = HashUtils.PrefixedHash("hello", '-', 10);
        Assert.Equal(9, bucket);
    }
    
    [Fact]
    public void TestPrefixedHashIndexOf()
    {
        long bucket = HashUtils.PrefixedHash("hello/foo", '/', 10);
        Assert.Equal(9, bucket);
        
        bucket = HashUtils.PrefixedHash("hello/bar", '/', 10);
        Assert.Equal(9, bucket);
        
        bucket = HashUtils.PrefixedHash("hello/nice", '/', 10);
        Assert.Equal(9, bucket);
    }
    
    [Fact]
    public void TestInversePrefixedHash()
    {
        long bucket = HashUtils.InversePrefixedHash("hello/foo/100", '/', 10);
        Assert.Equal(8, bucket);
        
        bucket = HashUtils.InversePrefixedHash("hello/foo/200", '/', 10);
        Assert.Equal(8, bucket);
        
        bucket = HashUtils.InversePrefixedHash("hello/foo/300", '/', 10);
        Assert.Equal(8, bucket);
    }
}