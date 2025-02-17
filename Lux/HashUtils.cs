namespace Lux;

public class HashUtils
{
    public static int ConsistentHash(string value, int divisor)
    {
        //uint computed = xxHash32.ComputeHash(value);

        //BigInteger computedInteger = new(Math.Abs(computed));
        //BigInteger bigDivisor = new(divisor);

        //return (int)(computedInteger % bigDivisor);
        
        return Math.Abs(value.GetHashCode()) % divisor;
    }
}