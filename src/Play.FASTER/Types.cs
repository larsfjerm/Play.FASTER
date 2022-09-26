using System.Diagnostics;
using FASTER.core;

namespace Play.FASTER;

public readonly record struct CacheKey(long Key) : IFasterEqualityComparer<CacheKey>
{
    public long GetHashCode64(ref CacheKey key) => 
        Utility.GetHashCode(key.Key);

    public bool Equals(ref CacheKey k1, ref CacheKey k2) => 
        k1.Key == k2.Key;
}

public readonly record struct CacheValue(long Value);

/// <summary>
/// Serializer for CacheKey - used if CacheKey is changed from struct to class
/// </summary>
public class CacheKeySerializer : BinaryObjectSerializer<CacheKey>
{
    public override void Serialize(ref CacheKey obj) =>
        writer.Write(obj.Key);

    public override void Deserialize(out CacheKey obj) =>
        obj = new CacheKey(reader.ReadInt64());
}

/// <summary>
/// Serializer for CacheValue - used if CacheValue is changed from struct to class
/// </summary>
public class CacheValueSerializer : BinaryObjectSerializer<CacheValue>
{
    public override void Deserialize(out CacheValue obj) =>
        obj = new CacheValue(reader.ReadInt64());

    public override void Serialize(ref CacheValue obj) =>
        writer.Write(obj.Value);
}

/// <summary>
/// User context to measure latency and/or check read result
/// </summary>
public record struct CacheContext(int Type, long Ticks);

public class CacheFunctions : SimpleFunctions<CacheKey, CacheValue, CacheContext>
{
    public override void ReadCompletionCallback(ref CacheKey key, ref CacheValue input, ref CacheValue output, CacheContext ctx, Status status, RecordMetadata recordMetadata)
    {
        if (ctx.Type == 0)
        {
            if (output.Value != key.Key)
                throw new Exception("Read error!");
        }
        else
        {
            long ticks = Stopwatch.GetTimestamp() - ctx.Ticks;

            if (status.NotFound) 
            {
                Console.WriteLine("Async: Value not found, latency = {0}ms", 1000 * (ticks - ctx.Ticks) / (double)Stopwatch.Frequency);
                return;
            }

            if (output.Value != key.Key)
                Console.WriteLine("Async: Incorrect value {0} found, latency = {1}ms", output.Value, new TimeSpan(ticks).TotalMilliseconds);
            else
                Console.WriteLine("Async: Correct value {0} found, latency = {1}ms", output.Value, new TimeSpan(ticks).TotalMilliseconds);
        }
    }
}
