using FASTER.core;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Play.FASTER
{
    class Program
    {
        static FasterKV<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext, CacheFunctions> faster;
        static int numOps = 0;

        static async Task Main()
        {
            var path = "logs";

            var log = Devices.CreateLogDevice(path + "/hlog.log", deleteOnClose: true);
            var objlog = Devices.CreateLogDevice(path + "/hlog.obj.log", deleteOnClose: true);

            var logSettings = new LogSettings { LogDevice = log, ObjectLogDevice = objlog };
            var checkpointSettings = new CheckpointSettings { CheckpointDir = path, CheckPointType = CheckpointType.FoldOver };
            var serializerSettings = new SerializerSettings<CacheKey, CacheValue> { keySerializer = () => new CacheKeySerializer(), valueSerializer = () => new CacheValueSerializer() };

            faster = new FasterKV<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext, CacheFunctions>
                (1L << 20, new CacheFunctions(), logSettings, checkpointSettings, serializerSettings);

            const int NumParallelTasks = 1;
            ThreadPool.SetMinThreads(2 * Environment.ProcessorCount, 2 * Environment.ProcessorCount);
            TaskScheduler.UnobservedTaskException += (object sender, UnobservedTaskExceptionEventArgs e) =>
            {
                Console.WriteLine($"Unobserved task exception: {e.Exception}");
                e.SetObserved();
            };

            var ctSource = new CancellationTokenSource();

            Task[] tasks = new Task[NumParallelTasks];
            for (int i = 0; i < NumParallelTasks; i++)
            {
                int local = i;
                tasks[i] = Task.Factory.StartNew(() => AsyncOperator(local, ctSource.Token), ctSource.Token, TaskCreationOptions.RunContinuationsAsynchronously, TaskScheduler.Default);
            }

            await Task.Factory.StartNew(() => ReportThread(ctSource.Token), ctSource.Token, TaskCreationOptions.RunContinuationsAsynchronously, TaskScheduler.Default);
            System.Console.WriteLine("ReportTask Started");

            await Task.Factory.StartNew(() => CommitThread(ctSource.Token), ctSource.Token, TaskCreationOptions.RunContinuationsAsynchronously, TaskScheduler.Default);
            System.Console.WriteLine("CommitTask Started");

            await Task.WhenAll(tasks);
            System.Console.WriteLine("AsyncOperatorTasks Started");

            System.Console.ReadLine();
        }

        /// <summary>
        /// Async operations on FasterKV
        /// </summary>
        static async Task AsyncOperator(int id, CancellationToken ct)
        {
            using var session = faster.NewSession(id.ToString());
            Random rand = new Random(id);

            bool batched = true;

            await Task.Yield();

            var context = new CacheContext();

            if (!batched)
            {
                // Single commit version - upsert each item and wait for commit
                // Needs high parallelism (NumParallelTasks) for perf
                // Needs separate commit thread to perform regular checkpoints
                while (!ct.IsCancellationRequested)
                {
                    try
                    {

                        var key = new CacheKey(rand.Next());
                        var value = new CacheValue(rand.Next());
                        await session.UpsertAsync(ref key, ref value, context, true, ct);

                        Interlocked.Increment(ref numOps);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{nameof(AsyncOperator)}({id}): {ex}");
                    }
                }
            }
            else
            {
                // Batched version - we enqueue many entries to memory,
                // then wait for commit periodically
                int count = 0;

                while (!ct.IsCancellationRequested)
                {

                    var key = new CacheKey(rand.Next());
                    var value = new CacheValue(rand.Next());
                    await session.UpsertAsync(ref key, ref value, context, token: ct);

                    if (count++ % 100 == 0)
                    {
                        await session.WaitForCommitAsync(ct);
                        Interlocked.Add(ref numOps, 100);
                    }
                }
            }
        }

        static async Task ReportThread(CancellationToken ct)
        {
            long lastTime = 0;
            long lastValue = numOps;

            Stopwatch sw = new Stopwatch();
            sw.Start();

            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(5_000, ct);

                var nowTime = sw.ElapsedMilliseconds;
                var nowValue = numOps;

                Console.WriteLine("Operation Throughput: {0} ops/sec, Tail: {1}",
                    (nowValue - lastValue) / (1000 * (nowTime - lastTime)), faster.Log.TailAddress);
                lastValue = nowValue;
                lastTime = nowTime;
            }
        }

        static async Task CommitThread(CancellationToken ct)
        {
            //Task<LinkedCommitInfo> prevCommitTask = null;
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(5_000, ct);

                faster.TakeFullCheckpoint(out _);
                await faster.CompleteCheckpointAsync();
            }
        }
    }
}
