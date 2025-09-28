using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;

namespace KafkaDemo.Api.Consumers;

public abstract class KafkaConsumerBase<TEvent>(
    string groupId,
    string topic,
    IConfiguration configuration,
    int maxParallelism = 1,
    int commitIntervalMs = 1000,
    int commitMessageBatchSize = 50) : BackgroundService where TEvent : class
{
    private readonly ConsumerConfig _config = new()
    {
        BootstrapServers = configuration.GetConnectionString("kafka"),
        GroupId = groupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        EnableAutoOffsetStore = false
    };

    protected abstract Task HandleMessageAsync(TEvent evt, CancellationToken ct);

    protected abstract Task HandleErrorAsync(TEvent? evt, Exception ex, CancellationToken ct);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(_config).Build();

        consumer.Subscribe(topic);

        var channel = Channel.CreateBounded<WorkItem>(new BoundedChannelOptions(Math.Max(100, maxParallelism * 20))
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true
        });

        var partitionStates = new ConcurrentDictionary<TopicPartition, PartitionState>();
        
        var workers = new List<Task>();
        
        var commitInterval = TimeSpan.FromMilliseconds(commitIntervalMs <= 0 ? 1000 : commitIntervalMs);
        
        var nextCommitAt = DateTime.UtcNow + commitInterval;
        
        var commitNeeded = 0;
        
        int processedSinceLastCommit = 0;

        // Start workers (skip if sequential)
        if (maxParallelism > 1)
        {
            for (int i = 0; i < maxParallelism; i++)
            {
                workers.Add(Task.Run(async () =>
                {
                    var reader = channel.Reader;

                    while (await reader.WaitToReadAsync(stoppingToken))
                    {
                        while (reader.TryRead(out var item))
                        {
                            var cr = item.Cr;
                            
                            TEvent? evt = null;
                            
                            try
                            {
                                evt = JsonSerializer.Deserialize<TEvent>(cr.Message.Value, JsonSerializerOptions.Web);

                                if (evt is null)
                                {
                                    continue; // skip null
                                }

                                await HandleMessageAsync(evt, stoppingToken);

                                partitionStates
                                    .GetOrAdd(cr.TopicPartition, _ => new PartitionState())
                                    .MarkProcessed(cr.Offset);

                                Interlocked.Exchange(ref commitNeeded, 1);

                                Interlocked.Increment(ref processedSinceLastCommit);
                            }
                            catch (Exception ex)
                            {
                                partitionStates
                                    .GetOrAdd(cr.TopicPartition, _ => new PartitionState())
                                    .MarkFailed(cr.Offset);

                                await HandleErrorAsync(evt, ex, stoppingToken);

                                Interlocked.Exchange(ref commitNeeded, 1); // allow commit of previous offsets
                            }
                        }
                    }
                }, stoppingToken));
            }
        }

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? cr = null;

                try
                {
                    cr = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    await HandleErrorAsync(null, ex, stoppingToken);
                    continue;
                }
                if (cr is null)
                {
                    continue;
                }

                if (maxParallelism <= 1)
                {
                    // Sequential path
                    TEvent? evt = null;

                    try
                    {
                        evt = JsonSerializer.Deserialize<TEvent>(cr.Message.Value, JsonSerializerOptions.Web);

                        if (evt is null)
                        {
                            continue;
                        }
                        
                        await HandleMessageAsync(evt, stoppingToken);
                        
                        consumer.Commit(cr); // single-thread commit
                    }
                    catch (Exception ex)
                    {
                        await HandleErrorAsync(evt, ex, stoppingToken);
                    }
                }
                else
                {
                    await channel.Writer.WriteAsync(new WorkItem(cr), stoppingToken);
                }

                bool intervalElapsed = DateTime.UtcNow >= nextCommitAt;
                
                bool batchHit = processedSinceLastCommit >= commitMessageBatchSize;

                if (maxParallelism > 1 &&
                    (batchHit || intervalElapsed || Interlocked.CompareExchange(ref commitNeeded, 0, 1) == 1))
                {
                    if (CommitCompletedOffsets(consumer, partitionStates))
                    {
                        processedSinceLastCommit = 0;
                    }
                    if (intervalElapsed)
                        nextCommitAt = DateTime.UtcNow + commitInterval;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // graceful stop
        }
        finally
        {
            // Close writer so workers finish
            channel.Writer.TryComplete();
            try
            {
                if (workers.Count > 0)
                    await Task.WhenAll(workers);
            }
            catch
            {
                // swallow worker exceptions (already reported via HandleErrorAsync)
            }

            CommitCompletedOffsets(consumer, partitionStates, drainAll: false);

            consumer.Close();
        }
    }

    private static bool CommitCompletedOffsets(
        IConsumer<string, byte[]> consumer,
        ConcurrentDictionary<TopicPartition, PartitionState> partitionStates,
        bool drainAll = false)
    {
        var tpos = new List<TopicPartitionOffset>();

        foreach (var (tp, ps) in partitionStates)
        {
            if (ps.TryGetNextCommitOffset(out var offset, drainAll))
            {
                tpos.Add(new TopicPartitionOffset(tp, offset + 1));
            }
        }

        if (tpos.Count == 0)
        {
            return false;
        }

        try
        {
            consumer.Commit(tpos);
        }
        catch
        {
        }
        return true;
    }

    private sealed class PartitionState
    {
        private readonly SortedDictionary<long, bool> _pending = new();
        
        private long _lastCommitted = -1;
        
        private readonly object _lock = new();

        public void MarkProcessed(Offset offset)
        {
            lock (_lock)
            {
                _pending[(long)offset] = true;
            }
        }

        public void MarkFailed(Offset offset)
        {
            lock (_lock)
            {
                _pending[(long)offset] = false;
            }
        }

        public bool TryGetNextCommitOffset(out long commitOffset, bool drainAll)
        {
            lock (_lock)
            {
                long candidate = _lastCommitted;
                long probe = candidate + 1;
                bool advanced = false;

                while (_pending.TryGetValue(probe, out var ok))
                {
                    if (!ok)
                    {
                        break;
                    }
                    candidate = probe;
                    probe++;
                    advanced = true;
                }

                if (!advanced)
                {
                    commitOffset = -1;
                    return false;
                }

                for (long k = _lastCommitted + 1; k <= candidate; k++)
                    _pending.Remove(k);

                _lastCommitted = candidate;
                commitOffset = candidate;
                return true;
            }
        }
    }

    // Work item passed to workers
    private sealed record WorkItem(ConsumeResult<string, byte[]> Cr);
}

