using System.Collections.Concurrent;
using System.Text.Json;

namespace KafkaDemo.Api.Consumers;

public abstract class KafkaConsumerBase<TEvent>(
    string groupId,
    string topic,
    string connectionString,
    ILogger<KafkaConsumerBase<TEvent>> logger,

    int maxParallelism = 1,
    int commitEvery = 10,              // commit every N messages
    TimeSpan? commitInterval = null    // or every some time
) : BackgroundService where TEvent : class
{
    private readonly ConsumerConfig _config = new()
    {
        BootstrapServers = connectionString,
        GroupId = groupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false, // we do it manually
        EnableAutoOffsetStore = false // we do it manually
    };

    private readonly TimeSpan _commitInterval = commitInterval ?? TimeSpan.FromSeconds(5);

    private readonly ConcurrentBag<Task> _runningTasks = [];

    private int _processedSinceCommit = 0;

    private DateTime _lastCommitTime = DateTime.UtcNow;

    protected abstract Task HandleMessageAsync(TEvent @event, CancellationToken cancellationToken);

    protected abstract Task HandleErrorAsync(TEvent? evt, Exception ex, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(_config).Build();

        consumer.Subscribe(topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string>? cr;

                try
                {
                    cr = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Consume error");

                    continue;
                }

                if (cr?.Message == null)
                {
                    continue;
                }

                var evt = JsonSerializer.Deserialize<TEvent>(cr.Message.Value, JsonSerializerOptions.Web);

                if (evt == null) continue;

                if (maxParallelism > 1)
                {
                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            await HandleMessageAsync(evt, stoppingToken);

                            // Store offset if success
                            consumer.StoreOffset(cr);

                            IncrementCommit(consumer);
                        }
                        catch (Exception ex)
                        {
                            //No store, the message will process again.
                            logger.LogError(ex, "Error processing message");

                           await HandleErrorAsync(evt, ex, stoppingToken);
                        }
                    }, stoppingToken);

                    _runningTasks.Add(task);
                }
                else
                {
                    try
                    {
                        await HandleMessageAsync(evt, stoppingToken);

                        consumer.StoreOffset(cr);

                        IncrementCommit(consumer);
                    }
                    catch (Exception ex)
                    {
                        //No store, the message will process again.
                        logger.LogError(ex, "Error processing message");

                        await HandleErrorAsync(evt, ex, stoppingToken);
                    }
                }

                // Clean completed Tasks
                while (_runningTasks.TryTake(out var finishedTask))
                {
                    if (!finishedTask.IsCompleted)
                    {
                        _runningTasks.Add(finishedTask); // no completed yet.
                    }
                }
            }
        }
        finally
        {
            Console.WriteLine("Closing consumer...");
            try
            {
                consumer.Commit(); // last commit
            }
            catch {  }

            consumer.Close();

            // wait for pending tasks
            await Task.WhenAll(_runningTasks);
        }
    }

    private void IncrementCommit(IConsumer<Ignore, string> consumer)
    {
        var count = Interlocked.Increment(ref _processedSinceCommit);

        var now = DateTime.UtcNow;

        if (count >= commitEvery || now - _lastCommitTime >= _commitInterval)
        {
            try
            {
                consumer.Commit();

                _processedSinceCommit = 0;

                _lastCommitTime = now;
            }
            catch (KafkaException ex)
            {
                logger.LogError(ex, "Commit failed");
            }
        }
    }
}