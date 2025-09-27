using Confluent.Kafka;
using System.Text.Json;
using System.Threading.Channels;

namespace KafkaDemo.Api.Consumers;

public abstract class KafkaConsumerBase<TEvent>(
    string groupId,
    string topic,
    IConfiguration configuration,
    int maxParallelism = 10) : BackgroundService where TEvent : class
{
    private readonly ConsumerConfig _config = new()
    {
        BootstrapServers = configuration.GetConnectionString("kafka"),
        GroupId = groupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        AllowAutoCreateTopics = false,
        CancellationDelayMaxMs = 500
    };

    private readonly SemaphoreSlim _semaphore = new(maxParallelism);

    protected abstract Task HandleMessageAsync(TEvent evt, CancellationToken ct);

    protected abstract Task HandleErrorAsync(TEvent? evt, Exception ex, CancellationToken ct);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, string>(_config).Build();

        consumer.Subscribe(topic);

        var tasks = new List<Task>();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? cr;

                try
                {
                    cr = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    await HandleErrorAsync(null, ex, stoppingToken);

                    continue;
                }

                if (cr == null)
                {
                    continue;
                }

                await _semaphore.WaitAsync(stoppingToken);

                var task = Task.Run(async () =>
                {
                    TEvent? evt = null;
                    try
                    {
                        evt = JsonSerializer.Deserialize<TEvent>(cr.Message.Value);

                        if (evt != null)
                        {
                            await HandleMessageAsync(evt, stoppingToken);

                            consumer.Commit(cr);
                        }
                    }
                    catch (Exception ex)
                    {
                        await HandleErrorAsync(evt, ex, stoppingToken);
                    }
                    finally
                    {
                        _semaphore.Release();
                    }
                }, stoppingToken);

                tasks.Add(task);
            }
        }
        catch (OperationCanceledException)
        {
            // normal shutdown
        }
        finally
        {
            // Wait for pending tasks
            await Task.WhenAll(tasks);

            consumer.Close();
        }
    }
}
