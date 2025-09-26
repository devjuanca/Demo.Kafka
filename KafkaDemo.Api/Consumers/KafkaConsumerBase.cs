using Confluent.Kafka;
using System.Text.Json;

namespace KafkaDemo.Api.Consumers;

public abstract class KafkaConsumerBase<TEvent>(string GroupId, string Topic, IConfiguration configuration) : BackgroundService where TEvent : class
{
    protected abstract Task HandleMessageAsync(TEvent evt, CancellationToken ct);

    protected abstract Task HandleErrorAsync(TEvent? evt, Exception ex, CancellationToken ct);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeEventLoop(stoppingToken), stoppingToken);
    }

    private async Task ConsumeEventLoop(CancellationToken cancellationToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = configuration.GetConnectionString("kafka"),
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        consumer.Subscribe(Topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var cr = consumer.Consume(cancellationToken);

                var evt = JsonSerializer.Deserialize<TEvent>(cr.Message.Value);

                try
                {
                    if (evt != null)
                    {
                        await HandleMessageAsync(evt, cancellationToken);
                    }
                }
                catch (ConsumeException ex)
                {
                    await HandleErrorAsync(evt, ex, cancellationToken);
                }
            }
        }
        finally
        {
            consumer.Close();
        }
    }
}
