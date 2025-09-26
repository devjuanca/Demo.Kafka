using Confluent.Kafka;
using EasyServiceRegister.Attributes;
using System.Text.Json;

namespace KafkaDemo.Api.Services;

[RegisterAsSingleton]
public class KafkaProducer(IProducer<string, string> producer)
{
    public async Task ProduceAsync<T>(string topic, string key, T value, CancellationToken ct = default)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = JsonSerializer.Serialize(value)
        };

        await producer.ProduceAsync(topic, message, ct);
    }
}