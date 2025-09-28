using EasyServiceRegister.Attributes;
using System.Text.Json;

namespace KafkaDemo.Api.Services;

[RegisterAsSingleton]
public class KafkaProducer(IProducer<string, byte[]> producer)
{
    private static readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

    public async Task<PersistenceStatus> ProduceAsync<T>(string topic, string key, T value, CancellationToken ct = default)
    {
        var message = new Message<string, byte[]>
        {
            Key = key,
            Value = JsonSerializer.SerializeToUtf8Bytes(value, _jsonOptions)
        };

        var deliveryResult = await producer.ProduceAsync(topic, message, ct);

        return deliveryResult.Status;
    }
}