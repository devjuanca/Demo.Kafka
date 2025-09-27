using EasyServiceRegister.Attributes;
using System.Threading.Channels;

namespace KafkaDemo.Api.Services;


[RegisterAsSingleton]
public class EventStreamService<TEvent> where TEvent : class
{
    private readonly Channel<TEvent> _channel;

    public EventStreamService()
    {
        _channel = Channel.CreateBounded<TEvent>(new BoundedChannelOptions(100) 
        { 
           AllowSynchronousContinuations = false,
           FullMode = BoundedChannelFullMode.DropOldest
        });
    }

    public ValueTask WriteAsync(TEvent evt, CancellationToken cancellationToken = default)
    {
       return _channel.Writer.WriteAsync(evt, cancellationToken);
    }

    public IAsyncEnumerable<TEvent> ReadAllAsync(CancellationToken cancellationToken = default)
    {
       return _channel.Reader.ReadAllAsync(cancellationToken);
    }
}