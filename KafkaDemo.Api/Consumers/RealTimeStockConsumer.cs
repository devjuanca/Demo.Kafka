using KafkaDemo.Api.Dtos;
using KafkaDemo.Api.Services;

namespace KafkaDemo.Api.Consumers;

public class RealtimeStockPriceConsumer(
    IConfiguration configuration,
    ILogger<RealtimeStockPriceConsumer> logger,
    EventStreamService<StockPriceChangedEvent> streamService)
    : KafkaConsumerBase<StockPriceChangedEvent>("realtime-stock-group", "stock-prices", configuration)
{
    protected override async Task HandleMessageAsync(StockPriceChangedEvent evt, CancellationToken ct)
    {
        await streamService.WriteAsync(evt, ct);

        logger.LogInformation("Streamed price update {Symbol} {Price}", evt.Symbol, evt.Price);
    }

    protected override async Task HandleErrorAsync(StockPriceChangedEvent? evt, Exception ex, CancellationToken ct)
    {
        logger.LogError(ex, "Error in realtime stock consumer");

        await Task.CompletedTask;
    }
}