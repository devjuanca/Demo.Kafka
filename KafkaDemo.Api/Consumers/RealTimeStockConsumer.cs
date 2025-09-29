namespace KafkaDemo.Api.Consumers;

public class RealtimeStockPriceConsumer(IConfiguration configuration, ILogger<RealtimeStockPriceConsumer> logger,
    EventStreamService<StockPriceChangedEvent> streamService)
    : KafkaConsumerBase<StockPriceChangedEvent>(
        groupId:"realtime-stock-group", 
        topic:"stock-prices",
        connectionString: configuration.GetConnectionString("kafka") ?? "", 
        logger: logger)
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