using KafkaDemo.Api.Entities;

namespace KafkaDemo.Api.Consumers;

public class StoreStockPriceConsumer(
    IDbContextFactory<StocksDbContext> contextFactory,
    ILogger<StoreStockPriceConsumer> logger,
    IConfiguration configuration,
    KafkaProducer kafkaProducer) : KafkaConsumerBase<StockPriceChangedEvent>("store-stock-price-group", "stock-prices", configuration)
{
    protected override async Task HandleMessageAsync(StockPriceChangedEvent evt, CancellationToken ct)
    {
        using var context = await contextFactory.CreateDbContextAsync(ct);

        var entity = new StockPriceSnapshot
        {
            Symbol = evt.Symbol,
            Price = evt.Price,
            Change = evt.Change,
            ChangePercent = evt.ChangePercent,
            UtcTimestamp = evt.UtcTimestamp
        };

        context.StockPrices.Add(entity);

        await context.SaveChangesAsync(ct);

        logger.LogInformation("Persisted price {Symbol} {Price} ({ChangePct}%)", evt.Symbol, evt.Price, evt.ChangePercent);
    }

    protected override async Task HandleErrorAsync(StockPriceChangedEvent? evt, Exception ex, CancellationToken ct)
    {
        logger.LogError(ex, "Error consuming stock price event");

        var errorDto = new StockPriceErrorDto
        {
            Consumer = nameof(StoreStockPriceConsumer),
            Error = ex.Message,
            Event = evt
        };
        await kafkaProducer.ProduceAsync("stock-prices-error", evt?.Symbol ?? "unknown", errorDto, ct);
    }
}