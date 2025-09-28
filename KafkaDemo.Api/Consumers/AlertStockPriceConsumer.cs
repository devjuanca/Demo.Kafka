namespace KafkaDemo.Api.Consumers;

public class AlertStockPriceConsumer(
    IConfiguration configuration,
    ILogger<AlertStockPriceConsumer> logger,
    KafkaProducer kafkaProducer) : KafkaConsumerBase<StockPriceChangedEvent>("alert-stock-price-group", "stock-prices", configuration)
{
    protected override Task HandleMessageAsync(StockPriceChangedEvent evt, CancellationToken ct)
    {
        if (Math.Abs(evt.ChangePercent) >= 0.05m)
        {
            var direction = evt.ChangePercent > 0 ? "up" : "down";

            logger.LogWarning(
                "Significant price move {Direction} for {Symbol}: change {ChangePct}% (current price {CurrentPrice})",
                direction,
                evt.Symbol,
                evt.ChangePercent,
                evt.Price);
        }

        return Task.CompletedTask;
    }

    protected override async Task HandleErrorAsync(StockPriceChangedEvent? evt, Exception ex, CancellationToken ct)
    {
        logger.LogError(ex, "Error consuming stock price event");

        var errorDto = new StockPriceErrorDto
        {
            Consumer = nameof(AlertStockPriceConsumer),
            Error = ex.Message,
            Event = evt
        };

        await kafkaProducer.ProduceAsync("stock-prices-error", evt?.Symbol ?? "unknown", errorDto, ct);
    }
}
