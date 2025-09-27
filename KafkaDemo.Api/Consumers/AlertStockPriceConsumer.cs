using KafkaDemo.Api.Dtos;
using KafkaDemo.Api.Persistence;
using KafkaDemo.Api.Services;
using Microsoft.EntityFrameworkCore;

namespace KafkaDemo.Api.Consumers;

public class AlertStockPriceConsumer(
    IDbContextFactory<StocksDbContext> contextFactory, 
    IConfiguration configuration, 
    ILogger<AlertStockPriceConsumer> logger,
    KafkaProducer kafkaProducer) : KafkaConsumerBase<StockPriceChangedEvent>("alert-stock-price-group", "stock-prices", configuration)
{
    protected override async Task HandleMessageAsync(StockPriceChangedEvent evt, CancellationToken ct)
    {
        using var context = await contextFactory.CreateDbContextAsync(ct);

        var changePercent = await context.StockPrices
            .Where(s => s.Symbol == evt.Symbol)
            .OrderByDescending(s => s.UtcTimestamp)
            .Select(s => s.ChangePercent)
            .FirstOrDefaultAsync(ct);

        if (Math.Abs(changePercent) >= 0.05m)
        {
            var direction = changePercent > 0 ? "up" : "down";

            logger.LogWarning(
                "Significant price move {Direction} for {Symbol}: change {ChangePct}% (current price {CurrentPrice})",
                direction,
                evt.Symbol,
                changePercent,
                evt.Price);
        }
        
    }

    protected override async Task HandleErrorAsync(StockPriceChangedEvent? evt, Exception ex, CancellationToken ct)
    {
        logger.LogError(ex, "Error consuming stock price event");

        await kafkaProducer.ProduceAsync("stock-prices-error", evt?.Symbol ?? "unknown", new { Consumer = nameof(AlertStockPriceConsumer), Error = ex.Message, Event = evt }, ct);
    }
}
