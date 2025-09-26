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

        var latestPrice = await context.StockPrices
            .Where(s => s.Symbol == evt.Symbol)
            .OrderByDescending(s => s.UtcTimestamp)
            .Select(s => s.Price)
            .FirstOrDefaultAsync(ct);

        if (latestPrice != 0 && Math.Abs((evt.Price - latestPrice) / latestPrice) >= 0.05m) // If price changed by 5% or more
        {
            logger.LogWarning("Significant price change detected for {Symbol}: {OldPrice} -> {NewPrice} ({ChangePct}%)", evt.Symbol, latestPrice, evt.Price, evt.ChangePercent);
        }
        
    }

    protected override async Task HandleErrorAsync(StockPriceChangedEvent? evt, Exception ex, CancellationToken ct)
    {
        logger.LogError(ex, "Error consuming stock price event");

        await kafkaProducer.ProduceAsync("stock-prices-error", evt?.Symbol ?? "unknown", new { Consumer = nameof(AlertStockPriceConsumer), Error = ex.Message, Event = evt }, ct);
    }
}
