using KafkaDemo.Api.Dtos;

namespace KafkaDemo.Api.Services;

public sealed class StockPriceService(KafkaProducer producer, ILogger<StockPriceService> logger) : IHostedService, IDisposable
{
    private readonly TimeSpan _interval = TimeSpan.FromMilliseconds(2000);
    private readonly CancellationTokenSource _cts = new();

    private readonly List<StockDefinition> _stocks =
    [
        new("MSFT", 420m),
        new("AAPL", 210m),
        new("GOOG", 3100m),
        new("AMZN", 180m),
        new("NVDA", 1300m),
        new("META", 520m)
    ];

    public Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("EventsProducerService started");
        _ = Task.Run(() => StockPriceSimulator(cancellationToken), cancellationToken);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("EventsProducerService stopping...");
        _cts.Cancel();
        return Task.CompletedTask;
    }

    private async Task StockPriceSimulator(CancellationToken externalToken)
    {
        // Tunables for price movement behavior
        const double largeMoveChance = 0.05;     // 5% of the time emit a 5% jump/drop
        const decimal largeMovePct = 0.05m;      // 5% spike magnitude
        const decimal regularMovePct = 0.02m;    // Normal max 2% drift per tick

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, externalToken);

        var current = _stocks.ToDictionary(s => s.Symbol, s => s.BasePrice);

        while (!linked.Token.IsCancellationRequested)
        {
            foreach (var stock in _stocks)
            {
                var oldPrice = current[stock.Symbol];

                decimal newPrice;

                if (Random.Shared.NextDouble() < largeMoveChance)
                {
                    // Force an exact +/-5% move
                    var direction = Random.Shared.Next(2) == 0 ? -1m : 1m;
                    newPrice = Math.Max(0.01m, oldPrice + oldPrice * largeMovePct * direction);
                }
                else
                {
                    // Regular small drift within +/-2%
                    var maxDelta = oldPrice * regularMovePct;
                    var delta = (decimal)(Random.Shared.NextDouble() * (double)maxDelta * 2) - maxDelta;
                    newPrice = Math.Max(0.01m, oldPrice + delta);
                }

                current[stock.Symbol] = newPrice;

                var change = newPrice - oldPrice;
                var changePct = oldPrice == 0 ? 0 : change / oldPrice * 100m;

                var evt = new StockPriceChangedEvent(
                    stock.Symbol,
                    decimal.Round(newPrice, 2),
                    decimal.Round(change, 2),
                    decimal.Round(changePct, 2),
                    DateTime.UtcNow
                );

                await producer.ProduceAsync("stock-prices", stock.Symbol, evt, linked.Token);
            }

            await Task.Delay(_interval, linked.Token);
        }
    }

    public void Dispose()
    {
        _cts.Dispose();
    }
}
