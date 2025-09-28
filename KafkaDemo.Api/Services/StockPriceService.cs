namespace KafkaDemo.Api.Services;

public sealed class StockPriceService(IHttpClientFactory httpClientFactory, ILogger<StockPriceService> logger, KafkaProducer producer) : IHostedService
{
    private readonly TimeSpan _interval = TimeSpan.FromMilliseconds(2000);

    public Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("EventsProducerService started");

        _ = Task.Run(() => StockPriceSimulator(cancellationToken), cancellationToken);

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("EventsProducerService stopping...");

        return Task.CompletedTask;
    }

    private async Task StockPriceSimulator(CancellationToken externalToken)
    {
        var httpClient = httpClientFactory.CreateClient("StockApi");

        while (!externalToken.IsCancellationRequested)
        {
            var stocksPrices = await httpClient.GetFromJsonAsync<List<StockPriceChangedDto>>("/stocks", externalToken);

            if (stocksPrices == null || stocksPrices.Count == 0)
            {
                logger.LogWarning("No stock prices received from the API");
                await Task.Delay(_interval, externalToken);
                continue;
            }

            foreach (var stock in stocksPrices)
            { 
              var evt = new StockPriceChangedEvent(stock.Symbol, stock.Price, stock.Change, stock.ChangePercent, stock.UtcTimestamp);

                await producer.ProduceAsync("stock-prices", stock.Symbol, evt, externalToken);
            }

            await Task.Delay(_interval, externalToken);
        }
    }
}