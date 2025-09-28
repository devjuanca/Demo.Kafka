using KafkaDemo.ServiceDefaults;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.Services.AddControllers();

builder.Services.AddOpenApi();

var app = builder.Build();

app.MapDefaultEndpoints();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapGet("/stocks", () => {

    List<StockPriceUpdateItem> results = [];

    List<StockDefinition> _stocks =
    [
        new("MSFT", 420m),
        new("AAPL", 210m),
        new("GOOG", 3100m),
        new("AMZN", 180m),
        new("NVDA", 1300m),
        new("META", 520m)
    ];

    // Tunables for price movement behavior
    const double largeMoveChance = 0.05;     // 5% of the time emit a 5% jump/drop

    const decimal largeMovePct = 0.05m;      // 5% spike magnitude

    const decimal regularMovePct = 0.02m;    // Normal max 2% drift per tick

    var current = _stocks.ToDictionary(s => s.Symbol, s => s.BasePrice);

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

        var evt = new StockPriceUpdateItem(
            stock.Symbol,
            decimal.Round(newPrice, 2),
            decimal.Round(change, 2),
            decimal.Round(changePct, 2),
            DateTime.UtcNow
        );

       results.Add(evt);
    }

    return results;
});

app.Run();

public sealed record StockDefinition(string Symbol, decimal BasePrice);

public sealed record StockPriceUpdateItem(
    string Symbol,
    decimal Price,
    decimal Change,
    decimal ChangePercent,
    DateTime UtcTimestamp
);