namespace KafkaDemo.Api.Dtos;

public sealed record StockPriceChangedEvent(
    string Symbol,
    decimal Price,
    decimal Change,
    decimal ChangePercent,
    DateTime UtcTimestamp
);
