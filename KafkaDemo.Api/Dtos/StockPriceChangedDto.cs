namespace KafkaDemo.Api.Dtos;

public sealed record StockPriceChangedDto(
    string Symbol,
    decimal Price,
    decimal Change,
    decimal ChangePercent,
    DateTime UtcTimestamp
);
