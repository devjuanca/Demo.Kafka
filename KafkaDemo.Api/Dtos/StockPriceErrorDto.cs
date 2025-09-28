namespace KafkaDemo.Api.Dtos;

public class StockPriceErrorDto
{
    public string Consumer { get; set; } = string.Empty;
    public string Error { get; set; } = string.Empty;
    public StockPriceChangedEvent? Event { get; set; }
}