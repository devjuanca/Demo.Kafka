using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace KafkaDemo.Api.Entities;

[Table("stock_price_snapshot")]
public class StockPriceSnapshot
{
    [Key]
    public long Id { get; set; }

    [Required]
    [MaxLength(16)]
    public string Symbol { get; set; } = default!;

    [Column(TypeName = "numeric(18,4)")]
    public decimal Price { get; set; }

    [Column(TypeName = "numeric(18,4)")]
    public decimal Change { get; set; }

    [Column(TypeName = "numeric(18,4)")]
    public decimal ChangePercent { get; set; }

    public DateTime UtcTimestamp { get; set; }
}
