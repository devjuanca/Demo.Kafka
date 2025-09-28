using KafkaDemo.Api.Entities;

namespace KafkaDemo.Api.Persistence;

public class StocksDbContext(DbContextOptions<StocksDbContext> options) : DbContext(options)
{
    public DbSet<StockPriceSnapshot> StockPrices => Set<StockPriceSnapshot>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<StockPriceSnapshot>(b =>
        {
            b.HasIndex(x => x.Symbol);
            b.HasIndex(x => x.UtcTimestamp);
        });
    }
}
