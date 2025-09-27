using Confluent.Kafka;
using EasyServiceRegister;
using KafkaDemo.Api.Consumers;
using KafkaDemo.Api.Dtos;
using KafkaDemo.Api.Persistence;
using KafkaDemo.Api.Services;
using KafkaDemo.ServiceDefaults;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

var (configuration, environment) = (builder.Configuration, builder.Environment);

builder.Services.AddServices(typeof(Program).Assembly);

builder.Services.AddDbContextFactory<StocksDbContext>(options =>
{
    var conn = configuration.GetConnectionString("StockPricesDatabase");

    options.UseNpgsql(conn, npgsql =>
    {
        npgsql.MigrationsHistoryTable("__ef_migrations_history", "public");
    });
});

builder.Services.AddSingleton<IProducer<string, string>>(_ =>
{
    var producerConfig = new ProducerConfig
    {
        BootstrapServers = configuration.GetConnectionString("kafka")
    };

    return new ProducerBuilder<string, string>(producerConfig).Build();
});

builder.Services.AddHostedService<StockPriceService>();

builder.Services.AddHostedService<StoreStockPriceConsumer>();

builder.Services.AddHostedService<AlertStockPriceConsumer>();

builder.Services.AddHostedService<RealtimeStockPriceConsumer>();

builder.AddServiceDefaults();

builder.Services.AddOpenApi();

builder.Services.AddCors(options =>
{
    options.AddPolicy("allow-any", policy =>
    {
        policy.AllowAnyHeader().AllowAnyMethod().AllowCredentials();
    });
});

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var factory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<StocksDbContext>>();

    await using var db = await factory.CreateDbContextAsync();

    await db.Database.MigrateAsync();
}

app.MapDefaultEndpoints();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.UseCors("blazor");

app.MapGet("/stock-prices/live", (EventStreamService<StockPriceChangedEvent> streamService, CancellationToken cancellationToken) =>
{
    var streamEvents = streamService.ReadAllAsync(cancellationToken);

    return TypedResults.ServerSentEvents(streamEvents, eventType: "stockPriceChanged");
});


app.Run();