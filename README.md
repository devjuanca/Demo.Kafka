# KafkaDemo (Demo)

Demo .NET 10 Aspire solution showing Kafka + PostgreSQL + real‑time streaming.

## Projects
- `KafkaDemo.AppHost`: Aspire AppHost wiring infrastructure (Kafka broker, Postgres, services) and lifecycle.
- `KafkaDemo.Api`: Minimal API producing & consuming Kafka messages, EF Core (PostgreSQL) persistence, SSE endpoint `/stock-prices/live`.
  - Producers: publishes stock price change events (`IProducer<string,string>` via Confluent.Kafka)
  - Consumers: 
    - `StoreStockPriceConsumer`: persists prices
    - `AlertStockPriceConsumer`: (alerts logic placeholder if present)
    - `RealtimeStockPriceConsumer`: pushes events into in‑memory stream for Server Sent Events
- `KafkaDemo.BlazorServer`: Simple UI (server) consuming the API/SSE.
- `KafkaDemo.ServiceDefaults`: Cross‑cutting defaults (OpenTelemetry, health, service discovery, resilience).

## Main Concepts
- Kafka topics (e.g. `stock-prices`) for stock price changed events.
- Background hosted services simulate & consume price updates.
- EF Core migrations applied automatically at startup.
- Streaming to clients with SSE (no WebSockets needed) for real‑time price updates.
- Observability via OpenTelemetry (metrics + traces) pluggable exporter (OTLP if env vars set).

## Run (Dev)
Prereqs: .NET 10 SDK preview, Docker (for Kafka + Postgres via Aspire), optional: curl.

1. Restore & run AppHost (boots all):
   ```bash
   dotnet run -p KafkaDemo.AppHost
   ```
2. API will expose health at `/health` and SSE at `/stock-prices/live`.
3. Browse Blazor UI (see AppHost console for URL) to watch live prices.

## Sample SSE Test
```bash
curl -N https://localhost:***** /stock-prices/live
```
(Replace with actual HTTPS port; disable cert validation or use `--insecure` for local dev.)

## Configuration
Connection strings & Kafka bootstrap servers pulled from Aspire generated resources (see AppHost). Override via environment variables / user secrets if desired.

Key Setting | Purpose
------------|--------
`ConnectionStrings:StockPricesDatabase` | Postgres DB connection
`ConnectionStrings:kafka` | Kafka bootstrap servers
`OTEL_EXPORTER_OTLP_ENDPOINT` | Enables OTLP exporter when set

## Migrations
Applied automatically on API startup. For manual add:
```bash
dotnet ef migrations add <Name> -p KafkaDemo.Api -s KafkaDemo.Api
```

## Extending
- Add new consumer: inherit `KafkaConsumerBase<T>` and register as hosted service.
- Add new event type: create DTO, produce JSON, update consumers/UI.
- Switch to WebSockets: add endpoint & push from `EventStreamService`.

## Notes
- Simplified for demo; not production hardened (error handling, retries, schema registry, security minimized).
- Consider adding topic creation automation & partition config for scale.

## License
Demo code; MIT.
