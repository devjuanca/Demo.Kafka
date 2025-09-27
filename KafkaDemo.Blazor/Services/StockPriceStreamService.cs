using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaDemo.Blazor.Services;

public sealed class StockPriceStreamService
{
    private CancellationTokenSource? _cts;
    private Task? _readerTask;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public async Task StartAsync(Func<StockPriceChangedEvent, Task> onEvent, CancellationToken ct, string? filterSymbol = null)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _cts.Token;

        var baseUrl = Environment.GetEnvironmentVariable("API_BASE") ?? "https://localhost:7195"; // adjust if different
        var url = baseUrl.TrimEnd('/') + "/stock-prices/live";

        _readerTask = Task.Run(async () =>
        {
            try
            {
                using var client = new HttpClient();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
                using var response = await client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, token);
                response.EnsureSuccessStatusCode();
                await using var stream = await response.Content.ReadAsStreamAsync(token);
                using var reader = new StreamReader(stream);
                while (!reader.EndOfStream && !token.IsCancellationRequested)
                {
                    var line = await reader.ReadLineAsync();
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    if (line.StartsWith("data:", StringComparison.OrdinalIgnoreCase))
                    {
                        var json = line[5..].Trim();
                        if (json.Length == 0) continue;
                        var evt = JsonSerializer.Deserialize<StockPriceChangedEvent>(json, JsonOptions);
                        if (evt is null) continue;
                        if (filterSymbol == null || evt.Symbol == filterSymbol)
                            await onEvent(evt);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"SSE error: {ex.Message}");
            }
        }, token);

        await Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        try { _cts?.Cancel(); } catch {}
        if (_readerTask is not null)
        {
            try { await _readerTask; } catch { }
        }
        _cts?.Dispose();
        _cts = null;
    }

    public async Task RestartAsync(Func<StockPriceChangedEvent, Task> onEvent, CancellationToken ct, string? filterSymbol)
    {
        await StopAsync();
        await StartAsync(onEvent, ct, filterSymbol);
    }
}

public record StockPriceChangedEvent(string Symbol, decimal Price, decimal Change, decimal ChangePercent, DateTime UtcTimestamp);
