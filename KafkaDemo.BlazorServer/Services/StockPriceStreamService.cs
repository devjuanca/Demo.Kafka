using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaDemo.BlazorServer.Services;

public sealed class StockPriceStreamService(IHttpClientFactory httpClientFactory)
{
    private CancellationTokenSource? _cts;

    private Task? _readerTask;

    public async Task StartAsync(Func<StockPriceChangedEvent, Task> onEvent, string? filterSymbol = null, CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var token = _cts.Token;

        _readerTask = Task.Run(async () =>
        {
            try
            {
                var client = httpClientFactory.CreateClient("api");

                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));

                using var response = await client.GetAsync("/stock-prices/live", HttpCompletionOption.ResponseHeadersRead, token);

                response.EnsureSuccessStatusCode();

                await using var stream = await response.Content.ReadAsStreamAsync(token);

                using var reader = new StreamReader(stream);

                while (!token.IsCancellationRequested)
                {
                    var line = await reader.ReadLineAsync();

                    if (line is null)
                    {
                        break; // End of stream
                    }


                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }

                    if (line.StartsWith("data:", StringComparison.OrdinalIgnoreCase))
                    {
                        var json = line[5..].Trim();

                        if (json.Length == 0) continue;

                        var evt = JsonSerializer.Deserialize<StockPriceChangedEvent>(json, JsonSerializerOptions.Web);

                        if (evt is null)
                        {
                            continue;
                        }


                        if (filterSymbol == null || evt.Symbol == filterSymbol)
                        {
                            await onEvent(evt);
                        }
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
        try 
        { 
            _cts?.Cancel(); 
        } 
        catch { }

        if (_readerTask is not null)
        {
            try { await _readerTask; } catch { }
        }
        _cts?.Dispose();

        _cts = null;
    }

    public async Task RestartAsync(Func<StockPriceChangedEvent, Task> onEvent, string? filterSymbol, CancellationToken ct)
    {
        await StopAsync();

        await StartAsync(onEvent, filterSymbol, ct);
    }
}

public record StockPriceChangedEvent(string Symbol, decimal Price, decimal Change, decimal ChangePercent, DateTime UtcTimestamp);
