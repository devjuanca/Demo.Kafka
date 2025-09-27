using KafkaDemo.BlazorServer.Services;
using KafkaDemo.ServiceDefaults;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.Services.AddRazorPages();

builder.Services.AddServerSideBlazor();

builder.Services.AddSingleton<StockPriceStreamService>();

builder.Services.AddCors(o => o.AddPolicy("allow-any", p => p.AllowAnyHeader().AllowAnyMethod().AllowAnyOrigin()));

builder.Services.AddHttpClient("api", client =>
{
    client.BaseAddress = new Uri("http+https://api");
});

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
}

app.MapDefaultEndpoints();

app.UseStaticFiles();

app.UseRouting();

app.UseCors("allow-any");

app.MapBlazorHub();

app.MapFallbackToPage("/_Host");

app.Run();
