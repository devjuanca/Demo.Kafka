var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("Postgres")
    .WithDataVolume()
    .WithPgAdmin(opt => opt.WithContainerName("ProstgresAdmin"));

var database = postgres.AddDatabase("StockPricesDatabase");

var kafka = builder.AddKafka("Kafka")
    .WithDataVolume()
    .WithKafkaUI(opt => opt.WithContainerName("KafkaUI"));

var stockFakeApi = builder.AddProject<Projects.KafkaDemo_StockFakeApi>("StockApi")
    .WithReference(kafka)
    .WaitFor(kafka)
    .WithReplicas(1);

var api = builder.AddProject<Projects.KafkaDemo_Api>("Api")
    .WithReference(kafka)
    .WithReference(database)
    .WithReference(stockFakeApi)
    .WaitFor(stockFakeApi)
    .WaitFor(kafka)
    .WaitFor(database)
    .WithReplicas(2);

var blazorServer = builder.AddProject<Projects.KafkaDemo_BlazorServer>("Blazor")
    .WithReference(api)
    .WaitFor(api);

builder.AddProject<Projects.KafkaDemo_StockFakeApi>("kafkademo-stockfakeapi");

builder.Build().Run();
