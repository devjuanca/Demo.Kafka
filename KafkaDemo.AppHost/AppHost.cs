var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("Postgres")
    .WithDataVolume()
    .WithPgAdmin(opt=>opt.WithContainerName("ProstgresAdmin"));

var database = postgres.AddDatabase("StockPricesDatabase");

var kafka = builder.AddKafka("Kafka")
    .WithDataVolume()
    .WithKafkaUI(opt=>opt.WithContainerName("KafkaUI"));


builder.AddProject<Projects.KafkaDemo_Api>("Api")
    .WithReference(kafka)
    .WithReference(database)
    .WaitFor(kafka)
    .WaitFor(database)
    //.WithExplicitStart()
    .WithReplicas(2);

builder.Build().Run();
