using System.IO;

var builder = DistributedApplication.CreateBuilder(args);

var storage = builder.AddAzureStorage("storage")
    .RunAsEmulator();
var blobs = storage.AddBlobs("blobs");

var kafka = builder.AddKafka("kafka")
    .WithKafkaUI();

var packageRoot = Path.GetFullPath(Path.Combine(builder.AppHostDirectory, "..", "..", ".."));

builder.AddExecutable(
        "durable-outbox-integration-tests",
        "uv",
        packageRoot,
        "run",
        "--extra",
        "azure",
        "--extra",
        "kafka",
        "pytest",
        "-m",
        "integration",
        "tests/integration")
    .WithReference(blobs)
    .WithReference(kafka)
    .WaitFor(blobs)
    .WaitFor(kafka)
    .WithEnvironment("DURABLE_OUTBOX_AZURITE_CONTAINER", "durable-outbox")
    .WithEnvironment("DURABLE_OUTBOX_KAFKA_TOPIC", "durable-outbox-it");

builder.Build().Run();
