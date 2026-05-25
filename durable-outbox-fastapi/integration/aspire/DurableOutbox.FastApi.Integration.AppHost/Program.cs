using System.IO;

var builder = DistributedApplication.CreateBuilder(args);

var storage = builder.AddAzureStorage("storage")
    .RunAsEmulator();
var blobs = storage.AddBlobs("blobs");

var kafka = builder.AddKafka("kafka")
    .WithKafkaUI();

var packageRoot = Path.GetFullPath(Path.Combine(builder.AppHostDirectory, "..", "..", ".."));

var api = builder.AddExecutable(
        "durable-outbox-fastapi",
        "uv",
        packageRoot,
        "run",
        "uvicorn",
        "durable_outbox_fastapi.app:create_app",
        "--factory",
        "--host",
        "127.0.0.1",
        "--port",
        "18088")
    .WithHttpEndpoint(port: 18088, targetPort: 18088, name: "http", isProxied: false)
    .WithReference(blobs)
    .WithReference(kafka)
    .WaitFor(blobs)
    .WaitFor(kafka)
    .WithEnvironment("DURABLE_OUTBOX_AZURITE_CONTAINER", "durable-outbox-fastapi");

builder.AddExecutable(
        "durable-outbox-fastapi-integration-tests",
        "uv",
        packageRoot,
        "run",
        "pytest",
        "-m",
        "integration",
        "tests/integration")
    .WithReference(blobs)
    .WithReference(kafka)
    .WaitFor(api)
    .WithEnvironment("DURABLE_OUTBOX_FASTAPI_BASE_URL", "http://127.0.0.1:18088")
    .WithEnvironment("DURABLE_OUTBOX_AZURITE_CONTAINER", "durable-outbox-fastapi");

builder.Build().Run();
