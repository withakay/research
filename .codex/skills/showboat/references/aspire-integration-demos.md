# Aspire Integration Demos

Use this reference when a Showboat demo needs to prove an Aspire-managed
integration path in this repository.

## Stable Demo Shape

Create one demo per integration surface:

- `durable-outbox-python/demos/aspire-azurite-kafka.md`
- `durable-outbox-fastapi/demos/aspire-http-publisher.md`

Each demo should:

1. State the contract being proven in a short note.
2. Run one helper script with `uvx showboat exec`.
3. Print stable key-value evidence only.
4. Leave the AppHost stopped when the command exits.

## Helper Script Contract

The helper script should:

- run from the package root,
- stop any matching AppHost before starting,
- start `aspire run --non-interactive --nologo --apphost <project>` in the background,
- poll `aspire ps --format Json --resources`,
- wait for the integration test executable resource to finish,
- print stable evidence lines,
- stop the AppHost in a trap,
- exit non-zero if the integration test resource exits non-zero.

Expected stable evidence:

```text
demo=<name>
apphost=<project>
container_runtime=podman
integration_resource=<resource display name>
integration_state=Finished
integration_exit_code=0
resource_health.<resource>=Healthy
```

Do not record full Aspire logs in the Showboat document during successful runs.
If a run fails, inspect logs separately, fix the cause, and use `showboat pop`
before recording the corrected command.
