---
name: showboat
description: Build reproducible Showboat demo documents that prove repo behavior with executable markdown. Use when asked to create demos, proof-of-work walkthroughs, or verification artifacts with showboat, especially for tests, CLIs, local services, Aspire AppHosts, and HTTP APIs.
---

# Showboat

Use this skill to create concise, reproducible demo documents with `showboat`.
Prefer executable evidence over narrative: each demo should prove one workflow by
recording the commands and stable outputs needed to replay it.

## Workflow

1. Confirm `showboat` is available with `uvx showboat --help` or `showboat --help`.
2. Put demo documents near the project they exercise, normally `demos/*.md`.
3. Initialize the document:
   ```bash
   uvx showboat init demos/<name>.md "<Readable Demo Title>"
   ```
4. Add context with `note`, then add proof with `exec`.
5. Use helper scripts for long-running or noisy workflows so the captured output is stable.
6. Use `showboat pop` immediately after failed or misleading entries.
7. Run `showboat verify <file>` when the document should be replayable without external setup surprises.

## Command Patterns

Use `uvx showboat` unless the repo pins a local `showboat` binary.

```bash
uvx showboat note demos/demo.md "This demonstrates the integration path."
uvx showboat exec demos/demo.md bash "uv run pytest -q"
uvx showboat verify demos/demo.md
uvx showboat extract demos/demo.md
```

For screenshots or browser-visible demos, prefer a browser automation skill or
tool to capture the image, then append it with:

```bash
uvx showboat image demos/demo.md '![Alt text](demos/screenshot.png)'
```

## Durable Outbox Integration Demos

For Aspire-backed demos in this repository:

- Keep the AppHost lifecycle inside a helper script.
- Set `ASPIRE_CONTAINER_RUNTIME=podman` unless the user asks otherwise.
- Stop any matching AppHost before starting a new run.
- Capture only stable proof lines in the Showboat output, such as resource name,
  final test state, exit code, and health statuses.
- Avoid capturing dashboard URLs, PIDs, ports chosen by Aspire, timestamps, or
  full logs unless the failure diagnosis requires them.

Read `references/aspire-integration-demos.md` before creating or changing an
Aspire-backed Showboat demo in this repo.
