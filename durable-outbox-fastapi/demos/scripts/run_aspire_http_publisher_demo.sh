#!/usr/bin/env bash
set -euo pipefail

package_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
aspire_dir="${package_dir}/integration/aspire"
apphost="DurableOutbox.FastApi.Integration.AppHost/DurableOutbox.FastApi.Integration.AppHost.csproj"
test_resource="durable-outbox-fastapi-integration-tests"
api_resource="durable-outbox-fastapi"
runtime="${ASPIRE_CONTAINER_RUNTIME:-podman}"
log_dir="${package_dir}/demos/.tmp"
mkdir -p "${log_dir}"
run_log="$(mktemp "${log_dir}/aspire-http-publisher.log.XXXXXX")"

cleanup() {
    (
        cd "${aspire_dir}"
        aspire stop --non-interactive --nologo --apphost "${apphost}" >/dev/null 2>&1 || true
    )
    if [[ -n "${run_pid:-}" ]]; then
        wait "${run_pid}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

(
    cd "${aspire_dir}"
    aspire stop --non-interactive --nologo --apphost "${apphost}" >/dev/null 2>&1 || true
)

(
    cd "${aspire_dir}"
    ASPIRE_CONTAINER_RUNTIME="${runtime}" aspire run --non-interactive --nologo --apphost "${apphost}"
) >"${run_log}" 2>&1 &
run_pid=$!

python3 - "${aspire_dir}" "${apphost}" "${test_resource}" "${api_resource}" "${runtime}" "${run_log}" "${run_pid}" <<'PY'
import json
import os
import subprocess
import sys
import time
from pathlib import Path

aspire_dir = Path(sys.argv[1])
apphost = sys.argv[2]
test_resource = sys.argv[3]
api_resource = sys.argv[4]
runtime = sys.argv[5]
run_log = Path(sys.argv[6])
run_pid = int(sys.argv[7])
deadline = time.monotonic() + 300


def resources() -> list[dict[str, object]]:
    result = subprocess.run(
        ["aspire", "ps", "--non-interactive", "--nologo", "--format", "Json", "--resources"],
        cwd=aspire_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    start = result.stdout.find("[")
    if result.returncode != 0 or start == -1:
        return []
    apps = json.loads(result.stdout[start:])
    found: list[dict[str, object]] = []
    for app in apps:
        found.extend(app.get("resources", []))
    return found


while time.monotonic() < deadline:
    current = resources()
    if not current:
        try:
            os.kill(run_pid, 0)
        except ProcessLookupError:
            print("integration_state=AppHostExited")
            print(f"log={run_log}")
            sys.exit(1)
    by_display = {str(item.get("displayName")): item for item in current}
    test = by_display.get(test_resource)
    if test and test.get("state") == "Finished":
        exit_code = test.get("exitCode")
        print("demo=durable-outbox-fastapi-aspire-http-publisher")
        print(f"apphost={apphost}")
        print(f"container_runtime={runtime}")
        print(f"integration_resource={test_resource}")
        print("integration_state=Finished")
        print(f"integration_exit_code={exit_code}")
        for name in (api_resource, "blobs", "kafka"):
            resource = by_display.get(name)
            if resource is not None:
                print(f"resource_health.{name}={resource.get('healthStatus')}")
        sys.exit(0 if exit_code == 0 else 1)
    time.sleep(2)

print("integration_state=TimedOut")
print(f"log={run_log}")
sys.exit(1)
PY
