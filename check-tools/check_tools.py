#!/usr/bin/env python3
"""Check whether common development tools are installed and report their versions."""

import shutil
import subprocess
import sys

# Each entry: (display name, binary name, version flag)
TOOLS = [
    ("Python", "python3", "--version"),
    ("pip", "pip3", "--version"),
    ("Node.js", "node", "--version"),
    ("npm", "npm", "--version"),
    ("Go", "go", "version"),
    ("Rust (rustc)", "rustc", "--version"),
    ("Cargo", "cargo", "--version"),
    ("dotnet", "dotnet", "--version"),
    ("Java", "java", "--version"),
    ("Git", "git", "--version"),
    ("Docker", "docker", "--version"),
    ("Make", "make", "--version"),
    ("CMake", "cmake", "--version"),
    ("GCC", "gcc", "--version"),
    ("Clang", "clang", "--version"),
    ("curl", "curl", "--version"),
    ("wget", "wget", "--version"),
    ("jq", "jq", "--version"),
    ("sqlite3", "sqlite3", "--version"),
]

GREEN = "\033[32m"
RED = "\033[31m"
RESET = "\033[0m"
BOLD = "\033[1m"


def check_tool(name: str, binary: str, flag: str) -> tuple[bool, str]:
    path = shutil.which(binary)
    if path is None:
        return False, ""
    try:
        result = subprocess.run(
            [binary, flag],
            capture_output=True,
            text=True,
            timeout=5,
        )
        version = (result.stdout or result.stderr).strip().split("\n")[0]
        return True, version
    except Exception:
        return True, "(installed but version unknown)"


def main() -> None:
    use_color = sys.stdout.isatty()

    def green(s: str) -> str:
        return f"{GREEN}{s}{RESET}" if use_color else s

    def red(s: str) -> str:
        return f"{RED}{s}{RESET}" if use_color else s

    def bold(s: str) -> str:
        return f"{BOLD}{s}{RESET}" if use_color else s

    print(bold("Development tool check"))
    print("=" * 40)

    found = 0
    missing = 0

    for name, binary, flag in TOOLS:
        ok, version = check_tool(name, binary, flag)
        if ok:
            found += 1
            status = green("OK")
            print(f"  {status}  {name}: {version}")
        else:
            missing += 1
            status = red("--")
            print(f"  {status}  {name}: not found")

    print("=" * 40)
    print(f"{found} found, {missing} not found")


if __name__ == "__main__":
    main()
