"""--- Import start ---"""

from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path

"""--- Import end ---"""

"""--- Config start ---"""
HERE = Path(__file__).resolve().parent
PYTHON = sys.executable

# Order matters: subscribers first, publisher (data) last
MODULES: list[tuple[str, Path]] = [
    ("optimization", HERE / "optimization" / "main.py"),
    ("forecast", HERE / "forecast" / "main.py"),
    ("risk", HERE / "risk" / "main.py"),
    ("data", HERE / "data" / "main.py"),
]

SHUTDOWN_TIMEOUT_S = 10.0
"""--- Config end ---"""


def spawn_modules() -> list[tuple[str, subprocess.Popen]]:
    """Start each module in its own session so terminal Ctrl+C reaches only the runner."""
    procs: list[tuple[str, subprocess.Popen]] = []
    for name, path in MODULES:
        print(f"[runner] starting {name}: {path}")
        procs.append(
            (name, subprocess.Popen([PYTHON, str(path)], start_new_session=True))
        )
        time.sleep(0.4)
    return procs


def terminate_all(procs: list[tuple[str, subprocess.Popen]]) -> None:
    """SIGTERM every still-running child, then SIGKILL any that overstay SHUTDOWN_TIMEOUT_S."""
    for _, p in procs:
        if p.poll() is None:
            try:
                p.terminate()
            except ProcessLookupError:
                pass
    deadline = time.monotonic() + SHUTDOWN_TIMEOUT_S
    for name, p in procs:
        try:
            p.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            print(
                f"[runner] {name} did not exit after {SHUTDOWN_TIMEOUT_S:.0f}s, killing"
            )
            p.kill()
            try:
                p.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                pass


def main() -> None:
    """Spawn all modules and clean up on signal or child death."""
    procs = spawn_modules()
    shutting_down = False

    def shutdown(*_: object) -> None:
        nonlocal shutting_down
        if shutting_down:
            return
        shutting_down = True
        print("\n[runner] shutting down modules")
        terminate_all(procs)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while True:
            for name, p in procs:
                if p.poll() is not None:
                    print(f"[runner] {name} exited with code {p.returncode}")
                    shutdown()
            time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
