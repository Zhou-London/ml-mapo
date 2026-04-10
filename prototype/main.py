"""Prototype runner.

Launches every standalone module of the demo quant system in its own
subprocess, wires their stdout/stderr through to the parent terminal, and
shuts them down cleanly on Ctrl+C or when any one of them exits.

Subscribers are started before the publisher so they have time to connect
their ZeroMQ SUB sockets before the data module begins publishing.
"""

from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
PYTHON = sys.executable

# Order matters: subscribers first, publisher (data) last.
MODULES: list[tuple[str, Path]] = [
    ("optimization", HERE / "optimization" / "main.py"),
    ("forecast", HERE / "forecast" / "main.py"),
    ("risk", HERE / "risk" / "main.py"),
    ("data", HERE / "data" / "main.py"),
]


def main() -> None:
    procs: list[tuple[str, subprocess.Popen]] = []
    shutting_down = False

    def shutdown(*_: object) -> None:
        nonlocal shutting_down
        if shutting_down:
            return
        shutting_down = True
        print("\n[runner] shutting down modules")
        for name, p in procs:
            if p.poll() is None:
                p.terminate()
        deadline = time.monotonic() + 5.0
        for name, p in procs:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                print(f"[runner] {name} did not exit, killing")
                p.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for name, path in MODULES:
        print(f"[runner] starting {name}: {path}")
        p = subprocess.Popen([PYTHON, str(path)])
        procs.append((name, p))
        time.sleep(0.4)

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
