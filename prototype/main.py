"""--- Import start ---"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from _logging import get_logger, run_module

"""--- Import end ---"""

"""--- Config start ---"""
HERE = Path(__file__).resolve().parent
PYTHON = sys.executable

# Order matters: subscribers first, publisher (data) last
MODULES: list[tuple[str, Path]] = [
    ("opt", HERE / "optimization" / "main.py"),
    ("forecast", HERE / "forecast" / "main.py"),
    ("risk", HERE / "risk" / "main.py"),
    ("data", HERE / "data" / "main.py"),
]

SHUTDOWN_TIMEOUT_S = 10.0
SPAWN_STAGGER_S = 0.4
"""--- Config end ---"""

log = get_logger("runner")


def _child_env() -> dict[str, str]:
    """Return an env that lets every child import the shared _logging module."""
    env = os.environ.copy()
    extra = str(HERE)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{extra}{os.pathsep}{existing}" if existing else extra
    return env


def spawn_modules() -> list[tuple[str, subprocess.Popen]]:
    """Start each module in its own session so terminal Ctrl+C reaches only the runner."""
    procs: list[tuple[str, subprocess.Popen]] = []
    env = _child_env()
    for name, path in MODULES:
        with log.pipeline("spawn", module=name, script=str(path.relative_to(HERE))):
            try:
                p = subprocess.Popen(
                    [PYTHON, str(path)],
                    start_new_session=True,
                    env=env,
                )
            except OSError as e:
                log.error(
                    "spawn failed",
                    module=name,
                    error_type=type(e).__name__,
                    error=str(e),
                )
                raise
            log.info("spawned", module=name, pid=p.pid)
            procs.append((name, p))
        time.sleep(SPAWN_STAGGER_S)
    return procs


def terminate_all(procs: list[tuple[str, subprocess.Popen]]) -> None:
    """SIGTERM every still-running child, then SIGKILL any that overstay SHUTDOWN_TIMEOUT_S."""
    with log.pipeline("shutdown", timeout_s=SHUTDOWN_TIMEOUT_S):
        for name, p in procs:
            if p.poll() is None:
                log.info("sigterm", module=name, pid=p.pid)
                try:
                    p.terminate()
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + SHUTDOWN_TIMEOUT_S
        for name, p in procs:
            try:
                code = p.wait(timeout=max(0.0, deadline - time.monotonic()))
                log.info("exited", module=name, pid=p.pid, code=code)
            except subprocess.TimeoutExpired:
                log.warn(
                    "kill after timeout",
                    module=name,
                    pid=p.pid,
                    timeout_s=SHUTDOWN_TIMEOUT_S,
                )
                p.kill()
                try:
                    p.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    log.error("kill timed out", module=name, pid=p.pid)


def _supervise() -> None:
    """Spawn the pipeline, supervise it, tear it down on exit."""
    procs = spawn_modules()
    shutting_down = False

    def shutdown(*_: object) -> None:
        nonlocal shutting_down
        if shutting_down:
            return
        shutting_down = True
        log.info("shutdown requested")
        terminate_all(procs)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info("supervising", modules=[n for n, _ in procs])
    try:
        while True:
            for name, p in procs:
                if p.poll() is not None:
                    level = log.error if p.returncode else log.info
                    level(
                        "child exited",
                        module=name,
                        pid=p.pid,
                        code=p.returncode,
                    )
                    shutdown()
            time.sleep(0.5)
    except KeyboardInterrupt:
        shutdown()


def main() -> None:
    _supervise()


if __name__ == "__main__":
    run_module("runner", main)
