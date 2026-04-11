"""Structured logger shared by every prototype module.

Output is a single line per event so it stays greppable and parseable by
both humans and LLM agents. Format:

    TS  LEVEL  module         stage                 message  k=v k=v

Environment overrides:
    MAPO_LOG_FORMAT=json   emit newline-delimited JSON instead of text
    MAPO_LOG_LEVEL=DEBUG   raise the verbosity floor (default INFO)
    NO_COLOR=1             disable ANSI colors even on a TTY
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40, "FATAL": 50}
_MIN_LEVEL = _LEVELS.get(os.environ.get("MAPO_LOG_LEVEL", "INFO").upper(), 20)
_JSON_MODE = os.environ.get("MAPO_LOG_FORMAT", "text").lower() == "json"
_USE_COLOR = sys.stdout.isatty() and not os.environ.get("NO_COLOR")

_LEVEL_COLOR = {
    "DEBUG": "\033[90m",
    "INFO": "\033[36m",
    "WARN": "\033[33m",
    "ERROR": "\033[31m",
    "FATAL": "\033[1;91m",
}
_MODULE_COLOR = {
    "runner": "\033[95m",
    "data": "\033[94m",
    "forecast": "\033[92m",
    "risk": "\033[93m",
    "opt": "\033[96m",
    "backtest": "\033[97m",
}
_RESET = "\033[0m"


def _paint(s: str, color: str) -> str:
    return f"{color}{s}{_RESET}" if _USE_COLOR and color else s


def _now() -> str:
    n = datetime.now(timezone.utc)
    return n.strftime("%Y-%m-%dT%H:%M:%S.") + f"{n.microsecond // 1000:03d}Z"


def _format_value(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.4g}"
    s = str(v)
    if any(c in s for c in " \t\"="):
        return json.dumps(s)
    return s


class Logger:
    """Per-module logger with a pipeline-stage stack.

    Use ``log.pipeline("stage.name")`` as a context manager to mark the start
    and end of a pipeline step; any log events emitted inside the block are
    tagged with the composed stage name, and the step's duration is reported
    on exit (or the exception is captured with a traceback on failure).
    """

    __slots__ = ("module", "_stages")

    def __init__(self, module: str) -> None:
        self.module = module
        self._stages: list[str] = []

    @property
    def stage(self) -> str:
        return ".".join(self._stages) if self._stages else ""

    @contextmanager
    def pipeline(self, name: str, **fields: Any) -> Iterator[None]:
        self._stages.append(name)
        self.info("start", **fields)
        t0 = time.monotonic()
        try:
            yield
        except BaseException as e:
            dur_ms = (time.monotonic() - t0) * 1000.0
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                self.info(
                    "interrupted",
                    reason=type(e).__name__,
                    duration_ms=f"{dur_ms:.1f}",
                )
            else:
                self.exception(
                    "failed",
                    error_type=type(e).__name__,
                    duration_ms=f"{dur_ms:.1f}",
                )
            raise
        else:
            dur_ms = (time.monotonic() - t0) * 1000.0
            self.info("done", duration_ms=f"{dur_ms:.1f}")
        finally:
            self._stages.pop()

    def _emit(
        self,
        level: str,
        msg: str,
        fields: dict[str, Any],
        block: list[str] | None = None,
    ) -> None:
        if _LEVELS[level] < _MIN_LEVEL:
            return
        ts = _now()
        stream = sys.stderr if _LEVELS[level] >= 30 else sys.stdout
        if _JSON_MODE:
            record = {
                "ts": ts,
                "level": level,
                "module": self.module,
                "stage": self.stage,
                "msg": msg,
                **fields,
            }
            if block:
                record["block"] = block
            print(json.dumps(record, default=str), file=stream, flush=True)
            return
        kv = "  ".join(f"{k}={_format_value(v)}" for k, v in fields.items())
        lvl_s = _paint(f"{level:<5}", _LEVEL_COLOR.get(level, ""))
        mod_s = _paint(f"{self.module:<8}", _MODULE_COLOR.get(self.module, ""))
        stage_s = _paint(f"{self.stage:<22}", _MODULE_COLOR.get(self.module, ""))
        line = f"{ts}  {lvl_s}  {mod_s}  {stage_s}  {msg}"
        if kv:
            line += f"  {kv}"
        print(line, file=stream, flush=True)
        if block:
            gutter = _paint("  \u2502 ", "\033[90m")
            for bln in block:
                print(f"{gutter}{bln}", file=stream, flush=True)

    def table(
        self,
        msg: str,
        rows: list[tuple[str, ...]],
        *,
        headers: tuple[str, ...] | None = None,
        level: str = "INFO",
        **fields: Any,
    ) -> None:
        """Emit an INFO line plus an indented aligned block of ``rows``.

        ``rows`` is a list of tuples of stringifiable values. When ``headers``
        is given, they're rendered above a separator. Column widths auto-size
        to the longest value. In JSON mode the rows are attached verbatim.
        """
        str_rows = [tuple(str(c) for c in r) for r in rows]
        if headers is not None:
            str_rows_with_hdr = [tuple(str(h) for h in headers), *str_rows]
        else:
            str_rows_with_hdr = str_rows
        if not str_rows_with_hdr:
            self._emit(level, msg, fields)
            return
        cols = len(str_rows_with_hdr[0])
        widths = [0] * cols
        for r in str_rows_with_hdr:
            for i, c in enumerate(r):
                if len(c) > widths[i]:
                    widths[i] = len(c)
        lines: list[str] = []
        if headers is not None:
            lines.append(
                "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
            )
            lines.append("  ".join("-" * widths[i] for i in range(cols)))
        for r in str_rows:
            padded = []
            for i, c in enumerate(r):
                try:
                    float(c)
                    padded.append(c.rjust(widths[i]))
                except ValueError:
                    padded.append(c.ljust(widths[i]))
            lines.append("  ".join(padded))
        self._emit(level, msg, fields, block=lines)

    def debug(self, msg: str, **fields: Any) -> None:
        self._emit("DEBUG", msg, fields)

    def info(self, msg: str, **fields: Any) -> None:
        self._emit("INFO", msg, fields)

    def warn(self, msg: str, **fields: Any) -> None:
        self._emit("WARN", msg, fields)

    def error(self, msg: str, **fields: Any) -> None:
        self._emit("ERROR", msg, fields)

    def fatal(self, msg: str, **fields: Any) -> None:
        self._emit("FATAL", msg, fields)

    def exception(self, msg: str, **fields: Any) -> None:
        """Emit an ERROR event and attach the current exception traceback."""
        self._emit("ERROR", msg, fields)
        tb = traceback.format_exc().rstrip()
        if tb and tb != "NoneType: None":
            prefix = _paint("  \u2502 ", "\033[31m")
            for ln in tb.splitlines():
                print(f"{prefix}{ln}", file=sys.stderr, flush=True)


def get_logger(module: str) -> Logger:
    return Logger(module)


def run_module(module: str, main_fn: Any) -> None:
    """Entry-point wrapper used by every module's ``if __name__ == '__main__'``.

    Guarantees that:
      * module start/stop events are logged with the pid
      * KeyboardInterrupt exits cleanly (code 0)
      * any unhandled exception is emitted as a structured ERROR with
        traceback and the process exits with code 1
    """
    log = get_logger(module)
    log.info("module up", pid=os.getpid())
    try:
        main_fn()
    except KeyboardInterrupt:
        log.info("module interrupted", signal="SIGINT")
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
        if code != 0:
            log.warn("module exiting", code=code)
        raise
    except BaseException:
        log.exception("module crashed")
        sys.exit(1)
    finally:
        log.info("module down")
