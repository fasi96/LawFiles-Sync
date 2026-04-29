"""
log.py — structured logging that's readable in Vercel's logs panel.

Vercel collects stdout/stderr per invocation. Each line we emit shows up as
one log row. JSON-per-line keeps it grep-friendly while staying human-readable.
"""

import json
import os
import sys
import time


_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}


def _level_threshold() -> int:
    return _LEVELS.get(os.environ.get("LOG_LEVEL", "INFO").upper(), 20)


def _emit(level: str, msg: str, **fields):
    if _LEVELS[level] < _level_threshold():
        return
    record = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "level": level, "msg": msg}
    if fields:
        record.update(fields)
    stream = sys.stderr if level in ("WARNING", "ERROR") else sys.stdout
    print(json.dumps(record, default=str), file=stream, flush=True)


def debug(msg: str, **fields):   _emit("DEBUG", msg, **fields)
def info(msg: str, **fields):    _emit("INFO", msg, **fields)
def warning(msg: str, **fields): _emit("WARNING", msg, **fields)
def error(msg: str, **fields):   _emit("ERROR", msg, **fields)
