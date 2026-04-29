"""
converter.py — sys.path shim for importing the LawFiles BCI converter.

The converter lives in a separate repo. We vendor it as a git submodule at
`vendor/LawFiles/`. For local dev you can override with the LAWFILES_PATH env
var (e.g. point at your existing clone at D:/LawFiles).

This module exposes the converter's public surface as a clean import:

    from lib.converter import (
        load_config, convert_row, BCIWriter,
        HeaderResolver, CSVRow,
        reset_link_counters,
    )
"""

import os
import sys


def _resolve_lawfiles_root() -> str:
    override = os.environ.get("LAWFILES_PATH", "").strip()
    if override:
        return override
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(here, "..", "vendor", "LawFiles"))


_LAWFILES_ROOT = _resolve_lawfiles_root()
_SRC = os.path.join(_LAWFILES_ROOT, "src")
_CONFIG = os.path.join(_LAWFILES_ROOT, "config")

if not os.path.isdir(_SRC):
    raise RuntimeError(
        f"LawFiles converter not found at {_LAWFILES_ROOT}. "
        f"Either add it as a git submodule at vendor/LawFiles, "
        f"or set LAWFILES_PATH to point at an existing clone."
    )

# Push the converter's src/ to the front of sys.path so its sibling imports
# (`from csv_reader import ...`, `from transforms import ...`) resolve.
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Re-export the converter's public API.
from convert import convert_row, load_config as _load_converter_config  # noqa: E402
from csv_reader import HeaderResolver, CSVRow, read_csv  # noqa: E402
from bci_writer import BCIWriter  # noqa: E402
from section_builders import _reset_link_counter, _reset_clink_counter  # noqa: E402


def load_converter_config():
    """
    Load field_mapping.json + defaults.json from the vendored LawFiles repo.
    Returns (mapping, defaults) — same shape as the converter's own load_config().
    """
    # The converter's load_config() resolves config dir relative to its own file,
    # which works fine because vendor/LawFiles/config/ exists alongside src/.
    return _load_converter_config()


def reset_link_counters():
    """
    Reset the LinkID and CLinkID counters used by section_builders.

    These are module-level globals in the converter. They MUST be reset before
    every entry conversion, otherwise IDs collide across multiple .bci files
    produced in the same Python process (which is the norm on Vercel — the
    runtime keeps the warm container around for many invocations).

    Counter seed values match what convert.py uses at the top of convert_row().
    """
    _reset_link_counter(3796460)
    _reset_clink_counter(3291287)


__all__ = [
    "convert_row",
    "load_converter_config",
    "reset_link_counters",
    "HeaderResolver",
    "CSVRow",
    "read_csv",
    "BCIWriter",
]
