#!/usr/bin/env python3
"""
scripts/test_adapter.py — smoke-test the GF -> CSVRow adapter end-to-end.

Steps:
  1. Load sample_form_schema.json + sample_entry.json from project root
     (created by `python scripts/dump_gf_entry.py [--form-schema]`).
  2. Run the adapter to produce (headers, [CSVRow]).
  3. Compare adapter headers against the real GF CSV export's headers
     (D:/LawFiles/data/sample/bankruptcy-questionnaire-2026-02-10.csv) and
     report any missing/extra/out-of-order columns.
  4. Run the converter end-to-end on the synthesized row and write a .bci
     to tmp/<entry_id>.bci. Print the conversion report.

Run with the LAWFILES_PATH override pointing at your local clone:
    LAWFILES_PATH=D:/LawFiles python scripts/test_adapter.py
"""

import csv
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from lib import gf_adapter  # noqa: E402
from lib.converter import (  # noqa: E402
    BCIWriter, HeaderResolver, convert_row, load_converter_config,
    reset_link_counters,
)


_REAL_CSV = "D:/LawFiles/data/sample/bankruptcy-questionnaire-2026-02-10.csv"


def _read_real_headers(path: str) -> list[str]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return next(csv.reader(f))


def _compare_headers(synth: list[str], real: list[str]) -> None:
    s_set = set(synth)
    r_set = set(real)
    missing = [h for h in real if h not in s_set]
    extra = [h for h in synth if h not in r_set]
    print(f"\n[headers] synth={len(synth)}  real={len(real)}  "
          f"missing={len(missing)}  extra={len(extra)}")

    # Critical headers — referenced by field_mapping.json. If any of these
    # are missing the converter cannot read them.
    critical = [
        "Your Name (First)", "Your Name (Middle)", "Your Name (Last)",
        "Your Name (Suffix)",
        "Your Current Address (Street Address)",
        "Your Current Address (City)",
        "Your Current Address (State / Province)",
        "Your Current Address (ZIP / Postal Code)",
        "Your Email Address (Enter Email)",
        "Your Phone Number",
        "Your Marital Status",
        "Do you plan on filing your case individually or jointly with your spouse?",
        "Household Members 1",
        "Entry Id",
    ]
    crit_missing = [h for h in critical if h not in s_set]
    if crit_missing:
        print(f"  ! CRITICAL HEADERS MISSING ({len(crit_missing)}):")
        for h in crit_missing:
            print(f"      {h!r}")
    else:
        print(f"  + all {len(critical)} critical headers present")

    if missing:
        print(f"\n[headers] missing from synth (first 25):")
        for h in missing[:25]:
            print(f"  - {h!r}")
        if len(missing) > 25:
            print(f"  ... +{len(missing) - 25} more")

    if extra:
        print(f"\n[headers] extra in synth (first 25 — usually fine):")
        for h in extra[:25]:
            print(f"  + {h!r}")
        if len(extra) > 25:
            print(f"  ... +{len(extra) - 25} more")


def _check_duplicates(synth: list[str], real: list[str]) -> None:
    """Verify duplicate-occurrence ordering matches — critical for
    HeaderResolver's '#N' suffix lookups (e.g. 'Cell Phone#3')."""
    from collections import Counter
    sc = Counter(synth)
    rc = Counter(real)
    dups = {h for h, n in rc.items() if n > 1}
    print(f"\n[duplicates] real CSV has {len(dups)} headers that occur >1 time")
    mismatches = []
    for h in dups:
        if sc[h] != rc[h]:
            mismatches.append((h, sc[h], rc[h]))
    if mismatches:
        print(f"  ! {len(mismatches)} duplicate-count mismatches (first 10):")
        for h, s_n, r_n in mismatches[:10]:
            print(f"      {h!r}: synth={s_n} real={r_n}")
    else:
        print(f"  + all duplicate occurrence counts match")


def _run_converter(headers, row, entry_id):
    print(f"\n[convert] running convert_row() on synthesized row...")
    mapping, _defaults = load_converter_config()
    resolver = HeaderResolver(headers)
    reset_link_counters()
    writer, report = convert_row(row, mapping, _defaults, resolver, show_progress=False)

    out_dir = os.path.join(_ROOT, "tmp")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{entry_id}.bci")
    writer.write(out_path)

    ok = len(report["success"])
    fail = len(report["errors"])
    total = ok + fail
    print(f"  wrote {out_path}  ({os.path.getsize(out_path)} bytes)")
    print(f"  sections: {ok}/{total} OK")
    if report["errors"]:
        print(f"  ! errors:")
        for err in report["errors"]:
            print(f"      [{err['section']}] {err['type']}: {err['error']}")
    return ok, fail


def main() -> int:
    schema_path = os.path.join(_ROOT, "sample_form_schema.json")
    entry_path = os.path.join(_ROOT, "sample_entry.json")
    if not os.path.exists(schema_path) or not os.path.exists(entry_path):
        print("Missing sample_form_schema.json or sample_entry.json. Run:")
        print("  python scripts/dump_gf_entry.py --form-schema")
        print("  python scripts/dump_gf_entry.py")
        return 1

    with open(schema_path, encoding="utf-8") as f:
        schema = json.load(f)
    with open(entry_path, encoding="utf-8") as f:
        entry = json.load(f)

    print(f"[adapter] entry id={entry.get('id')}  form id={schema.get('id')}")
    headers, rows = gf_adapter.adapt(schema, entry)
    print(f"  produced {len(headers)} headers, {len(rows)} row(s)")

    if os.path.exists(_REAL_CSV):
        real = _read_real_headers(_REAL_CSV)
        _compare_headers(headers, real)
        _check_duplicates(headers, real)
    else:
        print(f"\n[skip] real CSV not found at {_REAL_CSV}; skipping header diff")

    ok, fail = _run_converter(headers, rows[0], entry.get("id", "unknown"))
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
