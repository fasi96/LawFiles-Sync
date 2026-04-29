#!/usr/bin/env python3
"""
scripts/dump_gf_entry.py — dump one Gravity Forms entry (or the form schema)
to a local JSON file so we can see real field structure before building
the adapter.

Usage:
    # Dump the most recent entry on the form (default behaviour).
    python scripts/dump_gf_entry.py

    # Dump a specific entry by ID.
    python scripts/dump_gf_entry.py --entry-id 12345

    # List the 10 most recent entries (id + date + email) without saving.
    python scripts/dump_gf_entry.py --list

    # Dump the FORM schema (field definitions, labels, types) instead of
    # an entry. Use this when building the adapter — it tells us which
    # GF field IDs map to which CSV header text.
    python scripts/dump_gf_entry.py --form-schema

    # Override output path (default: project root / sample_entry.json).
    python scripts/dump_gf_entry.py --out /tmp/entry.json

Auth: HTTP Basic with GF_CONSUMER_KEY / GF_CONSUMER_SECRET. fleysherlaw.com
serves over HTTPS so Basic Auth is safe.

GF API endpoints used (REST API v2):
    GET /wp-json/gf/v2/forms/{form_id}              -> form schema
    GET /wp-json/gf/v2/forms/{form_id}/entries      -> list entries
    GET /wp-json/gf/v2/entries/{entry_id}           -> single entry
"""

import argparse
import json
import os
import sys

# Project root on sys.path so we can `from lib import config`.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import requests  # noqa: E402

from lib import config  # noqa: E402


def _api_base(base_url: str) -> str:
    return f"{base_url}/wp-json/gf/v2"


def _summarize_entry(entry: dict, max_fields: int = 25) -> None:
    """Print a compact summary of an entry's structure to stdout."""
    print(f"\nEntry summary")
    print(f"  id           : {entry.get('id')}")
    print(f"  form_id      : {entry.get('form_id')}")
    print(f"  date_created : {entry.get('date_created')}  (server tz)")
    print(f"  date_created_gmt: {entry.get('date_created_gmt')}")
    print(f"  status       : {entry.get('status')}")
    print(f"  source_url   : {entry.get('source_url')}")
    print(f"  total keys   : {len(entry)}")

    # Field-key columns: keys that are numeric or numeric.subindex (e.g. "19", "19.3")
    field_keys = [k for k in entry.keys()
                  if k.replace(".", "").isdigit()]
    field_keys.sort(key=lambda k: tuple(int(p) for p in k.split(".")))
    print(f"  field keys   : {len(field_keys)} (first {min(max_fields, len(field_keys))} below)")
    for k in field_keys[:max_fields]:
        v = entry.get(k, "")
        if isinstance(v, str) and len(v) > 70:
            v = v[:67] + "..."
        print(f"    {k:>6}  =  {v!r}")
    if len(field_keys) > max_fields:
        print(f"    ... ({len(field_keys) - max_fields} more — see the full JSON file)")


def _summarize_schema(form: dict, max_fields: int = 25) -> None:
    print(f"\nForm schema summary")
    print(f"  id          : {form.get('id')}")
    print(f"  title       : {form.get('title')}")
    fields = form.get("fields", [])
    print(f"  fields      : {len(fields)} (first {min(max_fields, len(fields))} below)")
    for f in fields[:max_fields]:
        fid = f.get("id")
        ftype = f.get("type")
        label = f.get("label", "")
        admin = f.get("adminLabel", "")
        inputs = f.get("inputs") or []
        sub = f"  ({len(inputs)} sub-inputs)" if inputs else ""
        shown = label or admin or "(no label)"
        if len(shown) > 60:
            shown = shown[:57] + "..."
        print(f"    [{fid:>4}] {ftype:<14} {shown}{sub}")
    if len(fields) > max_fields:
        print(f"    ... ({len(fields) - max_fields} more — see the full JSON file)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--entry-id", type=int, default=None,
                        help="Dump this specific entry ID (default: most recent on the form).")
    parser.add_argument("--list", action="store_true",
                        help="List the 10 most recent entry IDs and exit; do not write a file.")
    parser.add_argument("--form-schema", action="store_true",
                        help="Dump the form definition instead of an entry.")
    parser.add_argument("--out", default=None,
                        help="Output JSON path (default: <project>/sample_entry.json or sample_form_schema.json).")
    parser.add_argument("--page-size", type=int, default=10,
                        help="--list page size (default: 10).")
    args = parser.parse_args()

    cfg = config.load_gf()
    auth = (cfg.gf_consumer_key, cfg.gf_consumer_secret)
    base = _api_base(cfg.gf_base_url)

    if args.list:
        url = f"{base}/forms/{cfg.gf_form_id}/entries"
        params = {
            "paging[page_size]": args.page_size,
            "sorting[direction]": "DESC",
            "sorting[key]": "date_created",
        }
        print(f"GET {url}  (list, page_size={args.page_size})")
        r = requests.get(url, auth=auth, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        entries = data.get("entries", [])
        print(f"\nTotal entries on form {cfg.gf_form_id}: {data.get('total_count')}")
        print(f"Showing latest {len(entries)}:\n")
        for e in entries:
            eid = e.get("id")
            date = e.get("date_created")
            status = e.get("status")
            print(f"  id={eid:<8}  date={date}  status={status}")
        return 0

    if args.form_schema:
        url = f"{base}/forms/{cfg.gf_form_id}"
        out = args.out or os.path.join(_ROOT, "sample_form_schema.json")
        print(f"GET {url}")
        r = requests.get(url, auth=auth, timeout=30)
        r.raise_for_status()
        form = r.json()
        with open(out, "w", encoding="utf-8") as f:
            json.dump(form, f, indent=2, ensure_ascii=False)
        print(f"Wrote {out}")
        _summarize_schema(form)
        return 0

    # Default: dump one entry.
    if args.entry_id:
        url = f"{base}/entries/{args.entry_id}"
        params = None
        print(f"GET {url}")
    else:
        url = f"{base}/forms/{cfg.gf_form_id}/entries"
        params = {
            "paging[page_size]": 1,
            "sorting[direction]": "DESC",
            "sorting[key]": "date_created",
        }
        print(f"GET {url}  (latest entry)")

    r = requests.get(url, auth=auth, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if args.entry_id:
        entry = data
    else:
        entries = data.get("entries", [])
        if not entries:
            print(f"\nNo entries found on form {cfg.gf_form_id}. Try --list to confirm the form has any.")
            return 1
        entry = entries[0]

    out = args.out or os.path.join(_ROOT, "sample_entry.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(entry, f, indent=2, ensure_ascii=False)
    print(f"Wrote {out}")
    _summarize_entry(entry)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except requests.HTTPError as e:
        # Surface the response body — GF returns helpful error messages.
        body = ""
        try:
            body = e.response.text[:500]
        except Exception:
            pass
        print(f"\nHTTP {e.response.status_code} {e.response.reason}", file=sys.stderr)
        if body:
            print(f"Body: {body}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"\nError: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(2)
