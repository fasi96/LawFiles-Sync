"""
pipeline.py — orchestrates one sync run.

run() is called by both api/cron.py (scheduled) and api/run.py (manual).
Steps:
  1. Load Config + Dropbox client + GF client.
  2. Read last_run.json from Dropbox. If absent, seed via FIRST_RUN_BACKFILL.
  3. Fetch GF entries with id > last_entry_id, ascending.
  4. For each entry, within a wall-clock budget:
       a. reset converter LinkID counters
       b. adapt GF entry -> (headers, CSVRow)
       c. convert_row(...) -> BCIWriter -> bytes
       d. upload to Dropbox with deterministic filename
       e. record success / failure in run summary
  5. Advance last_entry_id to the highest successfully-uploaded id.
  6. Write last_run.json (single Dropbox call at end).
  7. Return JSON-serializable summary for the API handler to log + respond.

Idempotency contract:
  - Per-entry filename is deterministic: entry_<id>_<lastname>_<firstname>.bci
  - Upload uses add-mode + autorename:false, so a same-content re-upload
    is silently dedup'd by Dropbox; a different-content re-upload raises
    FileExists, which we surface as a non-fatal warning and skip.
  - State is advanced only by entries we successfully uploaded (or that
    Dropbox already had). A failing entry does NOT block state advance for
    later entries — we record the failure and keep going.

Time budget:
  Vercel Pro caps Python functions at 60s. We reserve 10s for setup +
  state write + response, leaving ~50s for entry processing. If we run
  out, we break the loop, write state, and return status='partial'. The
  next run picks up from where we stopped.
"""

import re
import time
import traceback
from typing import Optional

from lib import config, log
from lib.converter import (
    BCIWriter, HeaderResolver, convert_row,
    load_converter_config, reset_link_counters,
)
from lib.dropbox_client import DropboxClient, FileExists
from lib.gf_adapter import adapt
from lib.gf_client import GFClient


_STATE_FILE = "last_run.json"
_DEFAULT_BUDGET_S = 50


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _safe_filename_part(s: str) -> str:
    """Sanitize a string for use in a filename. Keep alnum, dash, underscore;
    collapse everything else to underscore. Empty -> 'Unknown'."""
    if not s:
        return "Unknown"
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", s.strip()).strip("_")
    return cleaned or "Unknown"


def _make_filename(entry_id, headers: list[str], row) -> str:
    """entry_<id>_<lastname>_<firstname>.bci"""
    resolver = HeaderResolver(headers)
    last_idx = resolver.resolve("Your Name (Last)")
    first_idx = resolver.resolve("Your Name (First)")
    last = row.get(last_idx, "") if last_idx is not None else ""
    first = row.get(first_idx, "") if first_idx is not None else ""
    return f"entry_{entry_id}_{_safe_filename_part(last)}_{_safe_filename_part(first)}.bci"


def _initial_state(cfg, gf: GFClient) -> dict:
    """Build the seed state when last_run.json doesn't exist."""
    if cfg.first_run_backfill == "none":
        max_id = gf.get_max_entry_id()
        seed = max_id if max_id is not None else 0
        log.info("pipeline.first_run_seed", strategy="none", last_entry_id=seed)
        return {
            "last_entry_id": seed,
            "seeded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "seed_strategy": "none",
            "runs": [],
        }
    # date-based seed: backfill from a specific date forward
    log.info("pipeline.first_run_seed", strategy="date",
             from_date=cfg.first_run_backfill)
    # We start from last_entry_id=0 and let pagination pull everything
    # whose id > 0. The first scheduled run does the bulk upload.
    return {
        "last_entry_id": 0,
        "seeded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "seed_strategy": f"date:{cfg.first_run_backfill}",
        "runs": [],
    }


# -----------------------------------------------------------------------
# Per-entry processing
# -----------------------------------------------------------------------

def _process_entry(entry: dict, form_schema: dict,
                   mapping, defaults,
                   dropbox: DropboxClient,
                   dry_run: bool = False) -> dict:
    """Adapt + convert + upload one entry. Returns a result dict."""
    entry_id = entry.get("id")
    result = {"entry_id": entry_id, "status": "ok",
              "filename": None, "bci_bytes": 0,
              "error": None, "error_type": None}

    try:
        headers, rows = adapt(form_schema, entry)
        if not rows:
            raise RuntimeError("adapter returned no rows")
        row = rows[0]

        filename = _make_filename(entry_id, headers, row)
        result["filename"] = filename

        resolver = HeaderResolver(headers)
        reset_link_counters()
        writer, conv_report = convert_row(row, mapping, defaults, resolver,
                                          show_progress=False)
        if conv_report["errors"]:
            result["status"] = "converted_with_section_errors"
            result["section_errors"] = [
                {"section": e["section"], "type": e["type"], "error": e["error"]}
                for e in conv_report["errors"]
            ]

        bci_bytes = writer.to_string().encode("utf-8")
        result["bci_bytes"] = len(bci_bytes)

        if dry_run:
            log.info("pipeline.dry_run.skip_upload",
                     entry_id=entry_id, filename=filename, bytes=len(bci_bytes))
            result["uploaded"] = False
            return result

        try:
            dropbox.upload_bytes(filename, bci_bytes, overwrite=False)
            result["uploaded"] = True
        except FileExists:
            # A previous run uploaded a file at this path with DIFFERENT
            # content. That's a name-collision warning, not a hard failure
            # — we don't want to overwrite arbitrarily, so we skip and flag.
            log.warning("pipeline.upload.name_collision",
                        entry_id=entry_id, filename=filename)
            result["uploaded"] = False
            result["status"] = "skipped_name_collision"

        return result

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        log.error("pipeline.entry.failed",
                  entry_id=entry_id,
                  error=str(e),
                  type=type(e).__name__,
                  traceback=traceback.format_exc())
        return result


# -----------------------------------------------------------------------
# Webhook entry point
# -----------------------------------------------------------------------

def process_webhook_entry(entry: dict) -> dict:
    """Process ONE entry that arrived via the Gravity Forms webhook.

    Same conversion + upload as the cron path, plus a state-file update
    (only if the entry's id is higher than the current high-water mark —
    monotonic, never goes backward, even under concurrent webhook calls).

    Returns the per-entry result dict the caller (api/webhook.py) sends
    back as the HTTP response. On failure, raises — caller responds 500
    so GF's Webhooks Add-On retries with backoff.
    """
    cfg = config.load()
    gf = GFClient(cfg.gf_base_url, cfg.gf_form_id,
                  cfg.gf_consumer_key, cfg.gf_consumer_secret)
    dropbox = DropboxClient(cfg.dropbox_app_key, cfg.dropbox_app_secret,
                            cfg.dropbox_refresh_token, cfg.dropbox_target_folder)

    form_schema = gf.get_form_schema()
    mapping, defaults = load_converter_config()

    result = _process_entry(entry, form_schema, mapping, defaults,
                            dropbox, dry_run=False)

    # Advance state ONLY if this entry's id is higher than what's recorded.
    # Webhooks can deliver out of order during concurrent submissions; we
    # never want last_entry_id to move backward.
    if result["status"] != "failed":
        try:
            eid = int(entry.get("id", 0))
        except (TypeError, ValueError):
            eid = 0
        if eid > 0:
            state = dropbox.read_json(_STATE_FILE)
            if state is None:
                state = _initial_state(cfg, gf)
            if eid > int(state.get("last_entry_id", 0)):
                state["last_entry_id"] = eid
                state["last_run_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                         time.gmtime())
                history = state.get("runs", [])
                history.append({
                    "at_utc": state["last_run_at_utc"],
                    "source": "webhook",
                    "entry_id": eid,
                    "uploaded": result.get("uploaded", False),
                    "status": result["status"],
                })
                state["runs"] = history[-20:]
                dropbox.write_json(_STATE_FILE, state)
                log.info("webhook.state_advanced", to_id=eid)

    return result


# -----------------------------------------------------------------------
# Main run
# -----------------------------------------------------------------------

def run(*, dry_run: bool = False, since_override: Optional[int] = None,
        budget_s: int = _DEFAULT_BUDGET_S) -> dict:
    """Execute one sync run. See module docstring."""
    started = time.time()
    cfg = config.load()
    deadline = started + budget_s

    summary: dict = {
        "status": "ok",
        "dry_run": dry_run,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "entries_processed": 0,
        "entries_uploaded": 0,
        "entries_failed": 0,
        "entries_skipped": 0,
        "starting_entry_id": None,
        "final_entry_id": None,
        "results": [],
    }

    gf = GFClient(cfg.gf_base_url, cfg.gf_form_id,
                  cfg.gf_consumer_key, cfg.gf_consumer_secret)
    dropbox = DropboxClient(cfg.dropbox_app_key, cfg.dropbox_app_secret,
                            cfg.dropbox_refresh_token, cfg.dropbox_target_folder)

    # 1. Load state
    state = dropbox.read_json(_STATE_FILE)
    if state is None:
        state = _initial_state(cfg, gf)
        # Persist the seed immediately so a manual rerun doesn't seed again.
        if not dry_run:
            dropbox.write_json(_STATE_FILE, state)
    starting_id = state.get("last_entry_id", 0)
    if since_override is not None:
        log.info("pipeline.since_override", original=starting_id, override=since_override)
        starting_id = int(since_override)
    summary["starting_entry_id"] = starting_id

    # 2. Form schema (one fetch per run — adapter needs it for every entry)
    log.info("pipeline.fetch_schema", form_id=cfg.gf_form_id)
    form_schema = gf.get_form_schema()

    # 3. Mapping/defaults from the vendored converter (cheap, in-memory load)
    mapping, defaults = load_converter_config()

    # 4. Iterate new entries within the wall-clock budget
    log.info("pipeline.fetch_entries", since_id=starting_id)
    final_id = starting_id
    for entry in gf.list_entries_since(starting_id):
        if time.time() > deadline:
            log.warning("pipeline.budget_exceeded",
                        budget_s=budget_s,
                        processed=summary["entries_processed"])
            summary["status"] = "partial_budget"
            break

        res = _process_entry(entry, form_schema, mapping, defaults,
                             dropbox, dry_run=dry_run)
        summary["entries_processed"] += 1
        summary["results"].append(res)
        if res["status"] == "failed":
            summary["entries_failed"] += 1
        elif res["status"] == "skipped_name_collision":
            summary["entries_skipped"] += 1
        elif res.get("uploaded"):
            summary["entries_uploaded"] += 1

        # Advance high-water mark only on non-failed entries (we want failed
        # entries to be retried next run).
        if res["status"] != "failed":
            final_id = max(final_id, int(entry["id"]))

    summary["final_entry_id"] = final_id

    # 5. Persist state if we made progress and we're not in a dry run.
    if not dry_run and final_id > starting_id:
        new_state = dict(state)
        new_state["last_entry_id"] = final_id
        new_state["last_run_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        # Trim runs history to the most recent 20 entries to keep the file small.
        history = new_state.get("runs", [])
        history.append({
            "at_utc": new_state["last_run_at_utc"],
            "starting_id": starting_id,
            "final_id": final_id,
            "uploaded": summary["entries_uploaded"],
            "failed": summary["entries_failed"],
            "skipped": summary["entries_skipped"],
            "status": summary["status"],
        })
        new_state["runs"] = history[-20:]
        dropbox.write_json(_STATE_FILE, new_state)
        log.info("pipeline.state_advanced",
                 from_id=starting_id, to_id=final_id)

    summary["elapsed_s"] = round(time.time() - started, 2)
    return summary
