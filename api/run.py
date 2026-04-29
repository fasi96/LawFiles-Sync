"""
api/run.py — manual-trigger endpoint.

Hit it from your browser, Postman, or curl while debugging — same pipeline as
the daily cron, but invokable on demand. Auth via either:
  - Authorization: Bearer $CRON_SECRET  header, OR
  - ?secret=$CRON_SECRET                query string

Optional query params:
  ?dry_run=1   — fetch + adapt + convert, but skip Dropbox uploads and
                 do not advance last_run.json
  ?since=N     — override starting last_entry_id for this invocation only
                 (does NOT update the state file's seed); use to reprocess
                 a specific tail of entries
"""

import json
import os
import sys
import traceback
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from lib import config, log, pipeline  # noqa: E402


def _is_authorized(headers, query: dict) -> bool:
    expected = os.environ.get("CRON_SECRET", "").strip()
    if not expected:
        return False
    auth = headers.get("Authorization", "")
    if auth == f"Bearer {expected}":
        return True
    return query.get("secret", [""])[0] == expected


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if not _is_authorized(self.headers, query):
            self._respond(401, {"error": "unauthorized"})
            return

        try:
            cfg = config.load()
            dry_run = query.get("dry_run", ["0"])[0] in ("1", "true", "yes")
            since_raw = query.get("since", [None])[0]
            since_override = int(since_raw) if since_raw and since_raw.isdigit() else None
            log.info("run.start", form_id=cfg.gf_form_id,
                     target=cfg.dropbox_target_folder,
                     dry_run=dry_run, since_override=since_override)
            result = pipeline.run(dry_run=dry_run, since_override=since_override)
            log.info("run.done",
                     status=result.get("status"),
                     processed=result.get("entries_processed"),
                     uploaded=result.get("entries_uploaded"),
                     failed=result.get("entries_failed"),
                     elapsed_s=result.get("elapsed_s"))
            self._respond(200, result)
        except Exception as e:
            log.error("run.failed", error=str(e),
                      type=type(e).__name__,
                      traceback=traceback.format_exc())
            self._respond(500, {"error": str(e), "type": type(e).__name__})

    def _respond(self, status: int, body: dict):
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        pass
