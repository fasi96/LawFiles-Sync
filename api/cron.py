"""
api/cron.py — daily Vercel cron handler.

Vercel calls GET /api/cron with header `Authorization: Bearer $CRON_SECRET`
based on the schedule in vercel.json. We require that header to match.

This handler is intentionally thin — all real work happens in lib.pipeline.run().
"""

import json
import os
import sys
import traceback
from http.server import BaseHTTPRequestHandler

# Project root on sys.path so `from lib import ...` works on Vercel.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from lib import config, log, pipeline  # noqa: E402


def _is_authorized(headers) -> bool:
    expected = os.environ.get("CRON_SECRET", "").strip()
    if not expected:
        return False
    auth = headers.get("Authorization", "")
    return auth == f"Bearer {expected}"


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not _is_authorized(self.headers):
            self._respond(401, {"error": "unauthorized"})
            return

        try:
            cfg = config.load()
            log.info("cron.start", form_id=cfg.gf_form_id,
                     target=cfg.dropbox_target_folder)
            result = pipeline.run()
            log.info("cron.done",
                     status=result.get("status"),
                     processed=result.get("entries_processed"),
                     uploaded=result.get("entries_uploaded"),
                     failed=result.get("entries_failed"),
                     elapsed_s=result.get("elapsed_s"))
            self._respond(200, result)
        except Exception as e:
            log.error("cron.failed", error=str(e),
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

    # silence default access logging — we use structured logs above
    def log_message(self, fmt, *args):
        pass
