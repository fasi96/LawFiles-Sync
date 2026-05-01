"""
api/webhook.py — Gravity Forms webhook receiver.

Configured in WP Admin under:
  Forms -> (Bankruptcy Questionnaire) -> Settings -> Webhooks
with:
  Method            : POST
  Request URL       : https://law-files-sync.vercel.app/api/webhook
  Request Format    : JSON
  Request Body      : All Fields
  Custom Headers    : X-Webhook-Secret : <value of WEBHOOK_SECRET env var>

When a form is submitted, GF posts the entry payload here within
~1 second. We adapt + convert + upload the .bci to Dropbox in the
foreground and reply 200 OK.

On failure we return 500 so the Webhooks Add-On retries with backoff.
The 4x/day cron remains as a catch-up safety net for anything that
slips through repeated webhook failures.

Idempotency: identical payloads produce identical .bci bytes at the
same Dropbox path, and Dropbox content-dedupes silently. So GF
retries are safe, and the cron rerunning over the same id is also
safe.

Auth: shared secret in X-Webhook-Secret header. Different from
CRON_SECRET (different attack surface — webhook URL is configured
in WP Admin, not in this codebase).
"""

import json
import os
import sys
import traceback
from http.server import BaseHTTPRequestHandler

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from lib import log, pipeline  # noqa: E402


def _is_authorized(headers) -> bool:
    expected = os.environ.get("WEBHOOK_SECRET", "").strip()
    if not expected:
        return False
    received = headers.get("X-Webhook-Secret", "")
    return received == expected


def _extract_entry(payload):
    """Be tolerant of GF Webhooks Add-On payload shape variations.

    Most common shape (Request Body = 'All Fields', Format = JSON):
        { "1": "value", "2.3": "value", "234.1": "value", "id": "16700", ... }
    Some versions wrap it:
        { "form_id": "8", "entry": {...} }   or
        { "entry": {...} }
    """
    if not isinstance(payload, dict):
        return None
    if "id" in payload and (isinstance(payload.get("id"), (str, int))):
        return payload
    if isinstance(payload.get("entry"), dict):
        return payload["entry"]
    return None


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if not _is_authorized(self.headers):
            self._respond(401, {"error": "unauthorized"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            payload = json.loads(body.decode("utf-8") or "{}")
        except (ValueError, UnicodeDecodeError) as e:
            log.warning("webhook.bad_body", error=str(e))
            self._respond(400, {"error": "invalid_json", "detail": str(e)})
            return

        entry = _extract_entry(payload)
        if entry is None:
            log.warning("webhook.cannot_extract_entry",
                        top_level_keys=list(payload.keys())[:20])
            self._respond(400, {"error": "could_not_extract_entry",
                                "hint": "expected entry id at top level or under 'entry'"})
            return

        entry_id = entry.get("id")
        log.info("webhook.start", entry_id=entry_id)

        try:
            result = pipeline.process_webhook_entry(entry)
            log.info("webhook.done",
                     entry_id=entry_id,
                     status=result.get("status"),
                     uploaded=result.get("uploaded"),
                     filename=result.get("filename"))
            self._respond(200, result)
        except Exception as e:
            log.error("webhook.failed",
                      entry_id=entry_id,
                      error=str(e),
                      type=type(e).__name__,
                      traceback=traceback.format_exc())
            # 500 -> GF Webhooks Add-On will retry with backoff
            self._respond(500, {"error": str(e), "type": type(e).__name__})

    def do_GET(self):
        # Webhook is POST only. Use /api/run for manual GET-based testing.
        self._respond(405, {"error": "method_not_allowed", "expected": "POST"})

    def _respond(self, status: int, body: dict):
        payload = json.dumps(body, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        pass
