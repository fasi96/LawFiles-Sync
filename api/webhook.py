"""
api/webhook.py — Gravity Forms webhook receiver (Flask/WSGI).

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

WSGI/Flask is used here (not BaseHTTPRequestHandler) because Vercel's
adapter for the http.server path mishandles certain request header
shapes — POSTs from WordPress's wp_remote_post and python-requests
hit a TypeError in vc__handler__python.py where a header value is
a list, and http.client.putheader rejects lists. The Flask/WSGI
path uses a different adapter that doesn't have this bug.
"""

import os
import sys
import traceback

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from flask import Flask, jsonify, request  # noqa: E402

from lib import log, pipeline  # noqa: E402


app = Flask(__name__)


def _is_authorized() -> bool:
    expected = os.environ.get("WEBHOOK_SECRET", "").strip()
    if not expected:
        return False
    return request.headers.get("X-Webhook-Secret", "") == expected


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
    if "id" in payload and isinstance(payload.get("id"), (str, int)):
        return payload
    if isinstance(payload.get("entry"), dict):
        return payload["entry"]
    return None


# Vercel routes /api/webhook to this WSGI app. Depending on runtime version
# Flask may see the full path or just "/" — match both with a catch-all.
@app.route("/", defaults={"_path": ""}, methods=["GET", "POST"])
@app.route("/<path:_path>", methods=["GET", "POST"])
def webhook(_path):
    if request.method != "POST":
        return jsonify({"error": "method_not_allowed", "expected": "POST"}), 405

    if not _is_authorized():
        return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(silent=True)
    if payload is None:
        log.warning("webhook.bad_body",
                    content_type=request.headers.get("Content-Type", ""))
        return jsonify({"error": "invalid_json"}), 400

    entry = _extract_entry(payload)
    if entry is None:
        keys = list(payload.keys())[:20] if isinstance(payload, dict) else None
        log.warning("webhook.cannot_extract_entry", top_level_keys=keys)
        return jsonify({"error": "could_not_extract_entry",
                        "hint": "expected entry id at top level or under 'entry'"}), 400

    entry_id = entry.get("id")
    log.info("webhook.start", entry_id=entry_id)

    try:
        result = pipeline.process_webhook_entry(entry)
        log.info("webhook.done",
                 entry_id=entry_id,
                 status=result.get("status"),
                 uploaded=result.get("uploaded"),
                 filename=result.get("filename"))
        return jsonify(result), 200
    except Exception as e:
        log.error("webhook.failed",
                  entry_id=entry_id,
                  error=str(e),
                  type=type(e).__name__,
                  traceback=traceback.format_exc())
        # 500 -> GF Webhooks Add-On retries with backoff
        return jsonify({"error": str(e), "type": type(e).__name__}), 500
