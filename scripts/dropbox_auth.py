#!/usr/bin/env python3
"""
scripts/dropbox_auth.py — one-time refresh-token bootstrap for Dropbox.

Run this LOCALLY once. It walks the OAuth code-grant flow and prints a
refresh token you paste into .env (and into Vercel's env-var settings).

Prerequisites:
  - DROPBOX_APP_KEY    set in .env  (from your app's Settings tab)
  - DROPBOX_APP_SECRET set in .env

Usage:
    python scripts/dropbox_auth.py

What it does:
  1. Builds an authorize URL with token_access_type=offline (this is the
     critical flag — without it Dropbox returns only a 4-hour access token,
     no refresh token).
  2. Opens that URL in your default browser (and prints it as fallback).
  3. You log in to Dropbox, click "Allow", and Dropbox shows you a code.
  4. You paste the code back into the terminal.
  5. The script POSTs to /oauth2/token with grant_type=authorization_code
     to exchange the code for {access_token, refresh_token, ...}.
  6. It prints the refresh_token and runs a whoami sanity check so you
     know the credentials work end-to-end before you wire them up.
"""

import os
import sys
import webbrowser

import requests

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from lib import config as _config  # noqa: E402  (loads .env at import time)
from lib.dropbox_client import DropboxClient  # noqa: E402


def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val or val.startswith("replace_me"):
        print(f"\nError: {name} is not set. Fill it in .env and rerun.",
              file=sys.stderr)
        sys.exit(1)
    return val


def main() -> int:
    app_key = _require("DROPBOX_APP_KEY")
    app_secret = _require("DROPBOX_APP_SECRET")

    auth_url = (
        "https://www.dropbox.com/oauth2/authorize"
        f"?client_id={app_key}"
        "&response_type=code"
        "&token_access_type=offline"
    )

    print()
    print("=" * 70)
    print(" Dropbox refresh-token bootstrap")
    print("=" * 70)
    print()
    print("Step 1.  Open this URL in your browser, sign in, and click 'Allow':")
    print()
    print(f"    {auth_url}")
    print()
    print("(Trying to open it for you now...)")
    try:
        webbrowser.open(auth_url, new=2)
    except Exception:
        pass
    print()
    print("Step 2.  Dropbox will show you an authorization code. Paste it here:")
    print()
    code = input("    Auth code: ").strip()
    if not code:
        print("\nNo code provided — aborting.", file=sys.stderr)
        return 1

    print()
    print("Exchanging code for tokens...")
    resp = requests.post(
        "https://api.dropboxapi.com/oauth2/token",
        data={
            "code": code,
            "grant_type": "authorization_code",
            "client_id": app_key,
            "client_secret": app_secret,
        },
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"\n    ERROR: HTTP {resp.status_code}", file=sys.stderr)
        print(f"    Body: {resp.text[:500]}", file=sys.stderr)
        print("\n    Common causes:", file=sys.stderr)
        print("      - Code expired (codes are single-use, valid for ~1 minute)", file=sys.stderr)
        print("      - App key/secret typo in .env", file=sys.stderr)
        print("      - You authorized a different Dropbox app", file=sys.stderr)
        return 2

    payload = resp.json()
    refresh_token = payload.get("refresh_token")
    if not refresh_token:
        print(f"\n    ERROR: response had no refresh_token.", file=sys.stderr)
        print(f"    Did the URL include 'token_access_type=offline'?", file=sys.stderr)
        print(f"    Response keys: {list(payload.keys())}", file=sys.stderr)
        return 3

    # Verify it works end-to-end.
    print("Verifying with whoami...")
    target_folder = os.environ.get("DROPBOX_TARGET_FOLDER", "/").strip() or "/"
    client = DropboxClient(app_key, app_secret, refresh_token, target_folder)
    try:
        me = client.whoami()
    except Exception as e:
        print(f"\n    ERROR during whoami: {e}", file=sys.stderr)
        return 4
    name = (me.get("name") or {}).get("display_name", "(unknown)")
    email = me.get("email", "(unknown)")

    print()
    print("=" * 70)
    print(" SUCCESS")
    print("=" * 70)
    print(f"  Authenticated as : {name} <{email}>")
    print(f"  Account ID       : {me.get('account_id', '')}")
    print()
    print("  Paste this into .env (and into Vercel's env-var settings):")
    print()
    print(f"    DROPBOX_REFRESH_TOKEN={refresh_token}")
    print()
    print("  This token does not expire unless you revoke the app or its")
    print("  app secret in the Dropbox developer console.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        sys.exit(130)
