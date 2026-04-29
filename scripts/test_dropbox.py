#!/usr/bin/env python3
"""
scripts/test_dropbox.py — smoke-test the Dropbox client end-to-end.

Requires DROPBOX_REFRESH_TOKEN to be filled in .env (run dropbox_auth.py
first). Performs:
  1. whoami — confirm credentials & account
  2. write_json("_smoketest.json", {...})  — overwrite mode
  3. read_json("_smoketest.json")          — round-trip the value
  4. upload_bytes("_smoketest.bci", ..., overwrite=False)  — first add
  5. upload_bytes(same path, overwrite=False)              — should raise FileExists
  6. cleanup: re-upload the smoketest paths with empty/marker content so
     we don't leave clutter (we do not delete because we don't grant
     the delete scope — we just overwrite with markers).

The smoketest writes to YOUR configured DROPBOX_TARGET_FOLDER. Files start
with `_smoketest_` so they're easy to spot and remove manually.
"""

import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from lib import config  # noqa: E402
from lib.dropbox_client import DropboxClient, FileExists, FileNotFound  # noqa: E402


def main() -> int:
    cfg = config.load()
    client = DropboxClient(
        app_key=cfg.dropbox_app_key,
        app_secret=cfg.dropbox_app_secret,
        refresh_token=cfg.dropbox_refresh_token,
        target_folder=cfg.dropbox_target_folder,
    )

    print(f"target folder: {cfg.dropbox_target_folder}")

    # 1. whoami
    me = client.whoami()
    name = (me.get("name") or {}).get("display_name", "?")
    print(f"[1/5] whoami           OK  ({name} <{me.get('email')}>)")

    # 2. write JSON
    state = {"smoketest_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
             "value": 42}
    client.write_json("_smoketest_state.json", state)
    print(f"[2/5] write_json       OK  (_smoketest_state.json)")

    # 3. read JSON
    got = client.read_json("_smoketest_state.json")
    assert got == state, f"round-trip mismatch: {got!r} != {state!r}"
    print(f"[3/5] read_json        OK  (round-trip equal)")

    # 4. add-mode upload (first time should succeed)
    test_path = f"_smoketest_add_{int(time.time())}.bci"
    client.upload_bytes(test_path, b"first content\r\n", overwrite=False)
    print(f"[4/5] upload(add) #1   OK  ({test_path})")

    # 5. add-mode upload to same path with DIFFERENT content — should raise
    # FileExists. Dropbox content-dedupes identical uploads (silent no-op),
    # so we use distinct bytes to force the conflict path. In the real
    # pipeline, identical-content reruns are also no-ops, which is fine.
    try:
        client.upload_bytes(test_path, b"second different content\r\n", overwrite=False)
        print(f"[5/5] upload(add) #2   FAIL  (expected FileExists, got success)")
        return 1
    except FileExists as e:
        print(f"[5/5] upload(add) #2   OK  (FileExists raised as expected)")

    # cleanup: overwrite the test files with markers so they're obvious
    client.upload_text("_smoketest_state.json",
                       '{"smoketest": "done — safe to delete"}',
                       overwrite=True)
    client.upload_bytes(test_path, b"smoketest done -- safe to delete\r\n",
                        overwrite=True)
    print()
    print("All checks passed.")
    print(f"Two marker files left in {cfg.dropbox_target_folder}:")
    print(f"  _smoketest_state.json")
    print(f"  {test_path}")
    print("Delete them by hand when convenient.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except FileNotFound as e:
        print(f"FileNotFound: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"\nFAIL: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(2)
