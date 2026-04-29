"""
gf_client.py — Gravity Forms REST API v2 client (read-only).

We only need three operations:
  - get_form_schema()        : fetch field definitions for adapter use
  - list_entries_since(id)   : iterate entries whose id > given id, ASC
  - get_max_entry_id()       : look up the current latest entry id (for
                                FIRST_RUN_BACKFILL=none seeding)

Auth is HTTP Basic with consumer key + secret. fleysherlaw.com serves over
HTTPS. All calls are paginated and time-bounded.
"""

from typing import Iterable, Optional

import requests

from lib import log


class GFClient:
    def __init__(self, base_url: str, form_id: str,
                 consumer_key: str, consumer_secret: str,
                 timeout: int = 30):
        self._base = f"{base_url.rstrip('/')}/wp-json/gf/v2"
        self._form_id = form_id
        self._auth = (consumer_key, consumer_secret)
        self._timeout = timeout

    def get_form_schema(self) -> dict:
        url = f"{self._base}/forms/{self._form_id}"
        resp = requests.get(url, auth=self._auth, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def get_max_entry_id(self) -> Optional[int]:
        """Return the highest entry id on the form, or None if there are no
        entries yet."""
        url = f"{self._base}/forms/{self._form_id}/entries"
        params = {
            "paging[page_size]": 1,
            "sorting[key]": "id",
            "sorting[direction]": "DESC",
        }
        resp = requests.get(url, auth=self._auth, params=params, timeout=self._timeout)
        resp.raise_for_status()
        entries = resp.json().get("entries", [])
        if not entries:
            return None
        return int(entries[0]["id"])

    def list_entries_since(self, last_entry_id: int,
                           page_size: int = 50,
                           hard_cap: int = 500) -> Iterable[dict]:
        """Yield entries with id > last_entry_id, in ASCENDING id order.

        We sort DESC at the API level (cheaper — doesn't require a full
        scan from page 1) and accumulate, then yield ASC client-side. This
        also makes it easy to terminate as soon as we hit an id <= last_entry_id.

        hard_cap protects against runaway pagination if state is corrupted.
        """
        url = f"{self._base}/forms/{self._form_id}/entries"
        page = 1
        accumulated: list[dict] = []
        while True:
            params = {
                "paging[page_size]": page_size,
                "paging[current_page]": page,
                "sorting[key]": "id",
                "sorting[direction]": "DESC",
            }
            resp = requests.get(url, auth=self._auth, params=params, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("entries", [])
            if not entries:
                break

            stop = False
            for e in entries:
                eid = int(e["id"])
                if eid <= last_entry_id:
                    stop = True
                    break
                accumulated.append(e)
                if len(accumulated) >= hard_cap:
                    log.warning("gf.list_entries.hard_cap_hit",
                                cap=hard_cap, last_seen_id=eid)
                    stop = True
                    break

            if stop:
                break

            # If we got a short page, we're at the end
            if len(entries) < page_size:
                break
            page += 1

        # Yield ASC (oldest-new first) so state advances monotonically as we
        # process; if we time out mid-batch, we've at least handled the oldest.
        for e in sorted(accumulated, key=lambda x: int(x["id"])):
            yield e
