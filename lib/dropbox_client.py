"""
dropbox_client.py — Dropbox v2 API client for the sync pipeline.

Why no SDK:
  The official `dropbox` Python package pulls in extra deps and a chunky
  surface area we don't need. We use four endpoints: token exchange, upload,
  download, get_metadata. Raw `requests` calls are smaller and easier to
  debug from Vercel logs.

OAuth model:
  We use the long-lived refresh-token flow. The refresh token is generated
  once via scripts/dropbox_auth.py and stored in DROPBOX_REFRESH_TOKEN.
  Every invocation exchanges it for a short-lived access token (~4h TTL),
  which we keep in instance state for the rest of the run.

Path semantics:
  All caller paths are RELATIVE to DROPBOX_TARGET_FOLDER. e.g.
      client.upload_bytes("entry_16615.bci", data)
  becomes /BCI Files/entry_16615.bci on disk.

Idempotency:
  upload_bytes(..., overwrite=False) uses Dropbox's "add" mode with
  autorename:false — if the file already exists at the same path, the
  request fails cleanly with FileExists; the caller should treat that as
  "already done" and continue.
  upload_bytes(..., overwrite=True) uses "overwrite" mode — used for the
  state file (last_run.json).
"""

import json
from typing import Optional

import requests

from lib import log


_OAUTH_TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
_RPC_BASE = "https://api.dropboxapi.com/2"
_CONTENT_BASE = "https://content.dropboxapi.com/2"


class DropboxError(Exception):
    """Wraps any non-2xx response from Dropbox with status + body."""

    def __init__(self, status: int, body: str, summary: str = ""):
        self.status = status
        self.body = body
        self.summary = summary
        super().__init__(f"Dropbox HTTP {status} {summary}: {body[:300]}")


class FileExists(DropboxError):
    """Raised by upload_bytes(overwrite=False) when path already exists."""


class FileNotFound(DropboxError):
    """Raised by download_bytes / read_json when path doesn't exist."""


class DropboxClient:
    def __init__(self, app_key: str, app_secret: str, refresh_token: str,
                 target_folder: str):
        self._app_key = app_key
        self._app_secret = app_secret
        self._refresh_token = refresh_token
        # Normalize: ensure leading slash, no trailing slash.
        tf = target_folder.strip()
        if not tf.startswith("/"):
            tf = "/" + tf
        self._target_folder = tf.rstrip("/")
        self._access_token: Optional[str] = None

    # -----------------------------------------------------------------
    # Auth
    # -----------------------------------------------------------------

    def _ensure_access_token(self, force_refresh: bool = False) -> str:
        if self._access_token and not force_refresh:
            return self._access_token
        log.debug("dropbox.refresh_token.exchange")
        resp = requests.post(
            _OAUTH_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self._app_key,
                "client_secret": self._app_secret,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise DropboxError(resp.status_code, resp.text,
                               "refresh_token grant failed")
        token = resp.json().get("access_token")
        if not token:
            raise DropboxError(resp.status_code, resp.text,
                               "no access_token in response")
        self._access_token = token
        return token

    # -----------------------------------------------------------------
    # Path helpers
    # -----------------------------------------------------------------

    def _resolve(self, path_in_folder: str) -> str:
        """Combine target folder + relative path. Always returns leading-slash."""
        rel = path_in_folder.strip()
        if rel.startswith("/"):
            rel = rel[1:]
        return f"{self._target_folder}/{rel}"

    @staticmethod
    def _safe_api_arg(obj: dict) -> str:
        """
        Dropbox-API-Arg header must be ASCII. Non-ASCII bytes in paths
        (e.g. unicode names) need \\uXXXX escaping. json.dumps with
        ensure_ascii=True (default) handles this.
        """
        return json.dumps(obj)

    # -----------------------------------------------------------------
    # File ops
    # -----------------------------------------------------------------

    def upload_bytes(self, path_in_folder: str, data: bytes,
                     overwrite: bool = False) -> dict:
        """Upload raw bytes. Raises FileExists if overwrite=False and path
        already exists. Returns the file metadata dict on success."""
        full_path = self._resolve(path_in_folder)
        mode = {"mode": "overwrite"} if overwrite else {"mode": "add",
                                                         "autorename": False}
        api_arg = {"path": full_path, "mute": True, **mode}

        def _do(token: str) -> requests.Response:
            return requests.post(
                f"{_CONTENT_BASE}/files/upload",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/octet-stream",
                    "Dropbox-API-Arg": self._safe_api_arg(api_arg),
                },
                data=data,
                timeout=60,
            )

        token = self._ensure_access_token()
        resp = _do(token)
        if resp.status_code == 401:
            log.warning("dropbox.upload.401_retry", path=full_path)
            token = self._ensure_access_token(force_refresh=True)
            resp = _do(token)

        if resp.status_code == 409:
            # path conflict — file exists in 'add' mode
            body = resp.text
            if "path/conflict/file" in body or '"reason": {".tag": "conflict"' in body \
                    or '"path": {".tag": "conflict"' in body:
                raise FileExists(resp.status_code, body,
                                 f"file already exists at {full_path}")
            raise DropboxError(resp.status_code, body, "upload conflict")

        if resp.status_code != 200:
            raise DropboxError(resp.status_code, resp.text,
                               f"upload failed for {full_path}")

        log.info("dropbox.upload.ok", path=full_path,
                 bytes=len(data),
                 mode="overwrite" if overwrite else "add")
        return resp.json()

    def upload_text(self, path_in_folder: str, text: str,
                    overwrite: bool = False) -> dict:
        return self.upload_bytes(path_in_folder, text.encode("utf-8"),
                                 overwrite=overwrite)

    def download_bytes(self, path_in_folder: str) -> bytes:
        """Download file content. Raises FileNotFound if path doesn't exist."""
        full_path = self._resolve(path_in_folder)
        api_arg = {"path": full_path}

        def _do(token: str) -> requests.Response:
            return requests.post(
                f"{_CONTENT_BASE}/files/download",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Dropbox-API-Arg": self._safe_api_arg(api_arg),
                },
                timeout=60,
            )

        token = self._ensure_access_token()
        resp = _do(token)
        if resp.status_code == 401:
            log.warning("dropbox.download.401_retry", path=full_path)
            token = self._ensure_access_token(force_refresh=True)
            resp = _do(token)

        if resp.status_code == 409:
            body = resp.text
            if "path/not_found" in body:
                raise FileNotFound(resp.status_code, body,
                                   f"no such file at {full_path}")
            raise DropboxError(resp.status_code, body, "download conflict")

        if resp.status_code != 200:
            raise DropboxError(resp.status_code, resp.text,
                               f"download failed for {full_path}")
        return resp.content

    def read_json(self, path_in_folder: str) -> Optional[dict]:
        """Read a JSON file. Returns None if the file doesn't exist."""
        try:
            data = self.download_bytes(path_in_folder)
        except FileNotFound:
            return None
        try:
            return json.loads(data.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise DropboxError(0, str(e),
                               f"corrupt JSON at {path_in_folder}") from e

    def write_json(self, path_in_folder: str, obj: dict) -> dict:
        """Write a JSON file (overwrites)."""
        text = json.dumps(obj, indent=2, ensure_ascii=False, default=str)
        return self.upload_text(path_in_folder, text, overwrite=True)

    # -----------------------------------------------------------------
    # Diagnostics
    # -----------------------------------------------------------------

    def whoami(self) -> dict:
        """Return account info — useful for the auth script's verification step."""
        token = self._ensure_access_token()
        resp = requests.post(
            f"{_RPC_BASE}/users/get_current_account",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            data="null",
            timeout=30,
        )
        if resp.status_code != 200:
            raise DropboxError(resp.status_code, resp.text, "whoami failed")
        return resp.json()


def from_env(cfg) -> DropboxClient:
    """Construct a client from a loaded Config object."""
    return DropboxClient(
        app_key=cfg.dropbox_app_key,
        app_secret=cfg.dropbox_app_secret,
        refresh_token=cfg.dropbox_refresh_token,
        target_folder=cfg.dropbox_target_folder,
    )
