"""
config.py — environment variable loading and validation.

On Vercel, env vars are injected automatically. Locally, we fall back to
loading them from a .env file at the project root if it exists.
"""

import os
from dataclasses import dataclass


def load_dotenv():
    """Minimal .env loader — no python-dotenv dep needed."""
    here = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(here, "..", ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip()
            # don't override values already set by the runtime
            os.environ.setdefault(k, v)


load_dotenv()


def _required(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val or val.startswith("replace_me"):
        raise RuntimeError(
            f"Environment variable {name} is missing or unset. "
            f"Set it in Vercel → Project → Settings → Environment Variables, "
            f"or in .env for local dev."
        )
    return val


@dataclass(frozen=True)
class Config:
    gf_base_url: str
    gf_form_id: str
    gf_consumer_key: str
    gf_consumer_secret: str

    dropbox_app_key: str
    dropbox_app_secret: str
    dropbox_refresh_token: str
    dropbox_target_folder: str

    cron_secret: str
    first_run_backfill: str
    log_level: str


@dataclass(frozen=True)
class GFConfig:
    """Subset of Config — just what's needed to talk to the GF API.
    Use this in scripts that don't need Dropbox or cron auth."""
    gf_base_url: str
    gf_form_id: str
    gf_consumer_key: str
    gf_consumer_secret: str


def load_gf() -> GFConfig:
    return GFConfig(
        gf_base_url=_required("GF_BASE_URL").rstrip("/"),
        gf_form_id=_required("GF_FORM_ID"),
        gf_consumer_key=_required("GF_CONSUMER_KEY"),
        gf_consumer_secret=_required("GF_CONSUMER_SECRET"),
    )


def load() -> Config:
    return Config(
        gf_base_url=_required("GF_BASE_URL").rstrip("/"),
        gf_form_id=_required("GF_FORM_ID"),
        gf_consumer_key=_required("GF_CONSUMER_KEY"),
        gf_consumer_secret=_required("GF_CONSUMER_SECRET"),
        dropbox_app_key=_required("DROPBOX_APP_KEY"),
        dropbox_app_secret=_required("DROPBOX_APP_SECRET"),
        dropbox_refresh_token=_required("DROPBOX_REFRESH_TOKEN"),
        dropbox_target_folder=_required("DROPBOX_TARGET_FOLDER").rstrip("/"),
        cron_secret=_required("CRON_SECRET"),
        first_run_backfill=os.environ.get("FIRST_RUN_BACKFILL", "none").strip(),
        log_level=os.environ.get("LOG_LEVEL", "INFO").strip().upper(),
    )
