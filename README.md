# LawFiles-Sync

Daily-scheduled Vercel Python function that pulls new Gravity Forms
submissions from fleysherlaw.com, runs each through the
[BCI converter](https://github.com/fasi96/LawFiles), and uploads the
resulting `.bci` files to a Dropbox folder. State (the last entry id we
processed) lives as `last_run.json` in the same Dropbox folder, so there
is no separate database or queue.

---

## What it does, end to end

```
Vercel cron (daily, 07:00 UTC)
   │
   ▼
GET /api/cron  ──Authorization: Bearer $CRON_SECRET
   │
   ▼
pipeline.run()
   │
   ├── Dropbox: read /BCI Files/last_run.json   (or seed it on first run)
   │
   ├── GF API: list entries with id > last_entry_id  (ASC)
   │
   ├── for each entry, within a 50s wall-clock budget:
   │      ├── adapter:    GF JSON         -> (headers, CSVRow)
   │      ├── converter:  CSVRow          -> .bci bytes
   │      └── Dropbox:    upload bytes to /BCI Files/entry_<id>_<last>_<first>.bci
   │
   └── Dropbox: write /BCI Files/last_run.json with the new high-water mark
```

Idempotent by design (you can rerun the same window safely):
- the high-water mark in `last_run.json` skips already-processed ids;
- per-entry filenames are deterministic, so reruns hit the same path;
- Dropbox content-dedupes identical re-uploads, and raises a clear
  `FileExists` only when content actually differs.

---

## Project layout

```
LawFiles-Sync/
├── README.md              you are here
├── .env.example           safe-to-commit template
├── .env                   real secrets, gitignored
├── .gitignore
├── requirements.txt       requests only
├── vercel.json            cron + 60s function timeout
│
├── api/
│   ├── cron.py            GET /api/cron — daily scheduled trigger
│   └── run.py             GET /api/run  — manual trigger for testing
│
├── lib/
│   ├── config.py          env var loader + .env support
│   ├── log.py             JSON-per-line logging (Vercel logs panel)
│   ├── converter.py       sys.path shim onto vendored LawFiles converter
│   ├── gf_client.py       Gravity Forms REST API v2 client
│   ├── gf_adapter.py      GF entry JSON -> CSVRow shape the converter expects
│   ├── dropbox_client.py  Dropbox v2 API client (refresh-token OAuth)
│   └── pipeline.py        orchestration — the function the handlers call
│
├── scripts/
│   ├── dump_gf_entry.py   dump one entry / form schema to JSON
│   ├── dropbox_auth.py    one-time refresh-token wizard
│   ├── test_dropbox.py    smoke-test the Dropbox client
│   └── test_adapter.py    smoke-test the GF -> converter adapter
│
└── vendor/
    └── LawFiles/          git submodule of the BCI converter repo
```

---

## Required environment variables

All listed in `.env.example`. Put real values in `.env` (local) AND in
**Vercel → Project → Settings → Environment Variables** (production).

| Variable | Where to get it |
|---|---|
| `GF_BASE_URL` | The WordPress site root, no trailing slash. `https://fleysherlaw.com` |
| `GF_FORM_ID` | Numeric form id of the bankruptcy questionnaire. Currently `8`. |
| `GF_CONSUMER_KEY` / `GF_CONSUMER_SECRET` | WP Admin → Forms → Settings → REST API → Add Key (Read permission) |
| `DROPBOX_APP_KEY` / `DROPBOX_APP_SECRET` | https://www.dropbox.com/developers/apps → your app → Settings tab |
| `DROPBOX_REFRESH_TOKEN` | Output of `python scripts/dropbox_auth.py` (one-time bootstrap) |
| `DROPBOX_TARGET_FOLDER` | Path inside Dropbox. For "App folder" apps this is sandbox-relative. Currently `/BCI Files` |
| `CRON_SECRET` | Any random string. Generate with `python -c "import secrets; print(secrets.token_urlsafe(32))"` |
| `FIRST_RUN_BACKFILL` | `none` (only future entries) or `YYYY-MM-DD` (one-time backfill). Default: `none`. |
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` / `ERROR`. Default: `INFO`. |

### Dropbox app permissions

On the app's Permissions tab, the following scopes must be enabled
(otherwise uploads return a `missing_scope` error):

- `files.content.write`
- `files.content.read`
- `files.metadata.write`
- `files.metadata.read`

Changing scopes invalidates existing refresh tokens, so generate the
refresh token AFTER you've finalized scopes.

---

## Local development

### One-time setup

```bash
cd D:/LawFiles-Sync
python -m venv .venv
. .venv/Scripts/activate            # Windows; on macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt

# add the converter as a submodule (or use LAWFILES_PATH override below)
git init
git submodule add https://github.com/fasi96/LawFiles.git vendor/LawFiles
git add .
git commit -m "Initial scaffold"

# fill in env vars
cp .env.example .env                 # then edit .env

# generate the Dropbox refresh token
python scripts/dropbox_auth.py
# paste the printed value back into .env as DROPBOX_REFRESH_TOKEN
```

If you don't want to add the submodule yet, point at an existing local
clone of the converter:

```bash
export LAWFILES_PATH=/path/to/LawFiles    # mac/linux
# or:  $env:LAWFILES_PATH = "D:/LawFiles"  # windows powershell
```

### Smoke tests

```bash
# Dump one entry and the form schema (sanity-check GF auth)
python scripts/dump_gf_entry.py --list
python scripts/dump_gf_entry.py
python scripts/dump_gf_entry.py --form-schema

# End-to-end: GF -> adapter -> converter, write a .bci to tmp/
python scripts/test_adapter.py

# Dropbox client: whoami + write/read state + idempotent upload
python scripts/test_dropbox.py

# Pipeline dry run: process the last few entries WITHOUT uploading
python -c "from lib import pipeline; import json; \
    print(json.dumps(pipeline.run(dry_run=True, since_override=16553), indent=2, default=str))"
```

---

## Deploying to Vercel

### 1. Push to GitHub

```bash
cd D:/LawFiles-Sync
gh repo create LawFiles-Sync --private --source=. --push
# or do it via the GitHub UI and `git push` manually
```

The repo MUST contain `vercel.json`, `requirements.txt`, the `api/` and
`lib/` directories, and the `vendor/LawFiles` submodule pointer.

### 2. Connect to Vercel

1. https://vercel.com/dashboard → **Add New** → **Project**
2. Import the GitHub repo. Vercel auto-detects Python.
3. Framework preset: **Other**. Root directory: `./`.
4. Click **Deploy** — the first deploy will fail because env vars aren't
   set yet. That's expected.

### 3. Set environment variables

**Settings → Environment Variables**. Add every key from `.env`, scoped
to **Production**, **Preview**, and **Development**. Paste values from
your local `.env` — **never** commit them.

Then **Settings → Deployments → Redeploy**.

### 4. Confirm cron is registered

**Settings → Cron Jobs** should show:

```
Path: /api/cron      Schedule: 0 7 * * *
```

If it doesn't appear, your project must be on the **Pro** plan (Hobby
caps cron to once-a-day with restricted features and no `maxDuration`
override).

### 5. Test with the manual trigger

```bash
# replace <prod-domain> and <secret> with your real values
curl "https://<prod-domain>.vercel.app/api/run?secret=<CRON_SECRET>&dry_run=1"
```

You should get back a JSON summary like the one from the local dry-run
test. If you see `entries_processed: 0`, that's correct when there are
no new entries since the last successful run.

To actually upload one or more entries, drop the `dry_run=1`:

```bash
curl "https://<prod-domain>.vercel.app/api/run?secret=<CRON_SECRET>"
```

### 6. Wait for cron to fire (or change the schedule)

The cron runs at `0 7 * * *` UTC by default. If you want it sooner for
testing, change the schedule in `vercel.json`, push, and redeploy.

---

## Operating the pipeline

### Reading logs

**Vercel Dashboard → Project → Deployments → (click a deployment) → Logs**.
Filter to the function — `api/cron.py` or `api/run.py`. Each line of our
output is JSON, so you can grep for fields:

```
"msg": "pipeline.fetch_entries"   start of an entry pull
"msg": "dropbox.upload.ok"        successful Dropbox write
"msg": "pipeline.entry.failed"    a single entry failed (others continue)
"msg": "pipeline.budget_exceeded" hit the 50s budget; partial run
"msg": "pipeline.state_advanced"  last_run.json moved forward
```

The summary the handler returns to the client (visible at the bottom of
each cron invocation) is the most useful single artifact:

```json
{
  "status": "ok",
  "entries_processed": 3,
  "entries_uploaded": 3,
  "entries_failed": 0,
  "starting_entry_id": 16590,
  "final_entry_id": 16615,
  "elapsed_s": 18.3
}
```

### Reprocessing a specific window

`?since=N` overrides the starting id for one invocation. State is still
advanced based on what we successfully upload, so this is the way to
backfill or recover from a partial state.

```bash
# upload entries 16500..present, regardless of last_run.json:
curl "https://<prod>.vercel.app/api/run?secret=<SECRET>&since=16499"
```

### Forcing a re-upload of an already-uploaded entry

The pipeline never overwrites an existing `.bci`. If you need to
regenerate one, delete the target file from Dropbox first, then rerun
`/api/run?since=<id-1>`.

### One-off backfill (initial deploy)

If you want to backfill a date range on first deploy, set
`FIRST_RUN_BACKFILL=YYYY-MM-DD` BEFORE you trigger any run. The pipeline
will treat last_entry_id=0 and try to process every entry on the form.
This will likely exceed the 60s timeout — run `/api/run` repeatedly
until `status` comes back `ok` with `entries_processed: 0`. Each run
advances `last_run.json` so progress is preserved.

### Rotating credentials

- **Dropbox refresh token**: re-run `python scripts/dropbox_auth.py`,
  paste new value into `.env` AND Vercel env vars, redeploy.
- **GF API key**: WP Admin → delete + re-create. Update `.env` + Vercel.
- **`CRON_SECRET`**: change the value in Vercel; remember to update any
  scripts/dashboards that hit `/api/run`.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| 401 from `/api/cron` or `/api/run` | `CRON_SECRET` mismatch between client and Vercel env. |
| `RuntimeError: Environment variable X is missing` | Variable unset OR still has `replace_me` placeholder. Set it in Vercel. |
| `invalid_grant: refresh token is malformed` | You pasted the auth code instead of the refresh token. Run `dropbox_auth.py`. |
| `missing_scope: files.content.write` | Dropbox app permissions weren't enabled before token was minted. Enable scopes, mint a new token. |
| `LawFiles converter not found at ...` | Submodule wasn't fetched. On Vercel, ensure the GitHub repo has the submodule pointer; locally use `LAWFILES_PATH`. |
| `entries_processed: 0` every run | All entries already uploaded. Confirm via `last_run.json` in Dropbox. |
| `status: partial_budget` | Backlog bigger than one 60s tick can handle. Subsequent runs continue automatically. |
| `status: skipped_name_collision` for an entry | Different content already uploaded to the same path. Investigate manually — usually a debtor name change after an earlier run. |

---

## Source repos

- **This sync project**: https://github.com/fasi96/LawFiles-Sync (TBD — push it)
- **BCI converter (vendored)**: https://github.com/fasi96/LawFiles
