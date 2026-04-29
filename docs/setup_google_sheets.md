# Google Sheets bridge — one-time setup

The export script (`scripts/export_to_sheets.py`) writes into a Google
Sheets workbook that Looker Studio reads from. This walks through the
GCP-side setup that needs to happen once.

## 1. Create (or reuse) a GCP project

Go to <https://console.cloud.google.com/projectcreate> and create a
project. Name it whatever — `hospital-dashboard` works. Note the
**Project ID** (not the display name).

## 2. Enable the Google Sheets API

In the project, open **APIs & Services → Library** and enable:

- **Google Sheets API**
- **Google Drive API** (needed for `open_by_key` scope)

Direct link template:
`https://console.cloud.google.com/apis/library/sheets.googleapis.com?project=<PROJECT_ID>`

## 3. Create a service account

Open **IAM & Admin → Service Accounts → Create service account**.

- Service account ID: `hospital-dashboard-export`
- Role: leave empty (no project-level IAM needed; access is granted per-sheet)
- Click **Done**

## 4. Download the JSON key

Open the service account → **Keys → Add key → Create new key → JSON**.
Save the file. Treat it like a password — **do not commit it**.

Add to `.env`:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/hospital-dashboard-sa.json
```

The repo's `.gitignore` already excludes `.env` and any `*.json`
credentials file. Confirm before committing.

## 5. Create the destination workbook

Go to <https://sheets.new> and create a new spreadsheet titled
`hospital-dashboard-data`. Copy the **file ID** from the URL:

```
https://docs.google.com/spreadsheets/d/<THIS_PART>/edit
```

Save it; you'll pass it to the script as `--workbook-id`.

## 6. Share the workbook with the service account

Open the workbook → **Share**. Paste the service account email — it
looks like:

```
hospital-dashboard-export@<project-id>.iam.gserviceaccount.com
```

Found on the service account detail page. Give it **Editor** access.
Without this share, the export script will fail with a 403.

## 7. Smoke test

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json \
PGHOST=localhost PGPORT=5433 PGDATABASE=hospital_dashboard \
PGUSER=looker_reader PGPASSWORD=apppass \
python scripts/export_to_sheets.py --workbook-id <gsheet_id>
```

Expected output (JSON log lines):

```
{"msg": "sheet_written", "sheet": "hospitals",                   "rows": 5426}
{"msg": "sheet_written", "sheet": "patient_experience_dim_long", "rows": 38312}
{"msg": "sheet_written", "sheet": "state_rankings",              "rows": 53}
{"msg": "sheet_written", "sheet": "top_bottom",                  "rows": 20}
{"msg": "export_finished", "status": "success", ...}
```

## 8. Connect Looker Studio

In Looker Studio, **Create → Data source → Google Sheets** for each of
the 5 tabs in the workbook. The first time you connect a tab, Looker
will ask you to authorize Sheets access — that's a separate auth from
the service account, tied to your personal Google account.

**Important:** when adding the data source, check **"Use first row as
headers"** for every sheet. The export writes headers in row 1.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `403: insufficient_scope` | Drive API not enabled | Step 2 |
| `403: caller does not have permission` | Sheet not shared with SA | Step 6 |
| `404: not found` on `open_by_key` | Wrong workbook ID | Step 5 |
| Numbers showing as text in Looker | Sheet column is text-typed | Re-run export; psycopg returns floats |
| `ModuleNotFoundError: gspread` | Deps not installed | `pip install -r requirements.txt` |

## Refresh cadence

CMS publishes Care Compare quarterly. After each refresh:

```bash
# 1. Pull new raw data
python -m src.ingest --all

# 2. Rebuild core + marts
python -m src.transform --all

# 3. Push to Sheets
python scripts/export_to_sheets.py --workbook-id <gsheet_id>
```

Looker Studio's data source for each tab can be set to refresh every
15 min (its minimum) — but in practice you only need to refresh on
the schedule above.
