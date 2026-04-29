# Hospital Patient Experience Analytics Dashboard

End-to-end analytics pipeline that pulls hospital-level patient experience and outcomes data from the CMS Provider Data Catalog (Care Compare), lands it in PostgreSQL through a `raw → core → marts` pattern, and surfaces patient satisfaction (HCAHPS), 30-day readmission, and overall hospital ratings in a Looker Studio dashboard. Built as a healthcare-domain companion to my NYC 311 operations dashboard, reusing the same Python + PostgreSQL + Looker Studio stack.

## Stack
- **Ingestion:** Python 3.11, `requests`, `pandas`
- **Warehouse:** PostgreSQL (`raw` / `core` / `marts` schemas)
- **Visualization:** Looker Studio
- **Source:** CMS Provider Data Catalog DKAN API (`data.cms.gov/provider-data/api/1`)

## Datasets (v1)
| 4x4 ID       | Dataset                          | Rows    | Role            |
|--------------|----------------------------------|---------|-----------------|
| `xubh-q36u`  | Hospital General Information     | 5,426   | Dimension       |
| `dgck-syfz`  | Patient Survey (HCAHPS)          | 325,652 | Fact (survey)   |
| `632h-zaca`  | Unplanned Hospital Visits        | 67,046  | Fact (readmits) |

## Quickstart
```bash
cp .env.example .env            # fill in secrets
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
psql -U postgres -f sql/00_init.sql -v app_password="'$(grep ^PGPASSWORD .env | cut -d= -f2)'"
```

## Layout
```
src/         # ingestion (CMS DKAN client, raw loader, core/marts builders)
sql/         # numbered, idempotent migrations
dashboards/  # Looker Studio config, screenshots
tests/       # pytest
scripts/     # one-off CLIs (refresh, backfill)
```
