# kantonslabor_coliminder

This ETL job connects to the OpenRemote instance which saves the coliminder data, authenticates via client credentials, and supports:

- Connectivity checks for dashboards, assets, and services
- Writing synthetic historical datapoints (for testing)
- Exporting historical datapoints for `Testwert` and `Testwert2` to CSV (default behavior)

## What the script does now

### Default run (no flags)

Running `etl.py` without flags:

1. Authenticates with OpenRemote
2. Runs connectivity checks
3. Finds assets named `Testwert` and `Testwert2`
4. Reads their historical datapoints (attribute-level, currently `Gehalt`)
5. Writes the result to:

`data/100530_badewasserqualitaet.csv`

### Optional seed modes

- `--seed-historical`  
  Writes a few recent synthetic datapoints (timestamped) to discovered attributes.

- `--seed-daily-random`  
  Writes one random datapoint per day, per selected attribute source, in a date range.

Use `--start-date`, `--end-date`, and `--max-writes` with the daily seed mode.

## Environment variables

The script uses these variables from `.env`:

- `API_COLIMINDER_BASE_URL`
- `API_COLIMINDER_REALM`
- `API_COLIMINDER_CLIENT_ID`
- `API_COLIMINDER_CLIENT_SECRET`

## Run

From `kantonslabor_coliminder/`:

```bash
uv run etl.py
```

Daily random backfill example:

```bash
uv run etl.py --seed-daily-random --start-date 2026-01-01
```

Recent synthetic points example:

```bash
uv run etl.py --seed-historical
```

## API endpoints used

- Token:
  - `/auth/realms/{realm}/protocol/openid-connect/token`
- Connectivity:
  - `GET /dashboard/all/{realm}`
  - `POST /asset/query`
  - `GET /service?realm={realm}`
- Historical write:
  - `PUT /asset/{assetId}/attribute/{attributeName}/{timestamp}`
  - `PUT /asset/attributes/timestamp` (attempted first for bulk)
- Historical read:
  - `POST /asset/datapoint/{assetId}/{attributeName}`

## Notes

- Dashboard is mainly for visualization.
- Reliable extraction for ETL is done via historical datapoint API endpoints.
- If no matching assets or datapoints are found, the CSV is still created (header-only).
