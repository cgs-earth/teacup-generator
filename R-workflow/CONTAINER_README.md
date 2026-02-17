# rezviz — Western Reservoir Conditions Daily Generator

Containerized R pipeline that fetches current reservoir storage for 214 western U.S. reservoirs, joins with 30-year historical statistics, and uploads a daily CSV to HydroShare.

## Quick Start

```bash
docker pull ghcr.io/cgs-earth/rezviz:latest

# Run for yesterday (default)
docker run --env-file .env ghcr.io/cgs-earth/rezviz:latest

# Run for an arbitrary date
docker run --env-file .env ghcr.io/cgs-earth/rezviz:latest 2026-01-15

# Keep the output CSV locally
docker run --env-file .env \
  -v $(pwd)/hydroshare:/app/hydroshare \
  ghcr.io/cgs-earth/rezviz:latest 2026-01-15
```

## Environment Variables

Create a `.env` file with your HydroShare credentials:

```
HYDROSHARE_USERNAME=user@example.com
HYDROSHARE_PASSWORD=yourpassword
```

Credentials are passed at runtime and never baked into the image.

## What It Does

1. Queries current storage values from 4 federal data sources
2. Converts elevation to storage for elevation-based reservoirs (e.g., Upper Klamath Lake)
3. Joins with day-of-year historical percentiles (1990-2020 baseline)
4. Detects new locations and auto-backfills 30 years of historical data
5. Filters out locations with insufficient historical coverage (< 20 water years)
6. Writes `droughtData{YYYYMMDD}.csv` to the `hydroshare/` directory
7. Uploads the CSV to [HydroShare resource 22b2f10103e5426a837defc00927afbd](https://www.hydroshare.org/resource/22b2f10103e5426a837defc00927afbd/)
8. Uploads updated parquet files to HydroShare if backfill occurred

## Data Sources

| Source | API | Locations |
|--------|-----|-----------|
| **RISE** | [WWDH EDR API](https://api.wwdh.internetofwater.app/collections/rise-edr) | ~191 reservoirs |
| **USACE** | [CDA Timeseries API](https://water.usace.army.mil) | 6 reservoirs (Cochiti, Abiquiu, Santa Rosa, Grand Coulee, Fort Peck, Lucky Peak) |
| **USGS** | [OGC API](https://api.waterdata.usgs.gov/ogcapi/v0/) | 6 reservoirs (Lahontan, Boca, Prosser Creek, Stampede, Cedar Bluff, Upper Klamath) |
| **CDEC** | [CDEC CSV Servlet](https://cdec.water.ca.gov) | 1 reservoir (Lake Tahoe) |

### Elevation-Based Reservoirs

Some reservoirs report water surface elevation instead of storage. These are automatically converted using elevation-capacity curves:

| Reservoir | USGS Parameter | Datum |
|-----------|----------------|-------|
| Upper Klamath Lake | 72275 | USBR Klamath Basin |

## Output

### Daily CSV

`droughtData{YYYYMMDD}.csv` — 214 rows (one per reservoir) with 26 columns including current storage, historical percentiles (p10-p90), percent of median/average/capacity, and the API URL used for each value.

### Backfill CSV

`backfill_{YYYYMMDD}.csv` — Generated when new reservoirs are added. Contains historical data rows with `Comment = "backfill"`.

## Image Details

- **Base**: `rocker/geospatial:4.4.2` (linux/amd64)
- **R packages**: httr2, dplyr, readr, lubridate, arrow, stringr, sf, curl, jsonlite
- **Config files**: `locations.geojson`, `elevation_storage_curves.csv`
- **Historical data**: `historical_baseline.parquet`, `historical_statistics.parquet` (downloaded from HydroShare during build)

## Build Process

The Docker image downloads the latest parquet files from HydroShare during build:

```bash
docker build -t ghcr.io/cgs-earth/rezviz:latest .
```

This ensures each new image has the most recent historical data, including any backfills from previous runs.

## Source Code

[cgs-earth/teacup-generator](https://github.com/cgs-earth/teacup-generator) — see `R-workflow/` directory.
