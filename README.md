# rezviz — Western Reservoir Conditions Daily Generator

Containerized R pipeline that fetches current reservoir storage for 213 western U.S. reservoirs, joins with 30-year historical statistics, and uploads a daily CSV to HydroShare.

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
2. Joins with day-of-year historical percentiles (1990-2020 baseline)
3. Filters out locations with insufficient historical coverage (< 20 water years)
4. Writes `droughtData{YYYYMMDD}.csv` to the `hydroshare/` directory
5. Uploads the CSV to [HydroShare resource 22b2f10103e5426a837defc00927afbd](https://www.hydroshare.org/resource/22b2f10103e5426a837defc00927afbd/)

## Data Sources

| Source | API | Locations |
|--------|-----|-----------|
| **RISE** | [WWDH EDR API](https://api.wwdh.internetofwater.app/collections/rise-edr) | ~201 reservoirs |
| **USACE** | [CDA Timeseries API](https://water.usace.army.mil) | 6 reservoirs (Cochiti, Abiquiu, Santa Rosa, Grand Coulee, Fort Peck, Lucky Peak) |
| **USGS** | [NWIS Daily Values](https://waterservices.usgs.gov/nwis/dv/) | 4 reservoirs (Lahontan, Boca, Prosser Creek, Stampede) |
| **CDEC** | [CDEC CSV Servlet](https://cdec.water.ca.gov) | 1 reservoir (Lake Tahoe) |

## Output

Each CSV contains 213 rows (one per reservoir) with 26 columns including current storage, historical percentiles (p10-p90), percent of median/average/capacity, and the API URL used for each value.

## Image Details

- **Base**: `rocker/geospatial:4.4.2` (linux/amd64)
- **R packages**: httr2, dplyr, readr, lubridate, arrow, stringr, sf, curl
- **Bundled data**: `locations.geojson`, `historical_statistics.parquet`, `historical_baseline.parquet`

## Source Code

[cgs-earth/teacup-generator](https://github.com/cgs-earth/teacup-generator) — see `R-workflow/` directory.
