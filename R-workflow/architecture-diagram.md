# R-Workflow Architecture Diagram

## Data Pipeline Overview

```mermaid
flowchart TB
    subgraph sources["Data Sources"]
        RISE["RISE API<br/>(via WWDH EDR)<br/>~191 reservoirs"]
        USACE["USACE CDA<br/>Timeseries API<br/>6 reservoirs"]
        USGS["USGS OGC API v0<br/>Daily Values<br/>6 reservoirs"]
        CDEC["CDEC<br/>CA Water Data<br/>1 reservoir"]
    end

    subgraph config["Configuration"]
        CSV["config/locations.csv<br/>214 reservoirs<br/>Include/Exclude decisions"]
        GEOJSON["config/locations.geojson<br/>Coordinates, DOI Region<br/>HUC6, State"]
        ELEV_CURVES["config/elevation_storage_curves.csv<br/>Elevation-to-storage lookup<br/>for elevation-based reservoirs"]
        MANUAL["data/manual/*.csv<br/>Manually downloaded<br/>for problem locations"]
    end

    subgraph setup["One-Time Setup"]
        BASELINE["setup_historical_baseline.R<br/>Fetch 30 years (1990-2020)"]
        HIST_DATA["output/historical_baseline.parquet<br/>1.6M observations"]
        HIST_STATS["output/historical_statistics.parquet<br/>366 days × 140 locations"]
    end

    subgraph daily["Daily Production"]
        GENERATOR["rezviz_data_generator.R<br/>Fetch current storage<br/>Join with historical stats<br/>Compute percentiles<br/>Auto-backfill new locations"]
    end

    subgraph output["Output"]
        DAILY_CSV["hydroshare/droughtDataYYYYMMDD.csv<br/>~140 reservoirs per day<br/>Storage + Historical Metrics"]
        BACKFILL_CSV["hydroshare/backfill_YYYYMMDD.csv<br/>Historical data for<br/>newly added locations"]
        HYDROSHARE["HydroShare Resource<br/>22b2f10103e5426a837defc00927afbd<br/>Public archive since 1990"]
    end

    subgraph consumers["Consumers"]
        WWDH["WWDH Reservoir Dashboard<br/>wwdh.internetofwater.app<br/>Teacup Visualizations"]
        RISE_VIZ["RISE Reservoir Conditions<br/>data.usbr.gov<br/>Interactive Map"]
    end

    %% Data flow for setup
    RISE --> BASELINE
    USACE --> BASELINE
    USGS --> BASELINE
    CDEC --> BASELINE
    BASELINE --> HIST_DATA
    HIST_DATA --> HIST_STATS

    %% Data flow for daily
    RISE --> GENERATOR
    USACE --> GENERATOR
    USGS --> GENERATOR
    CDEC --> GENERATOR
    HIST_STATS --> GENERATOR
    GEOJSON --> GENERATOR
    ELEV_CURVES --> GENERATOR
    GENERATOR --> DAILY_CSV
    GENERATOR --> BACKFILL_CSV
    DAILY_CSV --> HYDROSHARE
    BACKFILL_CSV --> HYDROSHARE

    %% Consumers
    HYDROSHARE --> WWDH
    HYDROSHARE --> RISE_VIZ

    %% Config relationships
    CSV --> GEOJSON
    MANUAL --> HIST_DATA
```

## Docker Deployment

```mermaid
flowchart LR
    subgraph image["ghcr.io/cgs-earth/rezviz:latest"]
        BASE["rocker/geospatial:4.4.2"]
        PKGS["R Packages<br/>httr2, dplyr, arrow<br/>sf, curl, jsonlite"]
        SCRIPT["rezviz_data_generator.R"]
        BUNDLED["Bundled Data<br/>locations.geojson<br/>historical_statistics.parquet<br/>elevation_storage_curves.csv"]
    end

    subgraph runtime["Runtime"]
        ENV[".env file<br/>HYDROSHARE_USERNAME<br/>HYDROSHARE_PASSWORD"]
        VOLUME["Volume mount<br/>./hydroshare:/app/hydroshare<br/>(optional)"]
    end

    subgraph cmd["Commands"]
        RUN1["docker run --env-file .env<br/>ghcr.io/cgs-earth/rezviz:latest"]
        RUN2["docker run --env-file .env<br/>ghcr.io/cgs-earth/rezviz:latest 2026-01-15"]
    end

    ENV --> RUN1
    ENV --> RUN2
    VOLUME --> RUN1
    VOLUME --> RUN2
```

## Data Sources Detail

```mermaid
flowchart LR
    subgraph rise_detail["RISE (191 locations)"]
        RISE_API["api.wwdh.internetofwater.app<br/>/collections/rise-edr/locations/{id}"]
    end

    subgraph usace_detail["USACE (6 locations)"]
        USACE_API["water.usace.army.mil/cda<br/>/reporting/providers/{district}/timeseries"]
        USACE_LOCS["Cochiti, Abiquiu, Santa Rosa<br/>Grand Coulee, Fort Peck, Lucky Peak"]
    end

    subgraph usgs_detail["USGS (6 locations)"]
        USGS_API["api.waterdata.usgs.gov<br/>/ogcapi/v0/collections/daily/items<br/>param 00054 (storage) or<br/>62614/62615 (elevation)"]
        USGS_LOCS["Lahontan, Boca, Prosser Creek<br/>Stampede, Cedar Bluff (storage)<br/>Upper Klamath (elevation→storage)"]
    end

    subgraph cdec_detail["CDEC (1 location)"]
        CDEC_API["cdec.water.ca.gov<br/>/dynamicapp/req/CSVDataServlet"]
        CDEC_LOCS["Lake Tahoe"]
    end
```

## Output CSV Schema

| Column | Description | Example |
|--------|-------------|---------|
| SiteName | Display name | "Lake Powell (Glen Canyon Dam)" |
| Lat, Lon | Coordinates | 37.0706, -111.4850 |
| State | State code | "AZ" |
| DoiRegion | DOI region | "Upper Colorado Basin" |
| Huc6 | Watershed | "140700" |
| DataUnits | Units | "af" |
| DataValue | Current storage | 7234567 |
| DataDate | Observation date | "02/09/2026" |
| DateQueried | Query date | "02/10/2026" |
| DataDateMax | Historical max | 24322000 |
| DataDateP90 | 90th percentile | 18234567 |
| DataDateP75 | 75th percentile | 15234567 |
| DataDateP50 | Median | 12234567 |
| DataDateP25 | 25th percentile | 9234567 |
| DataDateP10 | 10th percentile | 6234567 |
| DataDateMin | Historical min | 3234567 |
| DataDateAvg | Historical mean | 11234567 |
| DataValuePctMdn | Current/Median | 0.59 |
| DataValuePctAvg | Current/Average | 0.64 |
| StatsPeriod | Stats period | "10/1/1990 - 9/30/2020" |
| MaxCapacity | Dam capacity | 24322000 |
| PctFull | Current/Capacity | 0.30 |
| DataUrl | API endpoint | "https://api.wwdh..." |

## Elevation-to-Storage Conversion

For reservoirs that report water surface elevation instead of storage volume (e.g., Upper Klamath Lake), the workflow converts elevation to storage using lookup tables. This is supported for **all data sources** (RISE, USACE, USGS, CDEC).

```mermaid
flowchart LR
    subgraph sources["Any Data Source"]
        RISE_E["RISE<br/>Elevation data"]
        USACE_E["USACE<br/>Elevation data"]
        USGS_E["USGS<br/>param 62614/62615"]
        CDEC_E["CDEC<br/>Elevation data"]
    end

    subgraph conversion["Conversion"]
        CURVE["elevation_storage_curves.csv<br/>Elevation → Storage lookup"]
        INTERP["Linear Interpolation"]
    end

    subgraph result["Result"]
        STORAGE["Storage (af)<br/>Same format as<br/>direct storage sources"]
    end

    RISE_E --> INTERP
    USACE_E --> INTERP
    USGS_E --> INTERP
    CDEC_E --> INTERP
    CURVE --> INTERP
    INTERP --> STORAGE
```

**Current elevation-based locations:**
- Upper Klamath Lake (USGS 11507001) - 2017 KBAO elevation-capacity curve

**To add elevation-based reservoir from any source:**
1. Add elevation-storage curve to `config/elevation_storage_curves.csv`
2. Set `Storage Data Type = Elevation` in `config/locations.csv`

## Auto-Backfill Process

When a new location appears in `locations.geojson` but is not in `historical_baseline.parquet`, the daily script automatically backfills historical data.

```mermaid
flowchart TB
    subgraph detection["Detection"]
        GEOJSON_LOCS["Locations in geojson"]
        BASELINE_LOCS["Locations in baseline.parquet"]
        DIFF["New locations =<br/>geojson - baseline"]
    end

    subgraph backfill["Backfill Process"]
        FETCH["Fetch historical data<br/>1990-10-01 to 2020-09-30"]
        STATS["Compute day-of-year<br/>statistics"]
        UPDATE["Append to parquet files"]
    end

    subgraph output_bf["Output"]
        BACKFILL["backfill_YYYYMMDD.csv<br/>All historical rows<br/>for new locations"]
        HS["Upload to HydroShare"]
    end

    GEOJSON_LOCS --> DIFF
    BASELINE_LOCS --> DIFF
    DIFF --> FETCH
    FETCH --> STATS
    STATS --> UPDATE
    FETCH --> BACKFILL
    BACKFILL --> HS
```

**Triggers:**
- Newly added reservoirs in `locations.csv`
- Status change from "Do Not Include" to "Include"

## Historical Statistics Period

```mermaid
gantt
    title Historical Data Coverage (30 Water Years)
    dateFormat YYYY-MM-DD
    section Statistics Period
    Water Years 1991-2020    :1990-10-01, 2020-09-30
    section Daily Production
    Current Data             :2020-10-01, 2026-02-10
```
