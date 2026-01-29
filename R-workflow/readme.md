# WWDH Reservoir Visualization Data Generator - R Implementation

This R implementation provides a streamlined version of the Teacup Generator application that aggregates reservoir conditions data and generates daily CSV outputs for the WWDH Reservoir Dashboard teacup diagrams.

**Current scope**: This implementation queries data exclusively from RISE reservoirs via the WWDH API's `rise-edr` collection. Additional data sources (e.g., USBR, other state/federal sources) can be added in future versions with source-specific query logic.

## Overview

This implementation consolidates the functionality of the original .NET application into a single R script that:

1. Fetches current reservoir conditions data
2. Retrieves and processes historical data for statistical calculations
3. Calculates percentiles and summary statistics
4. Generates a daily CSV output
5. Posts the results to a designated HydroShare resource

## Files

- `rezviz_data_generator.R` - Main script containing all data fetching, processing, and upload logic
- `config/locations.csv` - Configuration file containing reservoir location metadata
- `data/` - Directory for intermediate data storage (optional, for debugging)
- `.env` - Environment file for API credentials (not tracked in git)
- `Dockerfile` - Docker container definition using Rocker
- `docker-compose.yml` - Docker Compose configuration for container orchestration
- `renv.lock` - R package dependency lock file (optional, for reproducibility)

## Requirements

```r
# Required R packages
library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)
library(readr)
```

## Configuration

### Environment Variables

Create a `.env` file in the script directory with the following variables:

```
WWDH_API_URL=https://api.wwdh.internetofwater.app
WWDH_COLLECTION=rise-edr
HYDROSHARE_USERNAME=your_username
HYDROSHARE_PASSWORD=your_password
HYDROSHARE_RESOURCE_ID=your_resource_id
```

### Location Configuration

The `config/locations.csv` file should contain the following columns:

- `location_id` - WWDH EDR location identifier for the reservoir
- `location_name` - Display name
- `max_capacity` - Maximum storage capacity (from metadata or external source)
- `display_order` - Order for output sorting
- Additional metadata as needed

Note: The WWDH API EDR `/locations` endpoint provides available location identifiers for the `rise-edr` collection.

### WWDH API Integration

The script uses the OGC Environmental Data Retrieval (EDR) API specification implemented by the WWDH API. Key endpoints:

**Get collection metadata:**
```
GET https://api.wwdh.internetofwater.app/collections/rise-edr?f=json
```

**List available locations:**
```
GET https://api.wwdh.internetofwater.app/collections/rise-edr/locations?f=json
```

**Query data for a specific location and time:**
```
GET https://api.wwdh.internetofwater.app/collections/rise-edr/location/{location_id}&datetime={iso8601_datetime}&f=json
```


The API returns data in CoverageJSON format, which the script parses to extract reservoir storage values.

## Functionality

### Data Retrieval

The script fetches data from the WWDH API (Water Data Workgroup Hub) EDR (Environmental Data Retrieval) endpoint for the `rise-edr` collection:

- **Current conditions**: Uses the `/location` query to retrieve reservoir storage values for specific dates and locations (up to 7 days lookback for most recent valid value)
- **Historical data**: Queries the `/cube` or `/position` endpoint for the period October 1, 1990 - September 30, 2020 to build statistical baselines

Example API calls:
- Locations list: `https://api.wwdh.internetofwater.app/collections/rise-edr/locations?f=json`
- All values subject to server limit: `https://api.wwdh.internetofwater.app/collections/rise-edr/locations/{locationId}`
- Historical range: `https://api.wwdh.internetofwater.app/collections/rise-edr/locations/{locationId})&datetime=1990-10-01/2020-09-30&f=json`

**Note**: This version only implements RISE data queries. Future versions can add support for additional sources (USBR, USGS, state agencies, etc.) by implementing source-specific query functions and adding a `data_source` column to `locations.csv`.

### Statistical Calculations

For each reservoir location, the script calculates:
- Percentiles (10th, 25th, 50th/median, 75th, 90th)
- Mean/average
- Standard deviation
- Percent of median (current value / 50th percentile)
- Percent of average (current value / mean)
- Percent full (current value / max capacity)

### Output Format

The daily CSV file contains one row per reservoir with columns:
- `date` - Date of observation
- `location_id` - Reservoir identifier
- `location_name` - Display name
- `current_value` - Most recent valid storage value
- `percentile_10`, `percentile_25`, `percentile_50`, `percentile_75`, `percentile_90`
- `mean`, `std_dev`
- `percent_median`, `percent_average`, `percent_full`
- `max_capacity`

Missing or failed calculations are represented as `NA`.

## Execution

### Manual Execution

```r
source("rezviz_data_generator.R")
```

The script will:
1. Load configuration and credentials
2. Fetch current and historical data
3. Calculate statistics
4. Generate CSV output
5. Upload to HydroShare

### Automated Execution (Recommended)

Schedule the script to run daily using cron (Linux/Mac) or Task Scheduler (Windows).

**Example cron entry (runs daily at 6 AM):**
```
0 6 * * * /usr/bin/Rscript /path/to/rezviz_data_generator.R >> /path/to/logs/rezviz.log 2>&1
```

## Docker Deployment

This application can be containerized using Rocker (R Docker images) for consistent, reproducible execution across environments.

### Dockerfile

```dockerfile
FROM rocker/r-ver:4.3.2

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency files first for better caching
COPY renv.lock* ./

# Install R packages
RUN R -e "install.packages(c('httr', 'jsonlite', 'dplyr', 'lubridate', 'readr', 'dotenv'))"

# Copy application files
COPY rezviz_data_generator.R ./
COPY config/ ./config/

# Create data directory
RUN mkdir -p data

# Set timezone (adjust as needed)
ENV TZ=America/Denver
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Run the script
CMD ["Rscript", "rezviz_data_generator.R"]
```

### docker-compose.yml

```yaml
version: '3.8'

services:
  rezviz-data-generator:
    build: .
    container_name: rise-rezviz-data-generator
    environment:
      - TZ=America/Denver
    env_file:
      - .env
    volumes:
      - ./config:/app/config:ro
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped
    # Uncomment to run on schedule (daily at 6 AM)
    # labels:
    #   - "ofelia.enabled=true"
    #   - "ofelia.job-exec.rezviz-daily.schedule=0 6 * * *"
    #   - "ofelia.job-exec.rezviz-daily.command=Rscript rezviz_data_generator.R"
```

### Building and Running with Docker

**Build the image:**
```bash
docker build -t rise-rezviz-data-generator .
```

**Run manually:**
```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  rise-rezviz-data-generator
```

**Run with Docker Compose:**
```bash
docker-compose up -d
```

**View logs:**
```bash
docker-compose logs -f
```

**Stop the container:**
```bash
docker-compose down
```

### Scheduled Execution with Docker

For automated daily execution, use one of these approaches:

**Option 1: Host cron calling Docker**
```bash
0 6 * * * docker-compose -f /path/to/docker-compose.yml run --rm rezviz-data-generator >> /path/to/logs/cron.log 2>&1
```


### Environment Variables for Docker

The `.env` file should be placed in the same directory as `docker-compose.yml`:

```
WWDH_API_URL=https://api.wwdh.internetofwater.app
WWDH_COLLECTION=rise-edr
HYDROSHARE_USERNAME=username
HYDROSHARE_PASSWORD=password
HYDROSHARE_RESOURCE_ID=your_resource_id
TZ=America/Denver
```

**Security note:** Never commit `.env` to version control. Add it to `.gitignore`.


## HydroShare Integration

The script posts the daily CSV file to a designated HydroShare resource using the HydroShare REST API. The resource is configured to maintain a time series of daily reservoir condition snapshots.

Each upload:
- Creates a new file named `reservoir_conditions_YYYY-MM-DD.csv`
- Updates resource metadata with the latest observation date
- Maintains running historical file for trend analysis/ quick database reload

## Error Handling

The script includes error handling for:
- API connection failures
- Missing or invalid data
- Failed statistical calculations
- HydroShare upload errors

Errors are logged with timestamps and detailed messages for debugging.

## Development and Testing

To test locally without uploading to HydroShare:

```r
# Set test mode flag
TEST_MODE <- TRUE
source("rezviz_data_generator.R")
```

This will generate the CSV file locally without attempting the HydroShare upload.

## Future Extensibility

### Adding Additional Data Sources

The current implementation is RISE-only. To add support for other reservoir data sources:

1. **Update `config/locations.csv`**: Add a `data_source` column (values: `rise`, `usbr`, `usgs`, etc.)

2. **Implement source-specific query functions** in the R script:
   ```r
   fetch_rise_data <- function(location_id, date_range) { ... }
   fetch_usbr_data <- function(location_id, date_range) { ... }
   fetch_usgs_data <- function(location_id, date_range) { ... }
   ```

3. **Add routing logic**:
   ```r
   fetch_data <- function(location) {
     switch(location$data_source,
       "rise" = fetch_rise_data(location$location_id, date_range),
       "usbr" = fetch_usbr_data(location$location_id, date_range),
       "usgs" = fetch_usgs_data(location$location_id, date_range),
       stop("Unknown data source: ", location$data_source)
     )
   }
   ```

4. **Update environment variables** as needed for additional API credentials

Each data source will likely require unique query patterns, authentication methods, and response parsing logic.

## Migration Notes

This R implementation differs from the original .NET application in the following ways:

- **Single script architecture**: All functionality consolidated into one file instead of multiple programs
- **WWDH API data source**: Uses the OGC EDR API via WWDH instead of direct RISE API calls, providing standardized geospatial data access
- **No teacup diagram generation**: Focuses on data aggregation and CSV output; visualization handled separately
- **HydroShare output**: Replaces local file storage with cloud-based resource management
- **Simplified scheduling**: Uses standard cron/Task Scheduler instead of custom PowerShell scripts
- **Historical data caching**: Historical statistics calculated on-demand rather than pre-computed

## Maintenance

### Updating Location Configuration

Edit `config/locations.csv` to add, remove, or modify reservoir locations. Changes will be reflected in the next script execution.

### Updating Historical Data Range

Modify the `HISTORICAL_START_DATE` and `HISTORICAL_END_DATE` constants in the script header to adjust the period used for statistical calculations.

Note: This documentation was developed with assistance from Claude (Anthropic) to translate the original .NET application architecture into an R-based workflow optimized for containerized deployment and cloud data sharing via HydroShare.
