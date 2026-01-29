#!/usr/bin/env Rscript
################################################################################
# setup_historical_baseline.R
#
# ONE-TIME SETUP: Generate historical baseline data and statistics for RISE 
# reservoirs from WWDH API (1990-10-01 to 2020-09-30).
#
# Run this ONCE before starting daily operations to create cached historical 
# data and pre-computed statistics.
#
# Author: Kyle Onda, Internet of Water
# Created: 2026-01-28
################################################################################

# CONFIGURATION
# - Load environment variables (WWDH_API_URL, WWDH_COLLECTION)
# - Set historical date range: 1990-10-01 to 2020-09-30 (9120 days)
# - Define output paths for cached data

# LOAD LOCATION METADATA
# - Read config/locations.csv
# - Contains: location_id, site_name, lat, lon, state, doi_region, huc8, 
#   max_capacity, teacup_url

# FOR EACH LOCATION:
#   1. Query WWDH API for full historical range
#      - Endpoint: /collections/rise-edr/position
#      - Parameters: locationId, datetime=1990-10-01/2020-09-30, f=json
#      - Parse CoverageJSON response to extract date/value pairs
#      - Handle API errors and retries
#   
#   2. Calculate historical statistics:
#      - Min, Max
#      - Percentiles: 10th, 25th, 50th (median), 75th, 90th
#      - Mean
#      - Count of observations
#   
#   3. Store results:
#      - Add to combined historical dataset (all dates/values for all locations)
#      - Add to statistics lookup table (one row per location)

# SAVE OUTPUTS
# - historical_baseline.parquet: Full time series for all locations (optional, for future use)
# - historical_statistics.parquet: Pre-computed stats, one row per location (REQUIRED for daily script)
# - historical_statistics.csv: Human-readable copy

# ERROR HANDLING
# - Log locations that fail
# - Continue processing remaining locations
# - Save partial results if script interrupted

# NOTES:
# - This may take 30-60 minutes depending on API speed
# - Consider batching requests or parallel processing if needed
# - Be respectful of API rate limits (add delays between requests)
