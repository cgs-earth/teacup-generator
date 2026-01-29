
# rezviz_data_generator.R
#
# DAILY SCRIPT: Query current reservoir conditions, combine with historical 
# statistics, generate output CSV, and upload to HydroShare.
#
# Designed to run daily via cron/Docker scheduler.
# Depends on historical_statistics.parquet created by setup_historical_baseline.R
#
# Author: Kyle Onda, CGS
# Created: 2026-01-28
################################################################################

# CONFIGURATION
# - Load environment variables:
#   * WWDH_API_URL, WWDH_COLLECTION
#   * HYDROSHARE_USERNAME, HYDROSHARE_PASSWORD, HYDROSHARE_RESOURCE_ID
#   * TEST_MODE (optional, default=FALSE)
# - Set target date (default: yesterday)
# - Define output filename: droughtDataYYYYMMDD.csv

# LOAD PRE-COMPUTED HISTORICAL STATISTICS
# - Read data/historical_statistics.parquet
# - This contains min/max/percentiles/mean for each location
# - Fast lookup, no need to recalculate

# LOAD LOCATION METADATA
# - Read config/locations.csv
# - Merge with historical statistics on location_id

# FOR EACH LOCATION:
#   1. Query WWDH API for current value (with 7-day lookback)
#      - First try: target date
#      - If no data: try target date - 1, -2, -3, up to -7 days
#      - Parse CoverageJSON response
#      - Extract most recent valid value (DataValue) and its date (DataDate)
#   
#   2. Join with location metadata and historical statistics
#   
#   3. Calculate derived fields:
#      - DataValuePctMdn = DataValue / DataDateP50
#      - DataValuePctAvg = DataValue / DataDateAvg  
#      - PctFull = DataValue / MaxCapacity
#   
#   4. Format output row with all required columns:
#      SiteName, Lat, Lon, State, DoiRegion, Huc8, DataUnits, DataValue,
#      DataDate, DateQueried, DataDateMax, DataDateP90, DataDateP75, 
#      DataDateP50, DataDateP25, DataDateP10, DataDateMin, DataDateAvg,
#      DataValuePctMdn, DataValuePctAvg, StatsPeriod, MaxCapacity, PctFull,
#      TeacupUrl, DataUrl, Comment

# GENERATE OUTPUT CSV
# - Combine all location rows into single data frame
# - Format dates as MM/DD/YYYY
# - Ensure decimal percentages (not percentages * 100)
# - Add header with space after comma: "SiteName, Lat, Lon, ..."
# - Write to droughtDataYYYYMMDD.csv

# HYDROSHARE UPLOAD
# IF NOT TEST_MODE:
#   1. Upload daily file (droughtDataYYYYMMDD.csv)
#   
#   2. Update historical archive:
#      - Download existing droughtData_historical.csv from HydroShare (if exists)
#      - Append today's rows
#      - Re-upload droughtData_historical.csv
#   
#   3. Update resource metadata:
#      - Set last update date
#      - Update temporal coverage end date

# LOGGING
# - Print summary: X locations processed, Y successful, Z failed
# - Log any API errors or missing data
# - Record runtime

# ERROR HANDLING
# - Continue processing if individual locations fail
# - Still generate output CSV with NA for failed locations
# - Alert if >20% of locations fail

# CLEANUP
# - Remove temporary files
# - Exit with appropriate status code

# NOTES:
# - Designed to run in <60 seconds (only queries current day)
# - Uses cached historical stats (no recalculation)
# - HydroShare authentication uses basic auth or API token
