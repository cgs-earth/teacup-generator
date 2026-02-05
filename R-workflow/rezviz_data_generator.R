# rezviz_data_generator.R
#
# DAILY SCRIPT: Query current reservoir conditions, combine with historical
# statistics, generate output CSV for teacup visualization, and upload to
# HydroShare.
#
# Designed to run daily via cron/scheduler.
# Depends on historical_statistics.parquet created by setup_historical_baseline.R
#
# HydroShare credentials loaded from .env file in the working directory.
# Expected .env format:
#   HYDROSHARE_USERNAME=user@example.com
#   HYDROSHARE_PASSWORD=yourpassword
#
# Author: Kyle Onda, CGS
# Created: 2026-01-28
################################################################################

library(httr2)
library(dplyr)
library(readr)
library(lubridate)
library(arrow)
library(stringr)
library(sf)

# Load .env file if present
if (file.exists(".env")) {
  readRenviron(".env")
}

################################################################################
# CONFIGURATION
################################################################################

# API base URL for RISE data via WWDH
WWDH_API_BASE <- "https://api.wwdh.internetofwater.app"

# HydroShare
HYDROSHARE_RESOURCE_ID <- "22b2f10103e5426a837defc00927afbd"
HYDROSHARE_BASE_URL <- "https://www.hydroshare.org"

# Paths
OUTPUT_DIR <- "output"
CONFIG_DIR <- "config"

# Target date (default: yesterday, can override via command line)
args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  TARGET_DATE <- as.Date(args[1])
} else {
  TARGET_DATE <- Sys.Date() - 1
}

# How many days to look back if no data on target date
LOOKBACK_DAYS <- 7

# Historical statistics period (numeric value matching .NET output)
STATS_PERIOD <- "10/1/1990 - 9/30/2020"

message(sprintf("=== Reservoir Data Generator ==="))
message(sprintf("Target date: %s", TARGET_DATE))
message(sprintf("Run time: %s", Sys.time()))

################################################################################
# LOAD HISTORICAL STATISTICS
################################################################################

stats_file <- file.path(OUTPUT_DIR, "historical_statistics.parquet")
if (!file.exists(stats_file)) {
  stop("Historical statistics file not found. Run setup_historical_baseline.R first.")
}

historical_stats <- read_parquet(stats_file)
message(sprintf("Loaded historical statistics: %d rows for %d locations",
                nrow(historical_stats),
                n_distinct(historical_stats$location_id)))

################################################################################
# LOAD LOCATION METADATA
################################################################################

locations_file <- file.path(CONFIG_DIR, "locations.geojson")
locations_sf <- st_read(locations_file, quiet = TRUE)

# Extract location metadata from geojson - ALL locations
locations <- locations_sf |>
  st_drop_geometry() |>
  transmute(
    name = Name,
    location_id = Identifier,
    capacity = as.numeric(str_remove_all(`Total.Capacity`, ",")),
    label_map = `Preferred.Label.for.Map.and.Table`,
    label_popup = `Preferred.Label.for.PopUp.and.Modal`,
    state = state,
    doi_region = doiRegion,
    huc6 = huc6,
    longitude = Longitude,
    latitude = Latitude
  )

message(sprintf("Loaded %d locations from geojson", nrow(locations)))

# Track which locations have historical statistics (for logging)
locations_with_stats <- unique(historical_stats$location_id)
n_with_stats <- sum(locations$location_id %in% locations_with_stats)
message(sprintf("  %d locations have historical statistics", n_with_stats))
message(sprintf("  %d locations will have NA for historical metrics",
                nrow(locations) - n_with_stats))

################################################################################
# DATA FETCHING FUNCTION
################################################################################

#' Fetch current storage value from RISE via WWDH API
#'
#' Tries the target date first, then looks back up to LOOKBACK_DAYS days
#' to find the most recent available value.
#'
#' @param location_id RISE location ID
#' @param target_date Target date to fetch
#' @param lookback_days How many days to look back if no data on target
#' @return list with: value, date, unit (or NAs if no data found)
#'
fetch_current_value <- function(location_id, target_date, lookback_days = LOOKBACK_DAYS) {

  # Try each day from target_date back to target_date - lookback_days
  for (days_back in 0:lookback_days) {
    query_date <- target_date - days_back
    start_date <- query_date
    # API requires day after intended end date to include the end date in results
    end_date <- query_date + 1

    url <- paste0(
      WWDH_API_BASE,
      "/collections/rise-edr/locations/", location_id,
      "?parameter-name=Storage",
      "&limit=10",
      "&datetime=", start_date, "/", end_date,
      "&f=csv"
    )

    tryCatch({
      response <- request(url) |>
        req_timeout(60) |>
        req_retry(max_tries = 2, backoff = ~ 2) |>
        req_perform()

      if (resp_status(response) != 200) next

      csv_content <- resp_body_string(response)
      if (nchar(csv_content) < 30) next

      data <- read_csv(csv_content, show_col_types = FALSE)
      if (nrow(data) == 0) next

      # Return first valid value
      return(list(
        value = data$value[1],
        date = as.Date(data$datetime[1]),
        unit = data$unit[1]
      ))

    }, error = function(e) {
      # Continue to next day
    })
  }

  # No data found
  return(list(value = NA, date = NA, unit = NA))
}

################################################################################
# MAIN PROCESSING LOOP
################################################################################

message("\n=== Fetching current values ===\n")

results <- list()

for (i in seq_len(nrow(locations))) {
  loc <- locations[i, ]
  location_id <- loc$location_id
  location_name <- loc$name

  message(sprintf("[%d/%d] %s (ID: %s)...",
                  i, nrow(locations), location_name, location_id))

  # Fetch current value
  current <- fetch_current_value(location_id, TARGET_DATE)

  if (is.na(current$value)) {
    message(sprintf("  No data found"))
    results[[i]] <- tibble(
      location_id = location_id,
      name = location_name,
      data_value = NA_real_,
      data_date = as.Date(NA),
      data_unit = NA_character_
    )
    next
  }

  message(sprintf("  Value: %s %s (date: %s)",
                  format(current$value, big.mark = ","),
                  current$unit,
                  current$date))

  results[[i]] <- tibble(
    location_id = location_id,
    name = location_name,
    data_value = current$value,
    data_date = current$date,
    data_unit = current$unit
  )

  # Rate limiting
  Sys.sleep(0.25)
}

# Combine results
current_data <- bind_rows(results)

################################################################################
# JOIN WITH HISTORICAL STATISTICS
################################################################################

message("\n=== Joining with historical statistics ===\n")

# Get the month and day for the target date to look up correct historical stats
target_month <- month(TARGET_DATE)
target_day <- day(TARGET_DATE)

# Get historical stats for today's calendar day
todays_stats <- historical_stats |>
  filter(month == target_month, day == target_day) |>
  select(location_id, min, max, p10, p25, p50, p75, p90, mean, unit)

# Join current data with location metadata and historical stats
# Use left_join so ALL locations appear even without stats
output_data <- current_data |>
  left_join(locations, by = c("location_id", "name")) |>
  left_join(todays_stats, by = "location_id", suffix = c("", "_hist")) |>
  mutate(
    # Calculate derived values (will be NA if stats or current value missing)
    pct_median = data_value / p50,
    pct_average = data_value / mean,
    pct_full = data_value / capacity,
    # Format dates
    data_date_fmt = format(data_date, "%m/%d/%Y"),
    date_queried = format(Sys.Date(), "%m/%d/%Y")
  )

################################################################################
# GENERATE OUTPUT CSV
################################################################################

message("\n=== Generating output CSV ===\n")

output_csv <- output_data |>
  transmute(
    SiteName = label_popup,
    Lat = latitude,
    Lon = longitude,
    State = state,
    DoiRegion = doi_region,
    Huc6 = huc6,
    DataUnits = coalesce(data_unit, unit),
    DataValue = data_value,
    DataDate = data_date_fmt,
    DateQueried = date_queried,
    DataDateMax = max,
    DataDateP90 = p90,
    DataDateP75 = p75,
    DataDateP50 = p50,
    DataDateP25 = p25,
    DataDateP10 = p10,
    DataDateMin = min,
    DataDateAvg = mean,
    DataValuePctMdn = pct_median,
    DataValuePctAvg = pct_average,
    StatsPeriod = STATS_PERIOD,
    MaxCapacity = capacity,
    PctFull = pct_full,
    TeacupUrl = NA_character_,
    DataUrl = NA_character_,
    Comment = NA_character_
  )

# Generate filename
output_filename <- sprintf("droughtData%s.csv", format(TARGET_DATE, "%Y%m%d"))
output_path <- file.path(OUTPUT_DIR, output_filename)

# Write CSV (standard comma-separated)
write_csv(output_csv, output_path, na = "")

message(sprintf("Output written to: %s", output_path))
message(sprintf("  Total locations: %d", nrow(output_csv)))
message(sprintf("  With data: %d", sum(!is.na(output_csv$DataValue))))
message(sprintf("  Missing data: %d", sum(is.na(output_csv$DataValue))))
message(sprintf("  With historical stats: %d", sum(!is.na(output_csv$DataDateP50))))
message(sprintf("  Without historical stats: %d", sum(is.na(output_csv$DataDateP50))))

################################################################################
# UPLOAD TO HYDROSHARE
################################################################################

message("\n=== Uploading to HydroShare ===\n")

hs_username <- Sys.getenv("HYDROSHARE_USERNAME", unset = "")
hs_password <- Sys.getenv("HYDROSHARE_PASSWORD", unset = "")

if (hs_username == "" || hs_password == "") {
  message("WARNING: HYDROSHARE_USERNAME and/or HYDROSHARE_PASSWORD not set.")
  message("Skipping HydroShare upload. Set environment variables to enable upload.")
} else {

  upload_to_hydroshare <- function(file_path, resource_id, username, password) {
    filename <- basename(file_path)

    # First, try to delete the existing file (if updating daily, old file may exist)
    delete_url <- sprintf("%s/hsapi/resource/%s/files/%s/",
                          HYDROSHARE_BASE_URL, resource_id, filename)

    tryCatch({
      request(delete_url) |>
        req_auth_basic(username, password) |>
        req_method("DELETE") |>
        req_timeout(60) |>
        req_perform()
      message(sprintf("  Deleted existing file: %s", filename))
    }, error = function(e) {
      message(sprintf("  No existing file to delete (or delete failed): %s", filename))
    })

    # Upload the new file
    upload_url <- sprintf("%s/hsapi/resource/%s/files/",
                          HYDROSHARE_BASE_URL, resource_id)

    response <- request(upload_url) |>
      req_auth_basic(username, password) |>
      req_body_multipart(file = curl::form_file(file_path)) |>
      req_timeout(120) |>
      req_perform()

    status <- resp_status(response)
    if (status >= 200 && status < 300) {
      message(sprintf("  Successfully uploaded %s to HydroShare resource %s",
                      filename, resource_id))
    } else {
      warning(sprintf("  Upload returned status %d", status))
    }

    return(status)
  }

  tryCatch({
    upload_to_hydroshare(output_path, HYDROSHARE_RESOURCE_ID, hs_username, hs_password)
  }, error = function(e) {
    message(sprintf("ERROR uploading to HydroShare: %s", e$message))
  })
}

################################################################################
# SUMMARY
################################################################################

message("\n=== Summary ===")
message(sprintf("Target date: %s", TARGET_DATE))
message(sprintf("Locations processed: %d", nrow(output_csv)))
message(sprintf("Locations with current data: %d (%.1f%%)",
                sum(!is.na(output_csv$DataValue)),
                100 * sum(!is.na(output_csv$DataValue)) / nrow(output_csv)))
message(sprintf("Locations with historical stats: %d (%.1f%%)",
                sum(!is.na(output_csv$DataDateP50)),
                100 * sum(!is.na(output_csv$DataDateP50)) / nrow(output_csv)))
message(sprintf("Output file: %s", output_path))
message(sprintf("Completed at: %s", Sys.time()))
