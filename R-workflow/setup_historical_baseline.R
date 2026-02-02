# setup_historical_baseline.R
#
# ONE-TIME SETUP: Generate historical baseline data and statistics for
# reservoirs from multiple data sources (1990-10-01 to 2020-09-30).
#
# Run this ONCE before starting daily operations to create cached historical
# data and pre-computed statistics.
#
# Author: Kyle Onda, Internet of Water
# Created: 2026-01-28
################################################################################

library(httr2)
library(dplyr)
library(readr)
library(lubridate)
library(arrow)
library(purrr)
library(tidyr)
library(stringr)

################################################################################
# CONFIGURATION
################################################################################

# Historical date range (30 water years)
START_DATE <- "1990-10-01"
END_DATE <- "2020-09-30"

# API base URL for RISE data via WWDH
WWDH_API_BASE <- "https://api.wwdh.internetofwater.app"

# Output paths
OUTPUT_DIR <- "output"
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

################################################################################
# LOAD LOCATION METADATA
################################################################################

locations_raw <- read_csv("config/locations.csv", show_col_types = FALSE)

# Filter to locations that are NOT "Do Not Include" and parse source info
# The "Post-Review Decision" column contains values like:
#   - "Include"
#   - "Include if possible"
#   - "Include (Omit % Avg if needed)"
#   - "Do Not Include"
locations <- locations_raw |>
  filter(`Post-Review Decision` != "Do Not Include") |>
  transmute(
    name = Name,
    decision = `Post-Review Decision`,
    source = `Source for Storage Data`,
    # Generalized location_id - interpretation depends on source_type:
    #   - For RISE: this is the RISE location ID
    #   - For USGS: extract site number from source URL
    #   - For USACE: extract location identifier from source URL
    #   - etc.
    location_id = `RISE Location ID`,
    capacity = as.numeric(str_remove_all(`Total Capacity`, ",")),
    label_map = `Preferred Label for Map and Table`,
    label_popup = `Preferred Label for PopUp and Modal`
  ) |>
  # Determine source type from the "Source for Storage Data" column
  mutate(
    source_type = case_when(
      source == "RISE" ~ "RISE",
      str_detect(source, "RISE \\(Pending\\)") ~ "RISE_PENDING",
      str_detect(source, "USACE") ~ "USACE",
      str_detect(source, "USGS") ~ "USGS",
      str_detect(source, "TROA") ~ "TROA",
      TRUE ~ "OTHER"
    )
  )

message(sprintf("Loaded %d locations (excluding 'Do Not Include')", nrow(locations)))

# Split locations by source type for processing
rise_locations <- locations |>
  filter(source_type == "RISE", !is.na(location_id), location_id != "--")

usace_locations <- locations |>
  filter(source_type == "USACE")

usgs_locations <- locations |>
  filter(source_type == "USGS")

message(sprintf("  - RISE: %d locations", nrow(rise_locations)))
message(sprintf("  - USACE: %d locations (not yet implemented)", nrow(usace_locations)))
message(sprintf("  - USGS: %d locations (not yet implemented)", nrow(usgs_locations)))

################################################################################
# DATA FETCHING FUNCTIONS BY SOURCE
################################################################################

#' Fetch historical storage data from RISE via WWDH API
#'
#' @param location_id RISE location ID (numeric or character)
#' @param start_date Start date (YYYY-MM-DD)
#' @param end_date End date (YYYY-MM-DD)
#' @return tibble with columns: date, value, unit (storage in acre-feet typically)
#'
#' API URL pattern:
#'   https://api.wwdh.internetofwater.app/collections/rise-edr/locations/{location_id}
#'     ?parameter-name=Storage&limit=9999&datetime={start}/{end}&f=csv
#'
fetch_rise_historical <- function(location_id, start_date = START_DATE, end_date = END_DATE, max_retries = 5) {

  url <- paste0(
    WWDH_API_BASE,
    "/collections/rise-edr/locations/", location_id,
    "?parameter-name=Storage",
    "&limit=20000",
    "&datetime=", start_date, "/", end_date,
    "&f=csv"
  )

  message(sprintf("  Fetching RISE location %s...", location_id))

  for (attempt in 1:max_retries) {
    tryCatch({
      response <- request(url) |>
        req_timeout(300) |>
        req_retry(max_tries = 5, backoff = ~ 5) |>
        req_perform()

      # Check HTTP status
      status <- resp_status(response)
      if (status != 200) {
        warning(sprintf("HTTP %d for location %s", status, location_id))
        return(tibble(date = Date(), value = numeric(), unit = character()))
      }

      # Parse CSV response
      csv_content <- resp_body_string(response)

      # Check if response is empty or malformed
      if (nchar(csv_content) < 30) {
        warning(sprintf("Empty or malformed response for location %s", location_id))
        return(tibble(date = Date(), value = numeric(), unit = character()))
      }

      data <- read_csv(csv_content, show_col_types = FALSE)

      # Verify expected columns exist
      required_cols <- c("datetime", "value", "unit")
      if (!all(required_cols %in% names(data))) {
        warning(sprintf("Missing columns for location %s. Got: %s",
                        location_id, paste(names(data), collapse = ", ")))
        return(tibble(date = Date(), value = numeric(), unit = character()))
      }

      # Standardize column names based on actual RISE API response
      # API returns: parameter, datetime, value, unit, x, y
      data <- data |>
        transmute(
          date = as.Date(datetime),
          value = value,
          unit = unit
        ) |>
        filter(!is.na(value))

      # Remove duplicate days - keep only the first instance of each day
      rows_before <- nrow(data)
      data <- data |>
        arrange(date) |>
        distinct(date, .keep_all = TRUE)
      rows_after <- nrow(data)

      if (rows_before != rows_after) {
        message(sprintf("    Removed %d duplicate days (kept first instance)", rows_before - rows_after))
      }

      return(data)

    }, error = function(e) {
      if (attempt < max_retries) {
        wait_time <- 10 * attempt  # 10, 20, 30, 40 seconds
        message(sprintf("  Attempt %d failed for location %s: %s. Waiting %ds before retry...",
                        attempt, location_id, e$message, wait_time))
        Sys.sleep(wait_time)
      } else {
        warning(sprintf("Failed to fetch location %s after %d attempts: %s",
                        location_id, max_retries, e$message))
      }
    })
  }

  return(tibble(date = Date(), value = numeric(), unit = character()))
}

#' Fetch historical data from USACE
#' @description Placeholder for USACE data fetching - TO BE IMPLEMENTED
fetch_usace_historical <- function(location_info, start_date = START_DATE, end_date = END_DATE) {
  # TODO: Implement USACE data fetching
  # USACE provides data via: https://water.usace.army.mil/overview/{district}/locations/{location}
  # Will need to parse the source URL from locations.csv to extract district and location
  # NOTE: Preserve unit information - USACE may use different units than RISE
  warning("USACE data fetching not yet implemented")
  return(tibble(date = Date(), value = numeric(), unit = character()))
}

#' Fetch historical data from USGS NWIS
#' @description Placeholder for USGS data fetching - TO BE IMPLEMENTED
fetch_usgs_historical <- function(location_info, start_date = START_DATE, end_date = END_DATE) {
  # TODO: Implement USGS data fetching
  # USGS NWIS API: https://waterservices.usgs.gov/nwis/dv/
  # Will need to extract site number from locations.csv source field
  # NOTE: Preserve unit information - USGS may use different units than RISE
  warning("USGS data fetching not yet implemented")
  return(tibble(date = Date(), value = numeric(), unit = character()))
}

################################################################################
# STATISTICS CALCULATION
################################################################################

#' Calculate day-of-year statistics from historical time series
#'
#' IMPORTANT: Statistics are computed separately for EACH calendar day (month-day).
#' This groups all historical values by their month-day combination, then computes
#' stats within each group. For example:
#'   - January 1:  Uses all Jan 1 values from 1991, 1992, ..., 2020 (~30 values)
#'   - January 2:  Uses all Jan 2 values from 1991, 1992, ..., 2020 (~30 values)
#'   - ...
#'   - December 31: Uses all Dec 31 values from 1990, 1991, ..., 2019 (~30 values)
#'   - February 29: Uses all Feb 29 values from leap years only (~7-8 values)
#'
#' This produces 366 rows of statistics per location (one for each possible
#' calendar day including Feb 29), enabling comparison of a current day's value
#' against the historical distribution for THAT SPECIFIC day of the year.
#'
#' @param data tibble with columns: date, value, unit
#' @param location_id Identifier for the location
#' @return tibble with columns: location_id, month, day, min, max, p10, p25, p50, p75, p90, mean, count, unit
#'
calculate_daily_stats <- function(data, location_id) {

  if (nrow(data) == 0) {
    return(tibble())
  }

  # Preserve the unit from the source data
  # (assumes all values for a location have the same unit)
  data_unit <- unique(data$unit)[1]

  data |>
    mutate(
      month = month(date),
      day = day(date)
    ) |>
    group_by(month, day) |>
    summarise(
      min = min(value, na.rm = TRUE),
      max = max(value, na.rm = TRUE),
      p10 = quantile(value, 0.10, na.rm = TRUE),
      p25 = quantile(value, 0.25, na.rm = TRUE),
      p50 = quantile(value, 0.50, na.rm = TRUE),  # median
      p75 = quantile(value, 0.75, na.rm = TRUE),
      p90 = quantile(value, 0.90, na.rm = TRUE),
      mean = mean(value, na.rm = TRUE),
      count = sum(!is.na(value)),
      .groups = "drop"
    ) |>
    mutate(
      location_id = location_id,
      unit = data_unit
    ) |>
    select(location_id, month, day, everything())
}

################################################################################
# MAIN PROCESSING LOOP
################################################################################

# Initialize result containers
all_historical_data <- list()
all_statistics <- list()
failed_locations <- character()

# Process RISE locations
message("\n=== Processing RISE locations ===\n")

for (i in seq_len(nrow(rise_locations))) {
  loc <- rise_locations[i, ]
  location_id <- loc$location_id
  location_name <- loc$name

  message(sprintf("[%d/%d] Processing %s (ID: %s)",
                  i, nrow(rise_locations), location_name, location_id))

  # Fetch historical data
  hist_data <- fetch_rise_historical(location_id)

  if (nrow(hist_data) == 0) {
    message(sprintf("  WARNING: No data returned for %s", location_name))
    failed_locations <- c(failed_locations, location_name)
    next
  }

  message(sprintf("  Retrieved %d observations", nrow(hist_data)))

  # Calculate day-of-year statistics
  stats <- calculate_daily_stats(hist_data, location_id)
  message(sprintf("  Computed statistics for %d day-of-year groups", nrow(stats)))

  # Store results
  all_historical_data[[location_id]] <- hist_data |> mutate(location_id = location_id)
  all_statistics[[location_id]] <- stats

  # Be respectful of API rate limits
  Sys.sleep(0.5)
}

# TODO: Add processing loops for USACE, USGS, and other sources here
# message("\n=== Processing USACE locations ===\n")
# ...

################################################################################
# COMBINE AND SAVE OUTPUTS
################################################################################

message("\n=== Saving outputs ===\n")

# Combine all historical data
historical_baseline <- bind_rows(all_historical_data)
message(sprintf("Total historical observations: %d", nrow(historical_baseline)))

# Combine all statistics
historical_statistics <- bind_rows(all_statistics)
message(sprintf("Total statistics rows: %d (should be ~366 per location)", nrow(historical_statistics)))

# Save as Parquet (efficient binary format)
write_parquet(historical_baseline, file.path(OUTPUT_DIR, "historical_baseline.parquet"))
write_parquet(historical_statistics, file.path(OUTPUT_DIR, "historical_statistics.parquet"))

# Save statistics as CSV for human readability
write_csv(historical_statistics, file.path(OUTPUT_DIR, "historical_statistics.csv"))

# Save list of failed locations
if (length(failed_locations) > 0) {
  writeLines(failed_locations, file.path(OUTPUT_DIR, "failed_locations.txt"))
  message(sprintf("\nWARNING: %d locations failed to process. See failed_locations.txt",
                  length(failed_locations)))
}

message("\n=== Done ===")
message(sprintf("Outputs saved to: %s/", OUTPUT_DIR))
message("  - historical_baseline.parquet (full time series)")
message("  - historical_statistics.parquet (day-of-year stats for daily script)")
message("  - historical_statistics.csv (human-readable copy)")
