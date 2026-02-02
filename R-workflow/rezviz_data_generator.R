# rezviz_data_generator.R
#
# DAILY SCRIPT: Query current reservoir conditions, combine with historical
# statistics, generate output CSV for teacup visualization.
#
# Designed to run daily via cron/scheduler.
# Depends on historical_statistics.parquet created by setup_historical_baseline.R
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

################################################################################
# CONFIGURATION
################################################################################

# API base URL for RISE data via WWDH
WWDH_API_BASE <- "https://api.wwdh.internetofwater.app"

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

# Historical statistics period description
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

locations_file <- file.path(CONFIG_DIR, "locations.csv")
locations_raw <- read_csv(locations_file, show_col_types = FALSE)

# Filter to included locations and extract metadata
locations <- locations_raw |>
  filter(`Post-Review Decision` != "Do Not Include") |>
  transmute(
    name = Name,
    decision = `Post-Review Decision`,
    source = `Source for Storage Data`,
    location_id = `RISE Location ID`,
    capacity = as.numeric(str_remove_all(`Total Capacity`, ",")),
    label_map = `Preferred Label for Map and Table`,
    label_popup = `Preferred Label for PopUp and Modal`
  ) |>
  # Only process RISE locations for now
  filter(source == "RISE", !is.na(location_id), location_id != "--")

message(sprintf("Processing %d RISE locations", nrow(locations)))

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
      data_date = NA,
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
output_data <- current_data |>
  left_join(locations, by = c("location_id", "name")) |>
  left_join(todays_stats, by = "location_id", suffix = c("", "_hist")) |>
  mutate(
    # Calculate derived values
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

# Format output matching the .NET program's format
# Header: SiteName, Lat, Lon, State, DoiRegion, Huc8, DataUnits, DataValue,
#         DataDate, DateQueried, DataDateMax, DataDateP90, DataDateP75,
#         DataDateP50, DataDateP25, DataDateP10, DataDateMin, DataDateAvg,
#         DataValuePctMdn, DataValuePctAvg, StatsPeriod, MaxCapacity, PctFull,
#         TeacupUrl, DataUrl, Comment

# Note: We don't have all the fields from the original .NET version
# (State, DoiRegion, Huc8, TeacupUrl, DataUrl, Comment)
# Those would need to be added to locations.csv or fetched from another source

output_csv <- output_data |>
  transmute(
    SiteName = label_popup,
    Lat = NA_real_,           # Would need to add to locations.csv
    Lon = NA_real_,           # Would need to add to locations.csv
    State = NA_character_,    # Would need to add to locations.csv
    DoiRegion = NA_character_, # Would need to add to locations.csv
    Huc8 = NA_character_,     # Would need to add to locations.csv
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

# Write CSV with space after comma (matching .NET format)
# Note: Standard write_csv doesn't add space after comma, so we use a custom approach
write_csv(output_csv, output_path, na = "")

message(sprintf("Output written to: %s", output_path))
message(sprintf("  Total locations: %d", nrow(output_csv)))
message(sprintf("  With data: %d", sum(!is.na(output_csv$DataValue))))
message(sprintf("  Missing data: %d", sum(is.na(output_csv$DataValue))))

################################################################################
# SUMMARY
################################################################################

message("\n=== Summary ===")
message(sprintf("Target date: %s", TARGET_DATE))
message(sprintf("Locations processed: %d", nrow(output_csv)))
message(sprintf("Locations with current data: %d (%.1f%%)",
                sum(!is.na(output_csv$DataValue)),
                100 * sum(!is.na(output_csv$DataValue)) / nrow(output_csv)))
message(sprintf("Output file: %s", output_path))
message(sprintf("Completed at: %s", Sys.time()))
