# process_manual_csvs.R
#
# Process manually downloaded CSV files for failed locations
# and add them to the historical baseline.
#
# Place CSV files named {location_id}.csv in the data/manual/ directory
# e.g., data/manual/267.csv, data/manual/282.csv, data/manual/489.csv
################################################################################

library(dplyr)
library(readr)
library(lubridate)
library(arrow)

MANUAL_DATA_DIR <- "data/manual"
OUTPUT_DIR <- "output"

# Historical date range (30 water years) - same as setup_historical_baseline.R
START_DATE <- as.Date("1990-10-01")
END_DATE <- as.Date("2020-09-30")

# Location IDs to process (the 3 "Include" failures)
location_ids <- c("267", "282", "489")

# Location names for reference
location_names <- c(
  "267" = "Avalon",
  "282" = "Brantley",
  "489" = "Sumner"
)

################################################################################
# PROCESS EACH CSV
################################################################################

all_data <- list()
all_stats <- list()

for (loc_id in location_ids) {
  csv_file <- file.path(MANUAL_DATA_DIR, paste0(loc_id, ".csv"))

  if (!file.exists(csv_file)) {
    message(sprintf("Skipping %s (%s) - file not found", loc_id, location_names[loc_id]))
    next
  }

  message(sprintf("Processing %s (%s)...", loc_id, location_names[loc_id]))

  data <- read_csv(csv_file, show_col_types = FALSE)

  if (nrow(data) == 0) {
    message(sprintf("  No data in file"))
    next
  }

  message(sprintf("  Found %d rows", nrow(data)))

  # Standardize columns and filter to historical date range
  data <- data |>
    transmute(
      location_id = loc_id,
      date = as.Date(datetime),
      value = value,
      unit = unit
    ) |>
    filter(!is.na(value)) |>
    filter(date >= START_DATE & date <= END_DATE)

  message(sprintf("  After date filter (%s to %s): %d rows", START_DATE, END_DATE, nrow(data)))

  # Remove duplicate days - keep only the first instance of each day
  rows_before <- nrow(data)
  data <- data |>
    arrange(date) |>
    distinct(location_id, date, .keep_all = TRUE)
  rows_after <- nrow(data)

  if (rows_before != rows_after) {
    message(sprintf("  Removed %d duplicate days (kept first instance)", rows_before - rows_after))
  }

  all_data[[loc_id]] <- data

  # Calculate day-of-year statistics
  data_unit <- unique(data$unit)[1]

  stats <- data |>
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
      p50 = quantile(value, 0.50, na.rm = TRUE),
      p75 = quantile(value, 0.75, na.rm = TRUE),
      p90 = quantile(value, 0.90, na.rm = TRUE),
      mean = mean(value, na.rm = TRUE),
      count = sum(!is.na(value)),
      .groups = "drop"
    ) |>
    mutate(
      location_id = loc_id,
      unit = data_unit
    ) |>
    select(location_id, month, day, everything())

  all_stats[[loc_id]] <- stats
  message(sprintf("  Computed statistics for %d day-of-year groups", nrow(stats)))
}

################################################################################
# COMBINE WITH EXISTING DATA
################################################################################

if (length(all_data) > 0) {
  new_historical <- bind_rows(all_data)
  new_statistics <- bind_rows(all_stats)

  message(sprintf("\nNew data: %d observations", nrow(new_historical)))
  message(sprintf("New statistics: %d rows", nrow(new_statistics)))

  # Load existing data if available
  existing_baseline_file <- file.path(OUTPUT_DIR, "historical_baseline.parquet")
  existing_stats_file <- file.path(OUTPUT_DIR, "historical_statistics.parquet")

  if (file.exists(existing_baseline_file)) {
    existing_baseline <- read_parquet(existing_baseline_file)
    # Remove any existing data for these locations (in case of re-run)
    existing_baseline <- existing_baseline |>
      filter(!location_id %in% location_ids)
    combined_baseline <- bind_rows(existing_baseline, new_historical)
    message(sprintf("Combined baseline: %d observations", nrow(combined_baseline)))
  } else {
    combined_baseline <- new_historical
  }

  if (file.exists(existing_stats_file)) {
    existing_stats <- read_parquet(existing_stats_file)
    existing_stats <- existing_stats |>
      filter(!location_id %in% location_ids)
    combined_stats <- bind_rows(existing_stats, new_statistics)
    message(sprintf("Combined statistics: %d rows", nrow(combined_stats)))
  } else {
    combined_stats <- new_statistics
  }

  # Save updated files
  write_parquet(combined_baseline, existing_baseline_file)
  write_parquet(combined_stats, existing_stats_file)
  write_csv(combined_stats, file.path(OUTPUT_DIR, "historical_statistics.csv"))

  message("\nUpdated parquet files saved!")

  # Also save the manual data separately for reference
  write_csv(new_historical, file.path(OUTPUT_DIR, "manual_locations_data.csv"))
  write_csv(new_statistics, file.path(OUTPUT_DIR, "manual_locations_stats.csv"))
  message("Manual location data also saved separately to output/manual_locations_*.csv")

} else {
  message("\nNo CSV files found to process.")
}
