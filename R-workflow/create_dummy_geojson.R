# create_dummy_geojson.R
#
# Create dummy geojson files with storage values and historical statistics
# for testing the dashboard visualization.
################################################################################

library(sf)
library(dplyr)
library(readr)
library(arrow)
library(stringr)

# Directories
CONFIG_DIR <- "config"
OUTPUT_DIR <- "output"
EXAMPLES_DIR <- "examples"

# Load locations geojson (now includes Active.Capacity)
message("Loading locations.geojson...")
locations <- st_read(file.path(CONFIG_DIR, "locations.geojson"), quiet = TRUE)
message(sprintf("  %d locations loaded", nrow(locations)))

# Load historical statistics
message("Loading historical statistics...")
stats <- read_parquet(file.path(OUTPUT_DIR, "historical_statistics.parquet"))
message(sprintf("  Stats for %d locations", n_distinct(stats$location_id)))

# Create examples directory if needed
dir.create(EXAMPLES_DIR, showWarnings = FALSE, recursive = TRUE)

# Pick two dates: Jan 15 and Jul 15 of 2026
dates <- c("2026-01-15", "2026-07-15")

for (target_date in dates) {
  d <- as.Date(target_date)
  target_month <- as.integer(format(d, "%m"))
  target_day <- as.integer(format(d, "%d"))

  message(sprintf("\nDate: %s - month: %d, day: %d", target_date, target_month, target_day))

  # Get stats for this day of year
  day_stats <- stats |>
    filter(month == target_month, day == target_day) |>
    select(location_id, p10, p25, p50, p75, p90, mean, min, max)

  message(sprintf("Stats available for %d locations", nrow(day_stats)))

  # Join stats to locations
  locations_with_stats <- locations |>
    left_join(day_stats, by = c("Identifier" = "location_id"))

  # Generate dummy current storage values for ALL locations
  # Use historical stats range if available, otherwise use 20-80% of capacity
  set.seed(as.integer(d))  # reproducible randomness per date

  # Check which locations have historical stats
  has_stats <- !is.na(locations_with_stats$p10)

  locations_with_stats <- locations_with_stats |>
    mutate(
      # Parse capacities - remove commas and handle "--" as NA
      TotalCapacityNum = as.numeric(str_remove_all(
        ifelse(is.na(`Total.Capacity`) | `Total.Capacity` == "--", NA, `Total.Capacity`), ",")),
      ActiveCapacityNum = as.numeric(str_remove_all(
        ifelse(is.na(`Active.Capacity`) | `Active.Capacity` == "--", NA, `Active.Capacity`), ",")),
      # Generate dummy storage for ALL locations
      # For locations with stats: use range between p10 and p90
      # For locations without stats: use 20-80% of capacity
      Storage = case_when(
        has_stats ~ runif(n(), p10, p90),
        TRUE ~ runif(n(), TotalCapacityNum * 0.2, TotalCapacityNum * 0.8)
      ),
      # Clamp to reasonable range
      Storage = pmin(Storage, TotalCapacityNum * 0.95),
      Storage = pmax(Storage, TotalCapacityNum * 0.05),
      StorageDate = target_date,
      # Round storage values
      Storage = round(Storage, 1),
      # Historical stats - null for locations without data
      HistMin = round(min, 1),
      HistMax = round(max, 1),
      HistP10 = round(p10, 1),
      HistP25 = round(p25, 1),
      HistP50 = round(p50, 1),
      HistP75 = round(p75, 1),
      HistP90 = round(p90, 1),
      HistMean = round(mean, 1),
      # Calculate PctFull for all locations (storage / total capacity)
      PctFull = round(Storage / TotalCapacityNum, 4),
      # PctMedian and PctAverage only for locations with historical stats
      PctMedian = ifelse(has_stats, round(Storage / p50, 4), NA_real_),
      PctAverage = ifelse(has_stats, round(Storage / mean, 4), NA_real_),
      StorageUnits = "af",
      # Rename columns for output
      SiteName = `Preferred.Label.for.PopUp.and.Modal`,
      MapLabel = `Preferred.Label.for.Map.and.Table`,
      SourceName = Source_Name,
      State = state,
      DoiRegion = doiRegion,
      Huc6 = huc6,
      TotalCapacity = `Total.Capacity`,
      ActiveCapacity = `Active.Capacity`
    ) |>
    select(
      Name,
      SiteName,
      MapLabel,
      Identifier,
      SourceName,
      Longitude, Latitude,
      State,
      DoiRegion,
      Huc6,
      TotalCapacity,
      ActiveCapacity,
      Storage,
      StorageDate,
      StorageUnits,
      HistMin,
      HistMax,
      HistP10,
      HistP25,
      HistP50,
      HistP75,
      HistP90,
      HistMean,
      PctFull,
      PctMedian,
      PctAverage,
      geometry
    )

  # Save to examples directory
  output_file <- file.path(EXAMPLES_DIR, sprintf("reservoirs_%s.geojson", gsub("-", "", target_date)))
  st_write(locations_with_stats, output_file, driver = "GeoJSON", delete_dsn = TRUE)
  message(sprintf("Saved: %s (%d features)", output_file, nrow(locations_with_stats)))
}

message("\nDone!")
