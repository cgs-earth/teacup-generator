# create_dummy_geojson.R
#
# Create dummy geojson files with storage values and historical statistics
# for testing the dashboard visualization.
################################################################################

library(sf)
library(dplyr)
library(readr)
library(arrow)

# Directories
CONFIG_DIR <- "config"
OUTPUT_DIR <- "output"
EXAMPLES_DIR <- "examples"

# Load locations geojson
locations <- st_read(file.path(CONFIG_DIR, "locations.geojson"), quiet = TRUE)

# Load historical statistics
stats <- read_parquet(file.path(OUTPUT_DIR, "historical_statistics.parquet"))

# Pick two dates in the past: Jan 15 and Jul 15 of 2025
dates <- c("2025-01-15", "2025-07-15")

for (target_date in dates) {
  d <- as.Date(target_date)
  target_month <- as.integer(format(d, "%m"))
  target_day <- as.integer(format(d, "%d"))

  message(sprintf("\nDate: %s - month: %d, day: %d", target_date, target_month, target_day))

  # Get stats for this day of year
  day_stats <- stats |>
    filter(month == target_month, day == target_day) |>
    select(location_id = location_id, p10, p90, mean, p50)

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
      # Parse capacity - remove commas and handle "--" as NA
      Total.Capacity.Num = as.numeric(gsub(",", "", ifelse(Total.Capacity == "--", NA, Total.Capacity))),
      # Generate dummy storage for ALL locations
      # For locations with stats: use range between p10 and p90
      # For locations without stats: use 20-80% of capacity
      Storage = case_when(
        has_stats ~ runif(n(), p10, p90),
        TRUE ~ runif(n(), Total.Capacity.Num * 0.2, Total.Capacity.Num * 0.8)
      ),
      # Clamp to reasonable range
      Storage = pmin(Storage, Total.Capacity.Num * 0.95),
      Storage = pmax(Storage, Total.Capacity.Num * 0.05),
      StorageDate = target_date,
      # Round storage values
      Storage = round(Storage, 1),
      # Historical stats - null for locations without data
      TenthPercentile = round(p10, 1),
      NinetiethPercentile = round(p90, 1),
      StorageAverage = round(mean, 1),
      # Calculate PctFull for all locations (storage / capacity)
      PctFull = round(Storage / Total.Capacity.Num, 4),
      # PctMedian and PctAverage only for locations with historical stats
      PctMedian = ifelse(has_stats, round(Storage / p50, 4), NA_real_),
      PctAverage = ifelse(has_stats, round(Storage / mean, 4), NA_real_)
    ) |>
    select(
      Name,
      SiteName = Preferred.Label.for.PopUp.and.Modal,
      MapLabel = Preferred.Label.for.Map.and.Table,
      Identifier,
      Longitude, Latitude,
      state, doiRegion, huc6,
      MaxCapacity = Total.Capacity,
      Storage,
      StorageDate,
      TenthPercentile,
      NinetiethPercentile,
      StorageAverage,
      PctFull,
      PctMedian,
      PctAverage,
      geometry
    )

  # Save to examples directory
  output_file <- file.path(EXAMPLES_DIR, sprintf("reservoirs_%s.geojson", gsub("-", "", target_date)))
  st_write(locations_with_stats, output_file, driver = "GeoJSON", delete_dsn = TRUE)
  message(sprintf("Saved: %s", output_file))
}

message("\nDone!")
