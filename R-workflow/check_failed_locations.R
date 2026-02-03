# check_failed_locations.R
#
# Diagnostic script to analyze failed locations from setup_historical_baseline.R
# Reads failed_locations.txt and cross-references with locations.csv to categorize.
#
# Author: Kyle Onda, Internet of Water
# Created: 2026-02-01
################################################################################

library(dplyr)
library(readr)
library(stringr)

################################################################################
# LOAD DATA
################################################################################

# Read failed locations
failed_file <- "output/failed_locations.txt"
if (!file.exists(failed_file)) {
  stop("failed_locations.txt not found. Run setup_historical_baseline.R first.")
}

failed_names <- readLines(failed_file)
message(sprintf("Found %d failed locations\n", length(failed_names)))

# Load location metadata
locations_raw <- read_csv("config/locations.csv", show_col_types = FALSE)  # User-curated input file

locations <- locations_raw |>
  filter(`Post-Review Decision` != "Do Not Include") |>
  transmute(
    name = Name,
    decision = `Post-Review Decision`,
    source = `Source for Storage Data`,
    location_id = `RISE Location ID`,
    capacity = as.numeric(str_remove_all(`Total Capacity`, ",")),
    label_map = `Preferred Label for Map and Table`
  ) |>
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

################################################################################
# MATCH FAILED LOCATIONS TO METADATA
################################################################################

failed_details <- locations |>
  filter(name %in% failed_names)

message("=== FAILED LOCATIONS BY DECISION CATEGORY ===\n")

failed_by_decision <- failed_details |>
  count(decision) |>
  arrange(desc(n))

for (i in seq_len(nrow(failed_by_decision))) {
  message(sprintf("  %-35s %d", failed_by_decision$decision[i], failed_by_decision$n[i]))
}

message("\n=== FAILED LOCATIONS BY SOURCE TYPE ===\n")

failed_by_source <- failed_details |>
  count(source_type) |>
  arrange(desc(n))

for (i in seq_len(nrow(failed_by_source))) {
  message(sprintf("  %-20s %d", failed_by_source$source_type[i], failed_by_source$n[i]))
}

message("\n=== DETAILED LIST ===\n")
message(sprintf("%-30s %-10s %-35s %s", "Name", "ID", "Decision", "Source"))
message(paste(rep("-", 100), collapse = ""))

failed_details <- failed_details |> arrange(decision, name)

for (i in seq_len(nrow(failed_details))) {
  row <- failed_details[i, ]
  message(sprintf("%-30s %-10s %-35s %s",
                  substr(row$name, 1, 30),
                  row$location_id,
                  row$decision,
                  row$source_type))
}

message(sprintf("\n\nTotal failed: %d out of %d RISE locations",
                nrow(failed_details),
                nrow(locations |> filter(source_type == "RISE"))))
