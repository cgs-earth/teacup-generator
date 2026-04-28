# populate_api_urls.R
#
# Populate the "Historical API URL" column in config/locations.csv.
# The URL is the API endpoint that returns the full 30-year baseline
# (1990-10-01 through 2020-09-30) for each reservoir, mirroring the
# fetch logic in rezviz_data_generator.R's backfill section.
#
# Run from the R-workflow/ directory:
#   Rscript scripts/populate_api_urls.R
#
# Re-run this whenever:
#   - A new location is added to locations.csv
#   - An Identifier or Source value changes
#   - BASELINE_START / BASELINE_END change in rezviz_data_generator.R
#
# create_locations_geojson.R passes all CSV columns through to the geojson,
# so the column appears in both files after running this script + the geojson
# regenerator.
################################################################################

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
})

CSV_PATH <- "config/locations.csv"

# Match the baseline window in rezviz_data_generator.R
BASELINE_START <- as.Date("1990-10-01")
BASELINE_END   <- as.Date("2020-09-30")

WWDH_API_BASE  <- "https://api.wwdh.internetofwater.app"

# Mirror classify_source() in rezviz_data_generator.R
classify_source <- function(source_str) {
  if (is.na(source_str) || source_str == "") return("unknown")
  s <- tolower(source_str)
  if (str_detect(s, "^rise"))                                      return("rise")
  if (str_detect(s, "usace") || str_detect(s, "water\\.usace"))    return("usace_cda")
  if (str_detect(s, "^usgs") || str_detect(s, "waterdata\\.usgs")) return("usgs")
  if (str_detect(s, "cdec\\.water\\.ca\\.gov"))                    return("cdec")
  return("unknown")
}

build_historical_api_url <- function(source_str, identifier, data_type) {
  if (is.na(identifier) || identifier == "" || identifier == "--" || identifier == "0") {
    return(NA_character_)
  }

  src <- classify_source(source_str)
  start <- format(BASELINE_START, "%Y-%m-%d")
  end   <- format(BASELINE_END, "%Y-%m-%d")
  end_plus1 <- format(BASELINE_END + 1, "%Y-%m-%d")

  if (src == "rise") {
    sprintf(
      "%s/collections/rise-edr/locations/%s?limit=50000&datetime=%s/%s&f=json",
      WWDH_API_BASE, identifier, start, end_plus1
    )

  } else if (src == "usace_cda") {
    slash_pos <- str_locate(identifier, "/")[1, "start"]
    if (is.na(slash_pos)) return(NA_character_)
    provider <- str_sub(identifier, 1, slash_pos - 1)
    ts_name  <- str_sub(identifier, slash_pos + 1)
    begin_str <- sprintf("%sT00:00:00.000Z", start)
    end_str   <- sprintf("%sT00:00:00.000Z", end_plus1)
    sprintf(
      "https://water.usace.army.mil/cda/reporting/providers/%s/timeseries?name=%s&begin=%s&end=%s&format=csv",
      provider, URLencode(ts_name, reserved = TRUE), begin_str, end_str
    )

  } else if (src == "usgs") {
    param_code <- if (!is.na(data_type) && tolower(data_type) == "elevation") {
      "62614"  # NGVD29 elevation; data generator falls back to 72275/62615 if empty
    } else {
      "00054"  # storage in acre-feet
    }
    sprintf(
      "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&monitoring_location_id=USGS-%s&parameter_code=%s&time=%s/%s&limit=50000",
      identifier, param_code, start, end
    )

  } else if (src == "cdec") {
    sprintf(
      "https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Stations=%s&SensorNums=15&dur_code=D&Start=%s&End=%s",
      identifier, start, end
    )

  } else {
    NA_character_
  }
}

################################################################################
# MAIN
################################################################################

message("Reading ", CSV_PATH, "...")
locs <- read_csv(CSV_PATH, show_col_types = FALSE)
message(sprintf("  %d rows", nrow(locs)))

urls <- mapply(
  build_historical_api_url,
  locs$`Source for Storage Data`,
  locs$Identifier,
  locs$`Storage Data Type`,
  USE.NAMES = FALSE
)

locs$`Historical API URL` <- urls

# Place the new column right after Identifier, for readability.
cols <- names(locs)
if ("Historical API URL" %in% cols && "Identifier" %in% cols) {
  cols <- cols[cols != "Historical API URL"]
  ident_pos <- which(cols == "Identifier")
  cols <- append(cols, "Historical API URL", after = ident_pos)
  locs <- locs[, cols]
}

# Summary
n_total <- nrow(locs)
n_with_url <- sum(!is.na(locs$`Historical API URL`))
message(sprintf("\n%d / %d rows now have a Historical API URL", n_with_url, n_total))

src_summary <- locs |>
  mutate(src = sapply(`Source for Storage Data`, classify_source)) |>
  group_by(src) |>
  summarize(
    rows      = n(),
    with_url  = sum(!is.na(`Historical API URL`)),
    .groups   = "drop"
  )
message("\nBy source type:")
print(src_summary)

write_csv(locs, CSV_PATH, na = "")
message("\nWrote updated CSV to ", CSV_PATH)
