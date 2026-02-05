# rezviz_data_generator.R
#
# DAILY SCRIPT: Query current reservoir conditions, combine with historical
# statistics, generate output CSV for teacup visualization, and upload to
# HydroShare.
#
# Designed to run daily via cron/scheduler.
# Depends on historical_statistics.parquet created by setup_historical_baseline.R
#
# Fetches data from multiple sources:
#   - RISE via WWDH EDR API (majority of locations)
#   - USACE CDA API (Cochiti, Abiquiu, Santa Rosa, Grand Coulee, Fort Peck, Lucky Peak)
#   - USGS NWIS (Lahontan, Boca, Prosser Creek, Stampede, Upper Klamath)
#   - CDEC (Tahoe)
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
OUTPUT_DIR       <- "output"
CONFIG_DIR       <- "config"
HYDROSHARE_DIR   <- "hydroshare"

# Target date (default: yesterday, can override via command line)
args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  TARGET_DATE <- as.Date(args[1])
} else {
  TARGET_DATE <- Sys.Date() - 1
}

# How many days to look back if no data on target date
LOOKBACK_DAYS <- 7

# Historical statistics period
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

# Filter out locations whose historical baseline does not adequately cover the
# full 30-water-year period (Oct 1990 – Sep 2020).  We require at least 20
# water years of observations; locations with fewer have a systematically
# incomplete period of record and their percentiles/means are unreliable.
MIN_WATER_YEARS <- 20

baseline_file <- file.path(OUTPUT_DIR, "historical_baseline.parquet")
if (file.exists(baseline_file)) {
  baseline <- read_parquet(baseline_file)
  wy_coverage <- baseline |>
    mutate(water_year = ifelse(month(date) >= 10, year(date) + 1, year(date))) |>
    group_by(location_id) |>
    summarize(n_water_years = n_distinct(water_year), .groups = "drop")

  inadequate <- wy_coverage |>
    filter(n_water_years < MIN_WATER_YEARS) |>
    pull(location_id)

  if (length(inadequate) > 0) {
    message(sprintf("  Excluding %d locations with < %d water years of baseline data:",
                    length(inadequate), MIN_WATER_YEARS))
    for (loc_id in inadequate) {
      nwy <- wy_coverage$n_water_years[wy_coverage$location_id == loc_id]
      message(sprintf("    %s (%d water years)", loc_id, nwy))
    }
    historical_stats <- historical_stats |>
      filter(!location_id %in% inadequate)
    message(sprintf("  Retained historical statistics for %d locations",
                    n_distinct(historical_stats$location_id)))
  }
  rm(baseline, wy_coverage, inadequate)
} else {
  message("  WARNING: historical_baseline.parquet not found; cannot verify coverage.")
}

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
    latitude = Latitude,
    source = `Source.for.Storage.Data`
  )

message(sprintf("Loaded %d locations from geojson", nrow(locations)))

# Track which locations have historical statistics (for logging)
locations_with_stats <- unique(historical_stats$location_id)
n_with_stats <- sum(locations$location_id %in% locations_with_stats)
message(sprintf("  %d locations have historical statistics", n_with_stats))
message(sprintf("  %d locations will have NA for historical metrics",
                nrow(locations) - n_with_stats))

################################################################################
# SOURCE TYPE CLASSIFICATION
################################################################################

#' Classify a location's data source type from the Source field in geojson
#'
#' @param source_str The "Source for Storage Data" field value
#' @return One of: "rise", "usace_cda", "usgs", "cdec", "unknown"
classify_source <- function(source_str) {
  if (is.na(source_str) || source_str == "") return("unknown")
  s <- tolower(source_str)
  if (str_detect(s, "^rise"))                                  return("rise")
  if (str_detect(s, "usace") || str_detect(s, "water\\.usace")) return("usace_cda")
  if (str_detect(s, "^usgs") || str_detect(s, "waterdata\\.usgs")) return("usgs")
  if (str_detect(s, "cdec\\.water\\.ca\\.gov"))                return("cdec")
  return("unknown")
}

################################################################################
# USACE CDA TIMESERIES NAME LOOKUP
#
# Maps location identifiers to their USACE CDA timeseries API parameters.
# Each entry: list(provider, ts_name)
################################################################################

usace_lookup <- list(
  # Cochiti - SPA district, 15-min DCP
  "305"         = list(provider = "spa",
                       ts_name  = "Cochiti.Stor.Inst.15Minutes.0.DCP-rev"),
  # Abiquiu - SPA district, 15-min DCP
  "abiquiu"     = list(provider = "spa",
                       ts_name  = "Abiquiu.Stor.Inst.15Minutes.0.DCP-rev"),
  # Santa Rosa - SPA district, 15-min DCP
  "Santa Rosa"  = list(provider = "spa",
                       ts_name  = "Santa Rosa.Stor.Inst.15Minutes.0.DCP-rev"),
  # Grand Coulee (Franklin D. Roosevelt) - NWD-P district, 1-hour
  "gcl"         = list(provider = "nwdp",
                       ts_name  = "GCL.Stor.Inst.1Hour.0.CBT-REV"),
  # Fort Peck - NWD-M district, ~1Day
  "FTPK"        = list(provider = "nwdm",
                       ts_name  = "FTPK.Stor.Inst.~1Day.0.Best-MRBWM"),
  # Lucky Peak - NWW district
  "luc"         = list(provider = "nww",
                       ts_name  = "LUC.Stor-Total.Inst.0.0.USBR-COMPUTED-REV")
)

################################################################################
# USGS SITE NUMBER LOOKUP
#
# Maps location identifiers to USGS site numbers for NWIS daily values.
# Parameter code 00054 = reservoir storage (acre-feet).
################################################################################

usgs_lookup <- list(
  "10312100" = "10312100",   # Lahontan
  "10344490" = "10344490",   # Boca
  "10340300" = "10340300",   # Prosser Creek
  "--"       = "10344300",   # Stampede (ID is "--" in geojson)
  "11507001" = "11507001"    # Upper Klamath (elevation site, may not have storage)
)

################################################################################
# CDEC STATION LOOKUP
#
# Maps location identifiers to CDEC station codes.
# Sensor 15 = reservoir storage.
################################################################################

cdec_lookup <- list(
  "THC" = "THC"   # Tahoe
)

################################################################################
# DATA FETCHING FUNCTIONS
################################################################################

#' Fetch from RISE via WWDH API
#' Returns list(value, date, unit, url)
fetch_rise <- function(location_id, target_date, lookback_days = LOOKBACK_DAYS) {
  for (days_back in 0:lookback_days) {
    query_date <- target_date - days_back
    start_date <- query_date
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

      data <- read_csv(I(csv_content), show_col_types = FALSE)
      if (nrow(data) == 0) next

      return(list(
        value = data$value[1],
        date  = as.Date(data$datetime[1]),
        unit  = data$unit[1],
        url   = url
      ))
    }, error = function(e) {
      # Continue to next day
    })
  }
  return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = NA_character_))
}

#' Fetch from USACE CDA API
#' Returns list(value, date, unit, url)
fetch_usace <- function(location_id, target_date, lookback_days = LOOKBACK_DAYS) {
  lookup <- usace_lookup[[as.character(location_id)]]
  if (is.null(lookup)) {
    message(sprintf("    USACE: no lookup entry for ID '%s'", as.character(location_id)))
    return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = NA_character_))
  }

  # Query the full lookback window in one call
  begin_date <- target_date - lookback_days
  end_date   <- target_date + 1
  begin_str  <- paste0(format(begin_date, "%Y-%m-%dT00:00:00"), ".000Z")
  end_str    <- paste0(format(end_date, "%Y-%m-%dT00:00:00"), ".000Z")

  url <- sprintf(
    "https://water.usace.army.mil/cda/reporting/providers/%s/timeseries?name=%s&begin=%s&end=%s&format=csv",
    lookup$provider,
    URLencode(lookup$ts_name, reserved = TRUE),
    begin_str, end_str
  )

  tryCatch({
    response <- request(url) |>
      req_timeout(60) |>
      req_retry(max_tries = 2, backoff = ~ 2) |>
      req_perform()

    body <- resp_body_string(response)

    # USACE CDA CSV has ## comment lines, then datetime,value rows
    # Strip \r from \r\n line endings
    body <- str_replace_all(body, "\r", "")
    lines <- str_split(body, "\n")[[1]]
    data_lines <- lines[!str_starts(lines, "##") & nchar(trimws(lines)) > 0]

    if (length(data_lines) == 0) {
      return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
    }

    # Parse: each line is "datetime,value"
    parsed <- tibble(raw = data_lines) |>
      mutate(
        datetime = str_extract(raw, "^[^,]+"),
        value    = as.numeric(str_extract(raw, "[^,]+$")),
        date     = as.Date(str_sub(datetime, 1, 10))
      ) |>
      filter(!is.na(value)) |>
      arrange(desc(date))

    if (nrow(parsed) == 0) {
      return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
    }

    # Find the most recent value on or before target_date
    recent <- parsed |> filter(date <= target_date)
    if (nrow(recent) == 0) recent <- parsed

    # Extract unit from ## comment lines
    unit_line <- lines[str_starts(lines, "##unit:")]
    unit_val <- if (length(unit_line) > 0) {
      trimws(str_remove(unit_line[1], "##unit:"))
    } else {
      "ac-ft"
    }

    return(list(
      value = recent$value[1],
      date  = recent$date[1],
      unit  = unit_val,
      url   = url
    ))
  }, error = function(e) {
    message(sprintf("    USACE fetch error: %s", conditionMessage(e)))
    return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
  })
}

#' Fetch from USGS NWIS daily values API
#' Parameter 00054 = reservoir storage (acre-feet)
#' Returns list(value, date, unit, url)
fetch_usgs <- function(location_id, target_date, lookback_days = LOOKBACK_DAYS) {
  site_no <- usgs_lookup[[as.character(location_id)]]
  if (is.null(site_no)) {
    return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = NA_character_))
  }

  start_date <- target_date - lookback_days
  end_date   <- target_date

  url <- sprintf(
    "https://waterservices.usgs.gov/nwis/dv/?sites=%s&parameterCd=00054&startDT=%s&endDT=%s&format=rdb",
    site_no, start_date, end_date
  )

  tryCatch({
    response <- request(url) |>
      req_timeout(60) |>
      req_retry(max_tries = 2, backoff = ~ 2) |>
      req_perform()

    body <- resp_body_string(response)
    lines <- str_split(body, "\n")[[1]]

    # Skip comment lines (start with #) and the format spec line (starts with 5s)
    data_start <- which(!str_starts(lines, "#") & nchar(trimws(lines)) > 0)
    if (length(data_start) < 3) {
      return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
    }

    # First non-comment line is header, second is format spec, rest is data
    header_line <- lines[data_start[1]]
    data_lines  <- lines[data_start[3:length(data_start)]]
    data_lines  <- data_lines[nchar(trimws(data_lines)) > 0]

    if (length(data_lines) == 0) {
      return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
    }

    # Parse tab-delimited: agency, site_no, datetime, value, qualifier
    parsed <- read_tsv(I(paste(c(header_line, data_lines), collapse = "\n")),
                       show_col_types = FALSE, col_types = cols(.default = "c"))

    # The value column name contains the parameter code pattern
    value_col <- names(parsed)[str_detect(names(parsed), "00054") & !str_detect(names(parsed), "_cd")]
    if (length(value_col) == 0) {
      return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
    }

    parsed <- parsed |>
      mutate(date  = as.Date(datetime),
             value = as.numeric(.data[[value_col[1]]])) |>
      filter(!is.na(value)) |>
      arrange(desc(date))

    if (nrow(parsed) == 0) {
      return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
    }

    return(list(
      value = parsed$value[1],
      date  = parsed$date[1],
      unit  = "af",
      url   = url
    ))
  }, error = function(e) {
    return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
  })
}

#' Fetch from CDEC API
#' Sensor 15 = reservoir storage
#' Returns list(value, date, unit, url)
fetch_cdec <- function(location_id, target_date, lookback_days = LOOKBACK_DAYS) {
  station <- cdec_lookup[[as.character(location_id)]]
  if (is.null(station)) {
    return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = NA_character_))
  }

  start_date <- target_date - lookback_days
  end_date   <- target_date

  url <- sprintf(
    "https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Stations=%s&SensorNums=15&dur_code=D&Start=%s&End=%s",
    station, start_date, end_date
  )

  tryCatch({
    response <- request(url) |>
      req_timeout(60) |>
      req_retry(max_tries = 2, backoff = ~ 2) |>
      req_perform()

    body <- resp_body_string(response)
    data <- read_csv(I(body), show_col_types = FALSE,
                     col_types = cols(`DATE TIME` = col_character(),
                                     `OBS DATE` = col_character(),
                                     .default = col_guess()))

    if (nrow(data) == 0 || !"VALUE" %in% names(data)) {
      return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
    }

    data <- data |>
      mutate(date  = as.Date(str_sub(`DATE TIME`, 1, 8), format = "%Y%m%d"),
             value = as.numeric(VALUE)) |>
      filter(!is.na(value)) |>
      arrange(desc(date))

    if (nrow(data) == 0) {
      return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
    }

    # CDEC UNITS column
    unit_val <- if ("UNITS" %in% names(data)) data$UNITS[1] else "AF"

    return(list(
      value = data$value[1],
      date  = data$date[1],
      unit  = tolower(unit_val),
      url   = url
    ))
  }, error = function(e) {
    return(list(value = NA, date = as.Date(NA), unit = NA_character_, url = url))
  })
}

#' Master fetch: dispatch to the correct source-specific function
#' Returns list(value, date, unit, url)
fetch_current_value <- function(location_id, source_str, target_date,
                                lookback_days = LOOKBACK_DAYS) {
  src_type <- classify_source(source_str)

  result <- switch(src_type,
    "rise"     = fetch_rise(location_id, target_date, lookback_days),
    "usace_cda" = fetch_usace(location_id, target_date, lookback_days),
    "usgs"     = fetch_usgs(location_id, target_date, lookback_days),
    "cdec"     = fetch_cdec(location_id, target_date, lookback_days),
    # For "unknown" or RISE (Pending), still try RISE
    fetch_rise(location_id, target_date, lookback_days)
  )

  return(result)
}

################################################################################
# MAIN PROCESSING LOOP
################################################################################

message("\n=== Fetching current values ===\n")

results <- list()

for (i in seq_len(nrow(locations))) {
  loc <- locations[i, ]
  location_id   <- loc$location_id
  location_name <- loc$name
  source_str    <- loc$source

  src_type <- classify_source(source_str)
  message(sprintf("[%d/%d] %s (ID: %s) [%s]...",
                  i, nrow(locations), location_name, location_id, src_type))

  # Fetch current value from the appropriate source
  current <- fetch_current_value(location_id, source_str, TARGET_DATE)

  if (is.na(current$value)) {
    message(sprintf("  No data found"))
    results[[i]] <- tibble(
      location_id = location_id,
      name        = location_name,
      data_value  = NA_real_,
      data_date   = as.Date(NA),
      data_unit   = NA_character_,
      data_url    = if (!is.null(current$url)) current$url else NA_character_
    )
    next
  }

  message(sprintf("  Value: %s %s (date: %s)",
                  format(current$value, big.mark = ","),
                  current$unit,
                  current$date))

  results[[i]] <- tibble(
    location_id = location_id,
    name        = location_name,
    data_value  = current$value,
    data_date   = current$date,
    data_unit   = current$unit,
    data_url    = current$url
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
target_day   <- day(TARGET_DATE)

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
    pct_median  = data_value / p50,
    pct_average = data_value / mean,
    pct_full    = data_value / capacity,
    # Format dates
    data_date_fmt = format(data_date, "%m/%d/%Y"),
    date_queried  = format(Sys.Date(), "%m/%d/%Y")
  )

################################################################################
# GENERATE OUTPUT CSV
################################################################################

message("\n=== Generating output CSV ===\n")

output_csv <- output_data |>
  transmute(
    SiteName       = label_popup,
    Lat            = latitude,
    Lon            = longitude,
    State          = state,
    DoiRegion      = doi_region,
    Huc6           = huc6,
    DataUnits      = coalesce(data_unit, unit),
    DataValue      = data_value,
    DataDate       = data_date_fmt,
    DateQueried    = date_queried,
    DataDateMax    = max,
    DataDateP90    = p90,
    DataDateP75    = p75,
    DataDateP50    = p50,
    DataDateP25    = p25,
    DataDateP10    = p10,
    DataDateMin    = min,
    DataDateAvg    = mean,
    DataValuePctMdn = pct_median,
    DataValuePctAvg = pct_average,
    StatsPeriod    = STATS_PERIOD,
    MaxCapacity    = capacity,
    PctFull        = pct_full,
    TeacupUrl      = NA_character_,
    DataUrl        = data_url,
    Comment        = NA_character_
  )

# Generate filename and write to hydroshare directory (git-ignored, uploaded to HS)
output_filename <- sprintf("droughtData%s.csv", format(TARGET_DATE, "%Y%m%d"))
if (!dir.exists(HYDROSHARE_DIR)) dir.create(HYDROSHARE_DIR, recursive = TRUE)
output_path <- file.path(HYDROSHARE_DIR, output_filename)

# Write CSV (standard comma-separated)
write_csv(output_csv, output_path, na = "")

message(sprintf("Output written to: %s", output_path))
message(sprintf("  Total locations: %d", nrow(output_csv)))
message(sprintf("  With data: %d", sum(!is.na(output_csv$DataValue))))
message(sprintf("  Missing data: %d", sum(is.na(output_csv$DataValue))))
message(sprintf("  With historical stats: %d", sum(!is.na(output_csv$DataDateP50))))
message(sprintf("  Without historical stats: %d", sum(is.na(output_csv$DataDateP50))))

# Source type breakdown
source_counts <- locations |>
  mutate(src_type = sapply(source, classify_source)) |>
  count(src_type)
message("\n  Source breakdown:")
for (j in seq_len(nrow(source_counts))) {
  message(sprintf("    %s: %d", source_counts$src_type[j], source_counts$n[j]))
}

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
