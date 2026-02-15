#!/usr/bin/env Rscript
# Backfill daily reports by:
# 1. Loading historical baseline (1990-10-01 to 2020-09-30)
# 2. Fetching recent data (2020-10-01 to today) once for all sources
# 3. Merging into complete dataset
# 4. Generating daily CSVs from the merged data (no API calls per day)
# 5. Batch uploading to HydroShare
#
# Usage: Rscript backfill_reports.R [end_date] [start_date]
# Default: today back to 1990-10-01

library(httr2)
library(dplyr)
library(readr)
library(lubridate)
library(arrow)
library(stringr)
library(sf)
library(jsonlite)
library(curl)

# Load .env file
if (file.exists(".env")) {
  readRenviron(".env")
}

args <- commandArgs(trailingOnly = TRUE)
END_DATE <- if (length(args) >= 1) as.Date(args[1]) else Sys.Date()
START_DATE <- if (length(args) >= 2) as.Date(args[2]) else as.Date("1990-10-01")

# Configuration
WWDH_API_BASE <- "https://api.wwdh.internetofwater.app"
HYDROSHARE_RESOURCE_ID <- "22b2f10103e5426a837defc00927afbd"
HYDROSHARE_BASE_URL <- "https://www.hydroshare.org"
OUTPUT_DIR <- "output"
HYDROSHARE_DIR <- "hydroshare"
CONFIG_DIR <- "config"
STATS_PERIOD <- "10/1/1990 - 9/30/2020"

# Historical baseline end date
BASELINE_END <- as.Date("2020-09-30")

message("=== Backfill Daily Reports ===")
message(sprintf("From: %s to %s", START_DATE, END_DATE))
message(sprintf("Total days: %d", as.integer(END_DATE - START_DATE) + 1))
message("")

################################################################################
# LOAD LOCATION METADATA
################################################################################

message("Loading location metadata...")
locations_sf <- st_read(file.path(CONFIG_DIR, "locations.geojson"), quiet = TRUE)

locations <- locations_sf |>
  st_drop_geometry() |>
  transmute(
    name = Name,
    location_id = Identifier,
    capacity = as.numeric(str_remove_all(`Total.Capacity`, ",")),
    active_capacity = as.numeric(str_remove_all(`Active.Capacity`, ",")),
    label_map = `Preferred.Label.for.Map.and.Table`,
    label_popup = `Preferred.Label.for.PopUp.and.Modal`,
    state = state,
    doi_region = doiRegion,
    huc6 = huc6,
    longitude = Longitude,
    latitude = Latitude,
    source = `Source.for.Storage.Data`
  )

message(sprintf("  Loaded %d locations", nrow(locations)))

################################################################################
# LOAD HISTORICAL BASELINE
################################################################################

message("Loading historical baseline data...")
baseline_file <- file.path(OUTPUT_DIR, "historical_baseline.parquet")
if (!file.exists(baseline_file)) {
  stop("Historical baseline not found. Run setup_historical_baseline.R first.")
}

historical_data <- read_parquet(baseline_file)
message(sprintf("  Loaded %d observations (%s to %s)",
                nrow(historical_data),
                min(historical_data$date),
                max(historical_data$date)))

################################################################################
# LOAD HISTORICAL STATISTICS
################################################################################

message("Loading historical statistics...")
stats_file <- file.path(OUTPUT_DIR, "historical_statistics.parquet")
historical_stats <- read_parquet(stats_file)
message(sprintf("  Loaded stats for %d locations", n_distinct(historical_stats$location_id)))

################################################################################
# FETCH RECENT DATA (2020-10-01 to today)
################################################################################

RECENT_START <- BASELINE_END + 1
RECENT_END <- END_DATE

if (RECENT_END >= RECENT_START) {
  message("")
  message(sprintf("=== Fetching recent data: %s to %s ===", RECENT_START, RECENT_END))
  message("")

  recent_data <- list()

  # Classify source type
  classify_source <- function(source_str) {
    if (is.na(source_str) || source_str == "") return("unknown")
    s <- tolower(source_str)
    if (str_detect(s, "^rise")) return("rise")
    if (str_detect(s, "usace") || str_detect(s, "water\\.usace")) return("usace")
    if (str_detect(s, "usgs") || str_detect(s, "waterdata\\.usgs")) return("usgs")
    if (str_detect(s, "cdec")) return("cdec")
    return("unknown")
  }

  locations <- locations |>
    mutate(source_type = sapply(source, classify_source))

  # USACE identifiers are now "provider/ts_name" format - parsed inline

  #---------------------------------------------------------------------------
  # Fetch RISE data (bulk)
  #---------------------------------------------------------------------------
  rise_locs <- locations |> filter(source_type == "rise", location_id != "--")
  message(sprintf("Fetching RISE data for %d locations...", nrow(rise_locs)))

  for (i in seq_len(nrow(rise_locs))) {
    loc <- rise_locs[i, ]
    loc_id <- loc$location_id

    if (i %% 20 == 0) message(sprintf("  [%d/%d] %s", i, nrow(rise_locs), loc$name))

    url <- paste0(
      WWDH_API_BASE,
      "/collections/rise-edr/locations/", loc_id,
      "?parameter-name=Storage",
      "&limit=20000",
      "&datetime=", RECENT_START, "/", RECENT_END + 1,
      "&f=csv"
    )

    tryCatch({
      response <- request(url) |>
        req_timeout(120) |>
        req_retry(max_tries = 3, backoff = ~ 5) |>
        req_perform()

      if (resp_status(response) == 200) {
        csv_content <- resp_body_string(response)
        if (nchar(csv_content) > 50) {
          data <- read_csv(csv_content, show_col_types = FALSE)
          if (nrow(data) > 0 && "datetime" %in% names(data)) {
            recent_data[[length(recent_data) + 1]] <- data |>
              transmute(
                location_id = loc_id,
                date = as.Date(datetime),
                value = value,
                unit = unit
              ) |>
              filter(!is.na(value)) |>
              distinct(date, .keep_all = TRUE)
          }
        }
      }
    }, error = function(e) {
      # Skip failed locations
    })

    Sys.sleep(0.25)
  }
  message(sprintf("  Retrieved data for %d RISE locations", length(recent_data)))

  #---------------------------------------------------------------------------
  # Fetch USACE data (bulk)
  #---------------------------------------------------------------------------
  usace_locs <- locations |> filter(source_type == "usace")
  message(sprintf("Fetching USACE data for %d locations...", nrow(usace_locs)))

  for (i in seq_len(nrow(usace_locs))) {
    loc <- usace_locs[i, ]
    loc_id <- as.character(loc$location_id)

    # Parse provider and ts_name from identifier format: "provider/ts_name"
    slash_pos <- str_locate(loc_id, "/")[1, "start"]
    if (is.na(slash_pos)) {
      message(sprintf("  [%d/%d] %s - invalid identifier format", i, nrow(usace_locs), loc$name))
      next
    }

    provider <- str_sub(loc_id, 1, slash_pos - 1)
    ts_name  <- str_sub(loc_id, slash_pos + 1)

    message(sprintf("  [%d/%d] %s", i, nrow(usace_locs), loc$name))

    begin_str <- paste0(format(RECENT_START, "%Y-%m-%dT00:00:00"), ".000Z")
    end_str <- paste0(format(RECENT_END + 1, "%Y-%m-%dT00:00:00"), ".000Z")

    url <- sprintf(
      "https://water.usace.army.mil/cda/reporting/providers/%s/timeseries?name=%s&begin=%s&end=%s&format=csv",
      provider, URLencode(ts_name, reserved = TRUE), begin_str, end_str
    )

    tryCatch({
      response <- request(url) |>
        req_timeout(300) |>
        req_retry(max_tries = 3, backoff = ~ 10) |>
        req_perform()

      body <- resp_body_string(response)
      body <- str_replace_all(body, "\r", "")
      lines <- str_split(body, "\n")[[1]]

      unit_line <- lines[str_starts(lines, "##unit:")]
      unit_val <- if (length(unit_line) > 0) trimws(str_remove(unit_line[1], "##unit:")) else "ac-ft"
      if (tolower(unit_val) == "ac-ft") unit_val <- "af"

      data_lines <- lines[!str_starts(lines, "##") & nchar(trimws(lines)) > 0]

      if (length(data_lines) > 0) {
        chunk <- tibble(raw = data_lines) |>
          mutate(
            datetime = str_extract(raw, "^[^,]+"),
            value = as.numeric(str_extract(raw, "[^,]+$")),
            date = as.Date(str_sub(datetime, 1, 10))
          ) |>
          filter(!is.na(value), !is.na(date)) |>
          group_by(date) |>
          summarize(value = last(value), .groups = "drop") |>
          mutate(location_id = loc_id, unit = unit_val) |>
          select(location_id, date, value, unit)

        recent_data[[length(recent_data) + 1]] <- chunk
      }
    }, error = function(e) {
      message(sprintf("    Error: %s", conditionMessage(e)))
    })

    Sys.sleep(1)
  }

  #---------------------------------------------------------------------------
  # Fetch USGS data (bulk)
  #---------------------------------------------------------------------------
  usgs_locs <- locations |> filter(source_type == "usgs", location_id != "--")
  message(sprintf("Fetching USGS data for %d locations...", nrow(usgs_locs)))

  for (i in seq_len(nrow(usgs_locs))) {
    loc <- usgs_locs[i, ]
    site_no <- loc$location_id

    message(sprintf("  [%d/%d] %s (USGS-%s)", i, nrow(usgs_locs), loc$name, site_no))

    # Fetch in yearly chunks to avoid API limits
    all_usgs <- list()
    chunk_start <- RECENT_START
    while (chunk_start <= RECENT_END) {
      chunk_end <- min(chunk_start + 365, RECENT_END)

      url <- sprintf(
        "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?f=json&monitoring_location_id=USGS-%s&parameter_code=00054&time=%s/%s&limit=500",
        site_no, chunk_start, chunk_end
      )

      tryCatch({
        response <- request(url) |>
          req_timeout(120) |>
          req_retry(max_tries = 3, backoff = ~ 5) |>
          req_perform()

        body <- resp_body_string(response)
        data <- fromJSON(body, simplifyVector = FALSE)
        features <- data$features

        if (length(features) > 0) {
          chunk <- tibble(
            date = as.Date(sapply(features, function(f) f$properties$time)),
            value = as.numeric(sapply(features, function(f) f$properties$value)),
            unit = sapply(features, function(f) f$properties$unit_of_measure)
          ) |>
            filter(!is.na(value)) |>
            mutate(unit = tolower(unit), unit = ifelse(unit == "acre-ft", "af", unit))

          all_usgs[[length(all_usgs) + 1]] <- chunk
        }
      }, error = function(e) {
        message(sprintf("    Chunk error: %s", conditionMessage(e)))
      })

      chunk_start <- chunk_end + 1
      Sys.sleep(0.5)
    }

    if (length(all_usgs) > 0) {
      recent_data[[length(recent_data) + 1]] <- bind_rows(all_usgs) |>
        mutate(location_id = site_no) |>
        distinct(date, .keep_all = TRUE) |>
        select(location_id, date, value, unit)
    }
  }

  #---------------------------------------------------------------------------
  # Fetch CDEC data (bulk)
  #---------------------------------------------------------------------------
  cdec_locs <- locations |> filter(source_type == "cdec")
  message(sprintf("Fetching CDEC data for %d locations...", nrow(cdec_locs)))

  for (i in seq_len(nrow(cdec_locs))) {
    loc <- cdec_locs[i, ]
    station <- loc$location_id

    message(sprintf("  [%d/%d] %s (CDEC-%s)", i, nrow(cdec_locs), loc$name, station))

    url <- sprintf(
      "https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Stations=%s&SensorNums=15&dur_code=D&Start=%s&End=%s",
      station, RECENT_START, RECENT_END
    )

    tryCatch({
      response <- request(url) |>
        req_headers(`User-Agent` = "Mozilla/5.0 (R httr2)") |>
        req_timeout(120) |>
        req_retry(max_tries = 3, backoff = ~ 5) |>
        req_perform()

      body <- resp_body_string(response)
      data <- read_csv(I(body), show_col_types = FALSE,
                       col_types = cols(`DATE TIME` = col_character(), .default = col_guess()))

      if (nrow(data) > 0 && "VALUE" %in% names(data)) {
        unit_val <- if ("UNITS" %in% names(data)) tolower(data$UNITS[1]) else "af"

        recent_data[[length(recent_data) + 1]] <- data |>
          mutate(
            date = as.Date(str_sub(`DATE TIME`, 1, 8), format = "%Y%m%d"),
            value = as.numeric(VALUE),
            unit = unit_val,
            location_id = station
          ) |>
          filter(!is.na(value)) |>
          distinct(date, .keep_all = TRUE) |>
          select(location_id, date, value, unit)
      }
    }, error = function(e) {
      message(sprintf("    Error: %s", conditionMessage(e)))
    })
  }

  # Combine recent data
  if (length(recent_data) > 0) {
    recent_combined <- bind_rows(recent_data)
    message(sprintf("\nTotal recent observations: %d", nrow(recent_combined)))
  } else {
    recent_combined <- tibble(location_id = character(), date = Date(), value = numeric(), unit = character())
  }
} else {
  recent_combined <- tibble(location_id = character(), date = Date(), value = numeric(), unit = character())
}

################################################################################
# COMBINE HISTORICAL + RECENT DATA
################################################################################

message("")
message("=== Combining historical and recent data ===")

all_data <- bind_rows(historical_data, recent_combined) |>
  arrange(location_id, date) |>
  distinct(location_id, date, .keep_all = TRUE)

message(sprintf("Total observations: %d", nrow(all_data)))
message(sprintf("Date range: %s to %s", min(all_data$date), max(all_data$date)))
message(sprintf("Locations with data: %d", n_distinct(all_data$location_id)))

################################################################################
# GENERATE DAILY CSVs
################################################################################

message("")
message("=== Generating daily CSVs ===")

dates <- seq(START_DATE, END_DATE, by = "1 day")
message(sprintf("Generating %d daily reports...", length(dates)))

dir.create(HYDROSHARE_DIR, showWarnings = FALSE, recursive = TRUE)

# Pre-compute location info for joining (include source_type for URL generation)
location_info <- locations |>
  select(location_id, name, capacity, active_capacity, label_popup, state, doi_region, huc6, longitude, latitude, source_type)

#' Generate API URL based on source type and location
#' @param source_type One of: rise, usace, usgs, cdec
#' @param location_id The location identifier
#' @param data_date The date of the data point
#' @return API URL string or NA
generate_data_url <- function(source_type, location_id, data_date) {
  if (is.na(source_type) || is.na(location_id) || is.na(data_date)) return(NA_character_)

  date_str <- format(data_date, "%Y-%m-%d")
  end_date <- format(data_date + 1, "%Y-%m-%d")

  switch(source_type,
    "rise" = sprintf("%s/collections/rise-edr/locations/%s?parameter-name=Storage&datetime=%s/%s&f=csv",
                     WWDH_API_BASE, location_id, date_str, end_date),
    "usace" = {
      lookup <- usace_lookup[[as.character(location_id)]]
      if (!is.null(lookup)) {
        sprintf("https://water.usace.army.mil/cda/reporting/providers/%s/timeseries?name=%s",
                lookup$provider, URLencode(lookup$ts_name, reserved = TRUE))
      } else {
        NA_character_
      }
    },
    "usgs" = sprintf("https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?sites=%s&startDate=%s&endDate=%s&parameterCode=00054",
                     location_id, date_str, date_str),
    "cdec" = sprintf("https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Stations=%s&SensorNums=15&dur_code=D&Start=%s&End=%s",
                     location_id, date_str, date_str),
    NA_character_
  )
}

generated_count <- 0
for (i in seq_along(dates)) {
  target_date <- dates[i]

  if (i %% 500 == 0 || i == length(dates)) {
    message(sprintf("  [%d/%d] %s", i, length(dates), target_date))
  }

  target_month <- month(target_date)
  target_day <- day(target_date)

  # Get historical stats for this day of year
  todays_stats <- historical_stats |>
    filter(month == target_month, day == target_day) |>
    select(location_id, min, max, p10, p25, p50, p75, p90, mean, unit)

  # Get data for this date (or most recent within 7 days)
  current_values <- all_data |>
    filter(date <= target_date, date >= target_date - 7) |>
    group_by(location_id) |>
    slice_max(date, n = 1, with_ties = FALSE) |>
    ungroup() |>
    select(location_id, data_value = value, data_date = date, data_unit = unit)

  # Join everything
  output_data <- location_info |>
    left_join(current_values, by = "location_id") |>
    left_join(todays_stats, by = "location_id", suffix = c("", "_hist")) |>
    rowwise() |>
    mutate(
      pct_median = data_value / p50,
      pct_average = data_value / mean,
      pct_full = data_value / capacity,
      data_date_fmt = format(data_date, "%m/%d/%Y"),
      date_queried = format(target_date, "%m/%d/%Y"),
      data_url = generate_data_url(source_type, location_id, data_date)
    ) |>
    ungroup()

  # Format output CSV
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
      ActiveCapacity = active_capacity,
      PctFull = pct_full,
      TeacupUrl = NA_character_,
      DataUrl = data_url,
      Comment = NA_character_
    )

  # Write CSV
  output_filename <- sprintf("droughtData%s.csv", format(target_date, "%Y%m%d"))
  output_path <- file.path(HYDROSHARE_DIR, output_filename)
  write_csv(output_csv, output_path, na = "")
  generated_count <- generated_count + 1
}

message(sprintf("\nGenerated %d CSV files", generated_count))

################################################################################
# CREATE COMPLETE ARCHIVE FILE
################################################################################

message("")
message("=== Creating complete archive file ===")

# Build a complete archive with all daily data
# Each row is a location-date combination with all metrics

archive_data <- list()

for (i in seq_along(dates)) {
  target_date <- dates[i]

  if (i %% 1000 == 0) {
    message(sprintf("  Processing archive [%d/%d] %s", i, length(dates), target_date))
  }

  target_month <- month(target_date)
  target_day <- day(target_date)

  # Get historical stats for this day of year
  todays_stats <- historical_stats |>
    filter(month == target_month, day == target_day) |>
    select(location_id, min, max, p10, p25, p50, p75, p90, mean, unit)

  # Get data for this date (or most recent within 7 days)
  current_values <- all_data |>
    filter(date <= target_date, date >= target_date - 7) |>
    group_by(location_id) |>
    slice_max(date, n = 1, with_ties = FALSE) |>
    ungroup() |>
    select(location_id, data_value = value, data_date = date, data_unit = unit)

  # Only include rows that have data
  daily_archive <- location_info |>
    inner_join(current_values, by = "location_id") |>
    left_join(todays_stats, by = "location_id", suffix = c("", "_hist")) |>
    mutate(
      report_date = target_date,
      pct_median = data_value / p50,
      pct_average = data_value / mean,
      pct_full = data_value / capacity
    ) |>
    select(
      report_date,
      location_id,
      name,
      state,
      doi_region,
      huc6,
      latitude,
      longitude,
      capacity,
      active_capacity,
      data_date,
      data_value,
      data_unit,
      hist_min = min,
      hist_max = max,
      hist_p10 = p10,
      hist_p25 = p25,
      hist_p50 = p50,
      hist_p75 = p75,
      hist_p90 = p90,
      hist_mean = mean,
      pct_median,
      pct_average,
      pct_full
    )

  if (nrow(daily_archive) > 0) {
    archive_data[[i]] <- daily_archive
  }
}

# Combine all archive data
complete_archive <- bind_rows(archive_data)

message(sprintf("Archive contains %d rows (%d locations × %d dates)",
                nrow(complete_archive),
                n_distinct(complete_archive$location_id),
                n_distinct(complete_archive$report_date)))

# Create filename with period of record
archive_filename_base <- sprintf("reservoir_storage_archive_%s_to_%s",
                                  format(START_DATE, "%Y%m%d"),
                                  format(END_DATE, "%Y%m%d"))

# Write as parquet (efficient for large data)
archive_parquet <- file.path(OUTPUT_DIR, paste0(archive_filename_base, ".parquet"))
write_parquet(complete_archive, archive_parquet)
message(sprintf("Saved archive: %s", archive_parquet))

# Also write as CSV for broader compatibility
archive_csv <- file.path(OUTPUT_DIR, paste0(archive_filename_base, ".csv"))
write_csv(complete_archive, archive_csv)
message(sprintf("Saved archive: %s", archive_csv))

################################################################################
# BATCH UPLOAD TO HYDROSHARE
################################################################################

message("")
message("=== Uploading to HydroShare ===")

hs_username <- Sys.getenv("HYDROSHARE_USERNAME", unset = "")
hs_password <- Sys.getenv("HYDROSHARE_PASSWORD", unset = "")

if (hs_username == "" || hs_password == "") {
  message("WARNING: HydroShare credentials not set. Skipping upload.")
  message("Set HYDROSHARE_USERNAME and HYDROSHARE_PASSWORD in .env")
} else {
  csv_files <- list.files(HYDROSHARE_DIR, pattern = "^droughtData.*\\.csv$", full.names = TRUE)
  message(sprintf("Found %d CSV files to upload", length(csv_files)))

  upload_count <- 0
  fail_count <- 0

  for (i in seq_along(csv_files)) {
    file_path <- csv_files[i]
    filename <- basename(file_path)

    if (i %% 100 == 0 || i == length(csv_files) || i == 1) {
      message(sprintf("  [%d/%d] %s", i, length(csv_files), filename))
    }

    tryCatch({
      upload_url <- sprintf("%s/hsapi/resource/%s/files/",
                            HYDROSHARE_BASE_URL, HYDROSHARE_RESOURCE_ID)

      response <- request(upload_url) |>
        req_auth_basic(hs_username, hs_password) |>
        req_body_multipart(file = curl::form_file(file_path)) |>
        req_timeout(60) |>
        req_perform()

      status <- resp_status(response)
      if (status >= 200 && status < 300) {
        upload_count <- upload_count + 1
      } else {
        fail_count <- fail_count + 1
      }
    }, error = function(e) {
      fail_count <<- fail_count + 1
    })

    Sys.sleep(0.25)
  }

  message(sprintf("\nUpload complete: %d succeeded, %d failed", upload_count, fail_count))
}

message("")
message("=== Backfill Complete ===")
