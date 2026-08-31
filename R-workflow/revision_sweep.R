# revision_sweep.R
#
# WEEKLY (or on-demand) REVISION SWEEP: re-fetch a recent window (default: the
# last 90 days) for every reservoir and re-publish it as a droughtData CSV so the
# downstream loader picks up values the source has REVISED since we first read
# them.
#
# Why this exists
# ---------------
# The daily run (rezviz_data_generator.R) publishes only the last 7 days. Sources
# (RISE/USBR, USACE, USGS, CDEC) routinely revise provisional values weeks or
# months later — a value we read on day 1 is not necessarily the value the source
# holds on day 60. Those late corrections were previously never re-read:
#
#   - The daily run's 7-day window has already moved past them.
#   - backfill_history.R only ADDS observations missing from the baseline; it
#     never re-reads a (location, date) we already hold, so a changed value is
#     invisible to it.
#
# This script closes that hole. It re-reads the whole window unconditionally and
# emits every observation in it, so a value that changed at the source overwrites
# the stale one in the database. That relies on the Cloud Run loader now
# UPSERTING (insert-or-update on SiteId.DataDate.parameter) rather than
# insert-or-ignore; under the old insert-or-ignore behavior these rows would be
# silently dropped as duplicates.
#
# How the values reach the database
# ---------------------------------
# Exactly the same path as the daily run: write droughtDataYYYYMMDD.csv, upload it
# to the HydroShare resource (replacing that day's file), then the workflow's
# `gcloud run jobs execute` step loads it. Naming the file with the normal daily
# convention is deliberate — the loader ingests only droughtDataYYYYMMDD.csv files
# and would not read a differently-named one. A later daily run overwriting that
# same filename with its 7-day version is harmless: the revised values are already
# in the database by then.
#
# Rows are column-identical to daily rows (same 27-column schema, real DataDate,
# real DataUrl) except Comment = "revision" for provenance.
#
# Usage:
#   Rscript revision_sweep.R                      # last 90 days, CSV dated yesterday
#   Rscript revision_sweep.R 2026-08-04            # CSV dated 2026-08-04
#   Rscript revision_sweep.R 2026-08-04 7166,393   # only these location_ids
#
#   # Via Docker (override the entrypoint)
#   docker run --entrypoint Rscript --env-file .env ghcr.io/cgs-earth/rezviz:latest revision_sweep.R
#
# Environment:
#   HYDROSHARE_USERNAME / HYDROSHARE_PASSWORD  (loaded from .env if present)
#   REVISION_DAYS=90              # size of the look-back window in days
#   REVISION_TARGET_DATE=...      # date the output CSV is named for (default: yesterday)
#   REVISION_LOCATION_IDS=a,b,c   # restrict the sweep to these location_ids
#   REVISION_COMMENT=revision     # value for the CSV Comment column
#   REVISION_ONLY_CHANGED=1       # emit ONLY observations that differ from the
#                                 #   stored baseline (smaller CSV; skips dates the
#                                 #   baseline is missing entirely — off by default)
#   REVISION_UPDATE_BASELINE=1    # also write revised values back into
#                                 #   historical_baseline.parquet on HydroShare
#                                 #   (off by default; see note near the bottom)
#   REVISION_DRY_RUN=1            # generate the CSV locally, skip all uploads
#
# Author: Kyle Onda, CGS
# Created: 2026-08-05
################################################################################

library(httr2)
library(dplyr)
library(readr)
library(lubridate)
library(arrow)
library(stringr)
library(sf)
library(jsonlite)

# Shared utilities: elevation->storage curves, classify_source(),
# build_drought_csv_rows(), and the per-source fetch_full_history().
source("helper_functions.R")

# Load .env file if present
if (file.exists(".env")) {
  readRenviron(".env")
}

################################################################################
# CONFIGURATION
################################################################################

WWDH_API_BASE <- "https://api.wwdh.internetofwater.app"

HYDROSHARE_RESOURCE_ID <- "22b2f10103e5426a837defc00927afbd"
HYDROSHARE_BASE_URL    <- "https://www.hydroshare.org"

OUTPUT_DIR     <- "output"
CONFIG_DIR     <- "config"
HYDROSHARE_DIR <- "hydroshare"

# StatsPeriod label and window — must match the daily script so the percentile
# columns on revision rows are identical to the ones on daily rows.
STATS_PERIOD   <- "10/1/1990 - 9/30/2020"
BASELINE_START <- as.Date("1990-10-01")
BASELINE_END   <- as.Date("2020-09-30")

# Minimum water years of baseline coverage required before we attach percentiles
# (same rule as the daily script: thinner records give unreliable percentiles).
MIN_WATER_YEARS <- 20

args <- commandArgs(trailingOnly = TRUE)

# Look-back window. 90 days (~3 months) covers the period in which the upstream
# sources actually revise provisional values.
REVISION_DAYS <- {
  override <- Sys.getenv("REVISION_DAYS", "")
  n <- suppressWarnings(as.integer(if (nzchar(override)) override else "90"))
  if (is.na(n) || n < 1) stop("REVISION_DAYS must be a positive integer")
  n
}

# Date the output CSV is named for. Defaults to yesterday, matching the daily
# script, so this run replaces the file the loader is already looking at.
TARGET_DATE <- {
  from_arg <- if (length(args) > 0 && nzchar(trimws(args[1]))) trimws(args[1]) else ""
  from_env <- Sys.getenv("REVISION_TARGET_DATE", "")
  raw <- if (nzchar(from_arg)) from_arg else from_env
  d <- if (nzchar(raw)) as.Date(raw) else Sys.Date() - 1
  if (is.na(d)) stop(sprintf("Could not parse target date: '%s'", raw))
  d
}

FETCH_START <- TARGET_DATE - REVISION_DAYS + 1
FETCH_END   <- TARGET_DATE

# Optional restriction to specific location_ids (positional arg 2 or env).
restrict_ids <- {
  from_arg <- if (length(args) > 1 && nzchar(trimws(args[2]))) trimws(args[2]) else ""
  from_env <- Sys.getenv("REVISION_LOCATION_IDS", "")
  raw <- if (nzchar(from_arg)) from_arg else from_env
  if (nzchar(raw)) trimws(str_split(raw, ",")[[1]]) else character(0)
}

REVISION_COMMENT <- {
  v <- Sys.getenv("REVISION_COMMENT", "revision")
  v  # may be set to "" to leave the Comment column blank
}

ONLY_CHANGED    <- Sys.getenv("REVISION_ONLY_CHANGED", "")    %in% c("1", "true", "TRUE")
UPDATE_BASELINE <- Sys.getenv("REVISION_UPDATE_BASELINE", "") %in% c("1", "true", "TRUE")
DRY_RUN         <- Sys.getenv("REVISION_DRY_RUN", "")         %in% c("1", "true", "TRUE")

# Relative difference above which a re-read value counts as "revised". Guards
# against float noise in the source APIs being reported as a revision.
CHANGE_TOLERANCE <- 1e-6

message("=== Reservoir Revision Sweep ===")
message(sprintf("Run time:      %s", Sys.time()))
message(sprintf("Window:        %s -> %s (%d days)", FETCH_START, FETCH_END, REVISION_DAYS))
message(sprintf("Output CSV:    droughtData%s.csv", format(TARGET_DATE, "%Y%m%d")))
if (length(restrict_ids) > 0) {
  message(sprintf("Restricted to %d location id(s): %s",
                  length(restrict_ids), paste(restrict_ids, collapse = ", ")))
}
if (ONLY_CHANGED)    message("REVISION_ONLY_CHANGED: emitting only observations that differ from the baseline")
if (UPDATE_BASELINE) message("REVISION_UPDATE_BASELINE: revised values will be written back to historical_baseline.parquet")
if (DRY_RUN)         message("DRY RUN: nothing will be uploaded to HydroShare")

if (FETCH_START <= BASELINE_END) {
  message(sprintf(paste0(
    "\nWARNING: the window reaches back into the statistics period (<= %s). ",
    "Day-of-year statistics are NOT recomputed here — run backfill_history.R if ",
    "the percentiles need to be refreshed."), BASELINE_END))
}

stats_file    <- file.path(OUTPUT_DIR, "historical_statistics.parquet")
baseline_file <- file.path(OUTPUT_DIR, "historical_baseline.parquet")

hs_username <- Sys.getenv("HYDROSHARE_USERNAME", "")
hs_password <- Sys.getenv("HYDROSHARE_PASSWORD", "")

################################################################################
# HYDROSHARE I/O
################################################################################

download_parquet_from_hydroshare <- function(filename, dest_path, resource_id, username, password) {
  url <- sprintf("%s/hsapi/resource/%s/files/%s/", HYDROSHARE_BASE_URL, resource_id, filename)
  tmp <- paste0(dest_path, ".tmp")
  tryCatch({
    req <- request(url) |> req_timeout(300)
    if (username != "" && password != "") {
      req <- req |> req_auth_basic(username, password)
    }
    req |> req_perform() |> resp_body_raw() |> writeBin(tmp)
    magic <- readBin(tmp, "raw", n = 4)
    if (rawToChar(magic) == "PAR1") {
      file.rename(tmp, dest_path)
      message(sprintf("  Updated %s from HydroShare (%.1f MB)", filename, file.size(dest_path) / 1e6))
      return(TRUE)
    } else {
      file.remove(tmp)
      message(sprintf("  WARNING: Downloaded %s failed validation, keeping local file", filename))
      return(FALSE)
    }
  }, error = function(e) {
    if (file.exists(tmp)) file.remove(tmp)
    message(sprintf("  WARNING: Could not download %s: %s - using local file", filename, e$message))
    return(FALSE)
  })
}

upload_to_hydroshare <- function(file_path, resource_id, username, password) {
  filename <- basename(file_path)

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

  upload_url <- sprintf("%s/hsapi/resource/%s/files/",
                        HYDROSHARE_BASE_URL, resource_id)
  response <- request(upload_url) |>
    req_auth_basic(username, password) |>
    req_body_multipart(file = curl::form_file(file_path)) |>
    req_timeout(300) |>
    req_perform()

  status <- resp_status(response)
  if (status >= 200 && status < 300) {
    message(sprintf("  Successfully uploaded %s", filename))
  } else {
    warning(sprintf("  Upload of %s returned status %d", filename, status))
  }
  status
}

################################################################################
# SYNC PARQUET FROM HYDROSHARE
################################################################################

message("\nSyncing parquet files from HydroShare...")
download_parquet_from_hydroshare("historical_baseline.parquet",   baseline_file, HYDROSHARE_RESOURCE_ID, hs_username, hs_password)
download_parquet_from_hydroshare("historical_statistics.parquet", stats_file,    HYDROSHARE_RESOURCE_ID, hs_username, hs_password)

if (!file.exists(stats_file) || !file.exists(baseline_file)) {
  stop("Parquet files not found and could not be downloaded from HydroShare.")
}

baseline <- read_parquet(baseline_file)
message(sprintf("Loaded baseline: %d observations for %d locations",
                nrow(baseline), n_distinct(baseline$location_id)))

historical_stats <- read_parquet(stats_file)
message(sprintf("Loaded statistics: %d rows for %d locations",
                nrow(historical_stats), n_distinct(historical_stats$location_id)))

# Same coverage rule as the daily script: locations with a thin period of record
# keep their raw values but get NA percentile columns.
wy_coverage <- baseline |>
  mutate(water_year = ifelse(month(date) >= 10, year(date) + 1, year(date))) |>
  group_by(location_id) |>
  summarize(n_water_years = n_distinct(water_year), .groups = "drop")

inadequate <- wy_coverage |>
  filter(n_water_years < MIN_WATER_YEARS) |>
  pull(location_id)

if (length(inadequate) > 0) {
  message(sprintf("  Excluding statistics for %d location(s) with < %d water years of baseline data",
                  length(inadequate), MIN_WATER_YEARS))
  historical_stats <- historical_stats |> filter(!location_id %in% inadequate)
}

################################################################################
# LOAD LOCATION METADATA (same shape as the daily script)
################################################################################

locations_file <- file.path(CONFIG_DIR, "locations.geojson")
locations_sf <- st_read(locations_file, quiet = TRUE)

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
    source = `Source.for.Storage.Data`,
    data_type = `Storage.Data.Type`
  )

locations$rise_param_id <- if ("RISE.Parameter.ID.for.Storage.Data" %in% names(locations_sf)) {
  locations_sf$`RISE.Parameter.ID.for.Storage.Data`
} else {
  NA_character_
}
locations$usgs_param_code <- if ("USGS.Parameter.Code" %in% names(locations_sf)) {
  locations_sf$`USGS.Parameter.Code`
} else {
  NA_character_
}

locations <- locations |>
  filter(!is.na(location_id), location_id != "--", location_id != "")
if (length(restrict_ids) > 0) {
  locations <- locations |> filter(location_id %in% restrict_ids)
}

if (nrow(locations) == 0) {
  stop("No locations to sweep (check REVISION_LOCATION_IDS / positional argument).")
}

message(sprintf("\nRe-reading %d location(s) over the revision window\n", nrow(locations)))

# Elevation->storage curves must be in scope for fetch_full_history().
elev_curves_data <- load_elevation_curves(CONFIG_DIR)
elev_curves    <- elev_curves_data$curves
elev_curve_ids <- elev_curves_data$ids

################################################################################
# RE-READ THE WINDOW FOR EVERY LOCATION
################################################################################

# Baseline values inside the window, used to report (and optionally filter to)
# what the sources actually changed.
prior <- baseline |>
  filter(date >= FETCH_START, date <= FETCH_END) |>
  distinct(location_id, date, .keep_all = TRUE) |>
  select(location_id, date, prior_value = value)

fetched_list <- list()
n_failed     <- 0
failed_ids   <- character(0)

for (i in seq_len(nrow(locations))) {
  loc      <- locations[i, ]
  loc_id   <- loc$location_id
  loc_name <- loc$name
  src_type <- classify_source(loc$source)

  message(sprintf("[%d/%d] %s (ID: %s) [%s]...",
                  i, nrow(locations), loc_name, loc_id, src_type))

  obs <- tryCatch(
    fetch_full_history(loc, FETCH_START, FETCH_END),
    error = function(e) {
      message(sprintf("    ERROR: %s", conditionMessage(e)))
      NULL
    }
  )

  if (is.null(obs) || nrow(obs) == 0) {
    message("    No data retrieved")
    n_failed   <- n_failed + 1
    failed_ids <- c(failed_ids, loc_id)
    Sys.sleep(1)
    next
  }

  src_url <- attr(obs, "source_url")
  if (is.null(src_url)) src_url <- NA_character_

  # Sources sometimes return values outside the requested range; clamp so the
  # published window is exactly the one we advertised.
  obs <- obs |>
    filter(!is.na(value), date >= FETCH_START, date <= FETCH_END) |>
    distinct(location_id, date, .keep_all = TRUE) |>
    mutate(data_url = src_url) |>
    select(location_id, date, value, unit, data_url)

  if (nrow(obs) == 0) {
    message("    No observations inside the window")
    Sys.sleep(1)
    next
  }

  changed <- obs |>
    left_join(prior |> filter(location_id == loc_id), by = c("location_id", "date")) |>
    filter(!is.na(prior_value),
           abs(value - prior_value) > CHANGE_TOLERANCE * pmax(abs(prior_value), 1))

  message(sprintf("    %d observation(s) in window; %d revised vs baseline",
                  nrow(obs), nrow(changed)))

  fetched_list[[length(fetched_list) + 1]] <- obs
  Sys.sleep(1)  # rate limiting for bulk fetches
}

if (length(fetched_list) == 0) {
  stop("No observations retrieved for any location — refusing to publish an empty CSV.")
}

fetched <- bind_rows(fetched_list)

################################################################################
# REVISION DIAGNOSTICS
################################################################################

comparison <- fetched |>
  left_join(prior, by = c("location_id", "date")) |>
  mutate(
    status = case_when(
      is.na(prior_value)                                                    ~ "new",
      abs(value - prior_value) > CHANGE_TOLERANCE * pmax(abs(prior_value), 1) ~ "revised",
      TRUE                                                                  ~ "unchanged"
    )
  )

n_revised   <- sum(comparison$status == "revised")
n_new       <- sum(comparison$status == "new")
n_unchanged <- sum(comparison$status == "unchanged")

message(sprintf("\n=== Revision summary (%s -> %s) ===", FETCH_START, FETCH_END))
message(sprintf("  Observations re-read: %d across %d location(s)",
                nrow(fetched), n_distinct(fetched$location_id)))
message(sprintf("  Revised at source:    %d", n_revised))
message(sprintf("  Absent from baseline: %d", n_new))
message(sprintf("  Unchanged:            %d", n_unchanged))

if (n_revised > 0) {
  top_revised <- comparison |>
    filter(status == "revised") |>
    group_by(location_id) |>
    summarize(n_revised = n(), .groups = "drop") |>
    left_join(locations |> select(location_id, name), by = "location_id") |>
    arrange(desc(n_revised))
  message("  Locations with revised values:")
  for (j in seq_len(min(20, nrow(top_revised)))) {
    message(sprintf("    %-40s (%s): %d",
                    top_revised$name[j], top_revised$location_id[j],
                    top_revised$n_revised[j]))
  }
  if (nrow(top_revised) > 20) {
    message(sprintf("    ... and %d more location(s)", nrow(top_revised) - 20))
  }
}

# Full per-observation diff, kept locally for inspection / debugging.
report_path <- file.path(OUTPUT_DIR, sprintf("revision_report_%s.csv",
                                             format(TARGET_DATE, "%Y%m%d")))
comparison |>
  filter(status != "unchanged") |>
  left_join(locations |> select(location_id, name), by = "location_id") |>
  transmute(location_id, name, date, prior_value, new_value = value, status) |>
  arrange(name, date) |>
  write_csv(report_path, na = "")
message(sprintf("  Diff written to %s", report_path))

################################################################################
# GENERATE OUTPUT CSV
################################################################################

publish <- if (ONLY_CHANGED) {
  comparison |>
    filter(status %in% c("revised", "new")) |>
    select(location_id, date, value, unit, data_url)
} else {
  fetched
}

if (nrow(publish) == 0) {
  message("\nNothing to publish (no revised or new observations). Exiting without upload.")
  quit(save = "no", status = 0)
}

output_rows <- build_drought_csv_rows(publish, locations, historical_stats,
                                     STATS_PERIOD, comment = REVISION_COMMENT)

# One row per (site, date). The loader upserts, and Postgres cannot apply two
# conflicting updates to the same key in a single statement, so duplicates must
# not reach the CSV.
output_csv <- output_rows |>
  distinct(SiteName, DataDate, .keep_all = TRUE) |>
  arrange(SiteName, DataDate)

if (nrow(output_csv) < nrow(output_rows)) {
  message(sprintf(paste0(
    "  NOTE: dropped %d duplicate (SiteName, DataDate) row(s) — check for two ",
    "location_ids sharing a popup label in locations.csv."),
    nrow(output_rows) - nrow(output_csv)))
}

output_filename <- sprintf("droughtData%s.csv", format(TARGET_DATE, "%Y%m%d"))
if (!dir.exists(HYDROSHARE_DIR)) dir.create(HYDROSHARE_DIR, recursive = TRUE)
output_path <- file.path(HYDROSHARE_DIR, output_filename)

write_csv(output_csv, output_path, na = "")

message(sprintf("\nOutput written to: %s", output_path))
message(sprintf("  Rows: %d for %d site(s)", nrow(output_csv), n_distinct(output_csv$SiteName)))
message(sprintf("  With historical stats: %d", sum(!is.na(output_csv$DataDateP50))))
message(sprintf("  Without historical stats: %d", sum(is.na(output_csv$DataDateP50))))

################################################################################
# OPTIONALLY WRITE REVISED VALUES BACK INTO THE BASELINE
################################################################################

# Off by default. The day-of-year statistics come from 1990-2020 only, so recent
# revisions never affect the published percentiles — the baseline is the raw
# archive. Enabling this keeps that archive in step with the sources; leaving it
# off means the parquet keeps the value we first read. Only (location_id, date)
# pairs we actually re-read are touched, so a source that failed this run cannot
# lose data.
baseline_changed <- FALSE
if (UPDATE_BASELINE) {
  message("\n=== Updating historical_baseline.parquet with revised values ===")

  revised_keys <- comparison |>
    filter(status %in% c("revised", "new")) |>
    select(location_id, date, value, unit)

  if (nrow(revised_keys) == 0) {
    message("  No revised or new observations; baseline left untouched.")
  } else {
    updated_baseline <- bind_rows(
      baseline |> anti_join(revised_keys, by = c("location_id", "date")),
      revised_keys
    ) |>
      arrange(location_id, date)

    write_parquet(updated_baseline, baseline_file)
    baseline_changed <- TRUE
    message(sprintf("  Applied %d revised/new observation(s); baseline now %d rows (was %d)",
                    nrow(revised_keys), nrow(updated_baseline), nrow(baseline)))
  }
}

################################################################################
# UPLOAD TO HYDROSHARE
################################################################################

if (DRY_RUN) {
  message("\nDRY RUN: skipping HydroShare upload.")
} else if (hs_username == "" || hs_password == "") {
  message("\nWARNING: HydroShare credentials not set; skipping upload.")
  message("Set HYDROSHARE_USERNAME and HYDROSHARE_PASSWORD to enable upload.")
} else {
  message("\n=== Uploading to HydroShare ===")
  tryCatch(
    upload_to_hydroshare(output_path, HYDROSHARE_RESOURCE_ID, hs_username, hs_password),
    error = function(e) stop(sprintf("ERROR uploading %s: %s", output_filename, e$message))
  )

  if (baseline_changed) {
    message(sprintf("Uploading historical_baseline.parquet (%.1f MB)...",
                    file.size(baseline_file) / 1e6))
    tryCatch(
      upload_to_hydroshare(baseline_file, HYDROSHARE_RESOURCE_ID, hs_username, hs_password),
      error = function(e) message(sprintf("ERROR uploading historical_baseline.parquet: %s", e$message))
    )
  }

  message("Upload complete — the workflow's loader step will now ingest this CSV.")
}

################################################################################
# SUMMARY
################################################################################

message("\n=== Revision sweep complete ===")
message(sprintf("Window:               %s -> %s (%d days)", FETCH_START, FETCH_END, REVISION_DAYS))
message(sprintf("Locations swept:      %d", nrow(locations)))
message(sprintf("Locations with no data: %d%s", n_failed,
                if (n_failed > 0) sprintf(" (%s)", paste(failed_ids, collapse = ", ")) else ""))
message(sprintf("Revised values found: %d", n_revised))
message(sprintf("Rows published:       %d", nrow(output_csv)))
message(sprintf("Output file:          %s", output_path))
message(sprintf("Completed at:         %s", Sys.time()))
