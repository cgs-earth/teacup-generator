#!/usr/bin/env Rscript
# Parallel backfill of daily reports with batch upload to HydroShare
#
# Usage: Rscript backfill_parallel.R [end_date] [start_date] [n_workers]
# Default: today back to 1990-10-01, 8 parallel workers
#
# Phase 1: Generate CSVs in parallel (no uploads)
# Phase 2: Batch upload all CSVs to HydroShare

library(parallel)
library(httr2)

args <- commandArgs(trailingOnly = TRUE)

END_DATE <- if (length(args) >= 1) as.Date(args[1]) else Sys.Date()
START_DATE <- if (length(args) >= 2) as.Date(args[2]) else as.Date("1990-10-01")
N_WORKERS <- if (length(args) >= 3) as.integer(args[3]) else 8

# Load .env file
if (file.exists(".env")) {
  readRenviron(".env")
}

HYDROSHARE_RESOURCE_ID <- "22b2f10103e5426a837defc00927afbd"
HYDROSHARE_BASE_URL <- "https://www.hydroshare.org"
HYDROSHARE_DIR <- "hydroshare"

# Generate sequence of dates
dates <- seq(END_DATE, START_DATE, by = "-1 day")

message("=== Parallel Backfill Daily Reports ===")
message(sprintf("From: %s to %s", END_DATE, START_DATE))
message(sprintf("Total days: %d", length(dates)))
message(sprintf("Workers: %d", N_WORKERS))
message("")

################################################################################
# PHASE 1: Generate CSVs in parallel (no upload)
################################################################################

message("=== Phase 1: Generating CSVs ===")
message("")

# Function to generate a single day's report (no upload)
generate_day <- function(date_str) {
  # Set env var to skip upload
  Sys.setenv(SKIP_HYDROSHARE_UPLOAD = "1")

  result <- tryCatch({
    # Run the generator script
    exit_code <- system2(
      "Rscript",
      args = c("rezviz_data_generator.R", date_str),
      stdout = FALSE,
      stderr = FALSE
    )
    if (exit_code == 0) "OK" else "FAILED"
  }, error = function(e) {
    paste("ERROR:", conditionMessage(e))
  })

  return(list(date = date_str, status = result))
}

# Create cluster
cl <- makeCluster(N_WORKERS)

# Export necessary environment
clusterExport(cl, c())

# Run in parallel with progress
start_time <- Sys.time()
results <- parLapply(cl, as.character(dates), generate_day)
end_time <- Sys.time()

stopCluster(cl)

# Summarize results
ok_count <- sum(sapply(results, function(r) r$status == "OK"))
failed <- Filter(function(r) r$status != "OK", results)

message("")
message(sprintf("Phase 1 complete in %.1f minutes", as.numeric(difftime(end_time, start_time, units = "mins"))))
message(sprintf("  Succeeded: %d", ok_count))
message(sprintf("  Failed: %d", length(failed)))

if (length(failed) > 0) {
  failed_dates <- sapply(failed, function(r) r$date)
  writeLines(failed_dates, "backfill_failed_dates.txt")
  message("  Failed dates written to backfill_failed_dates.txt")
}

################################################################################
# PHASE 2: Batch upload to HydroShare
################################################################################

message("")
message("=== Phase 2: Uploading to HydroShare ===")
message("")

hs_username <- Sys.getenv("HYDROSHARE_USERNAME", unset = "")
hs_password <- Sys.getenv("HYDROSHARE_PASSWORD", unset = "")

if (hs_username == "" || hs_password == "") {
  message("WARNING: HydroShare credentials not set. Skipping upload.")
} else {
  # Get list of CSV files to upload
  csv_files <- list.files(HYDROSHARE_DIR, pattern = "^droughtData.*\\.csv$", full.names = TRUE)
  message(sprintf("Found %d CSV files to upload", length(csv_files)))

  upload_count <- 0
  fail_count <- 0

  for (i in seq_along(csv_files)) {
    file_path <- csv_files[i]
    filename <- basename(file_path)

    if (i %% 100 == 0 || i == length(csv_files)) {
      message(sprintf("[%d/%d] Uploading %s...", i, length(csv_files), filename))
    }

    tryCatch({
      # Upload the file
      upload_url <- sprintf("%s/hsapi/resource/%s/files/",
                            HYDROSHARE_BASE_URL, HYDROSHARE_RESOURCE_ID)

      response <- request(upload_url) |>
        req_auth_basic(hs_username, hs_password) |>
        req_body_multipart(file = curl::form_file(file_path)) |>
        req_timeout(120) |>
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

    # Rate limiting
    Sys.sleep(0.5)
  }

  message("")
  message(sprintf("Upload complete: %d succeeded, %d failed", upload_count, fail_count))
}

message("")
message("=== Backfill Complete ===")
message(sprintf("Total time: %.1f minutes", as.numeric(difftime(Sys.time(), start_time, units = "mins"))))
