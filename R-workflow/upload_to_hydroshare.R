#!/usr/bin/env Rscript
# Upload all CSVs from hydroshare/ directory to HydroShare

library(httr2)
library(curl)

# Load .env file
if (file.exists(".env")) {
  readRenviron(".env")
}

HYDROSHARE_RESOURCE_ID <- "22b2f10103e5426a837defc00927afbd"
HYDROSHARE_BASE_URL <- "https://www.hydroshare.org"
HYDROSHARE_DIR <- "hydroshare"

message("=== Uploading to HydroShare ===")

hs_username <- Sys.getenv("HYDROSHARE_USERNAME", unset = "")
hs_password <- Sys.getenv("HYDROSHARE_PASSWORD", unset = "")

if (hs_username == "" || hs_password == "") {
  stop("HydroShare credentials not set. Set HYDROSHARE_USERNAME and HYDROSHARE_PASSWORD in .env")
}

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
      req_retry(max_tries = 3, backoff = ~ 5) |>
      req_perform()

    if (resp_status(response) %in% c(200, 201)) {
      upload_count <- upload_count + 1
    } else {
      fail_count <- fail_count + 1
      if (i <= 10 || fail_count <= 5) {
        message(sprintf("    Upload failed: HTTP %d", resp_status(response)))
      }
    }
  }, error = function(e) {
    fail_count <- fail_count + 1
    if (i <= 10 || fail_count <= 5) {
      message(sprintf("    Error: %s", conditionMessage(e)))
    }
  })

  # Rate limit to avoid overwhelming the server
  Sys.sleep(0.5)
}

message("")
message(sprintf("=== Upload Complete ==="))
message(sprintf("Uploaded: %d files", upload_count))
message(sprintf("Failed: %d files", fail_count))
