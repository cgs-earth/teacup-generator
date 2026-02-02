# fetch_489_by_year.R
#
# Fetch location 489 (Sumner) data one year at a time to avoid API timeouts,
# then concatenate into a single CSV file.
################################################################################

library(httr2)
library(dplyr)
library(readr)

WWDH_API_BASE <- "https://api.wwdh.internetofwater.app"
LOCATION_ID <- "489"

# Fetch one year of data
fetch_year <- function(location_id, year) {
  start_date <- paste0(year, "-01-01")
  end_date <- paste0(year, "-12-31")

  url <- paste0(
    WWDH_API_BASE,
    "/collections/rise-edr/locations/", location_id,
    "?parameter-name=Storage",
    "&limit=500",
    "&datetime=", start_date, "/", end_date,
    "&f=csv"
  )

  message(sprintf("Fetching year %s...", year))

  tryCatch({
    response <- request(url) |>
      req_timeout(120) |>
      req_retry(max_tries = 3, backoff = ~ 5) |>
      req_perform()

    status <- resp_status(response)
    if (status != 200) {
      message(sprintf("  HTTP %d for year %s", status, year))
      return(NULL)
    }

    csv_content <- resp_body_string(response)

    if (nchar(csv_content) < 30) {
      message(sprintf("  No data for year %s", year))
      return(NULL)
    }

    data <- read_csv(csv_content, show_col_types = FALSE)
    message(sprintf("  Got %d rows for year %s", nrow(data), year))
    return(data)

  }, error = function(e) {
    message(sprintf("  Error for year %s: %s", year, e$message))
    return(NULL)
  })
}

# Fetch all years from 1990-2020 (water years)
years <- 1990:2020
all_data <- list()

for (year in years) {
  data <- fetch_year(LOCATION_ID, year)
  if (!is.null(data) && nrow(data) > 0) {
    all_data[[as.character(year)]] <- data
  }
  Sys.sleep(1)  # Be respectful of API
}

# Combine all years
if (length(all_data) > 0) {
  combined <- bind_rows(all_data)
  message(sprintf("\nTotal rows: %d", nrow(combined)))

  # Save to CSV
  write_csv(combined, "489.csv")
  message("Saved to 489.csv")
} else {
  message("No data retrieved")
}
