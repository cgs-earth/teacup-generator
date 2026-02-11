#!/usr/bin/env Rscript
# Quick script to update DataUrl in existing CSVs without re-fetching data

library(dplyr)
library(readr)
library(stringr)
library(sf)

CONFIG_DIR <- "config"
HYDROSHARE_DIR <- "hydroshare"
WWDH_API_BASE <- "https://api.wwdh.internetofwater.app"

message("Loading location metadata...")
locations_sf <- st_read(file.path(CONFIG_DIR, "locations.geojson"), quiet = TRUE)

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

locations <- locations_sf |>
  st_drop_geometry() |>
  transmute(
    label_popup = `Preferred.Label.for.PopUp.and.Modal`,
    location_id = Identifier,
    source_type = sapply(`Source.for.Storage.Data`, classify_source)
  )

message(sprintf("  Loaded %d locations", nrow(locations)))

# USACE lookup
usace_lookup <- list(
  "305" = list(provider = "spa", ts_name = "Cochiti.Stor.Inst.15Minutes.0.DCP-rev"),
  "abiquiu" = list(provider = "spa", ts_name = "Abiquiu.Stor.Inst.15Minutes.0.DCP-rev"),
  "Santa Rosa" = list(provider = "spa", ts_name = "Santa Rosa.Stor.Inst.15Minutes.0.DCP-rev"),
  "gcl" = list(provider = "nwdp", ts_name = "GCL.Stor.Inst.1Hour.0.CBT-REV"),
  "FTPK" = list(provider = "nwdm", ts_name = "FTPK.Stor.Inst.~1Day.0.Best-MRBWM"),
  "luc" = list(provider = "nww", ts_name = "LUC.Stor-Total.Inst.0.0.USBR-COMPUTED-REV")
)

# Generate URL based on source type
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

# Get all CSV files
csv_files <- list.files(HYDROSHARE_DIR, pattern = "^droughtData.*\\.csv$", full.names = TRUE)
message(sprintf("Found %d CSV files to update", length(csv_files)))

updated_count <- 0
for (i in seq_along(csv_files)) {
  file_path <- csv_files[i]

  if (i %% 500 == 0 || i == length(csv_files) || i == 1) {
    message(sprintf("  [%d/%d] %s", i, length(csv_files), basename(file_path)))
  }

  # Read existing CSV
  data <- read_csv(file_path, show_col_types = FALSE)

  # Join with location info to get source_type and location_id
  data_updated <- data |>
    left_join(locations, by = c("SiteName" = "label_popup")) |>
    rowwise() |>
    mutate(
      data_date_parsed = if (!is.na(DataDate) && DataDate != "") {
        as.Date(DataDate, format = "%m/%d/%Y")
      } else {
        NA
      },
      DataUrl = generate_data_url(source_type, location_id, data_date_parsed)
    ) |>
    ungroup() |>
    select(-source_type, -location_id, -data_date_parsed)

  # Write back
  write_csv(data_updated, file_path, na = "")
  updated_count <- updated_count + 1
}

message(sprintf("\nUpdated %d CSV files with DataUrl", updated_count))
