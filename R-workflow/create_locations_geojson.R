# create_locations_geojson.R
#
# Create locations.geojson by:
# 1. Reading locations.csv (source of truth for reservoir metadata)
# 2. Merging in lat/lon from rise.geojson for RISE locations
# 3. Spatial join to get doiRegion, huc6, and state
# 4. Output as locations.geojson
################################################################################

library(dplyr)
library(readr)
library(sf)
library(jsonlite)
library(stringr)

CONFIG_DIR <- "config"
REFERENCE_DIR <- "data/reference"

################################################################################
# LOAD DATA
################################################################################

message("Loading locations.csv...")
locations <- read_csv(file.path(CONFIG_DIR, "locations.csv"), show_col_types = FALSE)
message(sprintf("  %d locations loaded", nrow(locations)))

message("Loading rise.geojson...")
rise <- st_read(file.path(REFERENCE_DIR, "rise.geojson"), quiet = TRUE)
message(sprintf("  %d RISE features loaded", nrow(rise)))

message("Loading nid.csv...")
nid <- read_csv(file.path(REFERENCE_DIR, "nid.csv"), skip = 1, show_col_types = FALSE)
message(sprintf("  %d NID dams loaded", nrow(nid)))

message("Loading doiRegions.geojson...")
doi_regions <- st_read(file.path(REFERENCE_DIR, "doiRegions.geojson"), quiet = TRUE)
message(sprintf("  %d DOI regions loaded", nrow(doi_regions)))

message("Loading huc6.geojson...")
huc6 <- st_read(file.path(REFERENCE_DIR, "huc6.geojson"), quiet = TRUE)
message(sprintf("  %d HUC6 regions loaded", nrow(huc6)))

message("Loading states.geojson...")
states <- st_read(file.path(REFERENCE_DIR, "states.geojson"), quiet = TRUE)
message(sprintf("  %d states loaded", nrow(states)))

################################################################################
# MERGE LAT/LON FROM RISE
################################################################################

message("\nMerging coordinates from RISE...")

# Extract coordinates from rise geojson
# Handle mixed geometry types by casting to POINT and extracting centroids
rise_points <- rise |>
  st_centroid() |>
  st_transform(4326)

rise_coords <- rise_points |>
  mutate(
    coords = st_coordinates(geometry),
    rise_lon = coords[, 1],
    rise_lat = coords[, 2]
  ) |>
  st_drop_geometry() |>
  select(rise_id = X_id, rise_lon, rise_lat)

# Convert Identifier to numeric for joining (RISE IDs are numeric)
locations <- locations |>
  mutate(Identifier_num = suppressWarnings(as.numeric(Identifier)))

# Join RISE coordinates where Longitude/Latitude are missing
locations <- locations |>
  left_join(rise_coords, by = c("Identifier_num" = "rise_id")) |>
  mutate(
    Longitude = coalesce(Longitude, rise_lon),
    Latitude = coalesce(Latitude, rise_lat)
  ) |>
  select(-rise_lon, -rise_lat, -Identifier_num)

coords_filled <- sum(!is.na(locations$Longitude) & !is.na(locations$Latitude))
message(sprintf("  %d locations now have coordinates", coords_filled))

################################################################################
# MERGE LAT/LON FROM NID (for locations still missing coordinates)
################################################################################

message("\nMerging coordinates from NID...")

# Extract dam name from parentheses in modal label
# Handle special cases where modal label is broken (e.g., "#NAME?" Excel error)
locations <- locations |>
  mutate(
    dam_name_from_label = str_extract(`Preferred Label for PopUp and Modal`, "\\([^)]+\\)"),
    dam_name_from_label = str_remove_all(dam_name_from_label, "[()]"),
    # Special case: Fort Peck has "#NAME?" as modal label
    dam_name_from_label = case_when(
      Name == "Fort Peck" ~ "Fort Peck Dam",
      TRUE ~ dam_name_from_label
    )
  )

# Get NID dams owned by Bureau of Reclamation
nid_bor <- nid |>
  filter(str_detect(`Owner Names`, "BUREAU OF RECLAMATION")) |>
  select(nid_dam_name = `Dam Name`, nid_lat = Latitude, nid_lon = Longitude)

# Join NID coordinates for locations still missing coords
locations <- locations |>
  left_join(nid_bor, by = c("dam_name_from_label" = "nid_dam_name")) |>
  mutate(
    Longitude = coalesce(Longitude, nid_lon),
    Latitude = coalesce(Latitude, nid_lat)
  ) |>
  select(-nid_lon, -nid_lat)

coords_after_bor <- sum(!is.na(locations$Longitude) & !is.na(locations$Latitude))
message(sprintf("  After BOR match: %d locations have coordinates", coords_after_bor))

# Also try USACE dams for remaining missing coords
nid_usace <- nid |>
  filter(str_detect(`Owner Names`, "USACE")) |>
  select(nid_dam_name = `Dam Name`, nid_lat = Latitude, nid_lon = Longitude)

locations <- locations |>
  left_join(nid_usace, by = c("dam_name_from_label" = "nid_dam_name")) |>
  mutate(
    Longitude = coalesce(Longitude, nid_lon),
    Latitude = coalesce(Latitude, nid_lat)
  ) |>
  select(-nid_lon, -nid_lat, -dam_name_from_label)

coords_after_usace <- sum(!is.na(locations$Longitude) & !is.na(locations$Latitude))
message(sprintf("  After USACE match: %d locations have coordinates", coords_after_usace))

################################################################################
# SPATIAL JOINS
################################################################################

message("\nPerforming spatial joins...")

# Filter to locations with coordinates and convert to sf
locations_with_coords <- locations |>
  filter(!is.na(Longitude) & !is.na(Latitude))

locations_sf <- st_as_sf(
  locations_with_coords,
  coords = c("Longitude", "Latitude"),
  crs = 4326,
  remove = FALSE
)

# Ensure all layers use the same CRS
doi_regions <- st_transform(doi_regions, 4326)
huc6 <- st_transform(huc6, 4326)
states <- st_transform(states, 4326)

# Spatial join for DOI regions
message("  Joining DOI regions...")
locations_sf <- st_join(locations_sf, doi_regions |> select(doiRegion_joined = REG_NAME), left = TRUE)

# Spatial join for HUC6
message("  Joining HUC6...")
locations_sf <- st_join(locations_sf, huc6 |> select(huc6_joined = huc6), left = TRUE)

# Spatial join for states
message("  Joining states...")
locations_sf <- st_join(locations_sf, states |> select(state_joined = stusps), left = TRUE)

# Update the original columns with joined values (only where originally empty)
locations_sf <- locations_sf |>
  mutate(
    doiRegion = coalesce(doiRegion, doiRegion_joined),
    huc6 = coalesce(huc6, huc6_joined),
    state = coalesce(state, state_joined)
  ) |>
  select(-doiRegion_joined, -huc6_joined, -state_joined)

message(sprintf("  DOI regions filled: %d", sum(!is.na(locations_sf$doiRegion))))
message(sprintf("  HUC6 filled: %d", sum(!is.na(locations_sf$huc6))))
message(sprintf("  States filled: %d", sum(!is.na(locations_sf$state))))

################################################################################
# OUTPUT GEOJSON
################################################################################

message("\nWriting locations.geojson...")

# Filter to only included locations for the output
included_locations <- locations_sf |>
  filter(`Post-Review Decision` != "Do Not Include")

message(sprintf("  %d included locations", nrow(included_locations)))

st_write(
  included_locations,
  file.path(CONFIG_DIR, "locations.geojson"),
  driver = "GeoJSON",
  delete_dsn = TRUE
)

message("\nDone! Output saved to config/locations.geojson")
