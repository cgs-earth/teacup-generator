# Dockerfile for rezviz_data_generator.R
# Generates daily reservoir conditions CSV and uploads to HydroShare
#
# Build:
#   docker build -t rezviz .
#
# Run (yesterday, default):
#   docker run --env-file .env rezviz
#
# Run for an arbitrary date:
#   docker run --env-file .env rezviz 2026-01-15
#
# Run and keep the CSV locally:
#   docker run --env-file .env -v $(pwd)/hydroshare:/app/hydroshare rezviz 2026-01-15
#
# Historical parquet files are bundled in the image. If new locations are
# backfilled at runtime, updated parquet files are uploaded to HydroShare.
# Rebuild the image periodically to incorporate the latest backfills.

FROM --platform=linux/amd64 rocker/geospatial:4.4.2

# Install R packages not included in rocker/geospatial
RUN install2.r --error --skipinstalled \
    httr2 \
    arrow \
    curl \
    jsonlite

WORKDIR /app

# Copy runtime files
COPY rezviz_data_generator.R .
COPY config/locations.geojson config/
COPY config/elevation_storage_curves.csv config/

# Create output directories
RUN mkdir -p hydroshare

# Bundle historical parquet files
COPY output/historical_baseline.parquet output/
COPY output/historical_statistics.parquet output/

# ENTRYPOINT lets us append the date argument at docker run time
ENTRYPOINT ["Rscript", "rezviz_data_generator.R"]
