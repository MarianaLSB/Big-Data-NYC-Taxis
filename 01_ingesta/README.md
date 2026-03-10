#!/bin/bash
# Script de ingesta: exporta NYC Taxi Trips de BigQuery a GCS
# Simula extracción de datos desde sistema On-premise (2016-2023)

export PROJECT_ID="project-bac9f556-1ccf-44de-801"
export BUCKET="gs://big-data-nyc-taxis-project-bac9f556-1ccf-44de-801"

for YEAR in 2016 2017 2018 2019 2020 2021 2022 2023; do
  echo "Exportando año $YEAR..."
  bq extract --destination_format=CSV \
    "bigquery-public-data:new_york_taxi_trips.tlc_yellow_trips_${YEAR}" \
    "$BUCKET/raw/yellow_trips_${YEAR}/trips_*.csv"
done

echo "Ingesta completa!"
gsutil du -sh $BUCKET/raw/
