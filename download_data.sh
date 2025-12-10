#!/bin/bash

# Create data directory if it doesn't exist
mkdir -p data
cd data

# Base URL for NYC Taxi Data (Parquet format)
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

# Years to download (User can adjust this)
YEARS=("2019" "2020" "2021")
MONTHS=$(seq -w 1 12)

echo "Starting download of NYC Yellow Taxi Data..."

for year in "${YEARS[@]}"; do
    for month in $MONTHS; do
        FILENAME="yellow_tripdata_${year}-${month}.parquet"
        URL="${BASE_URL}/${FILENAME}"
        
        echo "Downloading ${FILENAME}..."
        
        # Use wget if available, otherwise curl
        if command -v wget &> /dev/null; then
            wget -q --show-progress -c "$URL"
        else
            curl -O -C - "$URL"
        fi
        
        if [ $? -eq 0 ]; then
            echo "Successfully downloaded ${FILENAME}"
        else
            echo "Failed to download ${FILENAME}"
        fi
    done
done

echo "Download complete."
