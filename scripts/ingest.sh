#!/bin/sh

# Bash strict mode
set -euo pipefail
IFS=$'\n\t'

# A script for ingesting data from BDC into LakeFS.
echo Started ingest from BDC at `date`.

echo cleaning /data directory
rm -rf /data/*

# Step 1. Download the list of dbGaP IDs from BDC.
python bdc/get_bdc_studies_from_gen3.py /data/bdc_dbgap_ids.csv

# Step 2. Download the dbGaP XML files from BDC.
mkdir -p /data/bdc
python bdc/get_dbgap_data_dicts.py /data/bdc_dbgap_ids.csv --format CSV --field "Accession" --outdir /data/bdc --group-by Program

echo Uploading output to lakefs
python lakefsclient_upload.py  -l "/data/bdc/" -r "data/bdc" -e "bdc-test" -b "main"

# Report errors.
echo Downloads complete at `date`.
