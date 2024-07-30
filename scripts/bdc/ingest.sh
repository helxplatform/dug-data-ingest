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
#python lakefsclient_upload.py  -l "/data/bdc/BioLINCC/" -r "_BioLINCC/" -e "bdc-test2" -b "main"
#python lakefsclient_upload.py  -l "/data/bdc/COVID19/" -r "_COVID19/" -e "bdc-test2" -b "main"
#python lakefsclient_upload.py  -l "/data/bdc/DIR/" -r "_DIR/" -e "bdc-test" -b "main"
#python lakefsclient_upload.py  -l "/data/bdc/imaging/" -r "_imaging/" -e "bdc-test2" -b "main"
#python lakefsclient_upload.py  -l "/data/bdc/LungMAP/" -r "_LungMap/" -e "bdc-test2" -b "main"
#python lakefsclient_upload.py  -l "/data/bdc/NSRR/" -r "_NSRR/" -e "bdc-test2" -b "main"
#python lakefsclient_upload.py  -l "/data/bdc/parent/" -r "_parent/" -e "bdc-test2" -b "main"
#python lakefsclient_upload.py  -l "/data/bdc/PCGC/" -r "_pcgc/" -e "bdc-test2" -b "main"
#python lakefsclient_upload.py  -l "/data/bdc/RECOVER/" -r "_RECOVER/" -e "bdc-test2" -b "main"
python lakefsclient_upload.py  -l "/data/bdc/topmed/" -r "" -e "bdc-test2" -b "main"


# Report errors.
echo Downloads complete at `date`.
