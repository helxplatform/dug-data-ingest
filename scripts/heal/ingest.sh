#!/bin/sh

# Bash strict mode
set -euo pipefail
IFS=$'\n\t'

# A script for ingesting data from HEAL Platform dbGaP XML files into LakeFS.
echo Started ingest from HEAL Platform at `date`.

echo cleaning /data directory
rm -rf /data/*

# Step 1. Download the list of dbGaP IDs from BDC.
python heal/get_heal_platform_mds_data_dicts.py /data/heal

# Step 2. Upload the files to BDC.
echo Uploading dbGaP XML files to LakeFS
# -l: local path
# -r: remote path
# -e: repository
# -b: branch
python lakefsclient_upload.py  -l "/data/heal/dbGaPs" -r "" -e "bdc-test3" -b "main"

# Report errors.
echo Downloads complete at `date`.
