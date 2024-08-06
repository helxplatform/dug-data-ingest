#!/usr/bin/bash

# Bash strict mode
set -euo pipefail

# But not the IFS change, because we're not using arrays and it
# messes up RCLONE_FLAGS.

# A script for ingesting data from BDC into LakeFS.
echo Started ingest from BDC at `date`.

echo cleaning /data directory
rm -rf /data/*

# Step 1. Download the list of dbGaP IDs from BDC.
python bdc/get_bdc_studies_from_gen3.py /data/bdc_dbgap_ids.csv

# Step 2. Download the dbGaP XML files from BDC.
mkdir -p /data/bdc
python bdc/get_dbgap_data_dicts.py /data/bdc_dbgap_ids.csv --format CSV --field "Accession" --outdir /data/bdc --group-by Program

# Step 3. Upload the dbGaP XML files to BDC.
echo Uploading dbGaP XML files to LakeFS using Rclone.

# Set up RClone environment variables.
export RCLONE_CONFIG_LAKEFS_TYPE=s3
export RCLONE_CONFIG_LAKEFS_PROVIDER=Other
export RCLONE_CONFIG_LAKEFS_ENDPOINT="$LAKEFS_HOST"
export RCLONE_CONFIG_LAKEFS_ACCESS_KEY_ID="$LAKEFS_USERNAME"
export RCLONE_CONFIG_LAKEFS_SECRET_ACCESS_KEY="$LAKEFS_PASSWORD"
export RCLONE_CONFIG_LAKEFS_NO_CHECK_BUCKET=true

# We would normally put each project into its own LakeFS repository, configurable in
# this file, but for testing I'm going to put them all into the same repository.
LAKEFS_REPOSITORY="bdc-test4"

# Sync (https://rclone.org/commands/rclone_sync/)
RCLONE_FLAGS="--progress --track-renames --no-update-modtime"
# --progress: Display progress.
# --track-renames: If a file exists but has only been renamed, record that on the destination.
# --no-update-modtime: Don't update the last-modified time if the file is identical.

touch /data/bdc/test.txt
rclone sync "/data/bdc/BioLINCC/" "lakefs:$LAKEFS_REPOSITORY/main/BioLINCC/" $RCLONE_FLAGS
rclone sync "/data/bdc/COVID19/" "lakefs:$LAKEFS_REPOSITORY/main/COVID19/" $RCLONE_FLAGS
rclone sync "/data/bdc/DIR/" "lakefs:$LAKEFS_REPOSITORY/main/DIR/" $RCLONE_FLAGS
rclone sync "/data/bdc/imaging/" "lakefs:$LAKEFS_REPOSITORY/main/imaging/" $RCLONE_FLAGS
rclone sync "/data/bdc/LungMAP/" "lakefs:$LAKEFS_REPOSITORY/main/LungMAP/" $RCLONE_FLAGS
rclone sync "/data/bdc/NSRR/" "lakefs:$LAKEFS_REPOSITORY/main/NSRR/" $RCLONE_FLAGS
rclone sync "/data/bdc/parent/" "lakefs:$LAKEFS_REPOSITORY/main/parent/" $RCLONE_FLAGS
rclone sync "/data/bdc/PCGC/" "lakefs:$LAKEFS_REPOSITORY/main/PCGC/" $RCLONE_FLAGS
rclone sync "/data/bdc/RECOVER/" "lakefs:$LAKEFS_REPOSITORY/main/RECOVER/" $RCLONE_FLAGS
rclone sync "/data/bdc/topmed/" "lakefs:$LAKEFS_REPOSITORY/main/topmed/" $RCLONE_FLAGS

# Report errors.
echo Downloads complete at `date`.
