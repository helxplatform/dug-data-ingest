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
echo Uploading dbGaP XML files to LakeFS using Rclone.

# Set up RClone environment variables.
export RCLONE_CONFIG_LAKEFS_TYPE=s3
export RCLONE_CONFIG_LAKEFS_PROVIDER=Other
export RCLONE_CONFIG_LAKEFS_ENDPOINT="$LAKEFS_HOST"
export RCLONE_CONFIG_LAKEFS_ACCESS_KEY_ID="$LAKEFS_USERNAME"
export RCLONE_CONFIG_LAKEFS_SECRET_ACCESS_KEY="$LAKEFS_PASSWORD"
export RCLONE_CONFIG_LAKEFS_NO_CHECK_BUCKET=true

# Sync.
rclone sync "/data/heal/dbGaPs/" "lakefs:$LAKEFS_REPOSITORY/main/" --progress --track-renames

# Report errors.
echo Downloads complete at `date`.
