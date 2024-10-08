#!/bin/sh

# Bash strict mode (minus IFS change, since we're not using arrays).
set -euo pipefail

# A script for ingesting data from HEAL Platform dbGaP XML files into LakeFS.
START_DATE=$(date)
echo Started ingest from HEAL Platform at ${START_DATE}.

# Step 1. Prepare directories.
echo Cleaning /data directory
rm -rf /data/*

mkdir -p /data/logs

# Step 2. Download the list of dbGaP IDs from BDC.
python heal/get_heal_platform_mds_data_dicts.py /data/heal 2>&1 | tee /data/logs/get_heal_platform_mds_data_dicts.log

# Step 3. Upload the files to BDC.
echo Uploading dbGaP XML files to LakeFS using Rclone.

# Set up RClone environment variables.
export RCLONE_CONFIG_LAKEFS_TYPE=s3
export RCLONE_CONFIG_LAKEFS_PROVIDER=Other
export RCLONE_CONFIG_LAKEFS_ENDPOINT="$LAKEFS_HOST"
export RCLONE_CONFIG_LAKEFS_ACCESS_KEY_ID="$LAKEFS_USERNAME"
export RCLONE_CONFIG_LAKEFS_SECRET_ACCESS_KEY="$LAKEFS_PASSWORD"
export RCLONE_CONFIG_LAKEFS_NO_CHECK_BUCKET=true

RCLONE_FLAGS="--progress --track-renames --no-update-modtime"

# Sync (https://rclone.org/commands/rclone_sync/)
# --track-renames: If a file exists but has only been renamed, record that on the destination.
# --no-update-modtime: Don't update the last-modified time if the file is identical.
rclone sync "/data/heal/dbGaPs/" "lakefs:$LAKEFS_REPOSITORY/main/" $RCLONE_FLAGS

# Step 4. Upload logs with RClone.
rclone sync "/data/logs/" "lakefs:$LAKEFS_REPOSITORY/main/logs/" $RCLONE_FLAGS

# Step 5. Commit these changes. We could do this via lakefs CLI, but it's easier to just do it via curl.
curl -X POST -u "$LAKEFS_USERNAME:$LAKEFS_PASSWORD" "$LAKEFS_HOST/api/v1/repositories/$LAKEFS_REPOSITORY/branches/main/commits" \
  -H "Content-Type: application/json" \
  -d "{\"message\": \"Updated HEAL data dictionaries starting at ${START_DATE}.\"}"

# Note success.
echo Downloads complete at `date`.
