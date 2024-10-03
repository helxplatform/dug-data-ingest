#!/usr/bin/bash

# Bash strict mode (minus IFS change, since we're not using arrays).
set -euo pipefail

# A script for ingesting data from BDC into LakeFS.
START_DATE=$(date)
echo "Started ingest from BDC at ${START_DATE}."

# Step 1. Prepare directory.
echo Cleaning /data directory
rm -rf /data/*

echo Create log directory.
mkdir -p /data/logs

# Step 1. Download the list of dbGaP IDs from BDC.
python bdc/get_bdc_studies_from_gen3.py /data/bdc_dbgap_ids.csv 2>&1 | tee /data/logs/get_bdc_studies_from_gen3.txt

# Step 2. Download the dbGaP XML files from BDC.
mkdir -p /data/bdc
python bdc/get_dbgap_data_dicts.py /data/bdc_dbgap_ids.csv --format CSV --field "Accession" --outdir /data/bdc --group-by Program 2>&1 | tee /data/logs/get_dbgap_data_dicts.txt

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
rclone sync "/data/bdc/BioLINCC/" "lakefs:biolincc/main/" $RCLONE_FLAGS
rclone sync "/data/bdc/COVID19/" "lakefs:covid19-dbgap/main/" $RCLONE_FLAGS
rclone sync "/data/bdc/DIR/" "lakefs:dir-dbgap/main/" $RCLONE_FLAGS
rclone sync "/data/bdc/imaging/" "lakefs:imaging/main/" $RCLONE_FLAGS # TODO: repo not present
rclone sync "/data/bdc/LungMAP/" "lakefs:lungmap-dbgap/main/" $RCLONE_FLAGS
rclone sync "/data/bdc/NSRR/" "lakefs:nsrr-dbgap/main/" $RCLONE_FLAGS
rclone sync "/data/bdc/parent/" "lakefs:parent-dbgap/main/" $RCLONE_FLAGS
rclone sync "/data/bdc/PCGC/" "lakefs:pcgc-dbgap/main/" $RCLONE_FLAGS
rclone sync "/data/bdc/RECOVER/" "lakefs:recover-dbgap/main/" $RCLONE_FLAGS
rclone sync "/data/bdc/topmed/" "lakefs:topmed-gen3-dbgap/main/" $RCLONE_FLAGS

# Save logs into repository as well.
rclone sync "/data/logs" "lakefs:bdc-gen3-import/main/logs/" $RCLONE_FLAGS # TODO: repo not present

# Step 4. Commit these changes. We could do this via lakefs CLI, but it's easier to just do it via curl.
curl -X POST -u "$LAKEFS_USERNAME:$LAKEFS_PASSWORD" "$LAKEFS_HOST/api/v1/repositories/$LAKEFS_REPOSITORY/branches/main/commits" \
  -H "Content-Type: application/json" \
  -d "{\"message\": \"Updated BDC data dictionaries starting at ${START_DATE}.\"}"

# Report completion.
echo Downloads complete at `date`.
