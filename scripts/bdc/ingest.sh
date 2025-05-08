#!/usr/bin/bash

# Bash strict mode (minus IFS change, since we're not using arrays).
set -euo pipefail

# A script for ingesting data from BDC into LakeFS.
START_DATE=$(date)
echo "Started ingest from BDC at ${START_DATE}."

DATAROOT="${1:-/data}"

# Step 1. Prepare directory.
echo Cleaning /data directory
rm -rf "${DATAROOT:?}"/*

echo Create log directory.
mkdir -p "$DATAROOT"/logs

echo Create bdc ouptut directory.
mkdir -p "$DATAROOT"/bdc

# Step 1. Download the list of dbGaP IDs from BDC.
python bdc/get_bdc_studies_from_gen3.py "$DATAROOT"/bdc_dbgap_ids.csv --kgx-file "$DATAROOT"/bdc/bdc_studies_kgx.json 2>&1 | tee "$DATAROOT"/logs/get_bdc_studies_from_gen3.txt

# Step 2. Download the dbGaP XML files from BDC.
mkdir -p "$DATAROOT"/bdc
python bdc/get_dbgap_data_dicts.py "$DATAROOT"/bdc_dbgap_ids.csv --format CSV --field "Accession" --outdir "$DATAROOT"/bdc --group-by Program 2>&1 | tee "$DATAROOT"/logs/get_dbgap_data_dicts.txt

# Step 3. Upload the dbGaP XML files to BDC.
echo Uploading dbGaP XML files to LakeFS using Rclone.

# Set up RClone environment variables.
export RCLONE_CONFIG_LAKEFS_TYPE=s3
export RCLONE_CONFIG_LAKEFS_PROVIDER=Other
export RCLONE_CONFIG_LAKEFS_ENDPOINT="$LAKEFS_HOST"
export RCLONE_CONFIG_LAKEFS_ACCESS_KEY_ID="$LAKEFS_USERNAME"
export RCLONE_CONFIG_LAKEFS_SECRET_ACCESS_KEY="$LAKEFS_PASSWORD"
export RCLONE_CONFIG_LAKEFS_NO_CHECK_BUCKET=true

# Sync (https://rclone.org/commands/rclone_sync/)
RCLONE_FLAGS="--progress --track-renames --no-update-modtime"
# --progress: Display progress.
# --track-renames: If a file exists but has only been renamed, record that on the destination.
# --no-update-modtime: Don't update the last-modified time if the file is identical.

# Combined code for:
# - Copying a local directory to a LakeFS repository.
# - Commiting that repository.
# It takes three arguments:
#   sync_dir_to_lakefs(local_dir, repo_name, branch_name, subdir)
sync_dir_to_lakefs() {
	local local_dir=$1
	local repo_name=$2
	local branch_name=$3
	local subdir=$4

	# Sync the local directory to the remote directory.
	rclone sync "$local_dir" "lakefs:$repo_name/$branch_name/$subdir" "$RCLONE_FLAGS"

	# Commit the sync.
	curl -X POST -u "$LAKEFS_USERNAME:$LAKEFS_PASSWORD" "$LAKEFS_HOST/api/v1/repositories/$repo_name/branches/$branch_name/commits" \
		-H "Content-Type: application/json" \
		-d "{\"message\": \"Updated BDC data dictionaries starting at ${START_DATE}.\"}"
}

# Actually sync all the directories.
sync_dir_to_lakefs "$DATAROOT/bdc/BioLINCC/" "biolincc" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/COVID19/" "covid19-dbgap" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/DIR/" "dir-dbgap" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/imaging/" "imaging" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/LungMAP/" "lungmap-dbgap" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/NSRR/" "nsrr-dbgap" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/parent/" "parent-dbgap" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/PCGC/" "pcgc-dbgap" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/RECOVER/" "recover-dbgap" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/topmed/" "topmed-gen3-dbgap" "main" ""
sync_dir_to_lakefs "$DATAROOT/bdc/bdc_studies_kgx.json" "bdc-studies-kgx" "main" ""

# Save logs into repository as well.
sync_dir_to_lakefs "/data/logs" "bdc-roger" "main" "ingest-logs"

# Report completion.
echo "Downloads complete at $(date)."
