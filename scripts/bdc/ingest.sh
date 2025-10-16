#!/bin/bash
set -uo pipefail  # Exit on undefined vars and pipe failures (but not on command errors)

# ingest.sh - Integrated pipeline for dbGaP data download and XML generation based on (variable level metadata) pic_sure and (study level meta data) gen3.

# Usage: export PICSURE_TOKEN,LAKEFS_HOST,LAKEFS_USERNAME,LAKEFS_PASSWORD and LAKEFS_REPOSITORY   && ./ingest.sh [--output-dir DIR]


log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Set defaults
START_DATE=$(date)
# Use absolute path for output directory
OUTPUT_DIR="/data/bdc_metadata_ingest"

# A script for ingesting data from BDC into LakeFS.
log "Started ingest from BDC at ${START_DATE}."

# Validate required environment variables
if [ -z "${PICSURE_TOKEN:-}" ]; then
  log "WARNING: PICSURE_TOKEN not set. PicSure extraction may fail."
fi

if [ -z "${LAKEFS_HOST:-}" ] || [ -z "${LAKEFS_USERNAME:-}" ] || [ -z "${LAKEFS_PASSWORD:-}" ]; then
  log "ERROR: LakeFS credentials not set. Required: LAKEFS_HOST, LAKEFS_USERNAME, LAKEFS_PASSWORD"
  exit 1
fi

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --help) echo "Usage: export PICSURE_TOKEN=your_token && $0 [--output-dir DIR]"; exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Setup directories
log "Creating output directories in: $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
PICSURE_OUTPUT_PATH="$OUTPUT_DIR/picsure_md"
GEN3_OUTPUT_PATH="$OUTPUT_DIR/gen3_md"
XML_OUTPUT_PATH="$OUTPUT_DIR/xml_output"
mkdir -p "$PICSURE_OUTPUT_PATH" "$GEN3_OUTPUT_PATH" "$XML_OUTPUT_PATH"

# Export paths for tools
export PICSURE_TOKEN
export PICSURE_OUTPUT_PATH
export GEN3_OUTPUT_PATH

log "Starting pipeline..."

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Step 1: PicSure data extraction
log "Extracting PicSure data..."
python "$SCRIPT_DIR/get_bdc_studies_md_from_picsure.py" --output-dir "$PICSURE_OUTPUT_PATH"

# Step 1.1 Find PicSure data file
sleep 1
PICSURE_DATA_FILE=$(find "$PICSURE_OUTPUT_PATH" \( -name "cleaned_pic_sure_data*.csv" -o -name "picsure_studies*.csv" \) 2>/dev/null | sort -r | head -n 1)
if [ -n "$PICSURE_DATA_FILE" ]; then
  log "PicSure data file found: $PICSURE_DATA_FILE"
else
  log "WARNING: No PicSure data file found in $PICSURE_OUTPUT_PATH"
  PICSURE_DATA_FILE=""
fi


# Step 2: Gen3 data extraction
log "Extracting Gen3 data..."
python "$SCRIPT_DIR/get_bdc_studies_md_from_gen3.py" --output-dir "$GEN3_OUTPUT_PATH"


# Step 2.1 Find Gen3 data file
sleep 1
GEN3_DATA_FILE=$(find "$GEN3_OUTPUT_PATH" -name "gen3_studies_filtered*.csv" 2>/dev/null | sort -r | head -n 1)
if [ -n "$GEN3_DATA_FILE" ]; then
  log "Gen3 data file found: $GEN3_DATA_FILE"
else
  log "ERROR: No Gen3 data file found in $GEN3_OUTPUT_PATH"
  exit 1
fi


# Step 3: XML generation
log "Running dbGaP download with XML generation fallback..."
python "$SCRIPT_DIR/run_dbgap_xml_gen_fallback.py" --output-dir "$XML_OUTPUT_PATH" --gen3-csv "$GEN3_DATA_FILE" --picsure-csv "$PICSURE_DATA_FILE" --always-generate



# Step 4: Upload to LakeFS
log "Uploading dbGaP XML files to LakeFS using Rclone..."
# Set up RClone environment variables
export RCLONE_CONFIG_LAKEFS_TYPE=s3
export RCLONE_CONFIG_LAKEFS_PROVIDER=Other
export RCLONE_CONFIG_LAKEFS_ENDPOINT="$LAKEFS_HOST"
export RCLONE_CONFIG_LAKEFS_ACCESS_KEY_ID="$LAKEFS_USERNAME"
export RCLONE_CONFIG_LAKEFS_SECRET_ACCESS_KEY="$LAKEFS_PASSWORD"
export RCLONE_CONFIG_LAKEFS_NO_CHECK_BUCKET=true

# Use LakeFS repository from env var or default
LAKEFS_REPOSITORY="${LAKEFS_REPOSITORY:-bdc-ingest-logs}"
log "Using LakeFS repository: $LAKEFS_REPOSITORY"

# Rclone flags
RCLONE_FLAGS="--progress --track-renames --no-update-modtime"

# Function to sync directory to LakeFS and commit
sync_dir_to_lakefs() {
  local local_dir=$1
  local repo_name=$2
  local branch_name=$3
  local remote_subdir=$4
  
  log "Syncing $local_dir to LakeFS repository $repo_name/$branch_name/$remote_subdir"
  
  # Sync the local directory to the remote directory
  rclone sync "$local_dir" "lakefs:$repo_name/$branch_name/$remote_subdir" $RCLONE_FLAGS
  
  # Commit the sync
  curl -X POST -u "$LAKEFS_USERNAME:$LAKEFS_PASSWORD" "$LAKEFS_HOST/api/v1/repositories/$repo_name/branches/$branch_name/commits" \
    -H "Content-Type: application/json" \
    -d "{\"message\": \"Updated BDC data dictionaries starting at ${START_DATE}.\"}"
}

# Upload each program directory to the same path in LakeFS
log "Uploading program directories to LakeFS..."
echo $XML_OUTPUT_PATH

# Define program directories and their corresponding repositories
declare -A PROGRAMS=(
  ["biolincc"]="bdc-biolincc"
  ["covid19"]="bdc-covid19"
  ["curesc"]="bdc-curesc"
  ["dir"]="bdc-dir"
  ["heartfailure"]="bdc-heartfailure"
  ["imaging"]="bdc-imaging"
  ["lungmap"]="bdc-lungmap"
  ["nsrr"]="bdc-nsrr"
  ["parent"]="bdc-parent"
  ["recover"]="bdc-recover"
  ["reds"]="bdc-reds"
  ["topmed"]="bdc-topmed"
)

# Sync only directories that exist
for program in "${!PROGRAMS[@]}"; do
  local_dir="$XML_OUTPUT_PATH/$program"
  if [ -d "$local_dir" ]; then
    log "Found directory: $local_dir"
    sync_dir_to_lakefs "$local_dir" "${PROGRAMS[$program]}" "main" ""
  else
    log "Skipping $program: directory $local_dir does not exist"
  fi
done


# Upload specific XML processing logs directly to LakeFS
log "Uploading specific XML processing logs to LakeFS..."

# Log files for xml generation is upload it directly to ingest-logs
for log_file in "process.log" "processing_summary.csv" "processing_summary.txt"; do
  if [ -f "$XML_OUTPUT_PATH/$log_file" ]; then
    rclone copy "$XML_OUTPUT_PATH/$log_file" "lakefs:$LAKEFS_REPOSITORY/main/ingest-logs/" $RCLONE_FLAGS
    log "Uploaded $log_file to ingest-logs"
  fi
done

# Upload picsure_md and gen3_md directories
log "Uploading metadata directories to LakeFS..."
sync_dir_to_lakefs "$PICSURE_OUTPUT_PATH" "$LAKEFS_REPOSITORY" "main" "ingest-logs/picsure_md"
sync_dir_to_lakefs "$GEN3_OUTPUT_PATH" "$LAKEFS_REPOSITORY" "main" "ingest-logs/gen3_md"

log "LakeFS upload completed at $(date)"


# Done
log "Pipeline completed successfully"


exit 0