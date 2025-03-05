#!/bin/bash

# ingest.sh - Integrated pipeline for dbGaP data download and XML generation based on (variable level metadata) pic_sure and (study level meta data) gen3. 

# Usage: export PICSURE_TOKEN,LAKEFS_HOST,LAKEFS_USERNAME,LAKEFS_PASSWORD and LAKEFS_REPOSITORY   && ./ingest.sh [--output-dir DIR]

# Set defaults
OUTPUT_DIR="bdc-metadata-for-ingest"


# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --help) echo "Usage: export PICSURE_TOKEN=your_token && $0 [--output-dir DIR]"; exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Setup directories
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

# Step 1: PicSure data extraction
log "Extracting PicSure data..."
python get_bdc_studies_md_from_picsure.py --output-dir "$PICSURE_OUTPUT_PATH"

# Step 1.1 Find PicSure data file
sleep 1
PICSURE_DATA_FILE=$(find "$PICSURE_OUTPUT_PATH" -name "cleaned_pic_sure_data*.csv" -o -name "pic_sure_data*.csv" | sort -r | head -n 1)


# Step 2: Gen3 data extraction
log "Extracting Gen3 data..."
python get_bdc_studies_md_from_gen3.py --output-dir "$GEN3_OUTPUT_PATH"


# Step 2.1 Find Gen3 data file
sleep 1
GEN3_DATA_FILE=$(find "$GEN3_OUTPUT_PATH" -name "gen3_studies_filtered*.csv" | sort -r | head -n 1)


# Step 3: XML generation
log "Running dbGaP download with XML generation fallback..."
python run_dbgap_xml_gen_fallback.py --output-dir "$XML_OUTPUT_PATH" --gen3-csv "$GEN3_DATA_FILE" --picsure-csv "$PICSURE_DATA_FILE"

# Step 4: Upload to LakeFS
log "Uploading dbGaP XML files to LakeFS using Rclone..."
# Set up RClone environment variables
export RCLONE_CONFIG_LAKEFS_TYPE=s3
export RCLONE_CONFIG_LAKEFS_PROVIDER=Other
export RCLONE_CONFIG_LAKEFS_ENDPOINT="$LAKEFS_HOST"
export RCLONE_CONFIG_LAKEFS_ACCESS_KEY_ID="$LAKEFS_USERNAME"
export RCLONE_CONFIG_LAKEFS_SECRET_ACCESS_KEY="$LAKEFS_PASSWORD"
export RCLONE_CONFIG_LAKEFS_NO_CHECK_BUCKET=true

# Specify LakeFS repository
#LAKEFS_REPOSITORY="bdc-test6"

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
    -d "{\"message\": \"Updated BDC data dictionaries for $remote_subdir.\"}"
}

# Upload each program directory to the same path in LakeFS
log "Uploading program directories to LakeFS..."

# Find all program directories in XML output
for program_dir in "$XML_OUTPUT_PATH"/*; do
  if [ -d "$program_dir" ]; then
    program_name=$(basename "$program_dir")
    
    # Sync program directory to same-named directory in LakeFS repository
    sync_dir_to_lakefs "$program_dir" "$LAKEFS_REPOSITORY" "main" "$program_name"
    
    log "Uploaded $program_name to LakeFS"
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