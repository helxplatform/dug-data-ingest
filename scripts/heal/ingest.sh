#!/bin/bash

# Bash strict mode
set -euo pipefail
PYTHON=${PYTHON:-python3}

if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "ERROR: Python interpreter '$PYTHON' was not found."
    echo "Use PYTHON=python3 ./heal/ingest.sh or unset PYTHON to use the default python3 interpreter."
    exit 1
fi

# CONFIGURATION
DATA_DIR=${HEAL_INGEST_DATA_DIR:-/data}
SCRIPT_DIR=${HEAL_INGEST_SCRIPT_DIR:-heal}
CDE_LAKEFS_LOCATION=${HEAL_CDE_LAKEFS_LOCATION:-lakefs://heal-cdes/main/}
LAKEFS_OUTPUT=${HEAL_LAKEFS_OUTPUT:-lakefs://heal-mds-studies/test/}

# Set up LakeFS environment variables for lakefs_spec
export LAKEFS_SERVER_ENDPOINT_URL="$LAKEFS_HOST"
export LAKEFS_ACCESS_KEY_ID="$LAKEFS_USERNAME"
export LAKEFS_SECRET_ACCESS_KEY="$LAKEFS_PASSWORD"

# Also set LAKECTL format for lakectl CLI compatibility
export LAKECTL_SERVER_ENDPOINT_URL="$LAKEFS_HOST"
export LAKECTL_CREDENTIALS_ACCESS_KEY_ID="$LAKEFS_USERNAME"
export LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY="$LAKEFS_PASSWORD"

mkdir -p $DATA_DIR/logs

# Start from an empty output directory: it lives on a persistent volume, so files from earlier runs
# (including studies that no longer exist) would otherwise pile up there. lakeFS is unaffected, since
# only files written by this run are uploaded.
rm -rf "${DATA_DIR:?}/heal/heal_studies"

# Generate and upload HEAL MDS study files
echo "Generating HEAL MDS study files from Platform MDS at $(date)"
$PYTHON $SCRIPT_DIR/get_heal_studies.py $DATA_DIR/heal/heal_studies \
        --cde-lakefs-location "$CDE_LAKEFS_LOCATION" \
        --lakefs-output "$LAKEFS_OUTPUT" \
        2>&1 | tee $DATA_DIR/logs/get_heal_studies.txt
# With pipefail (set above), a failed run exits here with get_heal_studies.py's status. Don't grep
# the log for "ERROR" instead: the script logs non-fatal per-study problems at ERROR level, and by
# the time it exits the lakeFS merge and the Slack summary have already happened.

echo "Ingestion complete at $(date)"
