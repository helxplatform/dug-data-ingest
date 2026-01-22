#!/bin/bash
#
# BDC Data Ingestion Pipeline
#
# This script orchestrates the complete BDC data processing pipeline:
# 1. Fetch studies from BDC and Gen3 portals
# 2. Merge and clean the data
# 3. Update program names from Jira data
# 4. Generate all reports and output files
#
# Usage:
#   ./run_pipeline.sh [OPTIONS]
#
# Options:
#   -o, --output-dir DIR      Output directory (required)
#   -j, --jira-file FILE      Jira data file path (required for step 2)
#   -s, --step STEP           Run specific step only (1 or 2)
#   --bdc-url URL             BDC API URL (optional)
#   --gen3-url URL            Gen3 API URL (optional)
#   -h, --help                Show this help message
#
# Examples:
#   # Run full pipeline
#   ./run_pipeline.sh -o /path/to/output -j /path/to/jira.csv
#
#   # Run step 1 only (fetch studies)
#   ./run_pipeline.sh -o /path/to/output -s 1
#
#   # Run step 2 only (update programs)
#   ./run_pipeline.sh -o /path/to/output -j /path/to/jira.csv -s 2
#

set -e  # Exit on error
set -u  # Exit on undefined variable

# Default values
OUTPUT_DIR=""
JIRA_FILE=""
STEP="all"
#BDC_URL="https://search-dev.biodatacatalyst.renci.org/search-api"
BDC_URL="https://search.biodatacatalyst.renci.org/search-api"
GEN3_URL="https://gen3.biodatacatalyst.nhlbi.nih.gov/"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_header() {
    echo -e "${BLUE}======================================================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}======================================================================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "$1"
}

show_help() {
    sed -n '2,27p' "$0" | sed 's/^# //' | sed 's/^#//'
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -j|--jira-file)
            JIRA_FILE="$2"
            shift 2
            ;;
        -s|--step)
            STEP="$2"
            shift 2
            ;;
        --bdc-url)
            BDC_URL="$2"
            shift 2
            ;;
        --gen3-url)
            GEN3_URL="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$OUTPUT_DIR" ]]; then
    print_error "Output directory is required (-o or --output-dir)"
    exit 1
fi

if [[ "$STEP" == "all" || "$STEP" == "2" ]]; then
    if [[ -z "$JIRA_FILE" ]]; then
        print_error "Jira file is required for step 2 (-j or --jira-file)"
        exit 1
    fi
    if [[ ! -f "$JIRA_FILE" ]]; then
        print_error "Jira file not found: $JIRA_FILE"
        exit 1
    fi
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Print configuration
print_header "BDC Data Ingestion Pipeline"
print_info "Configuration:"
print_info "  Output directory: $OUTPUT_DIR"
print_info "  BDC URL: $BDC_URL"
print_info "  Gen3 URL: $GEN3_URL"
if [[ -n "$JIRA_FILE" ]]; then
    print_info "  Jira file: $JIRA_FILE"
fi
if [[ "$STEP" != "all" ]]; then
    print_info "  Step: $STEP"
fi
echo ""

# Step 1: Fetch studies from BDC and Gen3
if [[ "$STEP" == "all" || "$STEP" == "1" ]]; then
    print_header "STEP 1: Fetching Studies from BDC and Gen3"

    if python3 "$SCRIPT_DIR/fetch_studies.py" \
        --output-dir "$OUTPUT_DIR" \
        --bdc-url "$BDC_URL" \
        --gen3-url "$GEN3_URL"; then
        print_success "Step 1 completed successfully"
        echo ""
    else
        print_error "Step 1 failed"
        exit 1
    fi
fi

# Step 2: Update program names from Jira
if [[ "$STEP" == "all" || "$STEP" == "2" ]]; then
    print_header "STEP 2: Updating Program Names from Jira"

    # Find the input file from step 1 (now in program_table directory)
    INPUT_FILE="$OUTPUT_DIR/program_table/program_table.json"

    if [[ ! -f "$INPUT_FILE" ]]; then
        print_error "Input file not found: $INPUT_FILE"
        print_error "Please run step 1 first or provide the correct input file"
        exit 1
    fi

    if python3 "$SCRIPT_DIR/update_programs.py" \
        --jira-file "$JIRA_FILE" \
        --input-file "$INPUT_FILE" \
        --output-dir "$OUTPUT_DIR"; then
        print_success "Step 2 completed successfully"
        echo ""
    else
        print_error "Step 2 failed"
        exit 1
    fi
fi

# Final summary
print_header "Pipeline Completed Successfully!"
print_info ""
print_info "Generated files can be found in:"
print_info "  $OUTPUT_DIR"
print_info ""
print_info "Key output files:"
if [[ "$STEP" == "all" || "$STEP" == "1" ]]; then
    print_info "  Step 1 (Fetch):"
    print_info "    - Program table: $OUTPUT_DIR/program_table/program_table.json"
    print_info "    - Reference files: $OUTPUT_DIR/studies_on_bdc_portal/bdc_studies.json"
    print_info "                      $OUTPUT_DIR/studies_on_gen3_portal/raw_studies_on_gen3.json"
fi
if [[ "$STEP" == "all" || "$STEP" == "2" ]]; then
    print_info "  Step 2 (Update):"
    print_info "    - Program table (final): $OUTPUT_DIR/program_table/program_table_updated_*_minified.json"
    print_info "    - Program table (readable): $OUTPUT_DIR/program_table/program_table_updated_*.json"
    print_info "    - Reports: $OUTPUT_DIR/studies_on_gen3_portal/*_report_*.{json,log}"
    print_info "    - Missing NHLBI program names: $OUTPUT_DIR/studies_on_gen3_portal/studies_in_gen3_and_jira_missing_NHLBI_program_name_*.json"
fi
print_info ""
print_success "All done!"
