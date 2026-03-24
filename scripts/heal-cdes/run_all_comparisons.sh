#!/bin/bash
# Run all three comparison passes in sequence

# Directory configuration — set these to control what gets compared.
# DDM2_INPUT_DIR: source dug-data-model-2 files (will be copied and cleaned into DIR1)
# DIR1: cleaned dug-data-model-2 output; cleared before each run so the cleaner repopulates it
# DIR2: heal-cdes directory to compare against
DDM2_INPUT_DIR="data/dug-data-model-2026jan20-copied"
DIR1="data/dug-data-model-2026jan20"
DIR2="data/heal-cdes"

echo "=========================================="
echo "Dug Data Model v2 Comparison Analysis"
echo "=========================================="
echo ""

# DIR1 is the cleaned output of the preprocessor; clear it first so the
# cleaner starts fresh rather than merging with stale files.
echo "Clearing $DIR1 before preprocessing..."
rm -rf "$DIR1"
echo ""

echo "Running Preprocessor: Cleaning DDM2 files..."
python3 clean-ddm2-files.py "$DDM2_INPUT_DIR" "$DIR1"
echo ""

echo "Running Pass 1: Missing Files..."
python3 compare_pass1_missing_files.py "$DIR1" "$DIR2"
echo ""

echo "Running Pass 2: Detailed Differences..."
python3 compare_pass2_detailed_diff.py "$DIR1" "$DIR2"
echo ""

echo "Running Pass 3: Common Patterns..."
python3 compare_pass3_common_patterns.py
echo ""

echo "=========================================="
echo "All comparisons complete!"
echo "=========================================="
echo ""
echo "Generated files:"
echo "  - comparison_pass1_missing_files.csv"
echo "  - comparison_pass2_detailed_diff.csv"
echo "  - comparison_pass3_common_patterns.csv"
echo "  - comparison_pass3_filtered_diff.csv"
echo ""
echo "See COMPARISON_SUMMARY.md for detailed analysis."
