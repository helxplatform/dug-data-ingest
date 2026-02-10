#!/bin/bash
# Run all three comparison passes in sequence

echo "=========================================="
echo "Dug Data Model v2 Comparison Analysis"
echo "=========================================="
echo ""

echo "Running Pass 1: Missing Files..."
python3 compare_pass1_missing_files.py
echo ""

echo "Running Pass 2: Detailed Differences..."
python3 compare_pass2_detailed_diff.py
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
