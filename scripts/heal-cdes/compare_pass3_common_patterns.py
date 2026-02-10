#!/usr/bin/env python3
"""
Pass 3: Identify common differences across files and filter them out.
"""
import csv
from collections import defaultdict
from pathlib import Path
import re

def normalize_location(location):
    """
    Normalize a JSON path by replacing array indices with wildcards.
    e.g., $[0].metadata.permissible_values -> $[*].metadata.permissible_values
    """
    # Replace array indices [N] with [*]
    normalized = re.sub(r'\[\d+\]', '[*]', location)
    return normalized

def generalize_location(location):
    """
    Create a more general pattern by removing specific object keys at the end.
    e.g., $[*].metadata.permissible_values.1 -> $[*].metadata.permissible_values.*
    """
    parts = location.split('.')
    if len(parts) > 1:
        # Check if the last part looks like a specific value (number or short string)
        last_part = parts[-1]
        if last_part.isdigit() or (len(last_part) < 10 and not last_part.startswith('[')):
            return '.'.join(parts[:-1]) + '.*'
    return location

def load_pass2_data():
    """Load the detailed differences from pass 2."""
    differences = []
    with open('comparison_pass2_detailed_diff.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            differences.append(row)
    return differences

def identify_common_patterns(differences, total_files):
    """
    Identify patterns that appear frequently across files.
    Returns a dict of pattern -> list of files
    """
    # Pattern = (normalized_location, difference_type)
    pattern_to_files = defaultdict(set)
    pattern_to_examples = defaultdict(list)

    for diff in differences:
        normalized_loc = normalize_location(diff['location'])
        generalized_loc = generalize_location(normalized_loc)

        # Create patterns at different levels of specificity
        patterns = [
            (normalized_loc, diff['difference_type']),
            (generalized_loc, diff['difference_type'])
        ]

        for pattern in patterns:
            pattern_to_files[pattern].add(diff['filename'])
            if len(pattern_to_examples[pattern]) < 3:  # Keep a few examples
                pattern_to_examples[pattern].append({
                    'filename': diff['filename'],
                    'location': diff['location'],
                    'dir1_value': diff['dir1_value'],
                    'dir2_value': diff['dir2_value']
                })

    # Calculate percentages and create report
    common_patterns = []
    for pattern, files in pattern_to_files.items():
        location, diff_type = pattern
        percentage = (len(files) / total_files) * 100
        common_patterns.append({
            'location_pattern': location,
            'difference_type': diff_type,
            'num_files': len(files),
            'percentage': f"{percentage:.1f}%",
            'example_filename': pattern_to_examples[pattern][0]['filename'] if pattern_to_examples[pattern] else '',
            'example_location': pattern_to_examples[pattern][0]['location'] if pattern_to_examples[pattern] else '',
            'example_dir1_value': pattern_to_examples[pattern][0]['dir1_value'][:100] if pattern_to_examples[pattern] else '',
            'example_dir2_value': pattern_to_examples[pattern][0]['dir2_value'][:100] if pattern_to_examples[pattern] else ''
        })

    # Sort by number of files (descending)
    common_patterns.sort(key=lambda x: x['num_files'], reverse=True)

    return common_patterns, pattern_to_files

def should_filter_difference(diff, common_patterns_set, min_percentage=20):
    """
    Check if a difference should be filtered out based on common patterns.
    A pattern is considered "common" if it appears in >= min_percentage of files.
    """
    normalized_loc = normalize_location(diff['location'])
    generalized_loc = generalize_location(normalized_loc)

    patterns = [
        (normalized_loc, diff['difference_type']),
        (generalized_loc, diff['difference_type'])
    ]

    for pattern in patterns:
        if pattern in common_patterns_set:
            return True

    return False

def main():
    print("=== Pass 3: Common Pattern Analysis ===\n")

    # Load pass 2 data
    differences = load_pass2_data()
    print(f"Loaded {len(differences)} differences from pass 2")

    # Count unique files
    unique_files = set(diff['filename'] for diff in differences)
    total_files = len(unique_files)
    print(f"Across {total_files} files\n")

    # Identify common patterns
    common_patterns, pattern_to_files = identify_common_patterns(differences, total_files)

    # Write common patterns report
    common_report_path = "comparison_pass3_common_patterns.csv"
    with open(common_report_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['location_pattern', 'difference_type', 'num_files', 'percentage',
                      'example_filename', 'example_location', 'example_dir1_value', 'example_dir2_value']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(common_patterns)

    print(f"Common patterns report written to: {common_report_path}")
    print(f"Total patterns identified: {len(common_patterns)}")

    # Show top patterns
    print("\nTop 15 most common patterns:")
    for i, pattern in enumerate(common_patterns[:15], 1):
        print(f"  {i}. {pattern['location_pattern']} ({pattern['difference_type']}) - "
              f"{pattern['num_files']} files ({pattern['percentage']})")

    # Filter out common patterns (>= 20% of files)
    MIN_PERCENTAGE = 20
    common_patterns_set = set(
        (p['location_pattern'], p['difference_type'])
        for p in common_patterns
        if int(p['num_files']) >= (MIN_PERCENTAGE * total_files / 100)
    )

    print(f"\nFiltering out {len(common_patterns_set)} patterns that appear in >= {MIN_PERCENTAGE}% of files")

    filtered_differences = [
        diff for diff in differences
        if not should_filter_difference(diff, common_patterns_set, MIN_PERCENTAGE)
    ]

    # Write filtered report
    filtered_report_path = "comparison_pass3_filtered_diff.csv"
    with open(filtered_report_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['filename', 'location', 'difference_type',
                      'dir1_value', 'dir1_type', 'dir2_value', 'dir2_type']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered_differences)

    print(f"Filtered differences report written to: {filtered_report_path}")
    print(f"Differences after filtering: {len(filtered_differences)} (removed {len(differences) - len(filtered_differences)})")

    # Show files with remaining differences
    files_with_filtered_diffs = set(diff['filename'] for diff in filtered_differences)
    print(f"\nFiles with non-common differences: {len(files_with_filtered_diffs)}")
    if files_with_filtered_diffs:
        print(f"Examples: {', '.join(sorted(list(files_with_filtered_diffs))[:10])}")

if __name__ == "__main__":
    main()
