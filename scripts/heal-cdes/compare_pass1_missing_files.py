#!/usr/bin/env python3
"""
Pass 1: Report on files missing in one directory or the other.
"""
import os
from pathlib import Path
import csv

def get_json_files(directory):
    """Get all JSON filenames from a directory."""
    return set(f for f in os.listdir(directory) if f.endswith('.json'))

def main():
    dir1 = Path("data/dug-data-model-2026jan20")
    dir2 = Path("data/heal-cdes")

    files1 = get_json_files(dir1)
    files2 = get_json_files(dir2)

    # Files only in dir1
    only_in_dir1 = sorted(files1 - files2)
    # Files only in dir2
    only_in_dir2 = sorted(files2 - files1)
    # Files in both
    in_both = sorted(files1 & files2)

    # Write report
    report_path = "comparison_pass1_missing_files.csv"
    with open(report_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Status', 'Filename', 'Directory'])

        for filename in only_in_dir1:
            writer.writerow(['Only in dug-data-model-2026jan20', filename, str(dir1)])

        for filename in only_in_dir2:
            writer.writerow(['Only in heal-cdes', filename, str(dir2)])

    # Print summary
    print(f"=== Pass 1: Missing Files Report ===")
    print(f"\nTotal files in {dir1}: {len(files1)}")
    print(f"Total files in {dir2}: {len(files2)}")
    print(f"Files in both directories: {len(in_both)}")
    print(f"\nFiles only in {dir1}: {len(only_in_dir1)}")
    if only_in_dir1:
        print(f"  Examples: {', '.join(list(only_in_dir1)[:5])}")
    print(f"\nFiles only in {dir2}: {len(only_in_dir2)}")
    if only_in_dir2:
        print(f"  Examples: {', '.join(list(only_in_dir2)[:5])}")
    print(f"\nReport written to: {report_path}")

if __name__ == "__main__":
    main()
