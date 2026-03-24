#!/usr/bin/env python3
"""
Pass 2: For files present in both directories, produce a detailed report with JSON Path locations.
"""
import argparse
import json
import os
from pathlib import Path
import csv
from typing import Any, List, Tuple

def get_json_files(directory):
    """Get all JSON filenames from a directory."""
    return set(f for f in os.listdir(directory) if f.endswith('.json'))

def json_path_to_string(path):
    """Convert a JSON path list to a readable string."""
    result = "$"
    for item in path:
        if isinstance(item, int):
            result += f"[{item}]"
        else:
            result += f".{item}"
    return result

def get_context_from_path(data, path):
    """Get the parent context object from a path (one level up)."""
    if len(path) <= 1:
        # If path is at root or one level deep, return the full object
        return json.dumps(data, ensure_ascii=False, indent=2)[:500]

    # Navigate to parent (one level up from current path)
    parent_path = path[:-1]
    current = data
    try:
        for step in parent_path:
            current = current[step]
        return json.dumps(current, ensure_ascii=False, indent=2)[:500]
    except (KeyError, IndexError, TypeError):
        return '<context unavailable>'

def compare_values(val1, val2, path, differences, root1, root2):
    """Recursively compare two JSON values and collect differences."""
    current_path = json_path_to_string(path)

    # Check if types are different
    if type(val1) != type(val2):
        differences.append({
            'location': current_path,
            'difference_type': 'type_mismatch',
            'dir1_value': str(val1),
            'dir1_type': type(val1).__name__,
            'dir2_value': str(val2),
            'dir2_type': type(val2).__name__,
            'dir1_context': get_context_from_path(root1, path),
            'dir2_context': get_context_from_path(root2, path)
        })
        return

    # Handle dictionaries
    if isinstance(val1, dict):
        all_keys = set(val1.keys()) | set(val2.keys())
        for key in all_keys:
            if key not in val1:
                differences.append({
                    'location': f"{current_path}.{key}",
                    'difference_type': 'key_only_in_dir2',
                    'dir1_value': '<missing>',
                    'dir1_type': 'N/A',
                    'dir2_value': json.dumps(val2[key], ensure_ascii=False)[:200],
                    'dir2_type': type(val2[key]).__name__,
                    'dir1_context': get_context_from_path(root1, path + [key]),
                    'dir2_context': get_context_from_path(root2, path + [key])
                })
            elif key not in val2:
                differences.append({
                    'location': f"{current_path}.{key}",
                    'difference_type': 'key_only_in_dir1',
                    'dir1_value': json.dumps(val1[key], ensure_ascii=False)[:200],
                    'dir1_type': type(val1[key]).__name__,
                    'dir2_value': '<missing>',
                    'dir2_type': 'N/A',
                    'dir1_context': get_context_from_path(root1, path + [key]),
                    'dir2_context': get_context_from_path(root2, path + [key])
                })
            else:
                compare_values(val1[key], val2[key], path + [key], differences, root1, root2)

    # Handle lists
    elif isinstance(val1, list):
        if len(val1) != len(val2):
            differences.append({
                'location': current_path,
                'difference_type': 'list_length_mismatch',
                'dir1_value': f"length={len(val1)}",
                'dir1_type': 'list',
                'dir2_value': f"length={len(val2)}",
                'dir2_type': 'list',
                'dir1_context': get_context_from_path(root1, path),
                'dir2_context': get_context_from_path(root2, path)
            })
            # Continue to compare elements up to the minimum length

        for i in range(min(len(val1), len(val2))):
            compare_values(val1[i], val2[i], path + [i], differences, root1, root2)

        # Report extra items if lists have different lengths
        if len(val1) > len(val2):
            for i in range(len(val2), len(val1)):
                differences.append({
                    'location': f"{current_path}[{i}]",
                    'difference_type': 'item_only_in_dir1',
                    'dir1_value': json.dumps(val1[i], ensure_ascii=False)[:200],
                    'dir1_type': type(val1[i]).__name__,
                    'dir2_value': '<missing>',
                    'dir2_type': 'N/A',
                    'dir1_context': get_context_from_path(root1, path + [i]),
                    'dir2_context': get_context_from_path(root2, path + [i])
                })
        elif len(val2) > len(val1):
            for i in range(len(val1), len(val2)):
                differences.append({
                    'location': f"{current_path}[{i}]",
                    'difference_type': 'item_only_in_dir2',
                    'dir1_value': '<missing>',
                    'dir1_type': 'N/A',
                    'dir2_value': json.dumps(val2[i], ensure_ascii=False)[:200],
                    'dir2_type': type(val2[i]).__name__,
                    'dir1_context': get_context_from_path(root1, path + [i]),
                    'dir2_context': get_context_from_path(root2, path + [i])
                })

    # Handle primitive values
    else:
        if val1 != val2:
            differences.append({
                'location': current_path,
                'difference_type': 'value_mismatch',
                'dir1_value': json.dumps(val1, ensure_ascii=False)[:200] if not isinstance(val1, str) else val1[:200],
                'dir1_type': type(val1).__name__,
                'dir2_value': json.dumps(val2, ensure_ascii=False)[:200] if not isinstance(val2, str) else val2[:200],
                'dir2_type': type(val2).__name__,
                'dir1_context': get_context_from_path(root1, path),
                'dir2_context': get_context_from_path(root2, path)
            })

def compare_files(file1_path, file2_path):
    """Compare two JSON files and return list of differences."""
    with open(file1_path, 'r') as f:
        data1 = json.load(f)
    with open(file2_path, 'r') as f:
        data2 = json.load(f)

    differences = []
    compare_values(data1, data2, [], differences, data1, data2)
    return differences

def main():
    parser = argparse.ArgumentParser(description="Pass 2: Detailed JSON diff for files present in both directories.")
    parser.add_argument("dir1", help="First directory (e.g. cleaned dug-data-model)")
    parser.add_argument("dir2", help="Second directory (e.g. heal-cdes)")
    args = parser.parse_args()

    dir1 = Path(args.dir1)
    dir2 = Path(args.dir2)

    files1 = get_json_files(dir1)
    files2 = get_json_files(dir2)
    common_files = sorted(files1 & files2)

    print(f"=== Pass 2: Detailed File Comparison ===")
    print(f"Comparing {len(common_files)} files present in both directories...\n")

    all_differences = []
    files_with_differences = 0
    files_identical = 0

    for filename in common_files:
        file1_path = dir1 / filename
        file2_path = dir2 / filename

        differences = compare_files(file1_path, file2_path)

        if differences:
            files_with_differences += 1
            for diff in differences:
                all_differences.append({
                    'filename': filename,
                    **diff
                })
        else:
            files_identical += 1

    # Write detailed report
    report_path = "comparison_pass2_detailed_diff.csv"
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['filename', 'location', 'difference_type',
                      'dug_2026jan_value', 'dug_2026jan_type', 'heal_cdes_value', 'heal_cdes_type',
                      'dug_2026jan_context', 'heal_cdes_context']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Rename keys in all_differences to match new fieldnames
        for diff in all_differences:
            diff['dug_2026jan_value'] = diff.pop('dir1_value', '')
            diff['dug_2026jan_type'] = diff.pop('dir1_type', '')
            diff['heal_cdes_value'] = diff.pop('dir2_value', '')
            diff['heal_cdes_type'] = diff.pop('dir2_type', '')
            diff['dug_2026jan_context'] = diff.pop('dir1_context', '')
            diff['heal_cdes_context'] = diff.pop('dir2_context', '')

        writer.writerows(all_differences)

    print(f"Files identical: {files_identical}")
    print(f"Files with differences: {files_with_differences}")
    print(f"Total differences found: {len(all_differences)}")
    print(f"\nReport written to: {report_path}")

    # Show some examples
    if all_differences:
        print("\nExample differences:")
        for diff in all_differences[:10]:
            print(f"  {diff['filename']} @ {diff['location']}: {diff['difference_type']}")

if __name__ == "__main__":
    main()
