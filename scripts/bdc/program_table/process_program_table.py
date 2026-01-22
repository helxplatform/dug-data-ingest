#!/usr/bin/env python3
"""
===============================================================================
BDC Program Table Processor
===============================================================================

This script combines two processing steps for the BDC program table:
  1. Set "Extramural Research" program for studies with missing descriptions
  2. Expand the table by mapping communities to programs and creating duplicates

===============================================================================
INPUT
===============================================================================
A JSON file containing an array of study records with the following fields:
  - Accession: Study accession ID (e.g., "phs000123.v1.p1")
  - Study Name: Name of the study
  - Program: Current program assignment (may be empty)
  - Description: Program description (may be empty)
  - Community: Primary community classification
  - Community1: Secondary community classification (optional)
  - Community2: Tertiary community classification (optional)
  - Released: Release status (will be removed from output)
  - Subjects Count: Number of subjects (will be removed from output)
  - DOI Tombstone: DOI tombstone info (will be removed from output)

Example input record:
{
  "Accession": "phs000123.v1.p1",
  "Study Name": "Example Study",
  "Program": "",
  "Description": "",
  "Community": "TOPMed",
  "Community1": "CONNECTS",
  "Community2": ""
}

===============================================================================
OUTPUT
===============================================================================
1. program_table_final_YYYYMMDD_HHMMSS.json
   - Formatted JSON with processed records
   - Easy to read and inspect

2. program_table_final_YYYYMMDD_HHMMSS.min.json
   - Minified JSON for production upload
   - Smaller file size

3. program_table_processing_YYYYMMDD_HHMMSS.log
   - Detailed log of all processing steps and changes

4. program_table_report_YYYYMMDD_HHMMSS.txt
   - Comprehensive report with statistics and change details

===============================================================================
PROCESSING STEPS
===============================================================================

STEP 1: SET EXTRAMURAL RESEARCH FOR MISSING DESCRIPTIONS
---------------------------------------------------------
For studies with empty or missing descriptions:
  - Set Program = "Extramural Research"
  - Set Description = "Various HLBS"

Excluded from processing:
  - Studies with Program = "TOPMed_Common_Exchange_Area"
  - Studies with no valid Study Name

STEP 2: EXPAND TABLE BY COMMUNITY MAPPING
---------------------------------------------------------
For studies with Program = "Extramural Research":
  - Replace Program with the Community value (if available)
  - Update Description based on the new Program

For ALL studies with communities that don't match their Program:
  - Create duplicate records for each non-matching community
  - Set the duplicate's Program to the community value
  - Update Description based on the community

===============================================================================
PROGRAM TO DESCRIPTION MAPPING
===============================================================================
Program Name                                         -> Description
---------------------------------------------------------------------------
Bench to Bassinet                                    -> Pediatric Cardiovascular
BioLINCC                                             -> Various HLBS
C4R                                                  -> COVID-19 & Various HLBS
CONNECTS                                             -> COVID-19
Extramural Research                                  -> Various HLBS
Heartshare                                           -> Cardiovascular
Longitudinal Epidemiology Observational Study (LEOS)-> Longitudinal Observational
LungMAP                                              -> Pulmonary
NHLBI Intramural Research                            -> Various HLBS
National Sleep Research Resource (NSRR)              -> Sleep and Circadian Rhythms
PETAL Network                                        -> Pulmonary & COVID-19
Pediatric Heart Network (PHN)                        -> Pediatric Cardiovascular
RECOVER                                              -> Long COVID & PASC
Sickle Cell Disease                                  -> Sickle Cell Disease
TOPMed                                               -> Precision Medicine
Training                                             -> Open-Access; Training
(Any other program)                                  -> Various HLBS (default)

===============================================================================
USAGE
===============================================================================
python process_program_table.py --input-file path/to/program_table.json
python process_program_table.py --input-file path/to/program_table.json --output-dir path/to/output

===============================================================================
"""

import json
import argparse
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple
from collections import defaultdict


# =============================================================================
# CONSTANTS
# =============================================================================

# Program to Description mapping - used when assigning descriptions to programs
PROGRAM_DESCRIPTIONS = {
    "Bench to Bassinet": "Pediatric Cardiovascular",
    "BioLINCC": "Various HLBS",
    "C4R": "COVID-19 & Various HLBS",
    "CONNECTS": "COVID-19",
    "Extramural Research": "Various HLBS",
    "Heartshare": "Cardiovascular",
    "Longitudinal Epidemiology Observational Study (LEOS)": "Longitudinal Observational",
    "LungMAP": "Pulmonary",
    "NHLBI Intramural Research": "Various HLBS",
    "National Sleep Research Resource (NSRR)": "Sleep and Circadian Rhythms",
    "PETAL Network": "Pulmonary & COVID-19",
    "Pediatric Heart Network (PHN)": "Pediatric Cardiovascular",
    "RECOVER": "Long COVID & PASC",
    "Sickle Cell Disease": "Sickle Cell Disease",
    "TOPMed": "Precision Medicine",
    "Training": "Open-Access; Training",
}

# Fields to remove from final output (not needed for upload)
FIELDS_TO_REMOVE = ['Released', 'Subjects Count', 'DOI Tombstone']

# Programs to exclude from processing
EXCLUDED_PROGRAMS = ['TOPMed_Common_Exchange_Area']


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_description_for_program(program: str) -> str:
    """
    Get the appropriate Description for a given Program.

    Args:
        program: The program name

    Returns:
        The description for the program, or "Various HLBS" as default
    """
    return PROGRAM_DESCRIPTIONS.get(program, "Various HLBS")


def load_json(file_path: str) -> List[Dict[str, Any]]:
    """
    Load JSON file containing study records.

    Args:
        file_path: Path to the JSON file

    Returns:
        List of study dictionaries

    Raises:
        FileNotFoundError: If the file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
    """
    with open(file_path, 'r') as f:
        return json.load(f)


def save_json(data: List[Dict[str, Any]], file_path: Path, minify: bool = False) -> None:
    """
    Save data to JSON file, optionally removing specified fields.

    Args:
        data: List of study dictionaries to save
        file_path: Output file path
        minify: If True, save as minified JSON (no indentation)
    """
    # Remove specified fields before saving
    cleaned_data = []
    for study in data:
        cleaned_study = {k: v for k, v in study.items() if k not in FIELDS_TO_REMOVE}
        cleaned_data.append(cleaned_study)

    with open(file_path, 'w') as f:
        if minify:
            json.dump(cleaned_data, f, separators=(',', ':'))
        else:
            json.dump(cleaned_data, f, indent=2)


def count_studies_by_program(studies: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Count studies by program for statistics.

    Args:
        studies: List of study dictionaries

    Returns:
        Dictionary mapping program names to study counts
    """
    program_counts = defaultdict(int)
    for study in studies:
        program = study.get('Program', '(empty)')
        program_counts[program] += 1
    return dict(program_counts)


# =============================================================================
# STEP 1: SET EXTRAMURAL RESEARCH FOR MISSING DESCRIPTIONS
# =============================================================================

def step1_set_extramural_for_missing_descriptions(
    studies: List[Dict[str, Any]],
    log_func
) -> Tuple[List[Dict[str, Any]], List[Dict], List[Dict]]:
    """
    STEP 1: Set Program to "Extramural Research" for studies with empty descriptions.

    This step processes all studies and:
    - Identifies studies with empty or missing descriptions
    - Sets their Program to "Extramural Research"
    - Sets their Description to "Various HLBS"
    - Excludes studies from TOPMed_Common_Exchange_Area
    - Excludes studies with no valid Study Name

    Args:
        studies: List of study dictionaries from input file
        log_func: Function to write log messages

    Returns:
        Tuple of:
        - processed_studies: Studies after processing
        - changed_studies: List of studies that were changed
        - excluded_studies: List of studies that were excluded
    """
    log_func("=" * 100)
    log_func("STEP 1: SET EXTRAMURAL RESEARCH FOR MISSING DESCRIPTIONS")
    log_func("=" * 100)
    log_func("")

    processed_studies = []
    changed_studies = []
    excluded_studies = []
    skipped_no_name = []

    for study in studies:
        study_copy = study.copy()

        # Skip studies with no valid name
        study_name = study_copy.get('Study Name', '').strip()
        if not study_name or study_name == '(no name)':
            skipped_no_name.append({
                'Accession': study_copy.get('Accession', ''),
                'Study Name': study_name if study_name else '(empty)',
                'Reason': 'No valid study name'
            })
            continue

        # Get program name
        program = study_copy.get('Program', '').strip()

        # Exclude studies from specified programs
        if program in EXCLUDED_PROGRAMS:
            excluded_studies.append({
                'Accession': study_copy.get('Accession', ''),
                'Study Name': study_copy.get('Study Name', ''),
                'Program': program,
                'Description': study_copy.get('Description', ''),
                'Reason': f'Excluded program: {program}'
            })
            log_func(f"[EXCLUDED] {study_copy.get('Accession', '')} - Program: {program}")
            continue

        # Check if description is empty or missing
        description = study_copy.get('Description', '').strip()

        if not description:
            # Record original values
            original_program = program

            # Update to "Extramural Research" with description "Various HLBS"
            study_copy['Program'] = 'Extramural Research'
            study_copy['Description'] = 'Various HLBS'

            # Record the change
            changed_studies.append({
                'Accession': study_copy.get('Accession', ''),
                'Study Name': study_copy.get('Study Name', ''),
                'Original Program': original_program,
                'New Program': 'Extramural Research',
                'Original Description': '(empty)',
                'New Description': 'Various HLBS'
            })
            log_func(f"[CHANGED] {study_copy.get('Accession', '')} - Program: '{original_program}' -> 'Extramural Research'")

        processed_studies.append(study_copy)

    # Log skipped studies
    if skipped_no_name:
        log_func("")
        log_func(f"Skipped {len(skipped_no_name)} studies with no valid name")
        for s in skipped_no_name[:5]:  # Show first 5
            log_func(f"  - {s['Accession']}: {s['Study Name']}")
        if len(skipped_no_name) > 5:
            log_func(f"  ... and {len(skipped_no_name) - 5} more")

    log_func("")
    log_func(f"Step 1 Summary:")
    log_func(f"  - Studies processed: {len(processed_studies)}")
    log_func(f"  - Studies changed to Extramural Research: {len(changed_studies)}")
    log_func(f"  - Studies excluded: {len(excluded_studies)}")
    log_func(f"  - Studies skipped (no name): {len(skipped_no_name)}")
    log_func("")

    return processed_studies, changed_studies, excluded_studies


# =============================================================================
# STEP 2: EXPAND TABLE BY COMMUNITY MAPPING
# =============================================================================

def step2_expand_by_community(
    studies: List[Dict[str, Any]],
    log_func
) -> Tuple[List[Dict[str, Any]], List[Dict], List[Dict]]:
    """
    STEP 2: Expand table by mapping communities to programs.

    This step processes all studies and:
    - Replaces "Extramural Research" with Community value when available
    - Updates Description based on the new Program
    - Creates duplicate records for communities that don't match the Program

    Args:
        studies: List of study dictionaries from Step 1
        log_func: Function to write log messages

    Returns:
        Tuple of:
        - expanded_studies: Studies after expansion (may have more records)
        - extramural_replacements: List of Extramural Research replacements
        - duplicates_created: List of duplicate records created
    """
    log_func("=" * 100)
    log_func("STEP 2: EXPAND TABLE BY COMMUNITY MAPPING")
    log_func("=" * 100)
    log_func("")

    expanded_studies = []
    extramural_replacements = []
    duplicates_created = []

    for study in studies:
        program = study.get('Program', '').strip()
        description = study.get('Description', '').strip()

        # Get all communities
        community = study.get('Community', '').strip()
        community1 = study.get('Community1', '').strip()
        community2 = study.get('Community2', '').strip()
        all_communities = [c for c in [community, community1, community2] if c]

        # Case 1: If Program is "Extramural Research", replace with Community value
        if program == 'Extramural Research':
            study_copy = study.copy()

            if community:
                # Replace with primary community
                new_program = community
                new_description = get_description_for_program(new_program)

                extramural_replacements.append({
                    'accession': study.get('Accession', ''),
                    'study_name': study.get('Study Name', ''),
                    'old_program': program,
                    'new_program': new_program,
                    'old_description': description,
                    'new_description': new_description,
                    'communities': all_communities
                })

                log_func(f"[EXTRAMURAL -> COMMUNITY] {study.get('Accession', '')}")
                log_func(f"  Program: '{program}' -> '{new_program}'")
                log_func(f"  Description: '{description}' -> '{new_description}'")

                study_copy['Program'] = new_program
                study_copy['Description'] = new_description

            expanded_studies.append(study_copy)

            # Create duplicates for other communities that don't match new program
            current_program = study_copy['Program']
            non_matching = [c for c in all_communities if c and c != current_program]

            for nm_community in non_matching:
                dup = study_copy.copy()
                dup['Program'] = nm_community
                dup['Description'] = get_description_for_program(nm_community)
                expanded_studies.append(dup)

                duplicates_created.append({
                    'accession': study.get('Accession', ''),
                    'study_name': study.get('Study Name', ''),
                    'original_program': current_program,
                    'new_program': nm_community,
                    'new_description': dup['Description'],
                    'reason': 'Additional community from Extramural Research study'
                })
                log_func(f"  [DUPLICATE] Program: '{nm_community}', Description: '{dup['Description']}'")

        # Case 2: Program exists and may differ from some communities
        elif program:
            expanded_studies.append(study.copy())

            # Find communities that don't match the program
            non_matching = [c for c in all_communities if c and c != program]

            if non_matching:
                log_func(f"[COMMUNITY MISMATCH] {study.get('Accession', '')}")
                log_func(f"  Original Program: '{program}'")
                log_func(f"  Communities: {', '.join(all_communities)}")

                for nm_community in non_matching:
                    dup = study.copy()
                    dup['Program'] = nm_community
                    dup['Description'] = get_description_for_program(nm_community)
                    expanded_studies.append(dup)

                    duplicates_created.append({
                        'accession': study.get('Accession', ''),
                        'study_name': study.get('Study Name', ''),
                        'original_program': program,
                        'new_program': nm_community,
                        'new_description': dup['Description'],
                        'reason': 'Community does not match Program'
                    })
                    log_func(f"  [DUPLICATE] Program: '{nm_community}', Description: '{dup['Description']}'")

        # Case 3: No program, just add as is
        else:
            expanded_studies.append(study.copy())

    log_func("")
    log_func(f"Step 2 Summary:")
    log_func(f"  - Input studies: {len(studies)}")
    log_func(f"  - Extramural Research replacements: {len(extramural_replacements)}")
    log_func(f"  - Duplicates created: {len(duplicates_created)}")
    log_func(f"  - Output studies: {len(expanded_studies)}")
    log_func(f"  - Net increase: +{len(expanded_studies) - len(studies)}")
    log_func("")

    return expanded_studies, extramural_replacements, duplicates_created


# =============================================================================
# REPORTING
# =============================================================================

def generate_report(
    output_dir: Path,
    timestamp: str,
    original_count: int,
    step1_results: Tuple[List, List, List],
    step2_results: Tuple[List, List, List],
    final_data: List[Dict],
    original_program_counts: Dict[str, int],
    final_program_counts: Dict[str, int]
) -> Path:
    """
    Generate a comprehensive report of all processing.

    Args:
        output_dir: Directory for output files
        timestamp: Timestamp string for filename
        original_count: Original number of input records
        step1_results: Tuple from step 1 (processed, changed, excluded)
        step2_results: Tuple from step 2 (expanded, replacements, duplicates)
        final_data: Final processed data
        original_program_counts: Program distribution before processing
        final_program_counts: Program distribution after processing

    Returns:
        Path to the generated report file
    """
    report_file = output_dir / f'program_table_report_{timestamp}.txt'

    _, step1_changed, step1_excluded = step1_results
    _, extramural_replacements, duplicates_created = step2_results

    with open(report_file, 'w') as f:
        # Header
        f.write("=" * 100 + "\n")
        f.write("BDC PROGRAM TABLE PROCESSING REPORT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 100 + "\n\n")

        # Executive Summary
        f.write("=" * 100 + "\n")
        f.write("EXECUTIVE SUMMARY\n")
        f.write("=" * 100 + "\n")
        f.write(f"  Original record count:                    {original_count}\n")
        f.write(f"  Records excluded (special programs):      {len(step1_excluded)}\n")
        f.write(f"  Records set to Extramural Research:       {len(step1_changed)}\n")
        f.write(f"  Extramural Research -> Community:         {len(extramural_replacements)}\n")
        f.write(f"  Duplicate records created:                {len(duplicates_created)}\n")
        f.write(f"  Final record count:                       {len(final_data)}\n")
        f.write(f"  Net change:                               +{len(final_data) - original_count}\n")
        f.write("\n")

        # Program to Description mapping
        f.write("=" * 100 + "\n")
        f.write("PROGRAM TO DESCRIPTION MAPPING (Reference)\n")
        f.write("=" * 100 + "\n")
        f.write(f"{'Program':<55} {'Description':<35}\n")
        f.write("-" * 90 + "\n")
        for prog, desc in sorted(PROGRAM_DESCRIPTIONS.items()):
            f.write(f"{prog:<55} {desc:<35}\n")
        f.write("\n")

        # Program Distribution Comparison
        f.write("=" * 100 + "\n")
        f.write("PROGRAM DISTRIBUTION: BEFORE vs AFTER\n")
        f.write("=" * 100 + "\n")
        all_programs = sorted(set(original_program_counts.keys()) | set(final_program_counts.keys()))
        f.write(f"{'Program':<55} {'Before':<10} {'After':<10} {'Change':<10}\n")
        f.write("-" * 85 + "\n")
        for prog in all_programs:
            before = original_program_counts.get(prog, 0)
            after = final_program_counts.get(prog, 0)
            change = after - before
            change_str = f"+{change}" if change > 0 else str(change)
            f.write(f"{prog:<55} {before:<10} {after:<10} {change_str:<10}\n")
        f.write("-" * 85 + "\n")
        f.write(f"{'TOTAL':<55} {original_count:<10} {len(final_data):<10} +{len(final_data) - original_count}\n")
        f.write("\n")

        # Step 1 Details: Extramural Research assignments
        f.write("=" * 100 + "\n")
        f.write(f"STEP 1 DETAILS: STUDIES SET TO EXTRAMURAL RESEARCH ({len(step1_changed)} studies)\n")
        f.write("=" * 100 + "\n")
        if step1_changed:
            for i, rec in enumerate(step1_changed[:50], 1):  # Show first 50
                f.write(f"{i:>3}. {rec['Accession']:<25} {rec['Original Program'] or '(empty)':<25} -> Extramural Research\n")
            if len(step1_changed) > 50:
                f.write(f"... and {len(step1_changed) - 50} more\n")
        else:
            f.write("  No studies were set to Extramural Research.\n")
        f.write("\n")

        # Step 1 Details: Excluded studies
        f.write("=" * 100 + "\n")
        f.write(f"STEP 1 DETAILS: EXCLUDED STUDIES ({len(step1_excluded)} studies)\n")
        f.write("=" * 100 + "\n")
        if step1_excluded:
            for i, rec in enumerate(step1_excluded, 1):
                f.write(f"{i:>3}. {rec['Accession']:<25} Program: {rec['Program']:<30} Reason: {rec['Reason']}\n")
        else:
            f.write("  No studies were excluded.\n")
        f.write("\n")

        # Step 2 Details: Extramural Research replacements
        f.write("=" * 100 + "\n")
        f.write(f"STEP 2 DETAILS: EXTRAMURAL RESEARCH -> COMMUNITY ({len(extramural_replacements)} studies)\n")
        f.write("=" * 100 + "\n")
        if extramural_replacements:
            by_new_program = defaultdict(list)
            for rec in extramural_replacements:
                by_new_program[rec['new_program']].append(rec)

            f.write("Summary by New Program:\n")
            for prog, recs in sorted(by_new_program.items(), key=lambda x: -len(x[1])):
                f.write(f"  {prog}: {len(recs)} studies\n")
            f.write("\n")

            f.write("Detailed List:\n")
            f.write("-" * 100 + "\n")
            for i, rec in enumerate(extramural_replacements[:50], 1):
                f.write(f"{i:>3}. {rec['accession']:<25} -> {rec['new_program']:<40} ({rec['new_description']})\n")
            if len(extramural_replacements) > 50:
                f.write(f"... and {len(extramural_replacements) - 50} more\n")
        else:
            f.write("  No Extramural Research replacements were made.\n")
        f.write("\n")

        # Step 2 Details: Duplicates created
        f.write("=" * 100 + "\n")
        f.write(f"STEP 2 DETAILS: DUPLICATE STUDIES CREATED ({len(duplicates_created)} duplicates)\n")
        f.write("=" * 100 + "\n")
        if duplicates_created:
            by_new_program = defaultdict(list)
            for rec in duplicates_created:
                by_new_program[rec['new_program']].append(rec)

            f.write("Summary by New Program:\n")
            for prog, recs in sorted(by_new_program.items(), key=lambda x: -len(x[1])):
                f.write(f"  {prog}: {len(recs)} duplicates\n")
            f.write("\n")

            f.write("Detailed List:\n")
            f.write("-" * 120 + "\n")
            for i, rec in enumerate(duplicates_created[:50], 1):
                f.write(f"{i:>3}. {rec['accession']:<25} Original: {rec['original_program']:<25} -> Duplicate: {rec['new_program']:<25}\n")
            if len(duplicates_created) > 50:
                f.write(f"... and {len(duplicates_created) - 50} more\n")
        else:
            f.write("  No duplicate studies were created.\n")
        f.write("\n")

        # Footer
        f.write("=" * 100 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 100 + "\n")

    return report_file


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """
    Main execution function.

    Parses command line arguments and orchestrates the two-step processing:
    1. Set Extramural Research for studies with missing descriptions
    2. Expand table by community mapping
    """
    parser = argparse.ArgumentParser(
        description='Process BDC program table: set Extramural Research and expand by community',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python process_program_table.py --input-file data/program_table.json
  python process_program_table.py --input-file data/program_table.json --output-dir output/
        """
    )
    parser.add_argument(
        '--input-file',
        required=True,
        help='Path to input program_table.json file'
    )
    parser.add_argument(
        '--output-dir',
        default=None,
        help='Output directory (default: same directory as script)'
    )

    args = parser.parse_args()

    # Setup output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(__file__).parent

    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup timestamp for output files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Setup log file
    log_file = output_dir / f'program_table_processing_{timestamp}.log'

    def log(message, also_print=True):
        """Write message to log file and optionally print to console."""
        with open(log_file, 'a') as f:
            f.write(message + "\n")
        if also_print:
            print(message)

    try:
        # Header
        log("=" * 100)
        log("BDC PROGRAM TABLE PROCESSOR")
        log(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log("=" * 100)
        log("")

        # Load input file
        log(f"Loading input file: {args.input_file}")
        studies = load_json(args.input_file)
        original_count = len(studies)
        log(f"Loaded {original_count} studies")
        log("")

        # Get original program distribution
        original_program_counts = count_studies_by_program(studies)

        # =====================================================================
        # STEP 1: Set Extramural Research for missing descriptions
        # =====================================================================
        step1_processed, step1_changed, step1_excluded = step1_set_extramural_for_missing_descriptions(
            studies, log
        )

        # =====================================================================
        # STEP 2: Expand table by community mapping
        # =====================================================================
        final_data, extramural_replacements, duplicates_created = step2_expand_by_community(
            step1_processed, log
        )

        # Get final program distribution
        final_program_counts = count_studies_by_program(final_data)

        # =====================================================================
        # Save output files
        # =====================================================================
        log("=" * 100)
        log("SAVING OUTPUT FILES")
        log("=" * 100)
        log("")

        # Save formatted JSON
        output_file = output_dir / f'program_table_final_{timestamp}.json'
        save_json(final_data, output_file, minify=False)
        log(f"Saved formatted JSON: {output_file.name}")

        # Save minified JSON
        output_file_min = output_dir / f'program_table_final_{timestamp}.min.json'
        save_json(final_data, output_file_min, minify=True)

        # Get file sizes
        formatted_size = os.path.getsize(output_file)
        minified_size = os.path.getsize(output_file_min)
        reduction = ((formatted_size - minified_size) / formatted_size) * 100

        log(f"Saved minified JSON: {output_file_min.name}")
        log(f"  Formatted size:  {formatted_size:,} bytes")
        log(f"  Minified size:   {minified_size:,} bytes")
        log(f"  Size reduction:  {reduction:.1f}%")
        log("")

        # Generate report
        report_file = generate_report(
            output_dir=output_dir,
            timestamp=timestamp,
            original_count=original_count,
            step1_results=(step1_processed, step1_changed, step1_excluded),
            step2_results=(final_data, extramural_replacements, duplicates_created),
            final_data=final_data,
            original_program_counts=original_program_counts,
            final_program_counts=final_program_counts
        )
        log(f"Saved report: {report_file.name}")
        log(f"Saved log: {log_file.name}")
        log("")

        # =====================================================================
        # Final Summary
        # =====================================================================
        log("=" * 100)
        log("FINAL SUMMARY")
        log("=" * 100)
        log(f"  Original record count:                    {original_count}")
        log(f"  Records excluded (special programs):      {len(step1_excluded)}")
        log(f"  Records set to Extramural Research:       {len(step1_changed)}")
        log(f"  Extramural Research -> Community:         {len(extramural_replacements)}")
        log(f"  Duplicate records created:                {len(duplicates_created)}")
        log(f"  Final record count:                       {len(final_data)}")
        log(f"  Net change:                               +{len(final_data) - original_count}")
        log("")
        log("=" * 100)
        log(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log("=" * 100)

        print(f"\nProcessing completed successfully!")
        print(f"Output files in: {output_dir}")

        return 0

    except FileNotFoundError:
        print(f"Error: Input file not found: {args.input_file}", file=sys.stderr)
        return 1
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in input file: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
