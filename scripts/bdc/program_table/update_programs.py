#!/usr/bin/env python3
"""
Update program names for studies using NHLBI Jira data for program table ingestion.

PURPOSE:
    Updates the program table with correct program names from NHLBI Jira,
    ensuring the BDC portal's program search/filter feature has accurate data.

WORKFLOW:
    1. Reads Gen3 studies data (from fetch_studies.py output)
    2. Loads Jira data (CSV/Excel) with approved program associations
    3. Updates program names based on Jira
    4. Fills missing descriptions from same program studies
    5. Generates final program table and comprehensive reports

OUTPUT:
    - cleaned_studies_on_gen3_updated_*_upload_chart_minified.json (final program table)

Usage:
    python update_programs.py --jira-file path/to/jira.csv --input-file path/to/gen3_studies.json --output-dir path/to/output
"""

import argparse
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Tuple

import bdc_utils


class JiraProgramUpdater:
    """Handles updating program names from Jira data."""

    def __init__(self, jira_file: str, input_file: str, output_dir: str, logger: Any):
        """
        Initialize Jira program updater.

        Args:
            jira_file: Path to Jira data file (CSV or Excel)
            input_file: Path to input Gen3 studies JSON
            output_dir: Output directory for results
            logger: Logger instance
        """
        self.jira_file = jira_file
        self.input_file = input_file
        self.logger = logger

        # Setup output paths
        self.path_manager = bdc_utils.FilePathManager(output_dir)
        timestamp = self.path_manager.timestamp

        self.output_files = {
            # Final program table files in program_table directory
            'upload_chart': self.path_manager.get_program_table_path(f"program_table_updated_{timestamp}.json"),
            'upload_mini': self.path_manager.get_program_table_path(f"program_table_updated_{timestamp}_minified.json"),
            # Reports and intermediate files in gen3 directory
            'updated': self.path_manager.get_gen3_path(f"studies_updated_{timestamp}.json"),
            'update_report': self.path_manager.get_gen3_path(f"program_names_updated_report_{timestamp}.json"),
            'missing_desc': self.path_manager.get_gen3_path(f"studies_missing_NHLBI_program_name_{timestamp}.json"),
            'both_missing': self.path_manager.get_gen3_path(f"studies_in_gen3_and_jira_Which_are_missing_NHLBI_program_name_{timestamp}.json"),
            'comprehensive': self.path_manager.get_gen3_path(f"jira_studies_not_in_gen3_report_{timestamp}.log")
        }

    def _find_column(self, df: pd.DataFrame, *names: str) -> str:
        """
        Find a column by trying multiple possible names.
        Handles both old format (e.g., 'Accession') and new Jira export format
        (e.g., 'Custom field (Accession)').

        Args:
            df: DataFrame to search
            *names: Possible column name variations to try

        Returns:
            The actual column name found, or empty string if not found
        """
        for name in names:
            # Try exact match
            if name in df.columns:
                return name
            # Try with 'Custom field (...)' prefix (new Jira export format)
            custom_field_name = f'Custom field ({name})'
            if custom_field_name in df.columns:
                return custom_field_name
        return ''

    def _find_community_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Find all Community columns in the DataFrame.
        Handles both old format and new Jira export format with duplicate columns.

        Args:
            df: DataFrame to search

        Returns:
            List of community column names found
        """
        community_cols = []
        for col in df.columns:
            col_lower = col.lower()
            # Match 'Community', 'Community.1', 'Custom field (Community)', etc.
            if col_lower == 'community' or col_lower.startswith('community.'):
                community_cols.append(col)
            elif 'custom field (community)' in col_lower:
                community_cols.append(col)
        return community_cols

    def load_jira_data(self) -> Tuple[Dict[str, Dict[str, str]], set]:
        """
        Load and parse Jira data file.
        Supports both old CSV format and new Jira export format with 'Custom field (...)' columns.

        Returns:
            Tuple of (jira_dict, jira_accessions)
        """
        # Validate file
        if not Path(self.jira_file).exists():
            raise FileNotFoundError(f"Jira file not found: {self.jira_file}")

        file_ext = Path(self.jira_file).suffix.lower()
        if file_ext not in ['.csv', '.xlsx', '.xls']:
            raise ValueError(f"Unsupported file format: {file_ext}")

        self.logger.info(f"Loading Jira data from: {self.jira_file}")

        # Load data
        df = pd.read_csv(self.jira_file) if file_ext == '.csv' else pd.read_excel(self.jira_file)

        # Find column names (handles both old and new Jira formats)
        accession_col = self._find_column(df, 'Accession')
        program_col = self._find_column(df, 'Program(s)')
        gen3_program_col = self._find_column(df, 'Gen3 Program Name')
        status_col = self._find_column(df, 'Status')
        summary_col = self._find_column(df, 'Summary')

        if not accession_col:
            raise ValueError("Could not find Accession column in Jira file")

        self.logger.info(f"Column mapping: Accession='{accession_col}', Program(s)='{program_col}', "
                        f"Gen3 Program Name='{gen3_program_col}', Status='{status_col}', Summary='{summary_col}'")

        df_valid = df.dropna(subset=[accession_col])

        jira_dict = {}
        jira_accessions = set()

        # Find all Community columns
        community_cols = self._find_community_columns(df)
        self.logger.info(f"Found {len(community_cols)} Community columns in Jira data: {community_cols}")

        for _, row in df_valid.iterrows():
            accession = str(row[accession_col]).strip()
            base_acc = bdc_utils.extract_base_accession(accession)

            if base_acc:
                jira_accessions.add(base_acc)

                # Extract community values from all Community columns
                community_values = []
                for col in community_cols:
                    val = row.get(col, '')
                    community_values.append(str(val).strip() if pd.notna(val) else '')

                # Get program name from Program(s) field, or fall back to first Community value
                # (In some Jira exports, Community column contains the program names)
                program_name = ''
                if program_col:
                    program_name = str(row.get(program_col, '')).strip() if pd.notna(row.get(program_col, '')) else ''

                # If no Program(s) value, use first Community value as program name
                if not program_name and community_values:
                    first_community = community_values[0]
                    if first_community:
                        program_name = first_community

                jira_dict[base_acc] = {
                    'full_accession': accession,
                    'program_name': program_name,  # Only from Program(s) field, not Gen3 Program Name
                    'gen3_program_name': str(row.get(gen3_program_col, '')).strip() if gen3_program_col and pd.notna(row.get(gen3_program_col, '')) else '',
                    'status': str(row.get(status_col, '')).strip() if status_col and pd.notna(row.get(status_col, '')) else '',
                    'summary': str(row.get(summary_col, '')) if summary_col else '',
                    'community': community_values[0] if len(community_values) > 0 else '',
                    'community1': community_values[1] if len(community_values) > 1 else '',
                    'community2': community_values[2] if len(community_values) > 2 else ''
                }

        self.logger.info(f"Loaded {len(jira_dict)} studies from Jira")
        return jira_dict, jira_accessions

    def get_program_info(self, studies: List[Dict[str, str]], program_name: str) -> Tuple[str, str]:
        """
        Find correct casing and description for a program (case-insensitive).

        Args:
            studies: List of study dictionaries
            program_name: Program name to search for

        Returns:
            Tuple of (program_name_with_correct_case, description)
        """
        if not program_name:
            return program_name, ''

        program_lower = program_name.lower()
        for study in studies:
            study_prog = study.get('Program', '')
            if study_prog and study_prog.lower() == program_lower:
                desc = study.get('Description', '')
                return study_prog, desc if desc and desc.strip() else ''
        return program_name, ''

    def update_study_programs(
        self,
        studies: List[Dict[str, str]],
        jira_dict: Dict[str, Dict[str, str]],
        valid_programs_map: Dict[str, str] = None
    ) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
        """
        Update program names for studies based on Jira data.
        Creates duplicate entries for studies with multiple communities.
        Only uses program names that exist in BDC portal.

        Args:
            studies: List of Gen3 studies
            jira_dict: Jira data dictionary
            valid_programs_map: Dict mapping lowercase program names to original case

        Returns:
            Tuple of (additional_studies, updated_records, not_in_jira_records)
        """
        if valid_programs_map is None:
            valid_programs_map = {}

        # Programs to exclude entirely (not map to Extramural Research)
        excluded_program_names = {'tutorial', 'no program', ''}

        def get_valid_program(prog_name: str) -> tuple:
            """
            Get valid BDC program name.
            Returns (program_name, should_exclude) tuple.
            - If valid BDC program: returns (program_name, False)
            - If 'tutorial' or empty: returns (None, True) - exclude
            - If other invalid program: returns ('Extramural Research', False) - map to Extramural
            """
            if not prog_name or prog_name.lower() in excluded_program_names:
                return None, True  # Exclude tutorial and No Program
            # Check if program exists in BDC (case-insensitive)
            prog_lower = prog_name.lower()
            if prog_lower in valid_programs_map:
                return valid_programs_map[prog_lower], False
            # Not a valid BDC program - map to Extramural Research
            self.logger.info(f"Program '{prog_name}' not in BDC, using 'Extramural Research'")
            return 'Extramural Research', False

        additional_studies = []
        updated_records = []
        not_in_jira = []
        excluded_studies = []  # Studies with excluded programs (tutorial, No Program)

        for study in list(studies):
            accession = study.get('Accession', '')
            base_acc = bdc_utils.extract_base_accession(accession)
            current_prog = study.get('Program', '')
            current_desc = study.get('Description', '')

            # Check if accession is in phs_id format
            is_phs_id = accession.startswith('phs')

            # Validate current program
            if valid_programs_map:
                validated_prog, should_exclude = get_valid_program(current_prog)
                # For non-phs_id studies, exclude if they would be mapped to Extramural Research
                if not is_phs_id and validated_prog == 'Extramural Research':
                    should_exclude = True
                if should_exclude:
                    # Exclude tutorial, No Program, and non-phs_id studies defaulting to Extramural Research
                    reason = 'Non-phs_id study excluded from Extramural Research' if not is_phs_id else 'Program excluded (tutorial or No Program)'
                    excluded_studies.append({
                        'Accession': accession,
                        'Study Name': study.get('Study Name', ''),
                        'Invalid Program': current_prog or 'No Program',
                        'Reason': reason
                    })
                    self.logger.info(f"Excluding {accession}: {reason}")
                    studies.remove(study)
                    continue
                elif validated_prog != current_prog:
                    study['Program'] = validated_prog
                    current_prog = validated_prog

            if base_acc and base_acc in jira_dict:
                jira_info = jira_dict[base_acc]

                # Get all non-empty community values (community names are program names)
                communities = [c for c in [
                    jira_info.get('community', ''),
                    jira_info.get('community1', ''),
                    jira_info.get('community2', '')
                ] if c]

                # Get program name: first from Program(s), then fall back to first Community
                program_name = jira_info.get('program_name', '').strip()
                if not program_name and communities:
                    program_name = communities[0]

                # Validate program name against BDC programs
                if program_name:
                    validated_prog, should_exclude = get_valid_program(program_name)
                    # For non-phs_id studies, exclude if they would be mapped to Extramural Research
                    if not is_phs_id and validated_prog == 'Extramural Research':
                        should_exclude = True
                    if should_exclude:
                        program_name = None  # Don't update program if it's tutorial/empty
                    else:
                        program_name = validated_prog

                # Set all community fields from Jira as-is
                study['Community'] = communities[0] if len(communities) > 0 else ''
                study['Community1'] = communities[1] if len(communities) > 1 else ''
                study['Community2'] = communities[2] if len(communities) > 2 else ''

                if program_name:
                    # Update program name from Jira (validated against BDC programs)
                    first_prog, first_desc = self.get_program_info(studies, program_name)
                    if not first_desc and current_desc:
                        first_desc = current_desc

                    if first_prog.lower() != current_prog.lower():
                        study['Program'] = first_prog
                        if first_desc:
                            study['Description'] = first_desc

                        updated_records.append({
                            'Accession': accession,
                            'Study Name': study.get('Study Name', ''),
                            'Old Program': current_prog,
                            'New Program': first_prog,
                            'Old Description': current_desc,
                            'New Description': first_desc,
                            'Jira Status': jira_info['status'],
                            'Jira Summary': jira_info['summary'],
                            'Had Description': 'Yes' if current_desc.strip() else 'No',
                            'Multi Program': 'Yes' if len(communities) > 1 else 'No'
                        })
                        self.logger.info(f"Updated {accession}: Program '{current_prog}' -> '{first_prog}' (primary community: {communities[0] if communities else 'N/A'})")

                # Handle additional communities (create duplicates)
                # This duplicates the study for each additional community
                # Community values are validated as program names - skip if excluded (tutorial/empty)
                if len(communities) > 1:
                    for idx, community in enumerate(communities[1:], start=2):
                        # Validate community as program name
                        validated_community_prog, should_exclude = get_valid_program(community)
                        if should_exclude:
                            self.logger.info(f"Skipping community duplicate {accession}: '{community}' (tutorial/empty)")
                            continue
                        new_study = study.copy()
                        new_study['Program'] = validated_community_prog
                        # Get description for this program
                        _, prog_desc = self.get_program_info(studies, validated_community_prog)
                        if prog_desc:
                            new_study['Description'] = prog_desc
                        additional_studies.append(new_study)
                        self.logger.info(f"Added {accession}: Program '{current_prog}' -> '{validated_community_prog}' (community {idx}/{len(communities)}: {community})")

            else:
                # Study not in Jira - Community stays empty (Community comes from Jira only)
                study['Community'] = ''
                study['Community1'] = ''
                study['Community2'] = ''

                if not current_desc or not current_desc.strip():
                    # Track new study not in Jira
                    not_in_jira.append({
                        'Accession': accession,
                        'Study Name': study.get('Study Name', ''),
                        'Current Program': current_prog
                    })

        return additional_studies, updated_records, not_in_jira, excluded_studies

    def fill_descriptions(self, studies: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Fill missing descriptions from studies with same program.

        Args:
            studies: List of study dictionaries

        Returns:
            List of descriptions that were filled
        """
        self.logger.info("\nFilling missing descriptions from same program studies...")
        filled = []

        for study in studies:
            prog = study.get('Program', '')
            desc = study.get('Description', '')

            if prog and (not desc or not desc.strip()):
                _, found_desc = self.get_program_info(studies, prog)
                if found_desc:
                    study['Description'] = found_desc
                    filled.append({
                        'Accession': study.get('Accession', ''),
                        'Study Name': study.get('Study Name', ''),
                        'Program': prog,
                        'Description Added': found_desc
                    })
                    self.logger.info(f"Filled description for {study.get('Accession', '')}: Program '{prog}'")

        if filled:
            self.logger.info(f"Filled descriptions for {len(filled)} studies")

        return filled

    def identify_both_missing_desc(
        self,
        studies: List[Dict[str, str]],
        jira_dict: Dict[str, Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """
        Identify studies in both Gen3 and Jira but missing descriptions.

        Args:
            studies: List of study dictionaries
            jira_dict: Jira data dictionary

        Returns:
            List of studies missing descriptions
        """
        self.logger.info("\nIdentifying studies in both Gen3 and Jira but missing descriptions...")
        both_missing = []

        for study in studies:
            accession = study.get('Accession', '')
            base_acc = bdc_utils.extract_base_accession(accession)
            desc = study.get('Description', '')

            if base_acc and base_acc in jira_dict:
                if not desc or not desc.strip():
                    jira_info = jira_dict[base_acc]
                    both_missing.append({
                        'Accession': accession,
                        'Study Name': study.get('Study Name', ''),
                        'Program': study.get('Program', ''),
                        'Jira Program': jira_info['program_name'],
                        'Jira Status': jira_info['status'],
                        'Jira Summary': jira_info['summary']
                    })
                    self.logger.info(f"Found {accession}: In both Gen3 and Jira but missing description")

        if both_missing:
            self.logger.info(f"Found {len(both_missing)} studies in both Gen3 and Jira but missing description")

        return both_missing

    def save_comprehensive_report(
        self,
        updated_studies: List[Dict[str, str]],
        not_in_jira: List[Dict[str, str]],
        missing_from_gen3: List[Dict[str, str]]
    ) -> None:
        """Save comprehensive categorized report."""
        with open(self.output_files['comprehensive'], 'w') as f:
            bdc_utils.write_report_header(f, "COMPREHENSIVE STUDY STATUS REPORT")

            # Category 1: NEW studies
            bdc_utils.write_report_section(f, "CATEGORY 1: NEW STUDIES (In both Gen3 and Jira - Program names updated)", len(updated_studies))
            for study in updated_studies:
                f.write(f"Accession: {study['Accession']}\n")
                f.write(f"Study Name: {study['Study Name']}\n")
                f.write(f"Old Program: {study['Old Program']}\n")
                f.write(f"New Program: {study['New Program']}\n")
                f.write(f"Old Description: {study['Old Description']}\n")
                f.write(f"New Description: {study['New Description']}\n")
                f.write(f"Had Description Before: {study['Had Description']}\n")
                f.write(f"Jira Status: {study['Jira Status']}\n")
                f.write(f"Jira Summary: {study['Jira Summary']}\n")
                f.write("-" * 100 + "\n")

            # Category 2: OLD studies
            bdc_utils.write_report_section(f, "CATEGORY 2: OLD STUDIES (In Gen3 only - Not in Jira)", len(not_in_jira))
            for study in not_in_jira:
                f.write(f"Accession: {study['Accession']}\n")
                f.write(f"Study Name: {study['Study Name']}\n")
                f.write(f"Current Program: {study['Current Program']}\n")
                f.write("-" * 100 + "\n")

            # Category 3: NOT INGESTED
            bdc_utils.write_report_section(f, "CATEGORY 3: NOT INGESTED (In Jira only - Yet to be ingested by Gen3)", len(missing_from_gen3))
            for study in missing_from_gen3:
                f.write(f"Accession: {study['Accession']}\n")
                f.write(f"Program Name: {study['Gen3 Program Name']}\n")
                f.write(f"Status: {study['Status']}\n")
                f.write(f"Summary: {study['Summary']}\n")
                f.write("-" * 100 + "\n")

        self.logger.info(f"Comprehensive report saved to: {self.output_files['comprehensive']}")

    def run(self) -> int:
        """Execute the full update process."""
        try:
            # Load BDC studies first to get valid program names
            bdc_studies_file = self.path_manager.get_bdc_path("bdc_studies.json")
            bdc_studies = bdc_utils.load_json(bdc_studies_file)

            # Get valid program names from BDC (case-insensitive matching)
            valid_programs = set()
            valid_programs_map = {}  # lowercase -> original case
            for s in bdc_studies:
                prog = s.get('Program', '')
                if prog:
                    valid_programs.add(prog)
                    valid_programs_map[prog.lower()] = prog
            self.logger.info(f"Valid BDC programs: {sorted(valid_programs)}")

            # Load data
            self.logger.info("Loading Gen3 studies data...")
            gen3_studies = bdc_utils.load_json(self.input_file)
            self.logger.info(f"Loaded {len(gen3_studies)} studies from Gen3 data")

            jira_dict, jira_accessions = self.load_jira_data()

            # Track accessions
            gen3_accessions = {bdc_utils.extract_base_accession(s['Accession']) for s in gen3_studies if bdc_utils.extract_base_accession(s['Accession'])}

            # Update programs
            self.logger.info("\nUpdating program names from Jira...")
            additional, updated, not_in_jira, excluded = self.update_study_programs(gen3_studies, jira_dict, valid_programs_map)
            gen3_studies.extend(additional)
            if excluded:
                self.logger.info(f"Excluded {len(excluded)} studies with invalid programs")

            # Save program_table.json with Community fields (before filtering)
            program_table_with_community = self.path_manager.get_program_table_path("program_table.json")
            bdc_utils.save_json(gen3_studies, program_table_with_community)
            self.logger.info(f"Saved program_table.json with Community fields: {len(gen3_studies)} studies")

            # Fill descriptions
            filled = self.fill_descriptions(gen3_studies)

            # Identify both missing descriptions
            both_missing = self.identify_both_missing_desc(gen3_studies, jira_dict)

            # Separate studies with/without descriptions
            studies_with_desc, studies_missing_desc = bdc_utils.separate_studies_by_description(gen3_studies)
            self.logger.info(f"\nStudies with description: {len(studies_with_desc)}")
            self.logger.info(f"Studies missing description: {len(studies_missing_desc)}")

            # Find missing from Gen3
            missing_from_gen3 = [
                {
                    'Accession': jira_dict[base_acc]['full_accession'],
                    'Gen3 Program Name': jira_dict[base_acc].get('gen3_program_name', '') or jira_dict[base_acc]['program_name'],
                    'Program(s)': jira_dict[base_acc]['program_name'],
                    'Status': jira_dict[base_acc]['status'],
                    'Summary': jira_dict[base_acc]['summary']
                }
                for base_acc in jira_accessions if base_acc not in gen3_accessions
            ]

            # Save results
            self.logger.info("\nSaving results...")
            bdc_utils.save_json(studies_with_desc, self.output_files['updated'])
            bdc_utils.save_json(studies_with_desc, self.output_files['upload_chart'])
            bdc_utils.save_json(studies_with_desc, self.output_files['upload_mini'], minify=True)

            # Save YAML format
            import yaml
            yaml_file = self.path_manager.get_program_table_path(f"program_table_updated_{self.path_manager.timestamp}.yaml")
            yaml_data = {'program_study_mappings': studies_with_desc}
            with open(yaml_file, 'w') as f:
                yaml.dump(yaml_data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            self.logger.info(f"Saved YAML format to: {yaml_file}")

            # Save minified YAML format
            yaml_mini_file = self.path_manager.get_program_table_path(f"program_table_updated_{self.path_manager.timestamp}_minified.yaml")
            with open(yaml_mini_file, 'w') as f:
                yaml.dump(yaml_data, f, default_flow_style=True, allow_unicode=True, sort_keys=False)
            self.logger.info(f"Saved minified YAML format to: {yaml_mini_file}")

            if studies_missing_desc:
                bdc_utils.save_json(studies_missing_desc, self.output_files['missing_desc'])

            if both_missing:
                bdc_utils.save_json(both_missing, self.output_files['both_missing'])

            if updated:
                bdc_utils.save_json(updated, self.output_files['update_report'])

            self.save_comprehensive_report(updated, not_in_jira, missing_from_gen3)

            # Load BDC studies for final comparison
            bdc_studies_file = self.path_manager.get_bdc_path("bdc_studies.json")
            bdc_studies = bdc_utils.load_json(bdc_studies_file)

            # Calculate unique accessions
            from collections import Counter
            bdc_unique_accessions = set()
            for s in bdc_studies:
                base = bdc_utils.extract_base_accession(s.get('Accession', ''))
                if base:
                    bdc_unique_accessions.add(base)

            program_table_unique_accessions = set()
            for s in gen3_studies:
                base = bdc_utils.extract_base_accession(s.get('Accession', ''))
                if base:
                    program_table_unique_accessions.add(base)

            # Count studies by program
            bdc_program_counts = Counter()
            for s in bdc_studies:
                program = s.get('Program', '') or 'No Program'
                bdc_program_counts[program] += 1

            program_table_counts = Counter()
            for s in gen3_studies:
                program = s.get('Program', '') or 'No Program'
                program_table_counts[program] += 1

            # Final Summary with comparison
            self.logger.info("\n" + "="*80)
            self.logger.info("FINAL SUMMARY")
            self.logger.info("="*80)
            self.logger.info("")
            self.logger.info("--- Processing Stats ---")
            self.logger.info(f"Total studies in final program table: {len(gen3_studies)}")
            self.logger.info(f"Studies excluded (invalid program): {len(excluded)}")
            self.logger.info(f"Studies with program names updated: {len(updated)}")
            self.logger.info(f"Descriptions filled from same program: {len(filled)}")
            self.logger.info(f"Studies still missing description: {len(studies_missing_desc)}")
            self.logger.info(f"Studies in both Gen3 and Jira but missing description: {len(both_missing)}")
            self.logger.info(f"New studies (empty description) not in Jira: {len(not_in_jira)}")
            self.logger.info(f"Studies in Jira but not in Gen3: {len(missing_from_gen3)}")
            self.logger.info("")
            self.logger.info("--- BDC Portal vs Program Table Comparison ---")
            self.logger.info(f"BDC Portal: {len(bdc_studies)} study records ({len(bdc_unique_accessions)} unique accessions)")
            self.logger.info(f"Program Table: {len(gen3_studies)} study records ({len(program_table_unique_accessions)} unique accessions)")
            self.logger.info("")
            self.logger.info("--- Studies by Program Comparison ---")
            self.logger.info(f"{'Program':<60} {'BDC':>8} {'PrgTbl':>8} {'Change':>10}")
            self.logger.info("-" * 88)

            all_programs = sorted(set(bdc_program_counts.keys()) | set(program_table_counts.keys()))
            for program in all_programs:
                bdc_count = bdc_program_counts.get(program, 0)
                pt_count = program_table_counts.get(program, 0)
                diff = pt_count - bdc_count
                if diff > 0:
                    change = f"+{diff}"
                elif diff < 0:
                    change = str(diff)
                else:
                    change = "0"
                self.logger.info(f"{program:<60} {bdc_count:>8} {pt_count:>8} {change:>10}")

            self.logger.info("-" * 88)
            total_diff = len(gen3_studies) - len(bdc_studies)
            total_change = f"+{total_diff}" if total_diff > 0 else str(total_diff)
            self.logger.info(f"{'TOTAL':<60} {len(bdc_studies):>8} {len(gen3_studies):>8} {total_change:>10}")

            # --- Record Count Reconciliation (explains the TOTAL change) ---
            # Load DOI tombstone data
            doi_tombstone_accessions = set()
            doi_tombstone_records = []
            doi_tombstone_file = self.path_manager.get_gen3_path("bdc_studies_excluded_doi_tombstone.json")
            if Path(doi_tombstone_file).exists():
                doi_tombstone_records = bdc_utils.load_json(doi_tombstone_file)
                for s in doi_tombstone_records:
                    base = bdc_utils.extract_base_accession(s.get('Accession', ''))
                    if base:
                        doi_tombstone_accessions.add(base)

            # Count BDC records removed due to DOI tombstone
            bdc_doi_removed = [s for s in bdc_studies
                               if bdc_utils.extract_base_accession(s.get('Accession', '')) in doi_tombstone_accessions]

            # Count BDC records whose base accession is NOT in Gen3 at all (not in program table and not tombstoned)
            bdc_not_in_gen3 = [s for s in bdc_studies
                               if bdc_utils.extract_base_accession(s.get('Accession', '')) not in program_table_unique_accessions
                               and bdc_utils.extract_base_accession(s.get('Accession', '')) not in doi_tombstone_accessions]

            # Count records added: new Gen3 studies not in BDC
            pt_new_records = [s for s in gen3_studies
                              if bdc_utils.extract_base_accession(s.get('Accession', '')) not in bdc_unique_accessions]

            self.logger.info("")
            self.logger.info("--- Record Count Reconciliation ---")
            self.logger.info(f"  BDC Portal records (starting point):                     {len(bdc_studies):>6}")
            self.logger.info(f"  (-) BDC records removed: DOI Tombstone (deprecated):     {len(bdc_doi_removed):>6}")
            self.logger.info(f"  (-) BDC records removed: Not in Gen3:                    {len(bdc_not_in_gen3):>6}")
            self.logger.info(f"  (-) Records excluded: Non-phs_id / invalid program:      {len(excluded):>6}")
            self.logger.info(f"  (+) New records added: Gen3 studies not in BDC:           {len(pt_new_records):>6}")
            self.logger.info(f"  (+) Records added: Community duplicates from Jira:        {len(additional):>6}")
            net = len(bdc_studies) - len(bdc_doi_removed) - len(bdc_not_in_gen3) - len(excluded) + len(pt_new_records) + len(additional)
            self.logger.info(f"  (=) Expected program table records:                      {net:>6}")
            self.logger.info(f"  (=) Actual program table records:                        {len(gen3_studies):>6}")
            if net != len(gen3_studies):
                self.logger.info(f"  (*) Unaccounted difference:                               {len(gen3_studies) - net:>6}")

            # --- Removed: BDC studies removed due to DOI Tombstone ---
            if bdc_doi_removed:
                # Group by base accession for cleaner display
                from collections import defaultdict
                doi_by_acc = defaultdict(list)
                for s in bdc_doi_removed:
                    base = bdc_utils.extract_base_accession(s.get('Accession', ''))
                    doi_by_acc[base].append(s)

                self.logger.info("")
                self.logger.info(f"--- Removed: DOI Tombstone Studies ({len(bdc_doi_removed)} records, {len(doi_by_acc)} unique accessions) ---")
                self.logger.info(f"  Note: These studies are deprecated in Gen3 (source of truth) and removed from program table.")
                self.logger.info(f"  {'Accession':<45} {'Records':>8} {'Program in BDC'}")
                self.logger.info(f"  {'-'*90}")
                for acc in sorted(doi_by_acc.keys()):
                    records = doi_by_acc[acc]
                    programs = sorted(set(s.get('Program', '') for s in records if s.get('Program')))
                    self.logger.info(f"  {acc:<45} {len(records):>8} {', '.join(programs)}")

            # --- Removed: BDC studies not found in Gen3 ---
            if bdc_not_in_gen3:
                not_in_gen3_by_acc = defaultdict(list)
                for s in bdc_not_in_gen3:
                    base = bdc_utils.extract_base_accession(s.get('Accession', ''))
                    not_in_gen3_by_acc[base].append(s)

                self.logger.info("")
                self.logger.info(f"--- Removed: BDC Studies Not in Gen3 ({len(bdc_not_in_gen3)} records, {len(not_in_gen3_by_acc)} unique accessions) ---")
                self.logger.info(f"  Note: These are on BDC portal but not in Gen3 (source of truth).")
                self.logger.info(f"  {'Accession':<45} {'Records':>8} {'Program in BDC'}")
                self.logger.info(f"  {'-'*90}")
                for acc in sorted(not_in_gen3_by_acc.keys()):
                    records = not_in_gen3_by_acc[acc]
                    programs = sorted(set(s.get('Program', '') for s in records if s.get('Program')))
                    self.logger.info(f"  {acc:<45} {len(records):>8} {', '.join(programs)}")

            # --- Removed: Excluded non-phs_id / invalid program studies ---
            if excluded:
                self.logger.info("")
                self.logger.info(f"--- Removed: Excluded Studies ({len(excluded)} records) ---")
                self.logger.info(f"  {'Accession':<45} {'Program':<25} {'Reason'}")
                self.logger.info(f"  {'-'*100}")
                for ex in excluded:
                    self.logger.info(f"  {ex['Accession']:<45} {ex['Invalid Program']:<25} {ex['Reason']}")

            # --- Added: New Gen3 studies not in BDC ---
            pt_not_in_bdc = program_table_unique_accessions - bdc_unique_accessions
            if pt_not_in_bdc:
                self.logger.info("")
                self.logger.info(f"--- Added: New Studies from Gen3 ({len(pt_new_records)} records, {len(pt_not_in_bdc)} unique accessions) ---")
                self.logger.info(f"  Note: These are in Gen3 (source of truth) but not yet on the BDC portal.")
                self.logger.info(f"  {'Accession':<45} {'Program'}")
                self.logger.info(f"  {'-'*70}")
                for acc in sorted(pt_not_in_bdc):
                    prog = ''
                    for s in gen3_studies:
                        if bdc_utils.extract_base_accession(s.get('Accession', '')) == acc:
                            prog = s.get('Program', '')
                            break
                    self.logger.info(f"  {acc:<45} {prog}")

            # --- Updated: Program names changed from Jira ---
            if updated:
                self.logger.info("")
                self.logger.info(f"--- Updated: Program Names Changed from Jira ({len(updated)} records) ---")
                self.logger.info(f"  {'Accession':<45} {'Old Program':<30} {'New Program'}")
                self.logger.info(f"  {'-'*100}")
                for u in updated:
                    self.logger.info(f"  {u['Accession']:<45} {u['Old Program']:<30} {u['New Program']}")

            self.logger.info("")
            self.logger.info("="*80)
            self.logger.info("Processing completed successfully!")

            return 0

        except Exception as e:
            self.logger.error(f"Error: {str(e)}", exc_info=True)
            return 1


def main():
    parser = argparse.ArgumentParser(
        description='Update program names for Gen3 studies from NHLBI Jira data',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--jira-file', required=True, help='Path to Jira data file (CSV or Excel)')
    parser.add_argument('--input-file', required=True, help='Path to input Gen3 studies JSON file')
    parser.add_argument('--output-dir', required=True, help='Output directory for results')

    args = parser.parse_args()

    # Setup logging
    output_dir = Path(args.output_dir)
    bdc_utils.ensure_dir_exists(str(output_dir / "studies_on_gen3_portal"))
    log_file = output_dir / "update_programs.log"
    logger = bdc_utils.setup_logging(str(log_file))

    logger.info("="*80)
    logger.info("BDC Data Pipeline - Update Program Names")
    logger.info("="*80)
    logger.info(f"Jira file: {args.jira_file}")
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info("")

    updater = JiraProgramUpdater(args.jira_file, args.input_file, args.output_dir, logger)
    return updater.run()


if __name__ == "__main__":
    sys.exit(main())
