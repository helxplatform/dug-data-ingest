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

    def load_jira_data(self) -> Tuple[Dict[str, Dict[str, str]], set]:
        """
        Load and parse Jira data file.

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
        df_valid = df.dropna(subset=['Accession'])

        jira_dict = {}
        jira_accessions = set()

        # Find all Community columns (pandas renames duplicates to Community, Community.1, Community.2)
        community_cols = [col for col in df.columns if col == 'Community' or col.startswith('Community.')]
        self.logger.info(f"Found {len(community_cols)} Community columns in Jira data: {community_cols}")

        for _, row in df_valid.iterrows():
            accession = str(row['Accession']).strip()
            base_acc = bdc_utils.extract_base_accession(accession)

            if base_acc:
                jira_accessions.add(base_acc)

                # Extract community values from all Community columns
                community_values = []
                for col in community_cols:
                    val = row.get(col, '')
                    community_values.append(str(val).strip() if pd.notna(val) else '')

                jira_dict[base_acc] = {
                    'full_accession': accession,
                    'program_name': str(row.get('Program(s)', '')).strip() if pd.notna(row.get('Program(s)', '')) else '',
                    'status': str(row.get('Status', '')).strip() if pd.notna(row.get('Status', '')) else '',
                    'summary': str(row.get('Summary', '')),
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
        jira_dict: Dict[str, Dict[str, str]]
    ) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
        """
        Update program names for studies based on Jira data.

        Args:
            studies: List of Gen3 studies
            jira_dict: Jira data dictionary

        Returns:
            Tuple of (additional_studies, updated_records, not_in_jira_records)
        """
        additional_studies = []
        updated_records = []
        not_in_jira = []

        for study in list(studies):
            accession = study.get('Accession', '')
            base_acc = bdc_utils.extract_base_accession(accession)
            current_prog = study.get('Program', '')
            current_desc = study.get('Description', '')

            if base_acc and base_acc in jira_dict:
                jira_info = jira_dict[base_acc]
                programs = [p.strip() for p in jira_info['program_name'].split(',') if p.strip()]

                # Add Community fields from Jira
                study['Community'] = jira_info.get('community', '')
                study['Community1'] = jira_info.get('community1', '')
                study['Community2'] = jira_info.get('community2', '')

                if programs:
                    # Update first program
                    first_prog, first_desc = self.get_program_info(studies, programs[0])
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
                            'Multi Program': 'Yes' if len(programs) > 1 else 'No'
                        })
                        self.logger.info(f"Updated {accession}: '{current_prog}' -> '{first_prog}'")

                    # Handle additional programs (create duplicates)
                    for idx, prog_name in enumerate(programs[1:], start=2):
                        prog, prog_desc = self.get_program_info(studies, prog_name)
                        if not prog_desc and current_desc:
                            prog_desc = current_desc

                        new_study = study.copy()
                        new_study['Program'] = prog
                        if prog_desc:
                            new_study['Description'] = prog_desc

                        additional_studies.append(new_study)
                        updated_records.append({
                            'Accession': accession,
                            'Study Name': new_study.get('Study Name', ''),
                            'Old Program': current_prog,
                            'New Program': prog,
                            'Old Description': current_desc,
                            'New Description': prog_desc,
                            'Jira Status': jira_info['status'],
                            'Jira Summary': jira_info['summary'],
                            'Had Description': 'Yes' if current_desc.strip() else 'No',
                            'Multi Program': f'Yes (duplicate {idx}/{len(programs)})'
                        })
                        self.logger.info(f"Created duplicate {accession}: '{prog}' (program {idx}/{len(programs)})")

            else:
                # Study not in Jira - add empty Community fields for consistency
                if 'Community' not in study:
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

        return additional_studies, updated_records, not_in_jira

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
            # Load data
            self.logger.info("Loading Gen3 studies data...")
            gen3_studies = bdc_utils.load_json(self.input_file)
            self.logger.info(f"Loaded {len(gen3_studies)} studies from Gen3 data")

            jira_dict, jira_accessions = self.load_jira_data()

            # Track accessions
            gen3_accessions = {bdc_utils.extract_base_accession(s['Accession']) for s in gen3_studies if bdc_utils.extract_base_accession(s['Accession'])}

            # Update programs
            self.logger.info("\nUpdating program names from Jira...")
            additional, updated, not_in_jira = self.update_study_programs(gen3_studies, jira_dict)
            gen3_studies.extend(additional)

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
                    'Gen3 Program Name': jira_dict[base_acc]['program_name'],
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

            if studies_missing_desc:
                bdc_utils.save_json(studies_missing_desc, self.output_files['missing_desc'])

            if both_missing:
                bdc_utils.save_json(both_missing, self.output_files['both_missing'])

            if updated:
                bdc_utils.save_json(updated, self.output_files['update_report'])

            self.save_comprehensive_report(updated, not_in_jira, missing_from_gen3)

            # Summary
            self.logger.info("\n" + "="*80)
            self.logger.info("SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"Total Gen3 studies processed: {len(studies_with_desc)}")
            self.logger.info(f"Studies with program names updated: {len(updated)}")
            self.logger.info(f"Descriptions filled from same program: {len(filled)}")
            self.logger.info(f"Studies still missing description: {len(studies_missing_desc)}")
            self.logger.info(f"Studies in both Gen3 and Jira but missing description: {len(both_missing)}")
            self.logger.info(f"New studies (empty description) not in Jira: {len(not_in_jira)}")
            self.logger.info(f"Studies in Jira but not in Gen3: {len(missing_from_gen3)}")
            self.logger.info("="*80)
            self.logger.info("\nProcessing completed successfully!")

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
