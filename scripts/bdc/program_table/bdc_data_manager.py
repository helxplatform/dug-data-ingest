#!/usr/bin/env python3
"""
Core data management for BDC and Gen3 data operations.

This module handles:
- API interactions with BDC and Gen3
- Data downloading and processing
- Study validation and transformation
"""

import requests
import urllib.parse
import re
from typing import Dict, List, Any, Optional
import bdc_utils


class BDCDataManager:
    """Manages interactions with BDC API."""

    def __init__(self, base_url: str, logger: Optional[Any] = None):
        """
        Initialize BDC data manager.

        Args:
            base_url: BDC API base URL
            logger: Optional logger instance
        """
        self.base_url = base_url
        self.logger = logger

    def fetch_program_list(self) -> List[Dict[str, Any]]:
        """
        Fetch list of all programs from BDC.

        Returns:
            List of program dictionaries
        """
        url = f"{self.base_url}/program_list"
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("result", [])

    def fetch_program_studies(self, program_name: str) -> List[Dict[str, Any]]:
        """
        Fetch all studies for a specific program.

        Args:
            program_name: Name of the program

        Returns:
            List of study dictionaries
        """
        url = f"{self.base_url}/search_program"
        params = {"program_name": program_name}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get("result", [])

    def fetch_all_studies(self, show_detailed_studies: bool = False) -> tuple[List[Dict[str, str]], Dict[str, str]]:
        """
        Fetch all programs and their studies from BDC.

        Args:
            show_detailed_studies: If True, log individual study details

        Returns:
            Tuple of (studies_list, program_descriptions_dict)
        """
        programs = self.fetch_program_list()
        program_descriptions = {p["key"]: p.get("description", "") for p in programs}

        studies = []
        total_studies = 0

        for program in programs:
            program_name = program["key"]
            program_studies = self.fetch_program_studies(program_name)
            study_count = len(program_studies)
            total_studies += study_count

            if self.logger:
                self.logger.info(f"Program: {program_name}, Studies: {study_count}")

            for study in program_studies:
                studies.append(bdc_utils.build_study_dict(
                    accession=study.get('collection_id', 'N/A'),
                    study_name=study.get('collection_name', 'N/A'),
                    program=program_name,
                    description=program_descriptions.get(program_name, "")
                ))

        if self.logger:
            self.logger.info(f"Total programs: {len(programs)}, Total studies: {total_studies}")

        return studies, program_descriptions


class Gen3DataManager:
    """Manages interactions with Gen3 API."""

    def __init__(self, base_url: str, download_limit: int = 50, logger: Optional[Any] = None):
        """
        Initialize Gen3 data manager.

        Args:
            base_url: Gen3 API base URL
            download_limit: Number of records per API call
            logger: Optional logger instance
        """
        self.base_url = base_url
        self.download_limit = download_limit
        self.logger = logger

    def download_paginated_list(self, url: str) -> List[str]:
        """
        Download a paginated list from Gen3 API.

        Args:
            url: API endpoint URL

        Returns:
            Complete list of all items
        """
        complete_list = []
        offset = 0

        while True:
            paginated_url = f"{url}&limit={self.download_limit}&offset={offset}"

            try:
                response = requests.get(paginated_url)
                response.raise_for_status()
                partial_list = response.json()
                complete_list.extend(partial_list)

                if len(partial_list) < self.download_limit:
                    break

                offset += self.download_limit

            except requests.exceptions.RequestException as e:
                if self.logger:
                    self.logger.error(f"Error downloading from Gen3: {e}")
                raise

        return complete_list

    def fetch_study_metadata(self, study_id: str) -> Dict[str, Any]:
        """
        Fetch metadata for a single study.

        Args:
            study_id: Study identifier

        Returns:
            Study metadata dictionary
        """
        url = urllib.parse.urljoin(self.base_url, f'/mds/metadata/{study_id}')
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def extract_study_info(self, study_id: str) -> Optional[Dict[str, str]]:
        """
        Extract and normalize study information from Gen3 metadata.

        Args:
            study_id: Study identifier

        Returns:
            Dictionary with normalized study information or None if error
        """
        try:
            study_metadata = self.fetch_study_metadata(study_id)

            if 'gen3_discovery' not in study_metadata:
                return None

            gen3_discovery = study_metadata['gen3_discovery']

            # Extract released status
            released = gen3_discovery.get('released', '').strip()

            # Extract subjects count
            subjects_count = str(gen3_discovery.get('_subjects_count', ''))

            # Extract DOI tombstone status
            doi_tombstone = gen3_discovery.get('doi_tombstone', '')

            # Extract study name (try multiple fields)
            study_name = (
                gen3_discovery.get('full_name') or
                gen3_discovery.get('name') or
                gen3_discovery.get('short_name') or
                '(no name)'
            )

            # Extract program names from authz field
            program_names = []
            authz = gen3_discovery.get('authz', '')
            if authz:
                try:
                    match = re.fullmatch(r'^/programs/(.*)/projects/(.*)$', authz)
                    if match:
                        program_names.append(match.group(1))
                except Exception:
                    pass

            return bdc_utils.build_study_dict(
                accession=study_id,
                study_name=study_name,
                program='|'.join(sorted(set(filter(None, program_names)))),
                description="",  # Description will be filled from BDC data
                released=released,
                subjects_count=subjects_count,
                doi_tombstone=doi_tombstone
            )

        except requests.exceptions.RequestException as e:
            if self.logger:
                self.logger.error(f"Error processing study {study_id}: {e}")
            return None

    def fetch_all_studies(self) -> List[Dict[str, str]]:
        """
        Fetch all studies from Gen3 using the metadata endpoint with data=True.

        Returns:
            List of study dictionaries
        """
        # Use the new URL format with data=True to get all metadata in one call
        mds_url = urllib.parse.urljoin(
            self.base_url,
            '/mds/metadata?data=True&_guid_type=discovery_metadata'
        )

        if self.logger:
            self.logger.info(f"Fetching Gen3 metadata from: {mds_url}")

        # Download all metadata with pagination
        all_metadata = {}
        offset = 0

        while True:
            paginated_url = f"{mds_url}&limit={self.download_limit}&offset={offset}"

            try:
                response = requests.get(paginated_url)
                response.raise_for_status()
                partial_data = response.json()

                if not partial_data:
                    break

                # The response is a dict with study IDs as keys
                all_metadata.update(partial_data)

                if len(partial_data) < self.download_limit:
                    break

                offset += self.download_limit

            except requests.exceptions.RequestException as e:
                if self.logger:
                    self.logger.error(f"Error downloading from Gen3: {e}")
                raise

        if self.logger:
            self.logger.info(f"Found {len(all_metadata)} total studies in Gen3")

        # Process metadata
        studies = []

        for study_id, metadata in sorted(all_metadata.items()):
            if not metadata or not isinstance(metadata, dict):
                continue

            gen3_discovery = metadata.get('gen3_discovery', {})
            if not gen3_discovery:
                continue

            # Extract released status
            released = gen3_discovery.get('released', '').strip()

            # Extract subjects count
            subjects_count = str(gen3_discovery.get('_subjects_count', ''))

            # Extract DOI tombstone status
            doi_tombstone = gen3_discovery.get('doi_tombstone', '')

            # Extract study name (try multiple fields)
            study_name = (
                gen3_discovery.get('full_name') or
                gen3_discovery.get('name') or
                gen3_discovery.get('short_name') or
                '(no name)'
            )

            # Extract program names from authz field
            program_names = []
            authz = gen3_discovery.get('authz', '')
            if authz:
                try:
                    match = re.fullmatch(r'^/programs/(.*)/projects/(.*)$', authz)
                    if match:
                        program_names.append(match.group(1))
                except Exception:
                    pass

            study_info = bdc_utils.build_study_dict(
                accession=study_id,
                study_name=study_name,
                program='|'.join(sorted(set(filter(None, program_names)))),
                description="",  # Description will be filled from BDC data
                released=released,
                subjects_count=subjects_count,
                doi_tombstone=doi_tombstone
            )
            studies.append(study_info)

        if self.logger:
            # Count release statuses and subjects counts
            released_yes = sum(1 for s in studies if s.get('Released', '').lower() == 'yes')
            released_other = len(studies) - released_yes

            # Count subjects
            with_subjects = sum(1 for s in studies if s.get('Subjects Count', '').strip() and s.get('Subjects Count', '').strip() != '0')
            without_subjects = len(studies) - with_subjects

            # Count DOI tombstone statuses (checking for string 'True')
            doi_tombstone_true = sum(1 for s in studies if s.get('DOI Tombstone') == 'True')
            doi_tombstone_other = len(studies) - doi_tombstone_true

            self.logger.info(f"Downloaded {len(studies)} studies")
            self.logger.info(f"  Released=Yes: {released_yes}, Other: {released_other}")
            self.logger.info(f"  With subjects (>0): {with_subjects}, Without subjects (0 or empty): {without_subjects}")
            self.logger.info(f"  DOI Tombstone='True': {doi_tombstone_true}, Other/Empty: {doi_tombstone_other}")

        return studies


class StudyMerger:
    """Handles merging and comparison of study data from different sources."""

    def __init__(self, logger: Optional[Any] = None):
        """
        Initialize study merger.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger

    def merge_studies(
        self,
        reference_studies: List[Dict[str, str]],
        target_studies: List[Dict[str, str]]
    ) -> tuple[List[Dict[str, str]], List[str], List[str]]:
        """
        Merge two study lists, updating target with info from reference.

        Args:
            reference_studies: Reference studies (e.g., from BDC)
            target_studies: Target studies to update (e.g., from Gen3)

        Returns:
            Tuple of:
                - merged_studies: Combined and updated study list
                - new_accessions: Accessions in target but not in reference
                - missing_accessions: Accessions in reference but not in target
        """
        # Create lookup dictionary for reference studies
        reference_dict = {}
        reference_accessions = set()

        for study in reference_studies:
            base_acc = bdc_utils.extract_base_accession(study['Accession'])
            if base_acc:
                reference_dict[base_acc] = study
                reference_accessions.add(base_acc)

        # Process target studies
        existing_records = []
        new_records = []
        target_accessions = set()

        for study in target_studies:
            base_acc = bdc_utils.extract_base_accession(study['Accession'])
            if base_acc:
                target_accessions.add(base_acc)

                if base_acc in reference_dict:
                    # Update with reference information (preserve Released, Subjects Count, DOI Tombstone from Gen3)
                    released = study.get('Released', '')
                    subjects_count = study.get('Subjects Count', '')
                    doi_tombstone = study.get('DOI Tombstone', '')
                    study['Program'] = reference_dict[base_acc]['Program']
                    study['Description'] = reference_dict[base_acc]['Description']
                    study['Released'] = released
                    study['Subjects Count'] = subjects_count
                    study['DOI Tombstone'] = doi_tombstone
                    existing_records.append(study)
                else:
                    # New record not in reference (keep Released field from Gen3)
                    new_records.append(study)

        # Add Training program studies from reference (BDC) that are missing in target (Gen3)
        # Iterate through original reference_studies list to handle duplicate accessions
        training_from_bdc = []
        for study in reference_studies:
            if study['Program'] == 'Training':
                base_acc = bdc_utils.extract_base_accession(study['Accession'])
                if base_acc and base_acc not in target_accessions:
                    training_from_bdc.append(study)

        # Combine: existing records first, new Gen3-only records, then Training from BDC
        merged_studies = existing_records + new_records + training_from_bdc

        # Find missing accessions
        missing_accessions = list(reference_accessions - target_accessions)
        new_accessions = [rec['Accession'] for rec in new_records]

        if self.logger:
            self.logger.info(f"Merged studies: {len(merged_studies)} total")
            self.logger.info(f"New in target (Gen3 only): {len(new_accessions)}")
            if training_from_bdc:
                self.logger.info(f"Added Training from BDC: {len(training_from_bdc)}")
            self.logger.info(f"Missing from target: {len(missing_accessions)}")

        return merged_studies, new_accessions, missing_accessions

    def filter_by_doi_tombstone(
        self,
        studies: List[Dict[str, str]],
        return_excluded: bool = False
    ) -> tuple:
        """
        Filter out studies where DOI Tombstone is 'True' (string).

        Args:
            studies: List of study dictionaries
            return_excluded: If True, also return list of excluded studies

        Returns:
            If return_excluded is False: Tuple of (filtered_studies, excluded_count)
            If return_excluded is True: Tuple of (filtered_studies, excluded_count, excluded_studies)
        """
        filtered_studies = []
        excluded_studies = []

        for study in studies:
            doi_tombstone = study.get('DOI Tombstone', '')

            # Exclude if DOI tombstone is the string 'True'
            # The field is a string in Gen3 metadata, not a boolean
            if doi_tombstone == 'True':
                excluded_studies.append(study)
            else:
                filtered_studies.append(study)

        if self.logger:
            self.logger.info(f"Filtered by DOI tombstone (exclude 'True'): {len(filtered_studies)} kept, {len(excluded_studies)} excluded")

        if return_excluded:
            return filtered_studies, len(excluded_studies), excluded_studies
        return filtered_studies, len(excluded_studies)

    def filter_by_subjects_count(
        self,
        studies: List[Dict[str, str]]
    ) -> tuple[List[Dict[str, str]], int]:
        """
        Filter studies to only include those with Subjects Count > 0 and not empty.

        Args:
            studies: List of study dictionaries

        Returns:
            Tuple of (filtered_studies, excluded_count)
        """
        filtered_studies = []
        excluded_count = 0

        for study in studies:
            subjects_count_str = study.get('Subjects Count', '').strip()

            # Keep if subjects count is not empty and not "0"
            if subjects_count_str and subjects_count_str != '0':
                filtered_studies.append(study)
            else:
                excluded_count += 1

        if self.logger:
            self.logger.info(f"Filtered by subjects count (>0): {len(filtered_studies)} kept, {excluded_count} excluded")

        return filtered_studies, excluded_count

    def filter_valid_studies(
        self,
        studies: List[Dict[str, str]],
        required_fields: List[str] = None
    ) -> tuple[List[Dict[str, str]], int]:
        """
        Filter out studies that don't meet validation criteria.

        Args:
            studies: List of study dictionaries
            required_fields: Fields that must be non-empty (default: Accession, Study Name, Program)

        Returns:
            Tuple of (valid_studies, skipped_count)
        """
        if required_fields is None:
            required_fields = ['Accession', 'Study Name', 'Program']

        valid_studies = []
        skipped_count = 0

        for study in studies:
            is_valid, _ = bdc_utils.validate_study(study, required_fields)
            if is_valid:
                valid_studies.append(study)
            else:
                skipped_count += 1

        if self.logger:
            self.logger.info(f"Valid studies: {len(valid_studies)}, Skipped: {skipped_count}")

        return valid_studies, skipped_count


if __name__ == "__main__":
    print("BDC Data Manager Module - Ready for import")
