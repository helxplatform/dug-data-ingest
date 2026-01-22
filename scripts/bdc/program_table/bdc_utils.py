#!/usr/bin/env python3
"""
Shared utilities for BDC data ingestion scripts.

This module provides common functionality used across multiple scripts:
- File I/O operations (JSON loading/saving)
- Logging setup
- Data validation and transformation
- Path management
"""

import json
import logging
import sys
import os
from datetime import datetime
from typing import Any, Dict, List, Optional


def setup_logging(log_file: str, log_level: int = logging.INFO) -> logging.Logger:
    """
    Set up logging configuration with both file and console handlers.

    Args:
        log_file: Path to the log file
        log_level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    # Remove existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=log_level,
        format=logging_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def load_json(file_path: str) -> Any:
    """
    Load data from a JSON file.

    Args:
        file_path: Path to the JSON file

    Returns:
        Parsed JSON data

    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file is not valid JSON
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(data: Any, file_path: str, indent: Optional[int] = 2, minify: bool = False) -> None:
    """
    Save data to a JSON file.

    Args:
        data: Data to save
        file_path: Output file path
        indent: JSON indentation (default: 2, ignored if minify=True)
        minify: If True, save without whitespace
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'w', encoding='utf-8') as f:
        if minify:
            json.dump(data, f, separators=(',', ':'))
        else:
            json.dump(data, f, indent=indent)


def clean_json_newlines(data: Any) -> Any:
    """
    Recursively remove newlines from all string values in JSON data.

    Args:
        data: JSON data (dict, list, str, or primitive)

    Returns:
        Cleaned data with newlines removed from strings
    """
    if isinstance(data, str):
        return data.replace('\n', '')
    elif isinstance(data, list):
        return [clean_json_newlines(item) for item in data]
    elif isinstance(data, dict):
        return {key: clean_json_newlines(value) for key, value in data.items()}
    else:
        return data


def extract_base_accession(accession: str) -> Optional[str]:
    """
    Extract base accession from full accession string.

    Examples:
        phs000123.v1.p1.c1 -> phs000123
        phs000456.v2.p2 -> phs000456
        other_id -> other_id

    Args:
        accession: Full accession string

    Returns:
        Base accession or None if invalid
    """
    if not accession or not isinstance(accession, str):
        return None
    return accession.split('.')[0] if accession.startswith('phs') else accession


def validate_study(study: Dict[str, Any], required_fields: List[str]) -> tuple[bool, Optional[str]]:
    """
    Validate that a study has all required fields with non-empty values.

    Args:
        study: Study dictionary
        required_fields: List of required field names

    Returns:
        Tuple of (is_valid, error_message)
    """
    missing_fields = []

    for field in required_fields:
        if not study.get(field) or str(study[field]).strip() == '':
            missing_fields.append(field)

    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    return True, None


def generate_timestamp() -> str:
    """
    Generate a timestamp string for file naming.

    Returns:
        Timestamp in format YYYYMMDD_HHMMSS
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dir_exists(directory: str) -> None:
    """
    Create directory if it doesn't exist.

    Args:
        directory: Directory path to create
    """
    os.makedirs(directory, exist_ok=True)


def build_study_dict(accession: str, study_name: str, program: str, description: str = "", released: str = "", subjects_count: str = "", doi_tombstone: Any = "") -> Dict[str, Any]:
    """
    Build a standardized study dictionary.

    Args:
        accession: Study accession ID
        study_name: Study name
        program: Program name
        description: Program description (optional)
        released: Release status (optional)
        subjects_count: Number of subjects (optional)
        doi_tombstone: DOI tombstone status (optional)

    Returns:
        Standardized study dictionary
    """
    return {
        'Accession': accession,
        'Study Name': study_name,
        'Program': program,
        'Description': description,
        'Released': released,
        'Subjects Count': subjects_count,
        'DOI Tombstone': doi_tombstone
    }


def sort_studies_by_description(studies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sort studies with empty descriptions at the bottom.

    Args:
        studies: List of study dictionaries

    Returns:
        Sorted list of studies
    """
    return sorted(studies, key=lambda x: (x.get('Description', '') == '', x.get('Description', '')))


def separate_studies_by_description(studies: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Separate studies into those with and without descriptions.

    Args:
        studies: List of study dictionaries

    Returns:
        Tuple of (studies_with_desc, studies_without_desc)
    """
    studies_with_desc = [s for s in studies if s.get('Description', '').strip()]
    studies_without_desc = [s for s in studies if not s.get('Description', '').strip()]
    return studies_with_desc, studies_without_desc


def write_report_header(file_handle, title: str, width: int = 100) -> None:
    """
    Write a formatted header to a report file.

    Args:
        file_handle: Open file handle
        title: Header title
        width: Width of the separator line
    """
    file_handle.write("=" * width + "\n")
    file_handle.write(f"{title}\n")
    file_handle.write("=" * width + "\n\n")


def write_report_section(file_handle, title: str, count: int, width: int = 100) -> None:
    """
    Write a formatted section header to a report file.

    Args:
        file_handle: Open file handle
        title: Section title
        count: Count to display
        width: Width of the separator line
    """
    file_handle.write("\n" + "=" * width + "\n")
    file_handle.write(f"{title}\n")
    file_handle.write("=" * width + "\n\n")
    file_handle.write(f"Total: {count} studies\n\n")


class FilePathManager:
    """Manages file paths for BDC data processing."""

    def __init__(self, base_dir: str, timestamp: Optional[str] = None):
        """
        Initialize file path manager.

        Args:
            base_dir: Base directory for all data
            timestamp: Optional timestamp string (generated if not provided)
        """
        self.base_dir = base_dir
        self.timestamp = timestamp or generate_timestamp()
        self.bdc_dir = os.path.join(base_dir, "studies_on_bdc_portal")
        self.gen3_dir = os.path.join(base_dir, "studies_on_gen3_portal")
        self.program_table_dir = os.path.join(base_dir, "program_table")

        # Ensure directories exist
        ensure_dir_exists(self.bdc_dir)
        ensure_dir_exists(self.gen3_dir)
        ensure_dir_exists(self.program_table_dir)

    def get_bdc_path(self, filename: str) -> str:
        """Get path in BDC directory."""
        return os.path.join(self.bdc_dir, filename)

    def get_gen3_path(self, filename: str) -> str:
        """Get path in Gen3 directory."""
        return os.path.join(self.gen3_dir, filename)

    def get_program_table_path(self, filename: str) -> str:
        """Get path in program table directory."""
        return os.path.join(self.program_table_dir, filename)

    def get_timestamped_path(self, directory: str, base_name: str, extension: str) -> str:
        """
        Get timestamped file path.

        Args:
            directory: Directory path
            base_name: Base filename (without extension)
            extension: File extension (with or without dot)

        Returns:
            Full path with timestamp
        """
        if not extension.startswith('.'):
            extension = f'.{extension}'
        filename = f"{base_name}_{self.timestamp}{extension}"
        return os.path.join(directory, filename)


if __name__ == "__main__":
    # Test utilities
    print("BDC Utils Module - Test Functions")
    print(f"Timestamp: {generate_timestamp()}")
    print(f"Base accession: {extract_base_accession('phs000123.v1.p1.c1')}")
    print(f"Validation: {validate_study({'Accession': 'phs001', 'Study Name': 'Test'}, ['Accession', 'Study Name'])}")
