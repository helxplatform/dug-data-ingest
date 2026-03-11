"""
Format handler registry for heal-non-dd ingest.

Each handler module exports:
    extract(asset_path: Path, section_id: str, study_id: str) -> ExtractResult
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional


@dataclass
class ExtractResult:
    sections: list = field(default_factory=list)
    variables: list = field(default_factory=list)
    replace_file_section: bool = False


def get_handler(suffix: str) -> Optional[Callable]:
    """Return the extract function for the given file suffix, or None."""
    suffix = suffix.lower()
    if suffix == ".docx":
        from formats import docx
        return docx.extract
    if suffix == ".xlsx":
        from formats import xlsx
        return xlsx.extract
    if suffix == ".json":
        from formats import json as json_fmt
        return json_fmt.extract
    if suffix == ".pdf":
        from formats import pdf
        return pdf.extract
    return None
