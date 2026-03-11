"""
Format handler registry for heal-non-dd ingest.

Each handler module exports:
    extract(asset_path: Path, section_id: str, study_id: str) -> ExtractResult
"""
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional


@dataclass
class ExtractResult:
    sections: list = field(default_factory=list)
    variables: list = field(default_factory=list)
    replace_file_section: bool = False


def slug(text: str) -> str:
    """Convert text to a URL-safe slug for use in object IDs."""
    text = str(text).lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text


def headings_to_variables(
    items: list[tuple[bool, str]],
    section_id: str,
    fallback_name: str,
) -> list[dict]:
    """Convert a sequence of (is_heading, text) pairs into DugVariable dicts.

    Each heading starts a new variable whose description is the body text that
    follows it (joined by double newlines). If no headings are found, all text
    is collapsed into a single variable named *fallback_name*.
    """
    variables: list[dict] = []
    seen_slugs: dict[str, int] = {}

    def flush(heading: str, body: list[str]) -> None:
        base = slug(heading)
        count = seen_slugs.get(base, 0)
        seen_slugs[base] = count + 1
        var_id = f"{section_id}/{base}" if count == 0 else f"{section_id}/{base}_{count + 1}"
        variables.append({
            "id": var_id,
            "name": heading,
            "description": "\n\n".join(body),
            "type": "variable",
            "data_type": "text",
            "parents": [section_id],
            "parent_type": "section",
        })

    current_heading: Optional[str] = None
    current_body: list[str] = []
    pre_heading_body: list[str] = []

    for is_hdg, text in items:
        if is_hdg:
            if current_heading is not None:
                flush(current_heading, current_body)
            current_heading = text
            current_body = []
        else:
            (current_body if current_heading is not None else pre_heading_body).append(text)

    if current_heading is not None:
        flush(current_heading, current_body)
    elif pre_heading_body:
        flush(fallback_name, pre_heading_body)

    return variables


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
