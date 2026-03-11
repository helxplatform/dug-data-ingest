"""Word (.docx) format handler for heal-non-dd ingest."""
from pathlib import Path

from docx import Document

from formats import ExtractResult, headings_to_variables


def _is_heading(para) -> bool:
    if para.style.name.startswith("Heading"):
        return True
    runs = [r for r in para.runs if r.text.strip()]
    return bool(runs) and all(r.bold for r in runs)


def extract(asset_path: Path, section_id: str, study_id: str) -> ExtractResult:
    doc = Document(str(asset_path))
    items = []
    for para in doc.paragraphs:
        if _is_heading(para):
            items.append((True, para.text.strip()))
        elif para.text.strip():
            items.append((False, para.text.strip()))
    variables = headings_to_variables(items, section_id, asset_path.name)
    return ExtractResult(variables=variables)
