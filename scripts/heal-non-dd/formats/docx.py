"""Word (.docx) format handler for heal-non-dd ingest."""
import re
from pathlib import Path

from docx import Document

from formats import ExtractResult


def _slug(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text


def extract(asset_path: Path, section_id: str, study_id: str) -> ExtractResult:
    doc = Document(str(asset_path))
    variables = []
    seen_slugs: dict[str, int] = {}

    current_heading = None
    current_body: list[str] = []

    def flush(heading: str, body: list[str]) -> None:
        base = _slug(heading)
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

    def is_heading(para) -> bool:
        if para.style.name.startswith("Heading"):
            return True
        runs = [r for r in para.runs if r.text.strip()]
        return bool(runs) and all(r.bold for r in runs)

    for para in doc.paragraphs:
        if is_heading(para):
            if current_heading is not None:
                flush(current_heading, current_body)
            current_heading = para.text.strip()
            current_body = []
        else:
            if current_heading is not None and para.text.strip():
                current_body.append(para.text.strip())

    if current_heading is not None:
        flush(current_heading, current_body)

    return ExtractResult(sections=[], variables=variables, replace_file_section=False)
