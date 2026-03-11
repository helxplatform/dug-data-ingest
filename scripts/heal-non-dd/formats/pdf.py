"""PDF (.pdf) format handler for heal-non-dd ingest."""
from collections import defaultdict
from pathlib import Path
from statistics import median

import pdfplumber

from formats import ExtractResult, headings_to_variables


def _extract_lines(pdf):
    """Return (lines, body_size) where lines is a list of {text, avg_size, is_bold} dicts."""
    all_chars = []
    for page in pdf.pages:
        all_chars.extend(page.chars)

    if not all_chars:
        return [], 12.0

    # Compute body font size (median is robust to large title/header outliers)
    sizes = [c["size"] for c in all_chars if c.get("size")]
    body_size = median(sizes) if sizes else 12.0

    # Group chars into lines by bucketing top coordinate (2pt tolerance handles
    # slight vertical offsets from superscripts and floating-point imprecision)
    buckets = defaultdict(list)
    for c in all_chars:
        bucket = round(c["top"] / 2) * 2
        buckets[bucket].append(c)

    lines = []
    for top_key in sorted(buckets):
        line_chars = sorted(buckets[top_key], key=lambda c: c["x0"])
        text = "".join(c["text"] for c in line_chars).strip()
        if not text:
            continue
        char_sizes = [c["size"] for c in line_chars if c.get("size")]
        avg_size = sum(char_sizes) / len(char_sizes) if char_sizes else body_size
        fontnames = [c.get("fontname", "") for c in line_chars]
        is_bold = any(
            "Bold" in fn or "bold" in fn or "-BD" in fn or ",B" in fn
            for fn in fontnames
        )
        lines.append({"text": text, "avg_size": avg_size, "is_bold": is_bold})

    return lines, body_size


def _is_heading(line: dict, body_size: float) -> bool:
    text = line["text"]
    if len(text) > 120:
        return False
    # Larger font size than body (title pages, chapter headings)
    if line["avg_size"] > body_size * 1.15:
        return True
    # Bold at body size (common in formal reports and methods docs)
    if line["is_bold"] and line["avg_size"] >= body_size * 0.95:
        return True
    return False


def extract(asset_path: Path, section_id: str, study_id: str) -> ExtractResult:
    with pdfplumber.open(str(asset_path)) as pdf:
        lines, body_size = _extract_lines(pdf)

    if not lines:
        # Image-only / scanned PDF — no text layer
        return ExtractResult()

    items = [(_is_heading(line, body_size), line["text"]) for line in lines]
    variables = headings_to_variables(items, section_id, asset_path.name)
    return ExtractResult(variables=variables)
