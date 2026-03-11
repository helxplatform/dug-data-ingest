"""JSON (.json) format handler for heal-non-dd ingest."""
import json
from pathlib import Path

from formats import ExtractResult, slug


def _stringify(value) -> str:
    if isinstance(value, list) and all(not isinstance(v, (dict, list)) for v in value):
        return "; ".join(str(v) for v in value)
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _flatten(obj, path: tuple = ()):
    """Yield (key_path_tuple, value) for every leaf, recursing into dicts and lists of objects."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            current = path + (str(key),)
            if isinstance(value, dict):
                yield from _flatten(value, current)
            elif isinstance(value, list) and any(isinstance(v, (dict, list)) for v in value):
                yield from _flatten(value, current)
            else:
                yield current, value
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            current = path + (str(i),)
            if isinstance(item, (dict, list)):
                yield from _flatten(item, current)
            else:
                yield current, item


def extract(asset_path: Path, section_id: str, study_id: str) -> ExtractResult:
    with asset_path.open() as f:
        data = json.load(f)

    if not isinstance(data, dict):
        return ExtractResult(sections=[], variables=[], replace_file_section=False)

    variables = []
    seen_slugs: dict[str, int] = {}

    for key_path, value in _flatten(data):
        if value is None:
            continue
        base = "/".join(slug(p) for p in key_path)
        count = seen_slugs.get(base, 0)
        seen_slugs[base] = count + 1
        var_id = f"{section_id}/{base}" if count == 0 else f"{section_id}/{base}_{count + 1}"
        name = key_path[-1] if len(key_path) == 1 else ".".join(key_path)
        variables.append({
            "id": var_id,
            "name": name,
            "description": _stringify(value),
            "type": "variable",
            "data_type": "text",
            "parents": [section_id],
            "parent_type": "section",
        })

    return ExtractResult(sections=[], variables=variables, replace_file_section=False)
