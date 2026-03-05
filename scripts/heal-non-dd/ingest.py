"""
Ingest HEAL non-data-dictionary study assets into Dug Data Model v2 JSON files.

Usage:
    python ingest.py <input_dir> -o <output_dir>
"""
import json
import pathlib
import sys

import click
import yaml

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from formats import ExtractResult, get_handler


@click.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
@click.option(
    "--output", "-o", "output_dir",
    required=True,
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
    help="Output directory (must not already exist).",
)
def main(input_dir: str, output_dir: str) -> None:
    input_path = pathlib.Path(input_dir)
    output_path = pathlib.Path(output_dir)
    output_path.mkdir(parents=True)

    for study_dir in sorted(input_path.glob("HDP*")):
        if not study_dir.is_dir():
            continue

        metadata_file = study_dir / "metadata.yaml"
        with metadata_file.open() as f:
            raw = yaml.safe_load(f)

        study_id = raw["id"]
        study_name = raw["name"]
        description = raw.get("description", "")
        metadata = {k: v for k, v in raw.items() if k not in ("id", "name", "description")}

        all_objects = []
        assets_dir = study_dir / "assets"
        if assets_dir.exists():
            for asset_file in sorted(assets_dir.rglob("*")):
                if not asset_file.is_file():
                    continue
                section_id = str(pathlib.Path(study_dir.name) / asset_file.relative_to(study_dir))
                handler = get_handler(asset_file.suffix)
                if handler:
                    result = handler(asset_file, section_id, study_id)
                else:
                    result = ExtractResult(sections=[], variables=[], replace_file_section=False)

                if not result.replace_file_section:
                    file_section = {
                        "id": section_id,
                        "name": asset_file.name,
                        "description": asset_file.name,
                        "type": "section",
                        "parents": [study_id],
                        "parent_type": "study",
                        "variable_list": [v["id"] for v in result.variables],
                    }
                    all_objects.append(file_section)

                all_objects.extend(result.sections)
                all_objects.extend(result.variables)

        study = {
            "id": study_id,
            "name": study_name,
            "description": description,
            "type": "study",
            "section_list": [o["id"] for o in all_objects if o["type"] == "section"],
            "metadata": metadata,
        }

        objects = all_objects + [study]

        out_file = output_path / f"{study_id}.json"
        with out_file.open("w") as f:
            json.dump(objects, f, indent=2)

        click.echo(f"Wrote {out_file}")


if __name__ == "__main__":
    main()
