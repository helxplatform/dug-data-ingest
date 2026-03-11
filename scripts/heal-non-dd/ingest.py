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

    report = []
    total_studies = total_sections = total_variables = 0

    for study_dir in sorted(input_path.glob("HDP*")):
        if not study_dir.is_dir():
            continue

        entry = {
            "dir": study_dir.name,
            "id": None,
            "error": None,
            "skipped": [],
            "n_studies": 0,
            "n_sections": 0,
            "n_variables": 0,
        }
        report.append(entry)

        metadata_file = study_dir / "metadata.yaml"
        try:
            with metadata_file.open() as f:
                raw = yaml.safe_load(f)
            if not isinstance(raw, dict):
                raise ValueError("not a YAML mapping")
            study_id = raw["id"]
            study_name = raw["name"]
        except FileNotFoundError:
            entry["error"] = "missing metadata.yaml"
            click.echo(f"  SKIP {study_dir.name}: missing metadata.yaml", err=True)
            continue
        except (yaml.YAMLError, KeyError, ValueError) as exc:
            entry["error"] = f"malformed metadata.yaml: {exc}"
            click.echo(f"  SKIP {study_dir.name}: {entry['error']}", err=True)
            continue

        entry["id"] = study_id
        description = raw.get("description", "")
        downloaded_from = raw.get("downloaded_from")
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
                    entry["skipped"].append(asset_file)
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

        if isinstance(downloaded_from, str):
            for obj in all_objects:
                if obj["type"] == "section":
                    obj["action"] = downloaded_from
        elif isinstance(downloaded_from, dict):
            for key, url in downloaded_from.items():
                prefix = f"{study_id}/assets/{key}"
                for obj in all_objects:
                    if (
                        obj["type"] == "section"
                        and obj.get("parent_type") == "study"
                        and (obj["id"] == prefix or obj["id"].startswith(prefix + "/"))
                    ):
                        obj["action"] = url

        study = {
            "id": study_id,
            "name": study_name,
            "description": description,
            "type": "study",
            "action": downloaded_from if isinstance(downloaded_from, str) else "",
            "section_list": [o["id"] for o in all_objects if o["type"] == "section"],
            "metadata": metadata,
        }

        objects = all_objects + [study]

        entry["n_studies"]   = sum(1 for o in objects if o["type"] == "study")
        entry["n_sections"]  = sum(1 for o in objects if o["type"] == "section")
        entry["n_variables"] = sum(1 for o in objects if o["type"] == "variable")
        total_studies   += entry["n_studies"]
        total_sections  += entry["n_sections"]
        total_variables += entry["n_variables"]

        out_file = output_path / f"{study_id}.json"
        with out_file.open("w") as f:
            json.dump(objects, f, indent=2)

        click.echo(f"Wrote {out_file}")

    # --- Summary report ---
    n_ok = sum(1 for e in report if e["error"] is None)
    n_err = len(report) - n_ok

    click.echo("")
    click.echo("=== Summary ===")
    click.echo("")
    click.echo(f"HDP directories found: {len(report)}  ({n_ok} OK, {n_err} errors)")

    errors = [e for e in report if e["error"]]
    if errors:
        click.echo("")
        click.echo("Errors:")
        for e in errors:
            click.echo(f"  {e['dir']:<12}  {e['error']}")

    all_skipped = [(e, f) for e in report for f in e["skipped"]]
    if all_skipped:
        click.echo("")
        click.echo("Skipped asset files (no handler):")
        for entry, asset_file in all_skipped:
            rel = str(pathlib.Path(entry["dir"]) / asset_file.relative_to(
                input_path / entry["dir"]
            ))
            click.echo(f"  {entry['dir']:<12}  {rel}  ({asset_file.suffix or 'no ext'})")
        click.echo(f"  ({len(all_skipped)} total)")

    click.echo("")
    click.echo("Objects created:")
    col = 12
    click.echo(f"  {'Study':<{col}}  {'Studies':>7}  {'Sections':>8}  {'Variables':>9}")
    for e in report:
        if e["error"]:
            click.echo(f"  {e['dir']:<{col}}  {'(error)':>7}")
        else:
            click.echo(f"  {e['id']:<{col}}  {e['n_studies']:>7}  {e['n_sections']:>8}  {e['n_variables']:>9}")
    click.echo(f"  {'TOTAL':<{col}}  {total_studies:>7}  {total_sections:>8}  {total_variables:>9}")


if __name__ == "__main__":
    main()
