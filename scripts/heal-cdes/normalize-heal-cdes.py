#!/usr/bin/env python3
"""Normalize heal-cdes JSON files and rename them to HDPCDE<drupal_id>.json format."""

import json
import pathlib
import click

XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@click.command()
@click.argument("input_dir", default="data/heal-cdes",
                type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path))
@click.argument("output_dir", default="data/heal-cdes-normalized",
                type=click.Path(file_okay=False, path_type=pathlib.Path))
def main(input_dir, output_dir):
    """Normalize heal-cdes files and rename them to HDPCDE<drupal_id>.json."""
    output_dir.mkdir(parents=True, exist_ok=True)

    json_files = list(input_dir.glob("*.json"))
    click.echo(f"Processing {len(json_files)} files from {input_dir} -> {output_dir}")

    for path in json_files:
        data = json.loads(path.read_text())
        data = clean_entries(data)
        output_filename = get_hdp_filename(data)
        (output_dir / output_filename).write_text(json.dumps(data, indent=2))

    click.echo("Done.")


def get_hdp_filename(entries):
    """Return HDPCDE<drupal_id>.json using the drupal_id of the xlsx URL in the last section entry."""
    section = entries[-1]
    for url_obj in section.get("metadata", {}).get("urls", []):
        if url_obj.get("mime-type") == XLSX_MIME_TYPE:
            drupal_id = url_obj.get("drupal_id")
            if drupal_id:
                return f"HDPCDE{drupal_id}.json"
    raise ValueError(
        f"No xlsx URL with a drupal_id found in last entry of file (id={section.get('id')!r})"
    )


def clean_entries(entries):
    # Build index: section_id -> [variable_id, ...] (in file order)
    section_variables: dict[str, list[str]] = {}
    for entry in entries:
        if entry.get("type") == "variable":
            for parent_id in entry.get("parents", []):
                section_variables.setdefault(parent_id, []).append(entry["id"])

    for entry in entries:
        # Strip trailing/leading whitespace from section names
        if entry.get("type") == "section":
            if isinstance(entry.get("name"), str):
                entry["name"] = entry["name"].strip()
            # Populate missing variable_list
            if "variable_list" not in entry:
                entry["variable_list"] = section_variables.get(entry.get("id", ""), [])

        metadata = entry.get("metadata", {})
        if isinstance(metadata.get("enum"), list):
            metadata["enum"] = [v.strip() if isinstance(v, str) else v
                                for v in metadata["enum"]]
    return entries


if __name__ == "__main__":
    main()
