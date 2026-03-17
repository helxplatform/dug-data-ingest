#!/usr/bin/env python3
"""Clean/normalize dug-data-model-2 JSON files before comparison."""

import json
import pathlib
import click


@click.command()
@click.argument("input_dir", default="data/dug-data-model-2026jan20-copied",
                type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path))
@click.argument("output_dir", default="data/dug-data-model-2026jan20",
                type=click.Path(file_okay=False, path_type=pathlib.Path))
def main(input_dir, output_dir):
    """Clean dug-data-model-2 files to normalize content before comparison."""
    output_dir.mkdir(parents=True, exist_ok=True)

    json_files = list(input_dir.glob("*.json"))
    click.echo(f"Processing {len(json_files)} files from {input_dir} -> {output_dir}")

    for path in json_files:
        data = json.loads(path.read_text())
        data = clean_entries(data)
        (output_dir / path.name).write_text(json.dumps(data, indent=2))

    click.echo("Done.")


def clean_entries(entries):
    for entry in entries:
        metadata = entry.get("metadata", {})
        if isinstance(metadata.get("enum"), list):
            metadata["enum"] = [v.strip() if isinstance(v, str) else v
                                for v in metadata["enum"]]
    return entries


if __name__ == "__main__":
    main()
