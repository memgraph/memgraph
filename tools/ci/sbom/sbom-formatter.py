#!/usr/bin/env python3
import json
import os
import sys

from rich.console import Console
from rich.table import Table


def main():
    if len(sys.argv) != 2:
        print("Usage: sbom_to_table.py <sbom.json>")
        sys.exit(1)

    input_file = sys.argv[1]

    # Output filename: same base, .txt extension
    base = os.path.splitext(input_file)[0]
    output_file = base + ".txt"

    # Load JSON SBOM
    try:
        with open(input_file, "r") as f:
            sbom = json.load(f)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        sys.exit(1)

    components = sbom.get("components", [])

    # Extract needed fields
    rows = []
    for c in components:
        name = c.get("name", "")
        ctype = c.get("type", "")
        version = c.get("version", "")
        item = (name, ctype, version)
        if item not in rows:
            rows.append(item)

    # Sort by type, then name
    rows.sort(key=lambda r: (r[1] or "", r[0] or ""))

    # Build rich table
    table = Table(show_header=True, header_style="bold")
    table.add_column("name", style="bold", overflow="fold")
    table.add_column("type", overflow="fold")
    table.add_column("version", overflow="fold")

    for name, ctype, version in rows:
        table.add_row(name, ctype, version)

    # Render table to text
    console = Console(record=True)
    console.print(table)
    table_text = console.export_text(clear=False)

    # Write to txt file
    with open(output_file, "w") as f:
        f.write(table_text)

    print(f"Wrote table to {output_file}")


if __name__ == "__main__":
    main()
