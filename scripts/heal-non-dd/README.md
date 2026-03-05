# HEAL Non-Data Dictionary content

There are many sources of HEAL information that isn't formatted like a data dictionary. This directory
is an attempt to collect those sources and figure out techniques for converting them into Dug Data Model v2.

Input:
- The non-data dictionary content from the HEAL project, stored in a private GitHub repository.
- The Dug Data Model v2 from https://github.com/helxplatform/dug/blob/425735c772b8cb158ed9f13c65bbe9f3fa0482c8/src/dug/core/parsers/_base.py
  converted to JSON Schema.

Output:
- One file for every study (e.g. HDP01130.json) consists of a list of Dug Data Model v2 objects:
  - One DugStudy with the information from the metadata.yaml file.
  - One DugSection for every asset (or one DugSection per worksheet for Excel files).
  - DugVariables extracted from Word headings and Excel text cells.

## Supported Formats

### Microsoft Word (.docx)

Each heading in the document (such as "Introduction" or "Methods") becomes a
separate searchable entry in Dug. The text that follows the heading — up to the
next heading — is used as that entry's description, making its content
discoverable by keyword search. A Word document with ten headings produces ten
searchable entries, all linked back to the study.

### Microsoft Excel (.xlsx)

Each worksheet tab in the workbook becomes its own searchable section in Dug.
Within each sheet, every cell that contains a text value (not a number or a
formula result) becomes a searchable entry, identified by its cell coordinates
(such as "A1" or "C7"). This captures variable names, category labels, protocol
notes, and other descriptive content that researchers typically put in the
text-heavy cells of a research spreadsheet.
