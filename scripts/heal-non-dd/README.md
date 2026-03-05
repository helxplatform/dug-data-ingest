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
  - One DugSection for every asset.
