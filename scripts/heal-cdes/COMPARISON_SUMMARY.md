# Dug Data Model v2 Comparison Analysis

This document summarizes the three-pass comparison between two directories containing Dug Data Model v2 JSON files:
- `data/dug-data-model-2026jan20` (233 files)
- `data/heal-cdes` (238 files)

## Overview

All files with differences have been analyzed across 225 common files, with 43,134 total differences identified.

## Scripts

Three Python scripts were created to perform the analysis:

1. **`compare_pass1_missing_files.py`** - Identifies files missing in one directory or the other
2. **`compare_pass2_detailed_diff.py`** - Performs detailed JSON comparison with JSON Path locations
3. **`compare_pass3_common_patterns.py`** - Identifies common patterns and filters them out

## Results

### Pass 1: Missing Files

**Output:** `comparison_pass1_missing_files.csv`

- **Files only in dug-data-model-2026jan20:** 8 files
  - Examples: HDPCDE5661.json, HDPCDE5981.json, HDPCDE6056.json, HDPCDE6541.json, HDPCDE7076.json

- **Files only in heal-cdes:** 13 files
  - Examples: HDPCDE00001.json, HDPCDE00002.json, HDPCDE5091.json, HDPCDE5756.json, HDPCDE5986.json

- **Files in both directories:** 225 files

### Pass 2: Detailed Differences

**Output:** `comparison_pass2_detailed_diff.csv`

- **Total differences found:** 43,134
- **Files with differences:** 225 (all common files)
- **Files identical:** 0

The CSV contains the following columns:
- `filename` - Name of the JSON file
- `location` - JSON Path indicating where the difference is
- `difference_type` - Type of difference (see below)
- `dug_2026jan_value` - Value in dug-data-model-2026jan20
- `dug_2026jan_type` - Type of value in dug-data-model-2026jan20
- `heal_cdes_value` - Value in heal-cdes
- `heal_cdes_type` - Type of value in heal-cdes
- `dug_2026jan_context` - Parent object (one level up) from dug-data-model-2026jan20
- `heal_cdes_context` - Parent object (one level up) from heal-cdes

**Difference Types:**
- `key_only_in_dir1` - Field exists only in dug-data-model-2026jan20
- `key_only_in_dir2` - Field exists only in heal-cdes
- `value_mismatch` - Field exists in both but has different values
- `type_mismatch` - Field exists in both but has different data types
- `list_length_mismatch` - Arrays have different lengths
- `item_only_in_dir1` - Array element exists only in dir1
- `item_only_in_dir2` - Array element exists only in dir2

### Pass 3: Common Patterns

**Outputs:**
- `comparison_pass3_common_patterns.csv` - Common patterns with percentages
- `comparison_pass3_filtered_diff.csv` - Differences after filtering out common patterns

**Common Patterns (appearing in ≥20% of files):**

The top 15 most common patterns all appear in 99-100% of files:

1. **100% of files:** Metadata fields only in dug-data-model-2026jan20:
   - `instructions`
   - `short_description`
   - `crf_name`
   - `question_text`
   - `references`

2. **100% of files:** Parent references differ
   - dug-data-model-2026jan20 uses: `HEALCDE:craving-scale-3-item`
   - heal-cdes uses: `HDPCDE10006` (CDE ID)

3. **100% of files:** Section/CRF `id` and `description` values differ

4. **99.6% of files:** Section metadata differences:
   - `variable_list` only in dug-data-model-2026jan20
   - `drupal_id` only in heal-cdes

5. **98.7% of files:** Action URLs differ:
   - dug-data-model-2026jan20: `https://heal.nih.gov/files/CDEs/2024-11/...`
   - heal-cdes: `https://www.nih.gov/sites/default/files/CDEs/2026-01/...`

**After Filtering:**
- **Remaining differences:** 1,827 (removed 46,357 common differences)
- **Files with non-common differences:** 101 files

These remaining differences are file-specific variations such as:
- Expanded `permissible_values` (heal-cdes includes all intermediate values)
- Specific enum value differences
- Minor field variations unique to certain CDEs

**Context Columns:** Both Pass 2 and Pass 3 CSVs include `dug_2026jan_context` and `heal_cdes_context` columns that show the parent object (one level up from the difference location) from both directories. This makes it much easier to understand differences in context. For example, when comparing `permissible_values.6`, the context shows the entire `permissible_values` object from both files:

```
dug_2026jan_context: {"0": "No desire", "9": "Strong desire"}
heal_cdes_context: {"0": "No desire", "1": "1", ..., "9": "Strong desire"}
```

This immediately reveals that dug-data-model-2026jan20 only defines endpoint values while heal-cdes includes all intermediate values.

## Key Findings

### Structural Differences

1. **Metadata Richness:** dug-data-model-2026jan20 contains more descriptive metadata:
   - Instructions for data collection
   - Short descriptions
   - CRF (Case Report Form) names
   - Question text as presented to participants
   - Academic references

2. **Parent References:** Different parent referencing schemes:
   - dug-data-model-2026jan20 uses semantic IDs (e.g., `HEALCDE:craving-scale-3-item`)
   - heal-cdes uses CDE IDs (e.g., `HDPCDE10006`)

3. **Permissible Values:** heal-cdes tends to have more complete permissible_values:
   - For scales (0-9), dug-data-model-2026jan20 often shows only endpoints (0, 9)
   - heal-cdes typically includes all values (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

4. **URLs:** Different hosting locations for downloadable files:
   - dug-data-model-2026jan20: `heal.nih.gov/files/CDEs/2024-11/`
   - heal-cdes: `www.nih.gov/sites/default/files/CDEs/2026-01/`

5. **Data Types:** Some inconsistencies in data_type specifications:
   - Example: Craving Scale score is `number` in one, `integer` in the other

### Coverage Differences

- heal-cdes has 13 additional files (possibly newer CDEs)
- dug-data-model-2026jan20 has 8 files not in heal-cdes (possibly deprecated)

## Recommendations

1. **Decide on parent reference scheme:** Choose between semantic IDs vs. CDE IDs
2. **Metadata strategy:** Determine which metadata fields are essential
3. **Permissible values:** Standardize whether to include all values or just endpoints
4. **URL consolidation:** Use consistent hosting location for downloads
5. **Data type consistency:** Review and standardize data types across both sources
6. **File synchronization:** Investigate why certain files exist in only one directory

## Usage

To regenerate the analysis:

```bash
# Run all three passes
python3 compare_pass1_missing_files.py
python3 compare_pass2_detailed_diff.py
python3 compare_pass3_common_patterns.py
```

To adjust the "common pattern" threshold in Pass 3, edit the `MIN_PERCENTAGE` variable (default: 20%).
