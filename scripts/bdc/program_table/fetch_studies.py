#!/usr/bin/env python3
"""
Fetch and merge study data from BDC and Gen3 portals for program table ingestion.

PURPOSE:
    Generates the program table file used by the BDC portal's program search/filter feature.
    This table allows users to filter studies by program affiliation on the BDC portal.

WORKFLOW:
    1. Fetches studies from BDC portal (with program associations)
    2. Fetches studies from Gen3 portal
    3. Merges and filters the datasets
    4. Generates program table file ready for chart ingestion

OUTPUT:
    - cleaned_studies_on_gen3_sortedfile_to_upload_chart.json (program table file)

Usage:
    python fetch_studies.py --output-dir /path/to/output [--bdc-url URL] [--gen3-url URL]
"""

import argparse
import sys
from pathlib import Path

import bdc_utils
import bdc_data_manager


def main():
    parser = argparse.ArgumentParser(
        description='Fetch and merge study data from BDC and Gen3 portals',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--output-dir',
        required=True,
        help='Output directory for generated files'
    )
    parser.add_argument(
        '--bdc-url',
        default='https://search-dev.biodatacatalyst.renci.org/search-api',
        help='BDC API base URL (default: dev endpoint)'
    )
    parser.add_argument(
        '--gen3-url',
        default='https://gen3.biodatacatalyst.nhlbi.nih.gov/',
        help='Gen3 API base URL'
    )
    parser.add_argument(
        '--gen3-limit',
        type=int,
        default=50,
        help='Gen3 API pagination limit (default: 50)'
    )
    parser.add_argument(
        '--show-details',
        action='store_true',
        help='Show detailed study information in logs'
    )

    args = parser.parse_args()

    # Setup paths
    output_dir = Path(args.output_dir)
    path_manager = bdc_utils.FilePathManager(str(output_dir))

    # Setup logging
    log_file = Path(output_dir) / "data_pipeline.log"
    logger = bdc_utils.setup_logging(str(log_file))

    logger.info("="*80)
    logger.info("BDC Data Pipeline - Fetch Studies")
    logger.info("="*80)
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"BDC URL: {args.bdc_url}")
    logger.info(f"Gen3 URL: {args.gen3_url}")
    logger.info("")

    try:
        # Step 1: Fetch BDC studies
        logger.info("Step 1: Fetching studies from BDC portal...")
        bdc_manager = bdc_data_manager.BDCDataManager(args.bdc_url, logger)
        bdc_studies, program_descriptions = bdc_manager.fetch_all_studies(args.show_details)

        bdc_file = path_manager.get_bdc_path("bdc_studies.json")
        bdc_utils.save_json(bdc_studies, bdc_file)
        logger.info(f"Saved {len(bdc_studies)} BDC studies to: {bdc_file}")

        # Step 2: Fetch Gen3 studies
        logger.info("\nStep 2: Fetching studies from Gen3 portal...")
        gen3_manager = bdc_data_manager.Gen3DataManager(args.gen3_url, args.gen3_limit, logger)
        gen3_raw_studies = gen3_manager.fetch_all_studies()

        gen3_raw_file = path_manager.get_gen3_path("raw_studies_on_gen3.json")
        bdc_utils.save_json(gen3_raw_studies, gen3_raw_file)
        logger.info(f"Saved {len(gen3_raw_studies)} Gen3 studies to: {gen3_raw_file}")

        # Step 3: Filter by DOI tombstone
        logger.info("\nStep 3: Filtering by DOI tombstone...")
        merger = bdc_data_manager.StudyMerger(logger)
        gen3_no_tombstone, tombstone_filtered = merger.filter_by_doi_tombstone(gen3_raw_studies)

        # Step 4: Filter by subjects count (COMMENTED OUT FOR TESTING)
        # logger.info("\nStep 4: Filtering by subjects count...")
        # gen3_with_subjects, subjects_filtered = merger.filter_by_subjects_count(gen3_no_tombstone)

        # Step 5: Filter valid studies (COMMENTED OUT FOR TESTING)
        # logger.info("\nStep 5: Filtering valid studies...")
        # gen3_filtered_studies, skipped_count = merger.filter_valid_studies(gen3_with_subjects)
        # logger.info(f"Filtered {len(gen3_filtered_studies)} valid Gen3 studies (skipped {skipped_count})")

        # Use DOI tombstone filtered studies directly
        gen3_filtered_studies = gen3_no_tombstone
        logger.info(f"Using {len(gen3_filtered_studies)} Gen3 studies after DOI tombstone filter only")

        # Step 6: Merge BDC and Gen3 data
        logger.info("\nStep 6: Merging BDC and Gen3 data...")
        merged_studies, new_accessions, missing_accessions = merger.merge_studies(
            bdc_studies, gen3_filtered_studies
        )
        logger.info(f"Merged {len(merged_studies)} studies")

        # Log missing studies
        if missing_accessions:
            missing_file = path_manager.get_gen3_path("missing_studies_comparing_gen3_and_bdc.log")
            with open(missing_file, 'w') as f:
                f.write(f"Studies in BDC but not in Gen3 ({len(missing_accessions)} total):\n\n")
                for acc in sorted(missing_accessions):
                    f.write(f"{acc}\n")
            logger.info(f"Logged {len(missing_accessions)} missing studies to: {missing_file}")

        # Step 7: Sort by description and clean for program table
        logger.info("\nStep 7: Preparing program table file...")
        sorted_studies = bdc_utils.sort_studies_by_description(merged_studies)
        cleaned_studies = bdc_utils.clean_json_newlines(sorted_studies)

        # Save program table file to dedicated program_table directory
        program_table_file = path_manager.get_program_table_path("program_table.json")
        bdc_utils.save_json(cleaned_studies, program_table_file, minify=False)
        logger.info(f"Saved program table file to: {program_table_file}")

        # Summary
        logger.info("\n" + "="*80)
        logger.info("SUMMARY")
        logger.info("="*80)
        logger.info(f"BDC studies fetched: {len(bdc_studies)}")
        logger.info(f"Gen3 studies fetched (raw): {len(gen3_raw_studies)}")
        logger.info(f"Gen3 studies without DOI tombstone: {len(gen3_no_tombstone)}")
        logger.info(f"Gen3 studies filtered by DOI tombstone only: {tombstone_filtered}")
        # logger.info(f"Gen3 studies with subjects (>0): {len(gen3_with_subjects)}")
        logger.info(f"Gen3 studies after filtering: {len(gen3_filtered_studies)}")
        logger.info(f"Merged studies total: {len(merged_studies)}")
        logger.info(f"Program table entries: {len(cleaned_studies)}")
        logger.info(f"New studies (Gen3 only): {len(new_accessions)}")
        logger.info(f"Missing studies (BDC only): {len(missing_accessions)}")
        logger.info("="*80)
        logger.info(f"\nProgram table file ready: {program_table_file}")
        logger.info("Processing completed successfully!")

        return 0

    except Exception as e:
        logger.error(f"\nError: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
