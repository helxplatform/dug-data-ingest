#!/usr/bin/env python3
"""
XML generation tool for dbGaP studies using Gen3 metadata and PicSure variable data.
"""

import os
import sys
import logging
import pandas as pd
import argparse
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime

# Configuration
DEFAULT_OUTPUT_DIR = "bdc-studies"

def setup_console_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for h in logger.handlers[:]:
        logger.removeHandler(h)
    logger.addHandler(handler)

def read_gen3_metadata(gen3_metadata_file):
    try:
        df = pd.read_csv(gen3_metadata_file)
        df['study_id'] = df['Accession'].str.split('.').str[0]
        return df.set_index('study_id')
    except Exception as e:
        logging.error(f"Error reading study metadata file: {e}")
        raise

def extract_version_from_accession(accession):
    if pd.isna(accession) or not isinstance(accession, str):
        return 'v1'
    
    parts = accession.split('.')
    if len(parts) > 1:
        return parts[1]
    return 'v1'

def validate_study_data(picsure_df, gen3_metadata):
    if isinstance(gen3_metadata, pd.Series):
        metadata_dict = gen3_metadata.to_dict()
    else:
        metadata_dict = gen3_metadata
        
    required_metadata = ['Study Name', 'Program', 'Description']
    missing_fields = [field for field in required_metadata 
                     if field not in metadata_dict or pd.isna(metadata_dict[field])]
    
    if missing_fields:
        logging.warning(f"Missing required metadata fields: {missing_fields}")
        return False
        
    if picsure_df.empty:
        logging.warning("Study dataframe is empty")
        return False
        
    required_columns = ['dtId', 'derived_group_name', 'varId']
    missing_columns = [col for col in required_columns if col not in picsure_df.columns]
    if missing_columns:
        logging.warning(f"Missing required columns in variable dataframe: {missing_columns}")
        return False
        
    return True

def create_gap_exchange_xml(study_id, picsure_df, gen3_metadata):
    root = ET.Element("GaPExchange")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set("xsi:noNamespaceSchemaLocation", "./dbGaPEx2.1.5.xsd")
    
    metadata_dict = gen3_metadata.to_dict() if isinstance(gen3_metadata, pd.Series) else gen3_metadata
    
    meta_vars = ET.SubElement(root, "MetaVariables")
    ET.SubElement(meta_vars, "Submitter")
    ET.SubElement(meta_vars, "Method")
    
    ET.SubElement(root, "MetaLinks")
    ET.SubElement(root, "Projects")
    
    studies = ET.SubElement(root, "Studies")
    study = ET.SubElement(studies, "Study")
    study.set("source", "dbGaP")
    study.set("accession", metadata_dict['Accession'])
    study.set("parentStudy", metadata_dict['Accession'])
    study.set("createDate", "2022-11-16")
    study.set("modDate", metadata_dict.get('Last modified', '2022-11-16'))
    
    config = ET.SubElement(study, "Configuration")
    
    data_provider = ET.SubElement(config, "Data_Provider")
    data_provider.text = '''<table border="1">
        <tr><th>Title</th><th>Name</th><th>Institute</th></tr>
        <tr><td>Principal Investigator</td><td>J. Gustav Smith, MD, PhD</td><td>Department of Cardiology, Clinical Sciences, Lund University and Skåne University Hospital, Lund, Sweden</td></tr>
        </table>'''
    
    study_name = ET.SubElement(config, "StudyNameEntrez")
    study_name.text = metadata_dict.get('Study Name', '')
    
    study_name_report = ET.SubElement(config, "StudyNameReportPage")
    study_name_report.text = metadata_dict.get('Study Name', '')
    
    study_types = ET.SubElement(config, "StudyTypes")
    study_type = ET.SubElement(study_types, "StudyType")
    study_type.text = "Case Set"
    
    description = ET.SubElement(config, "Description")
    description.text = f"<p>{metadata_dict.get('Description', '')}</p>"
    
    program = ET.SubElement(config, "Program")
    program.text = metadata_dict.get('Program', '')
    
    if 'Notes' in metadata_dict and pd.notna(metadata_dict['Notes']):
        notes = ET.SubElement(config, "Notes")
        notes.text = metadata_dict['Notes']
    
    return root

def create_data_dict_xml(df_group, table_id, study_id, gen3_metadata):
    if isinstance(gen3_metadata, (dict, pd.Series)):
        study_version = extract_version_from_accession(gen3_metadata.get('Accession', ''))
    else:
        study_version = gen3_metadata
    
    root = ET.Element("data_table")
    root.set("id", table_id)
    root.set("study_id", f"{study_id}.{study_version}")
    root.set("participant_set", "1")
    
    desc = df_group['columnmeta_var_group_description'].iloc[0] if 'columnmeta_var_group_description' in df_group.columns else ''
    if pd.notna(desc):
        description = ET.SubElement(root, "description")
        description.text = str(desc)
    
    for _, row in df_group.iterrows():
        variable = ET.SubElement(root, "variable")
        
        var_id = row.get('varId', '')
        var_version = 'v1'
        
        variable.set("id", f"{var_id}.{var_version}")
        
        name = ET.SubElement(variable, "name")
        name.text = str(row.get('columnmeta_name', ''))
        
        desc = ET.SubElement(variable, "description")
        desc.text = str(row.get('columnmeta_description', ''))
        
        type_elem = ET.SubElement(variable, "type")
        if row.get('is_categorical', False):
            type_elem.text = "encoded value"
            if pd.notna(row.get('values', '')):
                try:
                    values = eval(str(row['values']))
                    for value in values:
                        value_elem = ET.SubElement(variable, "value")
                        if value == "Male":
                            value_elem.set("code", "1")
                        elif value == "Female":
                            value_elem.set("code", "2")
                        value_elem.text = str(value)
                except Exception as e:
                    logging.warning(f"Error processing values for variable {var_id}: {e}")
        else:
            type_elem.text = "string"
    
    return root

def save_xml(root, filepath, add_stylesheet=True):
    xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="    ")
    
    if add_stylesheet:
        xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n<?xml-stylesheet type="text/xsl" href="./datadict_v2.xsl"?>\n' + xml_str
    else:
        xml_str = '<?xml version=\'1.0\' encoding=\'utf-8\'?>\n' + xml_str
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(xml_str)
    
    logging.info(f"Created {os.path.basename(filepath)}")

def process_study(study_id, picsure_df, gen3_metadata, output_dir):
    try:
        logging.info(f"Processing study {study_id}")
        
        metadata_dict = gen3_metadata.to_dict() if isinstance(gen3_metadata, pd.Series) else gen3_metadata
        accession = metadata_dict.get("Accession", f"{study_id}")
        
        os.makedirs(output_dir, exist_ok=True)
        
        gap_root = create_gap_exchange_xml(study_id, picsure_df, gen3_metadata)
        gap_filepath = os.path.join(output_dir, f'GapExchange_{accession}.xml')
        save_xml(gap_root, gap_filepath, add_stylesheet=False)
        
        study_version = extract_version_from_accession(accession)
        
        grouped = picsure_df.groupby('dtId')
        for table_id, group in grouped:
            logging.info(f"Processing table {table_id} for study {study_id}")
            root = create_data_dict_xml(group,table_id,study_id,study_version)
            try:
                table_name = group['derived_group_name'].iloc[0]
                if pd.isna(table_name):
                    table_name = 'unnamed'
            except (KeyError, IndexError):
                table_name = 'unnamed'
            filename = f"{study_id}.{study_version}.{table_id}.{table_name}.data_dict.xml"
            filepath = os.path.join(output_dir, filename)
            save_xml(root, filepath)
        return True
    except Exception as e:
        logging.error(f"Error processing study {study_id}: {str(e)}")
        return False

def process_multiple_studies(picsure_df, gen3_metadata, output_path):
    success_count = 0
    failure_count = 0
    for study_id, metadata in gen3_metadata.iterrows():
        study_picsure_df = picsure_df[picsure_df['studyId'] == study_id].copy()
        
        if study_picsure_df.empty:
            logging.warning(f"Study {study_id} not found in PicSure data, skipping")
            continue
        study_dir = os.path.join(output_path, study_id)
        os.makedirs(study_dir, exist_ok=True)
        if validate_study_data(study_picsure_df, metadata):
            try:
                if process_study(study_id, study_picsure_df, metadata, study_dir):
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                logging.error(f"Error processing study {study_id}: {str(e)}")
                failure_count += 1
        else:
            logging.warning(f"Study {study_id} has invalid data, skipping")
    logging.info(f"Processed {success_count} studies successfully, {failure_count} failed")
    return success_count > 0

def run_xml_generation(study_accession=None, output_dir=None, 
                     picsure_csv_path=None, gen3_csv_path=None):
    setup_console_logging()
    output_path = output_dir if output_dir else DEFAULT_OUTPUT_DIR
    os.makedirs(output_path, exist_ok=True)
    picsure_df = pd.read_csv(picsure_csv_path)
    gen3_metadata = read_gen3_metadata(gen3_csv_path)

    
    if study_accession:
        study_base_id = study_accession.split('.')[0] if '.' in study_accession else study_accession
        exact_match = gen3_metadata[gen3_metadata['Accession'] == study_accession]
        if not exact_match.empty:
            study_gen3_meta = exact_match.iloc[0]
            study_id = study_base_id
        else:
            base_matches = gen3_metadata.index[gen3_metadata.index == study_base_id]
            if not base_matches.empty:
                study_id = study_base_id
                study_gen3_meta = gen3_metadata.loc[study_id]
            else:
                logging.error(f"Study accession {study_accession} not found in Gen3 metadata CSV")
                return False
        study_picsure_df = picsure_df[picsure_df['studyId'] == study_id].copy()
        
        if study_picsure_df.empty:
            logging.error(f"Study ID {study_id} not found in PicSure CSV")
            return False
        
        logging.info(f"Processing study {study_accession}...")
        
        if os.path.basename(output_path) == study_accession:
            study_dir = output_path
        else:
            study_dir = os.path.join(output_path, study_accession)
            os.makedirs(study_dir, exist_ok=True)
        
        if validate_study_data(study_picsure_df, study_gen3_meta):
            process_study(study_id, study_picsure_df, study_gen3_meta, study_dir)
            logging.info(f"Processing complete for study {study_accession}")
            return True
        else:
            logging.error(f"Study {study_accession} does not have valid data")
            return False
    else:
        logging.info("Processing all studies...")
        success = process_multiple_studies(picsure_df, gen3_metadata, output_path)
        logging.info("Processing complete!")
        return success

def main():
    parser = argparse.ArgumentParser(description='Generate XML for dbGaP studies')
    parser.add_argument('--output-dir', help='Output directory for XML files')
    parser.add_argument('--picsure-csv', required=True, help='Path to PicSure CSV file')
    parser.add_argument('--gen3-csv', required=True, help='Path to Gen3 CSV file')
    parser.add_argument('--study', help='Process only this study accession (e.g., phs000001.v3)')
    
    args = parser.parse_args()
    
    run_xml_generation(
        study_accession=args.study,
        output_dir=args.output_dir,
        picsure_csv_path=args.picsure_csv,
        gen3_csv_path=args.gen3_csv
    )

if __name__ == "__main__":
    main()