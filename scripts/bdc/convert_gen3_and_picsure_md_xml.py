import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
from datetime import datetime
import logging
import argparse
import sys


def parse_arguments():
    """Parse command line arguments with detailed help messages"""
    parser = argparse.ArgumentParser(
        description='''
        Generate XML files from CSV data for genomic studies.
        This script processes study study_md and data model information to create 
        standardized XML output files for each study.
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
        Example usage:
            %(prog)s variable_MD.csv study_md.csv
            %(prog)s variable_MD.csv study_md.csv --output-dir custom_output
            %(prog)s variable_MD.csv study_md.csv --log-level INFO
            
        The script will create:
        1. A directory for each study
        2. GapExchange XML files
        3. Data dictionary XML files
        4. Detailed log file with timestamp
        '''
    )

    # Required arguments
    parser.add_argument(
        'variable_md_csv',
        help='Path to the data model CSV file containing variable definitions and attributes'
    )
    
    parser.add_argument(
        'study_md_csv',
        help='Path to the study study_md CSV file containing study descriptions and properties'
    )

    # Optional arguments
    parser.add_argument(
        '--output-dir',
        default='output',
        help='Output directory for generated XML files (default: output)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set the logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-dir',
        default='logs',
        help='Directory for log files (default: logs)'
    )
    
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing files in output directory'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0',
        help='Show program version'
    )

    return parser.parse_args()

# Save xml
def save_xml(root, filepath, add_stylesheet=True):
    """Save XML to file"""
    try:
        # Get the XML string without any declaration first
        rough_xml = ET.tostring(root)
        xml_str = minidom.parseString(rough_xml).toprettyxml(indent="    ")
        
        # Remove any existing XML declarations to prevent duplicates as this was causing error
        lines = xml_str.splitlines()
        filtered_lines = [line for line in lines if not line.strip().startswith('<?xml')]
        
        xml_content = '\n'.join(filtered_lines)
        
        if add_stylesheet:
            final_xml = '<?xml version="1.0" encoding="UTF-8"?>\n<?xml-stylesheet type="text/xsl" href="./datadict_v2.xsl"?>\n' + xml_content
        else:
            final_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_content
        
        # Create directory for study if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(final_xml)
        
        logging.info(f"Created {filepath}")
    except Exception as e:
        logging.error(f"Error saving XML file {filepath}: {e}")
        raise


# Create a data xml files for each variable
def create_data_dict_xml(var_info, table_id_version, study_id_version,variable_id_version):

    """Create data dictionary XML """
    try:
        root = ET.Element("data_table")
        root.set("id", f"{table_id_version}")
        root.set("study_id", f"{study_id_version}")
        root.set("participant_set", "1")
        #root.set("date_created", datetime.now().strftime("%a %b %d %H:%M:%S %Y"))
        
        # Add variable description if available 
        variable_desc = var_info['columnmeta_var_group_description'].iloc[0] if 'columnmeta_var_group_description' in var_info.columns else ''
        if pd.notna(variable_desc):
            description = ET.SubElement(root, "description")
            description.text = str(variable_desc)
        
        # Process each variable
        for _, row in var_info.iterrows():
            variable = ET.SubElement(root, "variable")

            variable.set("id", f"{variable_id_version}")
            
            # Add name
            name = ET.SubElement(variable, "name")
            name.text = str(row.get('columnmeta_name', ''))
            
            # Add description for the variable
            desc = ET.SubElement(variable, "description")
            desc.text = str(row.get('columnmeta_description', ''))
            
            # Add type and values
            '''
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
                        logging.warning(f"Error processing values for variable {variable_id_version}: {e}")
            else:
                type_elem.text = "string"
            '''
        return root
        
    except Exception as e:
        logging.error(f"Error creating data dictionary XML: {e}")
        raise

# Create a gap exchnage file for each study
def create_gap_exchange_xml(study_df):
    """Create GapExchange XML with for a study """
    logging.info(f"Processing a study in gap exchange {study_df['study_phs'].iloc[0]}")
    try:
        root = ET.Element("GaPExchange")
        root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.set("xsi:noNamespaceSchemaLocation", "./dbGaPEx2.1.5.xsd")
        
        study_dict = study_df.iloc[0].to_dict() if isinstance(study_df, pd.DataFrame) else study_df.to_dict()
        meta_vars = ET.SubElement(root, "MetaVariables")
        ET.SubElement(meta_vars, "Submitter")
        ET.SubElement(meta_vars, "Method")
        
        ET.SubElement(root, "MetaLinks")
        ET.SubElement(root, "Projects")
        
        studies = ET.SubElement(root, "Studies")
        study = ET.SubElement(studies, "Study")
        study.set("source", "dbGaP")
        study.set("accession", study_dict['Accession'])
        study.set("parentStudy", study_dict['Accession'])
        study.set("createDate", "")
        study.set("modDate", study_dict['Last modified'])
        
        config = ET.SubElement(study, "Configuration")
        
        data_provider = ET.SubElement(config, "Data_Provider")
        data_provider.text = ''' '''
        
        study_name = ET.SubElement(config, "StudyNameEntrez")
        study_name.text = study_dict['Study Name']
        
        study_name_report = ET.SubElement(config, "StudyNameReportPage")
        study_name_report.text = study_dict['Study Name']
        
        study_types = ET.SubElement(config, "StudyTypes")
        study_type = ET.SubElement(study_types, "StudyType")
        study_type.text = "Case Set"
        
        description = ET.SubElement(config, "Description")
        description.text = f"<p>{study_dict['Description']}</p>"
        
        # Add program information
        program = ET.SubElement(config, "Program")
        program.text = study_dict['Program']
        
        # Add notes if present
        if pd.notna(study_dict.get('Notes')):
            notes = ET.SubElement(config, "Notes")
            notes.text = study_dict['Notes']
        
        return root
    except Exception as e:
        logging.error(f"Error creating GapExchange XML: {e}")
        raise

# Process each study
def process_study(study_id, variable_df, study_df, output_dir):
    #remove duplicates 
    study_df = study_df.drop_duplicates(subset=['study_phs'])
    """Process a single study with version handling"""
    try:
        logging.info(f"Processing study in process_study funtion {study_id}")

        # Create study directory
        study_dir = os.path.join(output_dir, study_id.split('.')[0])
        os.makedirs(study_dir, exist_ok=True)
        
        # Create GapExchange XML
        gap_root = create_gap_exchange_xml(study_df)
        study_dict = study_df.to_dict() if isinstance(study_df, pd.Series) else study_df
        gap_filepath = os.path.join(study_dir, f'GapExchange_{study_dict["Accession"].iloc[0]}.xml')
        save_xml(gap_root, gap_filepath, add_stylesheet=False)
        
        # Create data dictionary XMLs for each variable
        var_md = variable_df.groupby('dtId')
        for table_id, var_info in var_md:
            logging.info(f"Processing variable {var_info['derived_var_id'].iloc[0]} for study {study_id}")
            # Get table,study and varaible id and version

            table_id_version = var_info['derived_group_id'].iloc[0]
            variable_id_version = var_info['derived_var_id'].iloc[0]
            study_id_version = var_info['derived_study_id'].iloc[0]
            variable_name = var_info['derived_group_name'].iloc[0]

            root = create_data_dict_xml(
                var_info,
                table_id_version,
                study_id_version,
                variable_id_version,
            )
            filename = f"{study_id_version}.{table_id_version}.{variable_name}.data_dict.xml"
            filepath = os.path.join(study_dir, filename)
            save_xml(root, filepath)
            
    except Exception as e:
        logging.error(f"Error processing study {study_id}: {str(e)}")
        raise

# Process all studies
def process_multiple_studies(variable_md_df, study_md_df, output_dir):
    """Process all studies found in the data"""
    try:
        # Get unique study IDs from both datasets

        variable_md_studies = set(variable_md_df['derived_study_id'].unique())
        study_md_studies = set(study_md_df['Accession'].str.rsplit('.p', n=1).str[0].unique())
        
        common_studies = variable_md_studies.intersection(study_md_studies)
    
        logging.info(f"Found {len(common_studies)} studies to process")
        
        if variable_md_studies - study_md_studies:
            logging.warning(f"Studies missing study_md: {variable_md_studies - study_md_studies}")
        if study_md_studies - variable_md_studies:
            logging.warning(f"Studies missing variable_md: {study_md_studies - variable_md_studies}")
        
        logging.warning(f"common studies: {common_studies}")
        studies_processed = 0
        studies_skipped = 0
        
        for study_id in common_studies:
            logging.info(f"\nProcessing study: {study_id}")
            
            try:
                # Get study-specific data
                variable_df = variable_md_df[variable_md_df['derived_study_id'] == study_id].copy()
                study_df = study_md_df[study_md_df['study_phs'] == study_id].drop_duplicates().copy()

                try:
                    process_study(study_id, variable_df, study_df, output_dir)
                    logging.info(f"Successfully completed processing study: {study_id}")
                    studies_processed += 1
                except Exception as e:
                    logging.error(f"Error processing study {study_id}: {str(e)}")
                    studies_skipped += 1
                    
            except Exception as e:
                logging.error(f"Error preparing data for study {study_id}: {str(e)}")
                studies_skipped += 1
                continue
        
        # Log summary
        logging.info("\nProcessing Summary:")
        logging.info(f"Total studies found: {len(common_studies)}")
        logging.info(f"Studies successfully processed: {studies_processed}")
        logging.info(f"Studies skipped: {studies_skipped}")
                
    except Exception as e:
        logging.error(f"Error in processing multiple studies: {str(e)}")
        raise


def main():
   '''try:
       variable_md_csv = "/Users/ykale/Documents/Dev/dug-data-ingest/scripts/bdc/pic_sure_data_sample.csv"  # Your data model CSV path
       study_md_csv = "/Users/ykale/Documents/Dev/dug-data-ingest/scripts/bdc/gen3_data.csv"  # Your study_md CSV path
       output_dir = "/Users/ykale/Documents/Dev/dug-data-ingest/scripts/bdc/output_jan11"  # Output directory
       log_dir = "/Users/ykale/Documents/Dev/dug-data-ingest/scripts/bdc/logs_jan11"  # Log directory
       log_level = "INFO"  # Logging level
       overwrite = True  # Whether to overwrite existing files


   # Create args namespace object
   class Args:
       def __init__(self):
           self.variable_md_csv = variable_md_csv
           self.study_md_csv = study_md_csv
           self.output_dir = output_dir
           self.log_dir = log_dir
           self.log_level = log_level
           self.overwrite = overwrite
   
   args = Args()
   '''
   # Add argparse for command line arguments
   import argparse
   parser = argparse.ArgumentParser(description='Process study metadata and generate XML files')
   parser.add_argument('--variable_md_csv', required=True, help='Path to variable metadata CSV file')
   parser.add_argument('--study_md_csv', required=True, help='Path to study metadata CSV file') 
   parser.add_argument('--output_dir', required=True, help='Directory for output files')
   parser.add_argument('--log_dir', required=True, help='Directory for log files')
   parser.add_argument('--log_level', default='INFO', choices=['INFO'],
                       help='Logging level')
   parser.add_argument('--overwrite', action='store_true', help='Overwrite existing files')
   
   args = parser.parse_args()

   # Setup logging
   os.makedirs(args.log_dir, exist_ok=True)
   timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
   log_filename = os.path.join(args.log_dir, f'xml_generation_{timestamp}.log')
   
   # Configure logging with valid log level and ensure immediate output
   logger = logging.getLogger()
   logger.setLevel(getattr(logging, args.log_level))
   
   # Create file handler
   file_handler = logging.FileHandler(log_filename)
   file_handler.setLevel(getattr(logging, args.log_level))
   file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
   
   # Create console handler
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setLevel(getattr(logging, args.log_level))
   console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
   
   # Add handlers to logger
   logger.addHandler(file_handler)
   logger.addHandler(console_handler)

   # Log initial configuration
   logger.info(f"Starting XML generation process - Log file: {log_filename}")
   logger.info("Configuration:")
   logger.info(f"  Variable MD CSV: {args.variable_md_csv}")
   logger.info(f"  Study MD: {args.study_md_csv}")
   logger.info(f"  Output directory: {args.output_dir}")
   logger.info(f"  Log level: {args.log_level}")
   logger.info(f"  Overwrite existing files: {args.overwrite}")

   # Validate input files exist
   if not os.path.exists(args.variable_md_csv):
       logger.error(f"Variable MD CSV file not found: {args.variable_md_csv}")
       sys.exit(1)
   if not os.path.exists(args.study_md_csv):
       logger.error(f"Study MD CSV file not found: {args.study_md_csv}")
       sys.exit(1)

   # Create or check output directory
   if os.path.exists(args.output_dir) and not args.overwrite:
       logger.error(f"Output directory already exists and overwrite is set to False: {args.output_dir}")
       sys.exit(1)
   os.makedirs(args.output_dir, exist_ok=True)

   # Read input files
   logger.info("Reading input files...")
   
   # Read data model CSV
   try:
       variable_md_df = pd.read_csv(args.variable_md_csv)
       if variable_md_df.empty:
           logger.error("variable meta data (picsure) CSV is empty")
           sys.exit(1)
       logger.info(f"Successfully read variable meta data CSV: {len(variable_md_df)} rows")
   except pd.errors.EmptyDataError:
       logger.error("Data model CSV is empty")
       sys.exit(1)
   except Exception as e:
       logger.error(f"Error reading variable meta data CSV: {str(e)}")
       sys.exit(1)

   # Read study_md CSV
   try:
       study_md = pd.read_csv(args.study_md_csv) 
       study_md['study_phs'] = study_md['Accession'].str.rsplit('.p', n=1).str[0]
       if study_md.empty:
           logger.error("study_md CSV is empty")
           sys.exit(1)
       logger.info(f"Successfully read study_md CSV: {len(study_md)} studies")
   except pd.errors.EmptyDataError:
       logger.error("study_md CSV is empty")
       sys.exit(1)
   except Exception as e:
       logger.error(f"Error reading study_md CSV: {str(e)}")
       sys.exit(1)

   # Process studies
   try:
       logger.info("Processing studies...")
       process_multiple_studies(variable_md_df, study_md, args.output_dir)

       logger.info("\nProcessing complete!")
       logger.info(f"Log file has been saved to: {log_filename}")
   except Exception as e:
       logger.error(f"Error processing studies: {str(e)}")
       sys.exit(1)

   # Ensure all log messages are written
   for handler in logger.handlers:
       handler.flush()
       handler.close()
   
'''    except Exception as e:
       print(f"Critical error in main execution: {str(e)}")
       if logging.getLogger().handlers:
           logging.error(f"Critical error in main execution: {str(e)}")
       # Ensure logs are written even on error
       for handler in logging.getLogger().handlers:
           handler.flush()
           handler.close()
       sys.exit(1)'''

if __name__ == "__main__":
   main()