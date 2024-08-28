import sys
import os
import logging

# Add the directory containing AUD_304_ALIMMETADATA.py to the Python path
sys.path.append(os.path.dirname('C:/Users/sonia/Desktop/SQOPS_SERVER_PY/SQOPS_SERVER_PY/Local_to_brut/AUD_304_ALIMMETADATA.py'))
from AUD_304_ALIMMETADATA import AUD_304_ALIMMETADATA
from config import Config  # Assuming Config class is defined in config.py
from XML_parse import XMLParser  # Importing the XMLParser class
from database import Database  # Assuming Database class is defined in database.py

def main():
    db = None
    config_file = "configs/config.yaml"
    config = Config(config_file)

    # Retrieve JDBC parameters and create a Database instance
    jdbc_params = config.get_jdbc_parameters()
    logging.debug(f"JDBC Parameters: {jdbc_params}")

    db = Database(jdbc_params)
    db.set_jdbc_parameters(jdbc_params)  # Set JDBC parameters if needed
    db.connect_JDBC()  # Test the JDBC connection

    items_directory = config.get_param('Directories', 'items_directory')
    xml_parser = XMLParser()
    parsed_files_data = xml_parser.loop_parse(items_directory)
    logging.debug(f"Parsed Files Data: {parsed_files_data}")

    # Call the function with the configuration and parsed data
    AUD_304_ALIMMETADATA(config, db, parsed_files_data)

if __name__ == "__main__":
    main()