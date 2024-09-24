
import logging

from AUD_301_ALIMELEMENTNODE import AUD_301_ALIMELEMENTNODE
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

    #  Get the execution date
    execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
    execution_date = db.get_execution_date(execution_date_query)
    logging.info(f"Execution Date: {execution_date}")

    #  Execute LOCAL_TO_DBBRUT_QUERY
    local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
    logging.info(f"Executing query: {local_to_dbbrut_query}")
    local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
    logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

    items_directory = config.get_param('Directories', 'items_directory')
    xml_parser = XMLParser()
    parsed_files_data = xml_parser.loop_parse(items_directory)
    logging.debug(f"Parsed Files Data: {parsed_files_data}")

    # Call the function with the configuration and parsed data
    AUD_301_ALIMELEMENTNODE(config, db, parsed_files_data,execution_date,local_to_dbbrut_query_results)

if __name__ == "__main__":
    main()