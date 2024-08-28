import os
import logging
from typing import List, Tuple
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from XML_parse import XMLParser  # Importing the XMLParser class

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Overwrite the log file each time for clean logs
)

def AUD_303_BIGDATA_PARAMETERS(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]], batch_size=100):
    """
    Perform various database operations including retrieving execution dates,
    executing queries, deleting records, and inserting parsed XML data.

    Args:
        config (Config): Configuration instance for retrieving parameters.
        db (Database): Database instance for executing queries.
        parsed_files_data (List[Tuple[str, str, dict]]): List of parsed data from XML files.
        batch_size (int): Number of rows to insert in each batch.
    """
    try:
        # Step 1: Get the execution date
        execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        execution_date = db.get_execution_date(execution_date_query)
        logging.info(f"Execution Date: {execution_date}")

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        logging.info(f"Executing query: {local_to_dbbrut_query}")
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete output from `aud_bigdata` based on query results
        aud_bigdata_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in local_to_dbbrut_query_results
        ]
        if aud_bigdata_conditions_batch:
            db.delete_records_batch('aud_bigdata', aud_bigdata_conditions_batch)

        # Step 4: Execute `aud_bigdata` query
        aud_bigdata_query = config.get_param('queries', 'aud_bigdata')
        logging.info(f"Executing query: {aud_bigdata_query}")
        aud_bigdata_results = db.execute_query(aud_bigdata_query)
        logging.debug(f"aud_bigdata_results: {aud_bigdata_results}")

        # Step 5: Delete output from `aud_contextjob` based on query results
        aud_contextjob_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in aud_bigdata_results
        ]
        if aud_contextjob_conditions_batch:
            db.delete_records_batch('aud_contextjob', aud_contextjob_conditions_batch)

        # Step 6: Prepare batches for insertion into `aud_bigdata` and `aud_bigdata_elementvalue` tables
        aud_bigdata_batch = []
        aud_bigdata_elementvalue_batch = []

        for project_name, job_name, parsed_data in parsed_files_data:
            for param_data in parsed_data['parameters']:
                aud_bigdata_batch.append((
                    param_data['field'], param_data['name'], param_data['show'],
                    param_data['value'], project_name, job_name, execution_date
                ))

                for elementValue in param_data['elementValues']:
                    aud_bigdata_elementvalue_batch.append((
                        elementValue['elementRef'], elementValue['value'], param_data['name'],
                        project_name, job_name, execution_date
                    ))

        # Step 7: Insert data in batches
        if aud_bigdata_batch:
            insert_query = config.get_param('insert_queries', 'aud_bigdata')
            db.insert_data_batch(insert_query, 'aud_bigdata', aud_bigdata_batch)
        
        if aud_bigdata_elementvalue_batch:
            insert_query = config.get_param('insert_queries', 'aud_bigdata_elementvalue')
            db.insert_data_batch(insert_query, 'aud_bigdata_elementvalue', aud_bigdata_elementvalue_batch)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")
