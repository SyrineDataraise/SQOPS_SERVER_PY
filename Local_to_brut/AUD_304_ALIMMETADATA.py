import logging
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from XML_parse import XMLParser  # Importing the XMLParser class
from typing import List, Tuple
import csv

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Changed to DEBUG to capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Ensure the file is overwritten each time for clean logs
)


def AUD_304_ALIMMETADATA(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]], batch_size=100):
    """
    Perform various database operations including retrieving JDBC parameters, 
    executing queries, deleting records, and inserting data.

    Args:
        config (Config): An instance of the Config class for retrieving configuration parameters.
        db (Database): An instance of the Database class for executing database operations.
        parsed_files_data (List[Tuple[str, str, dict]]): A list where each tuple contains (project_name, job_name, parsed_data).
        batch_size (int): The number of rows to insert in each batch.
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

        # Step 3: Delete the output from aud_metadata based on the query results
        delete_conditions = []
        for result in local_to_dbbrut_query_results:
            project_name, job_name, _, _, _ = result  # Assuming result contains these fields in order
            delete_conditions.append({
                'NameProject': project_name,
                'NameJob': job_name
            })
        db.delete_records_batch('aud_metadata', delete_conditions)
        logging.info(f"Deleted records for projects/jobs: {[(d['NameProject'], d['NameJob']) for d in delete_conditions]}")

        # Step 4: Execute aud_metadata query
        aud_metadata_query = config.get_param('queries', 'aud_metadata')
        logging.info(f"Executing query: {aud_metadata_query}")
        aud_metadata_results = db.execute_query(aud_metadata_query)
        logging.debug(f"aud_metadata_results: {aud_metadata_results}")

        # Step 5: Delete records based on the aud_metadata query results
        delete_conditions = []
        for result in aud_metadata_results:
            project_name, job_name = result
            logging.debug(f"Preparing to delete records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")
            delete_conditions.append({
                'NameProject': project_name,
                'NameJob': job_name
            })
        db.delete_records_batch('aud_metadata', delete_conditions)
        logging.info(f"Deleted records for projects/jobs: {[(d['NameProject'], d['NameJob']) for d in delete_conditions]}")

        # Step 6: Collect parsed parameters data into batches
        data_batch = []
        for project_name, job_name, parsed_data in parsed_files_data:
            for node_data in parsed_data['nodes']:
                for elem_param in node_data['elementParameters']:
                    for elem_value in elem_param['elementValues']:
                        for meta in node_data['metadata']:
                            for column in meta['columns']:
                                params = (
                                    meta['connector'],
                                    meta['label'],
                                    meta['name'],
                                    column['comment'],
                                    int(column['key'] != 'false'),
                                    column['length'],
                                    column['name'],
                                    int(column['nullable'] != 'false'),
                                    column['pattern'],
                                    column['precision'],
                                    column['sourceType'],
                                    column['type'],
                                    int(column['usefulColumn'] != 'false'),
                                    column['originalLength'],
                                    column['defaultValue'],
                                    elem_value['value'],
                                    node_data['componentName'],
                                    project_name,
                                    job_name,
                                    execution_date
                                )
                                data_batch.append(params)

                                # If the batch size is reached, insert the data
                                if len(data_batch) == batch_size:
                                    db.insert_metadata('aud_metadata', data_batch)
                                    data_batch.clear()  # Clear the batch after insertion

        # Insert any remaining data that didn't fill a complete batch
        if data_batch:
            db.insert_metadata('aud_metadata', data_batch)

        # Step 7: Execute MetadataJoinElemntnode query
        metadata_join_element_node_query = config.get_param('queries', 'MetadataJoinElemntnode')
        logging.info(f"Executing query: {metadata_join_element_node_query}")
        metadata_join_element_node_results = db.execute_query(metadata_join_element_node_query)
        logging.debug(f"MetadataJoinElemntnode_results: {metadata_join_element_node_results}")

        # Step 8: Delete records based on MetadataJoinElemntnode query results
        delete_conditions = []
        for result in metadata_join_element_node_results:
            project_name, job_name = result
            logging.debug(f"Preparing to delete records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")
            delete_conditions.append({
                'NameProject': project_name,
                'NameJob': job_name
            })
        db.delete_records_batch('aud_metadata', delete_conditions)
        logging.info(f"Deleted records for projects/jobs: {[(d['NameProject'], d['NameJob']) for d in delete_conditions]}")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed")
