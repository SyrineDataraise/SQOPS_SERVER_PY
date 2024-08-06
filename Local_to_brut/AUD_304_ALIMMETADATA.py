import logging
import time
import gc
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from XML_parse import XMLParser  # Importing the XMLParser class
from typing import List, Tuple

# Configure logging
logging.basicConfig(filename='database_operations.log',
                    level=logging.DEBUG,  # Changed to DEBUG to capture all messages
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='w')  # Ensure the file is overwritten each time for clean logs

def process_database_operations(config, db, parsed_files_data: List[Tuple[str, str, dict]]):
    """
    Perform various database operations including retrieving JDBC parameters, 
    executing queries, deleting records, and inserting data.

    Args:
        config (Config): An instance of the Config class for retrieving configuration parameters.
        db (Database): An instance of the Database class for executing database operations.
        parsed_files_data (list of tuples): A list where each tuple contains (project_name, job_name, parsed_data).
    """
    try:
        # Step 1: Get the execution date
        execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        execution_date = db.get_execution_date(execution_date_query)
        logging.info(f"Execution Date: {execution_date}")

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY and delete records from aud_metadata
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        for project_name, job_name, *_ in local_to_dbbrut_query_results:
            db.delete_records('aud_metadata', NameProject=project_name, NameJob=job_name)
            logging.info(f"Deleted records from aud_metadata: PROJECT_NAME={project_name}, JOB_NAME={job_name}")

        # Step 4: Execute aud_metadata query and delete records from aud_contextjob
        aud_metadata_query = config.get_param('queries', 'aud_metadata')
        aud_metadata_results = db.execute_query(aud_metadata_query)
        for project_name, job_name in aud_metadata_results:
            db.delete_records('aud_contextjob', NameProject=project_name, NameJob=job_name)
            logging.info(f"Deleted records from aud_contextjob: PROJECT_NAME={project_name}, JOB_NAME={job_name}")

        # Step 7: Insert parsed parameters data into the `aud_metadata` table in batches
        insert_query = config.get_param('insert_queries', 'aud_metadata')
        batch_size = 100  # Adjust batch size as needed
        batch = []

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
                                batch.append(params)
                                if len(batch) >= batch_size:
                                    db.insert_data_batch(insert_query, 'aud_metadata', batch)
                                    logging.info(f"Inserted batch of parameter data into aud_metadata: {len(batch)} rows")
                                    batch.clear()
                                    gc.collect()  # Manually trigger garbage collection

        # Insert remaining data in the batch
        if batch:
            db.insert_data_batch(insert_query, 'aud_metadata', batch)
            logging.info(f"Inserted remaining batch of parameter data into aud_metadata: {len(batch)} rows")

        # Step 8: Execute MetadataJoinElementNode query and delete records from aud_metadata
        metadata_join_query = config.get_param('queries', 'MetadataJoinElemntnode')
        metadata_join_results = db.execute_query(metadata_join_query)
        for result in metadata_join_results:
            conditions = {
                'NameProject': result[0],
                'NameJob': result[1],
                'aud_componentValue': result[2]
            }
            db.delete_records('aud_metadata', **conditions)
            logging.info(f"Deleted records from aud_metadata based on MetadataJoinElementNode results: {conditions}")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed")


