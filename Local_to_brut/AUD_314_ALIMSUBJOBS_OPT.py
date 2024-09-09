import logging
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Changed to DEBUG to capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Ensure the file is overwritten each time for clean logs
)

def AUD_314_ALIMSUBJOBS_OPT(
    config: Config, 
    db: Database, 
    parsed_files_data: List[Tuple[str, str, dict]], 
    batch_size: int = 100
):
    """
    Perform operations including retrieving JDBC parameters, executing queries,
    deleting records, and inserting data in batches.

    Args:
        config (Config): An instance of the Config class for retrieving configuration parameters.
        db (Database): An instance of the Database class for executing database operations.
        parsed_files_data (List[Tuple[str, str, dict]]): List of parsed file data containing 
            project names, job names, and parsed data dictionaries.
        batch_size (int): The number of rows to process in each batch operation. Default is 100.
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

        # Step 3: Delete records from aud_elementvaluenode in batches
        batch_delete_conditions = []
        for result in local_to_dbbrut_query_results:
            project_name, job_name, *_ = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})

            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_elementvaluenode', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_elementvaluenode: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_elementvaluenode', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_elementvaluenode: {len(batch_delete_conditions)} rows")

        # Step 4: Execute aud_subjobs query
        aud_subjobs_query = config.get_param('queries', 'aud_subjobs')
        logging.info(f"Executing query: {aud_subjobs_query}")
        aud_subjobs_results = db.execute_query(aud_subjobs_query)
        logging.debug(f"aud_subjobs_results: {aud_subjobs_results}")

        # Step 5: Delete records from aud_subjobs in batches
        batch_delete_conditions = []
        for result in aud_subjobs_results:
            project_name, job_name = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})

            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_subjobs', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_subjobs: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_subjobs', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_subjobs: {len(batch_delete_conditions)} rows")

        # Step 6: Insert parsed data into the `aud_subjobs` table in batches
        insert_query = config.get_param('insert_queries', 'aud_subjobs')
        batch_insert = []

        for project_name, job_name, parsed_data in parsed_files_data:
            for data in parsed_data['subjobs']:
                for elem_param in data['elementParameters']:
                    if elem_param['name'] == "UNIQUE_NAME" or elem_param['name'] == "SUBJOB_TITLE":
                        field = elem_param['field']
                        name = elem_param['name']
                        value = elem_param['value']

                        params = (project_name, job_name, execution_date, name, value)
                        batch_insert.append(params)

                        if len(batch_insert) >= batch_size:
                            db.insert_data_batch(insert_query, 'aud_subjobs', batch_insert)
                            logging.info(f"Inserted batch of data into aud_subjobs: {len(batch_insert)} rows")
                            batch_insert.clear()

        # Insert remaining data in the batch
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_subjobs', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_subjobs: {len(batch_insert)} rows")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")
