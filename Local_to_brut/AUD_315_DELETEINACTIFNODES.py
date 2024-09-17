import logging
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Set logging to capture all messages for debugging
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Overwrite the log file for each run
)

def AUD_315_DELETEINACTIFNODES(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],  batch_size: int = 100):
    """
    Deletes inactive nodes from 'aud_job_fils' and 'aud_elementnode' tables based on ActiveNodes queries.
    
    Args:
        config (Config): Configuration instance for retrieving query parameters.
        db (Database): Database instance to execute queries and perform operations.
        parsed_files_data (List[Tuple[str, str, dict]]): List containing project names, job names, and parsed data.
        execution_date (str): Execution date used in database operations.
        batch_size (int): The number of rows processed in each batch. Default is 100.
    """
    try:
        # Step 1: Retrieve and execute query to get active nodes for 'job_fils'
        active_nodes_job_fils_query = config.get_param('queries', 'ActiveNodes_job_fils')
        logging.info(f"Executing query: {active_nodes_job_fils_query}")
        active_nodes_job_fils_results = db.execute_query(active_nodes_job_fils_query)
        logging.debug(f"ActiveNodes_job_fils_results: {active_nodes_job_fils_results}")

        # Step 2: Delete records from 'aud_job_fils' table in batches
        batch_delete_conditions = []
        for result in active_nodes_job_fils_results:
            aud_component_value, aud_name_project, aud_name_job = result
            batch_delete_conditions.append({
                'aud_nameproject': aud_name_project, 
                'aud_namejob': aud_name_job, 
                'aud_ComponentValue': aud_component_value
            })

            # If batch size is reached, delete records in batch
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_job_fils', batch_delete_conditions)
                logging.info(f"Batch deleted {len(batch_delete_conditions)} records from aud_job_fils")
                batch_delete_conditions.clear()

        # Delete any remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_job_fils', batch_delete_conditions)
            logging.info(f"Batch deleted remaining {len(batch_delete_conditions)} records from aud_job_fils")

        # Step 3: Retrieve and execute query to get active nodes for 'elementnode'
        active_nodes_elementnode_query = config.get_param('queries', 'ActiveNodes_elementnode')
        logging.info(f"Executing query: {active_nodes_elementnode_query}")
        active_nodes_elementnode_results = db.execute_query(active_nodes_elementnode_query)
        logging.debug(f"ActiveNodes_elementnode_results: {active_nodes_elementnode_results}")

        # Step 4: Delete records from 'aud_elementnode' table in batches
        batch_delete_conditions = []
        for result in active_nodes_elementnode_results:
            aud_component_value, aud_name_project, aud_name_job = result
            batch_delete_conditions.append({
                'aud_nameproject': aud_name_project, 
                'aud_namejob': aud_name_job, 
                'aud_ComponentValue': aud_component_value
            })

            # If batch size is reached, delete records in batch
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_elementnode', batch_delete_conditions)
                logging.info(f"Batch deleted {len(batch_delete_conditions)} records from aud_elementnode")
                batch_delete_conditions.clear()

        # Delete any remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_elementnode', batch_delete_conditions)
            logging.info(f"Batch deleted remaining {len(batch_delete_conditions)} records from aud_elementnode")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        # Ensure the database connection is always closed
        if db:
            db.close()
            logging.info("Database connection closed.")
