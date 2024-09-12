# Import necessary modules
import logging
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Overwrite the file each time for clean logs
)

def AUD_323_ALIMELEMENTNODEFILTER(
    config: Config, 
    db: Database, 
    parsed_files_data: List[Tuple[str, str, dict]], 
    batch_size: int = 5
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
        # Step 1: Execute aud_elementnode_filter
        aud_elementnode_filter_query = config.get_param('queries', 'aud_elementnode_filter')
        logging.info(f"Executing query: {aud_elementnode_filter_query}")
        aud_elementnode_filter_query_results = db.execute_query(aud_elementnode_filter_query)
        logging.debug(f"aud_elementnode_filter_query_results: {aud_elementnode_filter_query_results}")

        # Step 2: Insert data into aud_elementnode_filter in batches
        batch_insert = []
        insert_query = config.get_param('insert_queries', 'aud_elementnode_filter')
        for result in aud_elementnode_filter_query_results:
            aud_componentName, aud_field, aud_nameElementNode, aud_show, aud_valueElementNode, aud_ComponementValue, NameProject, NameJob, exec_date = result
            # Add result to batch insert list
            batch_insert.append(result)

            if len(batch_insert) >= batch_size:
                db.insert_data_batch('aud_elementnode_filter', insert_query ,  batch_insert)
                logging.info(f"Inserted batch of data into aud_elementnode_filter: {len(batch_insert)} rows")
                batch_insert.clear()

        # Insert remaining data in the batch
        if batch_insert:
            db.insert_data_batch('aud_elementnode_filter', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_elementnode_filter: {len(batch_insert)} rows")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")
