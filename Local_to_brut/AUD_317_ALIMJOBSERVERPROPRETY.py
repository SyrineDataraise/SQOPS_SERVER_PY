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

def AUD_317_ALIMJOBSERVERPROPRETY(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]], file_path: str, batch_size: int = 100):
    """
    Inserts data into the 'aud_talendjobserver_properties' table from a CSV file in batches.
    
    Args:
        config (Config): Configuration instance for retrieving query parameters.
        db (Database): Database instance to execute queries and perform operations.
        parsed_files_data (List[Tuple[str, str, dict]]): List containing project names, job names, and parsed data.
        file_path (str): Path to the CSV file containing the data to be inserted.
        batch_size (int): The number of rows processed in each batch. Default is 100.
    """
    try:
        # Insert data into 'aud_talendjobserver_properties' from the CSV file in batches
        table_name = 'aud_talendjobserver_properties'
        db.insert_from_csv_batch(file_path, table_name, batch_size)
        logging.info(f"Inserted data from {file_path} into {table_name} in batches of {batch_size}")

    except Exception as e:
        # Log any errors that occur during the process
        logging.error(f"An error occurred during data insertion: {e}", exc_info=True)

    finally:
        # Ensure the database connection is always closed
        if db:
            db.close()
            logging.info("Database connection closed.")
