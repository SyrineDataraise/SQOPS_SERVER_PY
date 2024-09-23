import logging
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from typing import List, Tuple

# Configure logging to capture detailed information for debugging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Capture all log levels, especially useful for tracing issues
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Overwrite log file each time for fresh logging data
)

def AUD_319_ALIMDOCCONTEXTGROUP(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]], batch_size: int = 100):
    """
    Inserts parsed context group data into the 'aud_doccontextgroup' table in batches after truncating the table.

    Args:
        config (Config): An instance of the Config class for retrieving query parameters.
        db (Database): An instance of the Database class for executing database operations.
        parsed_files_data (List[Tuple[str, str, dict]]): A list containing project names, job names, and parsed context group data.
        batch_size (int): The number of rows to process in each batch operation. Default is 100.
    """
    try:
        # Step 1: Truncate the 'aud_doccontextgroup' table before inserting new data
        logging.info("Truncating table 'aud_doccontextgroup'.")
        db.truncate_table('aud_doccontextgroup')
        logging.info("Table 'aud_doccontextgroup' truncated successfully.")

        # Step 2: Prepare the insert query for 'aud_doccontextgroup'
        insert_query = config.get_param('insert_queries', 'aud_doccontextgroup')
        batch_insert = []

        # Step 3: Iterate over parsed files and insert context group data
        logging.info(f"Starting to process {len(parsed_files_data)} files.")
        for nameproject, job_name, parsed_data in parsed_files_data:
            logging.debug(f"Processing project: {nameproject}, job: {job_name}")
            for data in parsed_data['TalendProperties']:
                for prop in data['properties']:
                    # Extract values from properties
                    namecontextgroup = prop['label']
                    purpose = prop['purpose']
                    description = prop['description']
                    version = prop['version']
                    statusCode = prop['statusCode']
                    item = prop['item']
                    displayName = prop['displayName']
                    id= prop ['id']

                    # Create the tuple of values to insert
                    params = (namecontextgroup, nameproject, purpose, description, version, statusCode, item, displayName, id)
                    logging.debug(f"Preparing to insert row: {params}")
                    batch_insert.append(params)

                    # Insert data in batches if the batch size is reached
                    if len(batch_insert) >= batch_size:
                        logging.info(f"Inserting batch of {len(batch_insert)} rows into 'aud_doccontextgroup'.")
                        db.insert_data_batch(insert_query, 'aud_doccontextgroup', batch_insert)
                        logging.info(f"Inserted batch of {len(batch_insert)} rows into 'aud_doccontextgroup'.")
                        batch_insert.clear()

        # Step 4: Insert any remaining data that didn't fill a full batch
        if batch_insert:
            logging.info(f"Inserting remaining {len(batch_insert)} rows into 'aud_doccontextgroup'.")
            db.insert_data_batch(insert_query, 'aud_doccontextgroup', batch_insert)
            logging.info(f"Inserted remaining {len(batch_insert)} rows into 'aud_doccontextgroup'.")

    except Exception as e:
        # Log any errors encountered during the process
        logging.error(f"An error occurred during the batch insert operation: {e}", exc_info=True)

    finally:
        # Ensure the database connection is properly closed
        if db:
            db.close()
            logging.info("Database connection closed.")
