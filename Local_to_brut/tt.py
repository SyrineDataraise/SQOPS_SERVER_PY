import logging
from typing import List, Tuple
from config import Config
from database import Database

def AUD_701_CONVERTSCREENSHOT(
    config: Config,
    db: Database,
    parsed_files_data: List[Tuple[str, str, dict]],
    execution_date: str,
    local_to_dbbrut_query_results: tuple,
    batch_size=100
):
    """
    Perform various database operations including retrieving execution dates,
    executing queries, deleting records, and inserting parsed XML data.

    Args:
        config (Config): Configuration instance for retrieving parameters.
        db (Database): Database instance for executing queries.
        parsed_files_data (List[Tuple[str, str, dict]]): List of parsed data from XML files.
        execution_date (str): The execution date to use in data insertion.
        local_to_dbbrut_query_results (tuple): Results from the local to DB brut query.
        batch_size (int): Number of rows to insert in each batch.
    """
    try:
        # Step 1: Delete records from 'aud_screenshot' based on query results
        aud_screenshot_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in local_to_dbbrut_query_results
        ]
        if aud_screenshot_conditions_batch:
            logging.info(f"Deleting records from aud_screenshot: {len(aud_screenshot_conditions_batch)} items.")
            db.delete_records_batch('aud_screenshot', aud_screenshot_conditions_batch)

        # Step 2: Execute 'aud_screenshot' query and retrieve results
        aud_screenshot_query = config.get_param('queries', 'aud_screenshot')
        logging.info(f"Executing query: {aud_screenshot_query}")
        aud_screenshot_results = db.execute_query(aud_screenshot_query)

        # Step 3: Delete records from 'aud_contextjob' based on 'aud_screenshot' query results
        aud_contextjob_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in aud_screenshot_results
        ]
        if aud_contextjob_conditions_batch:
            logging.info(f"Deleting records from aud_contextjob: {len(aud_contextjob_conditions_batch)} items.")
            db.delete_records_batch('aud_contextjob', aud_contextjob_conditions_batch)

        # Step 4: Prepare batch data for insertion into 'aud_screenshot'
        aud_screenshot_batch = []

        for nameproject, namejob, parsed_data in parsed_files_data:
            for screenshot in parsed_data.get('screenshots', []):
                cle = screenshot.get('key')
                screenshot_value = screenshot.get('value')
                width = screenshot.get('width')
                height = screenshot.get('height')

                # Only insert data if screenshot_value exists
                if screenshot_value:
                    params = (namejob, nameproject, screenshot_value, cle, execution_date, width, height)
                    aud_screenshot_batch.append(params)

                # Step 5: Insert data in batches if batch size is met
                if len(aud_screenshot_batch) == batch_size:
                    insert_query = config.get_param('insert_queries', 'aud_screenshot')
                    logging.info(f"Inserting batch of {batch_size} rows into aud_screenshot.")
                    db.insert_data_batch(insert_query, 'aud_screenshot', aud_screenshot_batch)
                    aud_screenshot_batch.clear()  # Clear the batch after insertion

        # Step 6: Insert any remaining data in the batch
        if aud_screenshot_batch:
            insert_query = config.get_param('insert_queries', 'aud_screenshot')
            db.insert_data_batch(insert_query, 'aud_screenshot', aud_screenshot_batch)
            logging.info(f"Inserted remaining {len(aud_screenshot_batch)} rows into aud_screenshot.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        logging.info("Operation completed!")
        # Uncomment to close the database connection if needed
        # if db:
        #     db.close()
