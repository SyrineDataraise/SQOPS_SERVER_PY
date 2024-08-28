import os
import logging
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from XML_parse import XMLParser  # Importing the XMLParser class

# Configure logging
logging.basicConfig(
    filename='element_node_operations.log',
    level=logging.DEBUG,  # Changed to DEBUG to capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Ensure the file is overwritten each time for clean logs
)

def AUD_301_ALIMELEMENTNODE(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]]):
    """
    Perform operations including retrieving JDBC parameters, executing queries,
    deleting records, and inserting data in batches.

    Args:
        config (Config): An instance of the Config class for retrieving configuration parameters.
        db (Database): An instance of the Database class for executing database operations.
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
        logging.info(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete records from aud_elementvaluenode in batches
        batch_size = 300
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

        # Step 4: Execute aud_elementnode query
        aud_elementnode_query = config.get_param('queries', 'aud_elementnode')
        logging.info(f"Executing query: {aud_elementnode_query}")
        aud_elementnode_results = db.execute_query(aud_elementnode_query)
        logging.info(f"aud_elementnode_results: {aud_elementnode_results}")

        # Step 5: Delete records from aud_elementvaluenode in batches
        batch_delete_conditions = []
        for result in aud_elementnode_results:
            project_name, job_name = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_elementvaluenode', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_elementvaluenode: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_elementvaluenode', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_elementvaluenode: {len(batch_delete_conditions)} rows")

        # Step 6: Loop over `.item` files in the items_directory and parse them
        items_directory = config.get_param('Directories', 'items_directory')
        xml_parser = XMLParser("")
        filenames = [f for f in os.listdir(items_directory) if f.endswith('.item')]
        insert_batch_size = 10000
        batch_insert = []

        for filename in filenames:
            project_name, job_name, parsed_data = xml_parser.loop_parse(filename, items_directory)

            # Step 7: Insert parsed data into the `aud_elementnode` table in batches
            insert_query = config.get_param('insert_queries', 'aud_elementnode')
            for data in parsed_data['nodes']:
                componentName = data['componentName']
                field = data['field']
                name = data['name']
                show = data['show']
                value = data['value']
                
                # Adjust the value of `Componement_UniqueName` as needed
                Componement_UniqueName = value if field == 'TEXT' and name == 'UNIQUE_NAME' else None
                params = (componentName, field, name, show, value, Componement_UniqueName, project_name, job_name, execution_date)
                batch_insert.append(params)
                
                if len(batch_insert) >= insert_batch_size:
                    db.insert_data_batch(insert_query, 'aud_elementnode', batch_insert)
                    logging.info(f"Inserted batch of data into aud_elementnode: {len(batch_insert)} rows")
                    batch_insert.clear()

        # Insert remaining data in the batch
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_elementnode', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_elementnode: {len(batch_insert)} rows")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed")


