import logging
from config import Config
from database import Database
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

def AUD_310_ALIMLIBRARY(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]], batch_size=100):
    try:
        # Step 1: Get the execution date
        execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        execution_date = db.get_execution_date(execution_date_query)
        logging.info(f"Execution Date: {execution_date}")

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete records from aud_library in batches
        delete_conditions = []
        for result in local_to_dbbrut_query_results:
            project_name, job_name, *_ = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_library', delete_conditions)
                logging.info(f"Batch deleted records from aud_library: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_library', delete_conditions)
            logging.info(f"Deleted remaining records from aud_library: {len(delete_conditions)} rows")

        # Step 4: Execute aud_library query
        aud_library_query = config.get_param('queries', 'aud_library')
        aud_library_results = db.execute_query(aud_library_query)
        logging.debug(f"aud_library_results: {aud_library_results}")

        # Step 5: Delete records from aud_library based on query results
        delete_conditions = []
        for result in aud_library_results:
            project_name, job_name = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_library', delete_conditions)
                logging.info(f"Batch deleted records from aud_library: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_library', delete_conditions)
            logging.info(f"Deleted remaining records from aud_library: {len(delete_conditions)} rows")

        # Step 6: Insert parsed data into aud_library in batches
        insert_query = config.get_param('insert_queries', 'aud_library')
        logging.debug(f"Insert Query: {insert_query}")
        batch_insert = []
        for project_name, job_name, parsed_data in parsed_files_data:
            for data in parsed_data['nodes']:
                for elem_param in data['elementParameters']:
                    # Assign componentName based on specific condition
                    field = elem_param['field']
                    name = elem_param['name']
                    value = elem_param['value']

                    if field == 'TEXT' and name == 'UNIQUE_NAME':
                        componentName = value

                    # Check if conditions for componentName and elem_param value are met
                    if (componentName is not None and 
                        ("Java" in componentName or "tLibraryLoad" in componentName) and 
                        (value is not None and "//" not in value)):

                        # Only insert if name is "IMPORT" or "LIBRARY"
                        if name in ["IMPORT", "LIBRARY"]:
                            aud_typeInput = name

                            # Determine aud_libraryImport based on condition
                            aud_libraryImport = value # if name == "LIST_DELIMITER" else "&,&" eliminé!!!!
                            

                            # Prepare the parameters tuple
                            params = (componentName, aud_typeInput, aud_libraryImport, project_name, job_name, execution_date)
                            batch_insert.append(params)

                            # Insert in batches
                            if len(batch_insert) >= batch_size:
                                db.insert_data_batch(insert_query, 'aud_library', batch_insert)
                                logging.info(f"Inserted batch of data into aud_library: {len(batch_insert)} rows")
                                batch_insert.clear()

        # Insert any remaining data
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_library', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_library: {len(batch_insert)} rows")



    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()
            logging.info("Database connection closed.")
