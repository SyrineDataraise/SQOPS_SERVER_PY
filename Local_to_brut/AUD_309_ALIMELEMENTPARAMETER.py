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

def AUD_309_ALIMELEMENTPARAMETER(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],exec_date : str,  batch_size=100):
    try:

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete records from aud_elementparameter in batches
        delete_conditions = []
        for result in local_to_dbbrut_query_results:
            project_name, job_name, *_ = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_elementparameter', delete_conditions)
                logging.info(f"Batch deleted records from aud_elementparameter: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_elementparameter', delete_conditions)
            logging.info(f"Deleted remaining records from aud_elementparameter: {len(delete_conditions)} rows")

        # Step 4: Execute aud_elementparameter query
        aud_elementparameter_query = config.get_param('queries', 'aud_elementparameter')
        aud_elementparameter_results = db.execute_query(aud_elementparameter_query)
        logging.debug(f"aud_elementparameter_results: {aud_elementparameter_results}")

        # Step 5: Delete records from aud_elementparameter based on query results
        delete_conditions = []
        for result in aud_elementparameter_results:
            project_name, job_name = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_elementparameter', delete_conditions)
                logging.info(f"Batch deleted records from aud_elementparameter: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_elementparameter', delete_conditions)
            logging.info(f"Deleted remaining records from aud_elementparameter: {len(delete_conditions)} rows")

        # Step 6: Insert parsed data into aud_elementparameter in batches
        insert_query = config.get_param('insert_queries', 'aud_elementparameter')
        logging.debug(f"Insert Query: {insert_query}")
        batch_insert = []

        for NameProject, NameJob, parsed_data in parsed_files_data:
            logging.info(f"Processing project: {project_name}, job: {job_name}")
            for parameter in parsed_data['parameters']:

                for elementParameter in parameter['elementParameter']:
                    aud_field = elementParameter['field']
                    aud_name = elementParameter['name']
                    aud_show = elementParameter['show']
                    aud_value = elementParameter['value']
                    params =   (aud_field,aud_name,aud_show,aud_value,NameProject,NameJob,exec_date)
                    batch_insert.append(params)


                    if len(batch_insert) >= batch_size:
                        try:
                            db.insert_data_batch(insert_query, 'aud_elementparameter', batch_insert)
                            logging.info(f"Inserted batch of data into aud_elementparameter: {len(batch_insert)} rows")
                        except Exception as insert_error:
                            logging.error(f"Error during batch insert: {insert_error}", exc_info=True)
                        finally:
                            batch_insert.clear()

        if batch_insert:
            try:
                logging.info("Inserting remaining batch into database.")
                db.insert_data_batch(insert_query, 'aud_elementparameter', batch_insert)
                logging.info(f"Inserted remaining batch of data into aud_elementparameter: {len(batch_insert)} rows")
            except Exception as insert_error:
                logging.error(f"Error during final batch insert: {insert_error}", exc_info=True)

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()
            logging.info("Database connection closed.")
