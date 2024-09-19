import logging
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from XML_parse import XMLParser  # Importing the XMLParser class
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Changed to DEBUG to capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Ensure the file is overwritten each time for clean logs
)

def AUD_312_ALIMJOBFILS(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str,batch_size=100):
    """
    Perform operations including retrieving JDBC parameters, executing queries,
    deleting records, and inserting data in batches.

    Args:
        config (Config): An instance of the Config class for retrieving configuration parameters.
        db (Database): An instance of the Database class for executing database operations.
        parsed_files_data (List[Tuple[str, str, dict]]): List of parsed file data containing project names, job names, and parsed data dictionaries.
    """
    try:

        # Step 2: Execute audit_jobs_delta
        audit_jobs_delta = config.get_param('queries', 'audit_jobs_delta')
        logging.info(f"Executing query: {audit_jobs_delta}")
        audit_jobs_delta_results = db.execute_query(audit_jobs_delta)
        logging.debug(f"audit_jobs_delta_results: {audit_jobs_delta_results}")

        # Step 3: Delete records from aud_elementvaluenode in batches
      
        batch_delete_conditions = []

        for result in audit_jobs_delta_results:
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

        # Step 4: Execute aud_job_fils query
        aud_job_fils_query = config.get_param('queries', 'aud_job_fils')
        logging.info(f"Executing query: {aud_job_fils_query}")
        aud_job_fils_results = db.execute_query(aud_job_fils_query)
        logging.debug(f"aud_job_fils_results: {aud_job_fils_results}")

        # Step 5: Delete records from aud_job_fils in batches
        batch_delete_conditions = []
        for result in aud_job_fils_results:
            project_name, job_name = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_job_fils', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_job_fils: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_job_fils', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_job_fils: {len(batch_delete_conditions)} rows")

        # Step 6: Insert parsed data into the `aud_job_fils` table in batches
        insert_query = config.get_param('insert_queries', 'aud_job_fils')
        batch_insert = []


        for project_name, job_name, parsed_data in parsed_files_data:
            for data in parsed_data['nodes']:
                for elem_param in data['elementParameters']:
                    if data['componentName'] == "tRunJob":
                        componentName = data['componentName']
                        field = elem_param['field']
                        name = elem_param['name']
                        show = elem_param['show']
                        value = elem_param['value']

                        # Adjust the value of `Componement_UniqueName` as needed
                        Componement_UniqueName = value if field == 'TEXT' and name == 'UNIQUE_NAME' else None
                        params = ( project_name, job_name, componentName,Componement_UniqueName, field, name, show, value,  execution_date)
                        batch_insert.append(params)

                        if len(batch_insert) >= batch_size:
                            db.insert_data_batch(insert_query, 'aud_job_fils', batch_insert)
                            logging.info(f"Inserted batch of data into aud_job_fils: {len(batch_insert)} rows")
                            batch_insert.clear()

            # Insert remaining data in the batch
            if batch_insert:
                db.insert_data_batch(insert_query, 'aud_job_fils', batch_insert)
                logging.info(f"Inserted remaining batch of data into aud_job_fils: {len(batch_insert)} rows")
        # Step 7: Execute Update_job_fils query
        Update_job_fils_query = config.get_param('queries', 'Update_job_fils')
        logging.info(f"Executing query: {Update_job_fils_query}")
        Update_job_fils_results = db.execute_query(Update_job_fils_query)
        logging.debug(f"Update_job_fils_results: {Update_job_fils_results}")


# Updating tables left !!!!!!!!



    
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")
