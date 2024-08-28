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

def AUD_302_ALIMCONTEXTJOB(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]], batch_size=100):
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

        # Step 3: Delete the output from aud_contextjob based on the query results
        aud_contextjob_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in local_to_dbbrut_query_results
        ]
        if aud_contextjob_conditions_batch:
            db.delete_records_batch('aud_contextjob', aud_contextjob_conditions_batch)
            logging.info(f"Deleted {len(aud_contextjob_conditions_batch)} records from aud_contextjob")

        # Step 4: Execute aud_contextjob query
        aud_contextjob_query = config.get_param('queries', 'aud_contextjob')
        logging.info(f"Executing query: {aud_contextjob_query}")
        aud_contextjob_results = db.execute_query(aud_contextjob_query)
        logging.debug(f"aud_contextjob_results: {aud_contextjob_results}")

        # Step 5: Delete the output from aud_contextjob based on the query results
        aud_contextjob_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in aud_contextjob_results
        ]
        if aud_contextjob_conditions_batch:
            db.delete_records_batch('aud_contextjob', aud_contextjob_conditions_batch)
            logging.info(f"Deleted {len(aud_contextjob_conditions_batch)} records from aud_contextjob")

        # Step 6: Prepare batch insertion for aud_contextjob
        aud_contextjob_data_batch = []
        aud_contextgroupdetail_data_batch = []

        for project_name, job_name, parsed_data in parsed_files_data:
            for context in parsed_data['contexts']:
                environementContextName = context['name']
                for param in context['contextParameters']:
                    comment = param['comment']
                    nameContext = param['name']
                    prompt = param['prompt']
                    promptNeeded = 0 if param['promptNeeded'] == 'false' else 1
                    typeContext = param['type']
                    valueContext = param['value']
                    repositoryContextId = param['repositoryContextId']

                    # Prepare data for insertion into aud_contextjob
                    aud_contextjob_data_batch.append((
                        environementContextName, nameContext, prompt, promptNeeded, typeContext, valueContext, repositoryContextId, project_name, job_name, execution_date
                    ))

                    # Prepare data for insertion into aud_contextgroupdetail
                    aud_contextgroupdetail_data_batch.append((
                        nameContext, comment, project_name, environementContextName, execution_date
                    ))

        if aud_contextjob_data_batch:
            insert_query = config.get_param('insert_queries', 'aud_contextjob')
            db.insert_data_batch(insert_query, 'aud_contextjob', aud_contextjob_data_batch)
            logging.info(f"Inserted {len(aud_contextjob_data_batch)} rows into aud_contextjob")

        if aud_contextgroupdetail_data_batch:
            insert_query = config.get_param('insert_queries', 'aud_contextgroupdetail')
            db.insert_data_batch(insert_query, 'aud_contextgroupdetail', aud_contextgroupdetail_data_batch)
            logging.info(f"Inserted {len(aud_contextgroupdetail_data_batch)} rows into aud_contextgroupdetail")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")
