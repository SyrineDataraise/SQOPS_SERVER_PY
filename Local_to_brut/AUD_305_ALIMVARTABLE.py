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

def AUD_305_ALIMVARTABLE(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str, batch_size=100):
    """
    Perform operations including retrieving JDBC parameters, executing queries,
    deleting records, and inserting data in batches.

    Args:
        config (Config): An instance of the Config class for retrieving configuration parameters.
        db (Database): An instance of the Database class for executing database operations.
        parsed_files_data (List[Tuple[str, str, dict]]): List of parsed file data containing project names, job names, and parsed data dictionaries.
        batch_size (int): The size of batches for deletions and insertions.
    """
    try:
        # Step 1: Get the execution date
        # execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        # execution_date = db.get_execution_date(execution_date_query)
        # logging.info(f"Execution Date: {execution_date}")

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        logging.info(f"Executing query: {local_to_dbbrut_query}")
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete records from aud_vartable in batches
        batch_delete_conditions = []

        for result in local_to_dbbrut_query_results:
            project_name, job_name, *_ = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_vartable', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_vartable: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_vartable', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_vartable: {len(batch_delete_conditions)} rows")

        # Step 4: Execute aud_vartable query
        vartableJoinElemntnode_query = config.get_param('queries', 'aud_vartable')
        logging.info(f"Executing query: {vartableJoinElemntnode_query}")
        aud_vartable_results = db.execute_query(vartableJoinElemntnode_query)
        logging.debug(f"aud_vartable_results: {aud_vartable_results}")

        # Step 5: Delete records from aud_vartable in batches
        batch_delete_conditions = []
        for result in aud_vartable_results:
            project_name, job_name = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_vartable', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_vartable: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_vartable', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_vartable: {len(batch_delete_conditions)} rows")

        # Step 6: Insert parsed data into the `aud_vartable` table in batches
        insert_query = config.get_param('insert_queries', 'aud_vartable')
        batch_insert = []
        insert_batch_size = batch_size  # Use the provided batch size

        for project_name, job_name, parsed_data in parsed_files_data:
            logging.info(f"Processing project: {project_name}, job: {job_name}")
            for data in parsed_data['nodes']:
                componentName = data['componentName']
                logging.debug(f"Processing component: {componentName}")

                for elem_param in data['elementParameters']:
                    field = elem_param['field']
                    name = elem_param['name']
                    show = elem_param['show']
                    value = elem_param['value']
                    Componement_UniqueName = value if field == 'TEXT' and name == 'UNIQUE_NAME' else Componement_UniqueName
                    logging.debug(f"Element parameter - field: {field}, name: {name}, value: {value}")

                for nodeData in data['nodeData']:
                    shellMaximized = nodeData['uiPropefties'].get('shellMaximized', 0)
                    aud_Var = nodeData['varTables'].get('name', '')
                    aud_sizeState = nodeData['varTables'].get('sizeState', '')
                    logging.debug(f"Node data - shellMaximized: {shellMaximized}, aud_Var: {aud_Var}, aud_sizeState: {aud_sizeState}")

                    for mapperTableEntries in nodeData['mapperTableEntries']:
                        aud_nameVar = mapperTableEntries.get('name', '')
                        aud_expressionVar = mapperTableEntries.get('expression', '')
                        aud_type = mapperTableEntries.get('type', '')
                        logging.debug(f"mapperTableEntries - nameVar: {aud_nameVar}, expressionVar: {aud_expressionVar}, type: {aud_type}")

                        params = (
                            componentName, Componement_UniqueName, aud_Var, aud_sizeState, 
                            aud_nameVar, aud_expressionVar, aud_type, shellMaximized, 
                            project_name, job_name, execution_date
                        )
                        batch_insert.append(params)

                        if len(batch_insert) >= insert_batch_size:
                            db.insert_data_batch(insert_query, 'aud_vartable', batch_insert)
                            logging.info(f"Inserted batch of data into aud_vartable: {len(batch_insert)} rows")
                            batch_insert.clear()

        # Insert remaining data in the batch
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_vartable', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_vartable: {len(batch_insert)} rows")

        # Step 7: Execute vartableJoinElemntnode query
        vartableJoinElemntnode_query = config.get_param('queries', 'vartableJoinElemntnode')
        logging.info(f"Executing query: {vartableJoinElemntnode_query}")
        vartableJoinElemntnode_results = db.execute_query(vartableJoinElemntnode_query)
        logging.debug(f"vartableJoinElemntnode_results: {vartableJoinElemntnode_results}")

        # Step 5: Delete records from aud_vartable in batches
        batch_delete_conditions = []
        for result in vartableJoinElemntnode_results:
            project_name, job_name = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_vartable', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_vartable: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('vartableJoinElemntnode_results', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_vartable: {len(batch_delete_conditions)} rows")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")
