import os
import logging
from typing import List, Tuple

from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from XML_parse import XMLParser  # Importing the XMLParser class
# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Changed to DEBUG to capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Ensure the file is overwritten each time for clean logs
)
def AUD_303_ALIMNODE(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]], batch_size=10):
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

        # Step 3: Delete the output from aud_node based on the query results
        aud_node_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in local_to_dbbrut_query_results
        ]
        if aud_node_conditions_batch:
            db.delete_records_batch('aud_node', aud_node_conditions_batch)

        # Step 4: Execute aud_node query
        aud_node_query = config.get_param('queries', 'aud_node')
        logging.info(f"Executing query: {aud_node_query}")
        aud_node_results = db.execute_query(aud_node_query)
        logging.debug(f"aud_node_results: {aud_node_results}")

        # Step 5: Delete the output from aud_contextjob based on the query results
        aud_contextjob_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in aud_node_results
        ]
        if aud_contextjob_conditions_batch:
            db.delete_records_batch('aud_contextjob', aud_contextjob_conditions_batch)

        # Step 6: Prepare data batch for insertion into `aud_node`
        aud_node_data_batch = []
        for project_name, job_name, parsed_data in parsed_files_data:
            for data in parsed_data['nodes']:
                for elem_param in data['elementParameters']:
                    componentName = data['componentName']
                    componentVersion = data['componentVersion']
                    offsetLabelX = data['offsetLabelX']
                    offsetLabelY = data['offsetLabelY']
                    posX = data['posX']
                    posY = data['posY']
                    field = elem_param['field']
                    name = elem_param['name']
                    show = elem_param['show']
                    value = elem_param['value']

                    # Adjust the value of `Componement_UniqueName` as needed
                    Componement_UniqueName = value if field == 'TEXT' and name == 'UNIQUE_NAME' else None
                    aud_node_data_batch.append((
                        componentName, componentVersion, offsetLabelX, offsetLabelY, posX, posY, Componement_UniqueName, project_name, job_name, execution_date
                    ))

        if aud_node_data_batch:
            insert_query = config.get_param('insert_queries', 'aud_node')
            db.insert_data_batch(insert_query, 'aud_node', aud_node_data_batch)

        # Step 7: Execute NodeJoinElementnode
        NodeJoinElementnode_query = config.get_param('queries', 'NodeJoinElementnode')
        logging.info(f"Executing query: {NodeJoinElementnode_query}")
        NodeJoinElementnode_results = db.execute_query(NodeJoinElementnode_query)
        logging.debug(f"NodeJoinElementnode_results: {NodeJoinElementnode_results}")

        # Step 8: Delete the output from aud_node based on the query results
        aud_node_delete_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1], 'aud_componentValue': result[2]}
            for result in NodeJoinElementnode_results
        ]
        if aud_node_delete_conditions_batch:
            db.delete_records_batch('aud_node', aud_node_delete_conditions_batch)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")
