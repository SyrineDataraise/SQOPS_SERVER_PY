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

def AUD_311_ALIMELEMENTVALUENODE(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]], batch_size=100):
    try:
        # Step 1: Get the execution date
        execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        exec_date = db.get_execution_date(execution_date_query)
        logging.info(f"Execution Date: {exec_date}")

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        logging.info(f"Executing query: {local_to_dbbrut_query}")
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete the output from aud_elementvaluenode based on the query results
        aud_elementvaluenode_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in local_to_dbbrut_query_results
        ]
        if aud_elementvaluenode_conditions_batch:
            db.delete_records_batch('aud_elementvaluenode', aud_elementvaluenode_conditions_batch)

        # Step 4: Execute aud_elementvaluenode query
        aud_elementvaluenode_query = config.get_param('queries', 'aud_elementvaluenode')
        logging.info(f"Executing query: {aud_elementvaluenode_query}")
        aud_elementvaluenode_results = db.execute_query(aud_elementvaluenode_query)
        logging.debug(f"aud_elementvaluenode_results: {aud_elementvaluenode_results}")

        # Step 5: Delete the output from aud_contextjob based on the query results
        aud_contextjob_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in aud_elementvaluenode_results
        ]
        if aud_contextjob_conditions_batch:
            db.delete_records_batch('aud_contextjob', aud_contextjob_conditions_batch)

        # Step 6: Prepare data batch for insertion into `aud_elementvaluenode`
        insert_query = config.get_param('insert_queries', 'aud_elementvaluenode')
        batch_insert = []
        aud_id = 1
        
        for NameProject, NameJob, parsed_data in parsed_files_data:
            for data in parsed_data['nodes']:
                for elem_param in data['elementParameters']:
                    aud_componentName = data['componentName']
                    componentVersion = data['componentVersion']
                    offsetLabelX = data['offsetLabelX']
                    offsetLabelY = data['offsetLabelY']
                    aud_posX = data['posX']
                    aud_posY = data['posY']
                    aud_typeField = elem_param['field']
                    name = elem_param['name']
                    show = elem_param['show']
                    value = elem_param['value']
                   
                    # Initialize variables
                    aud_elementRef = ""
                    aud_valueElementRef = ""
                    aud_columnName = ""

                    for elemValue in elem_param['elementValue']:
                        aud_columnName = elemValue['value'].replace("\"", "")
                        aud_elementRef = elemValue['elementRef']
                        aud_valueElementRef = elemValue['value']

                    # Adjust the value of `Componement_UniqueName` as needed
                    aud_componentValue = value if aud_typeField == 'TEXT' and name == 'UNIQUE_NAME' else aud_componentValue

                    params = (
                        aud_componentName, aud_posX, aud_posY, aud_typeField, 
                        aud_elementRef, aud_valueElementRef, aud_id, aud_columnName, 
                        aud_componentValue, NameProject, NameJob, exec_date
                    )
                    batch_insert.append(params)

                    if len(batch_insert) >= batch_size:
                        db.insert_data_batch(insert_query, 'aud_elementvaluenode', batch_insert)
                        logging.info(f"Inserted batch of data into aud_elementvaluenode: {len(batch_insert)} rows")
                        batch_insert.clear()
                
                    aud_id += 1

        # Insert remaining data in the batch
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_elementvaluenode', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_elementvaluenode: {len(batch_insert)} rows")

        # Step 7: Execute NodeJoinElementnode query
        # NodeJoinElementnode_query = config.get_param('queries', 'NodeJoinElementnode')
        # logging.info(f"Executing query: {NodeJoinElementnode_query}")
        # NodeJoinElementnode_results = db.execute_query(NodeJoinElementnode_query)
        # logging.debug(f"NodeJoinElementnode_results: {NodeJoinElementnode_results}")

        # # Step 8: Delete the output from aud_elementvaluenode based on the query results
        # aud_elementvaluenode_delete_conditions_batch = [
        #     {'NameProject': result[0], 'NameJob': result[1], 'aud_componentValue': result[2]}
        #     for result in NodeJoinElementnode_results
        # ]
        # if aud_elementvaluenode_delete_conditions_batch:
        #     db.delete_records_batch('aud_elementvaluenode', aud_elementvaluenode_delete_conditions_batch)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")
