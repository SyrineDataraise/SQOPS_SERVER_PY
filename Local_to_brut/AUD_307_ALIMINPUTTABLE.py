import logging
from config import Config
from database import Database
from XML_parse import XMLParser
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)

def AUD_307_ALIMINPUTTABLE(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],exec_date : str, batch_size=100):
    try:

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete records from aud_inputtable in batches
        delete_conditions = []
        for result in local_to_dbbrut_query_results:
            project_name, job_name, *_ = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_inputtable', delete_conditions)
                logging.info(f"Batch deleted records from aud_inputtable: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_inputtable', delete_conditions)
            logging.info(f"Deleted remaining records from aud_inputtable: {len(delete_conditions)} rows")

        # Step 4: Execute aud_inputtable query
        aud_inputtable_query = config.get_param('queries', 'aud_inputtable')
        aud_inputtable_results = db.execute_query(aud_inputtable_query)
        logging.debug(f"aud_inputtable_results: {aud_inputtable_results}")

        # Step 5: Delete records from aud_inputtable based on query results
        delete_conditions = []
        for result in aud_inputtable_results:
            project_name, job_name = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_inputtable', delete_conditions)
                logging.info(f"Batch deleted records from aud_inputtable: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_inputtable', delete_conditions)
            logging.info(f"Deleted remaining records from aud_inputtable: {len(delete_conditions)} rows")

        # Step 6: Insert parsed data into aud_inputtable in batches
        insert_query = config.get_param('insert_queries', 'aud_inputtable')
        batch_insert = []
        batch_size = 100  # Define your batch size

        for NameProject, NameJob, parsed_data in parsed_files_data:
            logging.info(f"Processing project: {NameProject}, job: {NameJob}")
            for data in parsed_data['nodes']:
                aud_componentName = data['componentName']

                # Extract 'aud_componentValue' based on element parameters
                aud_componentValue = None
                for elem_param in data['elementParameters']:
                    field = elem_param['field']
                    name = elem_param['name']
                    value = elem_param['value']
                    if field == 'TEXT' and name == 'UNIQUE_NAME':
                        aud_componentValue = value
                
                # Process node data
                for nodeData in data['nodeData']:
                    input_tables = nodeData.get('inputTables', {})

                    aud_lookupMode = input_tables.get('lookupMode')
                    aud_matchingMode = input_tables.get('matchingMode')
                    aud_nameRowInput = input_tables.get('nameRowInput', None)  # Example of fetching input data
                    aud_sizeState = input_tables.get('sizeState', None)
                    aud_nameColumnInput = input_tables.get('nameColumnInput', None)
                    aud_activateExpressionFilterInput = input_tables.get('activateExpressionFilterInput', None)
                    aud_expressionFilterInput = input_tables.get('expressionFilterInput', None)
                    aud_activateCondensedTool = input_tables.get('activateCondensedTool', None)
                    aud_innerJoin = input_tables.get('innerJoin', None)
                    persistent = input_tables.get('persistent', None)

                    # Extract mapper table entries
                    for mapper_entry in input_tables.get('mapperTableEntries', []):
                        aud_expressionJoin = mapper_entry.get('expression')
                        aud_nameColumnInput = mapper_entry.get('name')
                        aud_type = mapper_entry.get('type')
                        aud_nullable = 1 if mapper_entry.get('nullable') == 'true' else 0
                        aud_operator = mapper_entry.get('operator')

                        # Prepare the parameters for insertion
                        params = (
                            aud_componentName, aud_lookupMode, aud_matchingMode, aud_nameRowInput, aud_sizeState,
                            aud_nameColumnInput, aud_type, aud_nullable, aud_expressionJoin, aud_operator,
                            aud_activateExpressionFilterInput, aud_expressionFilterInput, aud_componentValue,
                            aud_activateCondensedTool, aud_innerJoin, persistent, NameProject, NameJob, exec_date
                        )

                        batch_insert.append(params)

                        # Insert the batch when the size limit is reached
                        if len(batch_insert) >= batch_size:
                            db.insert_data_batch(insert_query, 'aud_inputtable', batch_insert)
                            logging.info(f"Inserted batch of data into aud_inputtable: {len(batch_insert)} rows")
                            batch_insert.clear()

                    if aud_nameColumnInput is None:
                        logging.warning("aud_nameColumnInput is None, skipping this entry.")

        # Insert remaining data after the loop
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_inputtable', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_inputtable: {len(batch_insert)} rows")

        # Step 7: Execute inputtableJoinElemntnode query and delete records
        inputtableJoinElemntnode_query = config.get_param('queries', 'inputtableJoinElemntnode')
        inputtableJoinElemntnode_results = db.execute_query(inputtableJoinElemntnode_query)
        logging.debug(f"inputtableJoinElemntnode_results: {inputtableJoinElemntnode_results}")

        delete_conditions = []
        for result in inputtableJoinElemntnode_results:
            project_name, job_name = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('inputtableJoinElemntnode_results', delete_conditions)
                logging.info(f"Batch deleted records from aud_inputtable: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('inputtableJoinElemntnode_results', delete_conditions)
            logging.info(f"Deleted remaining records from aud_inputtable: {len(delete_conditions)} rows")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()
            logging.info("Database connection closed.")
