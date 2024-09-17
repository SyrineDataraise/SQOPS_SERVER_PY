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

def AUD_306_ALIMOUTPUTTABLE(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str, batch_size=100):
    try:
        # Step 1: Get the execution date
        # execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        # execution_date = db.get_execution_date(execution_date_query)
        # logging.info(f"Execution Date: {execution_date}")

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete records from aud_outputTable in batches
        delete_conditions = []
        for result in local_to_dbbrut_query_results:
            project_name, job_name, *_ = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_outputTable', delete_conditions)
                logging.info(f"Batch deleted records from aud_outputTable: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_outputTable', delete_conditions)
            logging.info(f"Deleted remaining records from aud_outputTable: {len(delete_conditions)} rows")

        # Step 4: Execute aud_outputTable query
        aud_outputTable_query = config.get_param('queries', 'aud_outputTable')
        aud_outputTable_results = db.execute_query(aud_outputTable_query)
        logging.debug(f"aud_outputTable_results: {aud_outputTable_results}")

        # Step 5: Delete records from aud_outputTable based on query results
        delete_conditions = []
        for result in aud_outputTable_results:
            project_name, job_name = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_outputTable', delete_conditions)
                logging.info(f"Batch deleted records from aud_outputTable: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_outputTable', delete_conditions)
            logging.info(f"Deleted remaining records from aud_outputTable: {len(delete_conditions)} rows")

        # Step 6: Insert parsed data into aud_outputTable in batches
        insert_query = config.get_param('insert_queries', 'aud_outputTable')
        batch_insert = []

        for project_name, job_name, parsed_data in parsed_files_data:
            logging.info(f"Processing project: {project_name}, job: {job_name}")
            for data in parsed_data['nodes']:
                aud_componentName = data['componentName']

                for elem_param in data['elementParameters']:
                    field = elem_param['field']
                    name = elem_param['name']
                    value = elem_param['value']
                    aud_componentValue = value if field == 'TEXT' and name == 'UNIQUE_NAME' else aud_componentValue

                for nodeData in data['nodeData']:
                    output_tables = nodeData.get('outputTables', {})
                    
                    aud_OutputName = output_tables.get('name')
                    aud_sizeState = output_tables.get('sizeState')
                    aud_activateCondensedTool = 1 if output_tables.get('activateCondensedTool') == 'true' else 0
                    aud_reject = output_tables.get('reject')
                    aud_rejectInnerJoin = output_tables.get('rejectInnerJoin')
                    aud_activateExpressionFilter = 1 if output_tables.get('activateExpressionFilter') == 'true' else 0
                    aud_expressionFilterOutput = output_tables.get('expressionFilter')

                    for mapper_entry in output_tables.get('mapperTableEntries', []):
                        aud_expressionOutput = mapper_entry.get('expression')
                        aud_nameColumnOutput = mapper_entry.get('name')
                        aud_type = mapper_entry.get('type')
                        aud_nullable = 1 if mapper_entry.get('nullable') == 'true' else 0

                        if aud_OutputName:
                            params = (
                                aud_componentName, aud_OutputName, aud_sizeState, aud_activateCondensedTool, aud_reject, 
                                aud_rejectInnerJoin, aud_expressionOutput, aud_nameColumnOutput, aud_type, aud_nullable, 
                                aud_activateExpressionFilter, aud_expressionFilterOutput, aud_componentValue, project_name, 
                                job_name, execution_date
                            )
                            batch_insert.append(params)

                            if len(batch_insert) >= batch_size:
                                db.insert_data_batch(insert_query, 'aud_outputTable', batch_insert)
                                logging.info(f"Inserted batch of data into aud_outputTable: {len(batch_insert)} rows")
                                batch_insert.clear()
                        else:
                            logging.warning("aud_OutputName is None, skipping this entry.")

        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_outputTable', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_outputTable: {len(batch_insert)} rows")

        # Step 7: Execute outputtableJoinElemntnode query and delete records
        outputtableJoinElemntnode_query = config.get_param('queries', 'outputtableJoinElemntnode')
        outputtableJoinElemntnode_results = db.execute_query(outputtableJoinElemntnode_query)
        logging.debug(f"outputtableJoinElemntnode_results: {outputtableJoinElemntnode_results}")

        delete_conditions = []
        for result in outputtableJoinElemntnode_results:
            project_name, job_name = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('outputtableJoinElemntnode_results', delete_conditions)
                logging.info(f"Batch deleted records from aud_outputTable: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('outputtableJoinElemntnode_results', delete_conditions)
            logging.info(f"Deleted remaining records from aud_outputTable: {len(delete_conditions)} rows")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()
            logging.info("Database connection closed.")
