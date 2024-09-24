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

def AUD_301_ALIMELEMENTNODE(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str,local_to_dbbrut_query_results:tuple,batch_size=100 ):
    """
    Perform operations including retrieving JDBC parameters, executing queries,
    deleting records, and inserting data in batches.

    Args:
        config (Config): An instance of the Config class for retrieving configuration parameters.
        db (Database): An instance of the Database class for executing database operations.
        parsed_files_data (List[Tuple[str, str, dict]]): List of parsed file data containing project names, job names, and parsed data dictionaries.
    """
    try:

        # Step 3: Delete records from aud_elementvaluenode in batches
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
        logging.debug(f"aud_elementnode_results: {aud_elementnode_results}")

        # Step 5: Delete records from aud_elementnode in batches
        batch_delete_conditions = []
        for result in aud_elementnode_results:
            project_name, job_name = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_elementnode', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_elementnode: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_elementnode', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_elementnode: {len(batch_delete_conditions)} rows")

        # Step 6: Insert parsed data into the `aud_elementnode` table in batches
        insert_query = config.get_param('insert_queries', 'aud_elementnode')
        batch_insert = []

        for project_name, job_name, parsed_data in parsed_files_data:
            for data in parsed_data['nodes']:
                for elem_param in data['elementParameters']:
                    componentName = data['componentName']
                    field = elem_param['field']
                    name = elem_param['name']
                    show = elem_param['show']
                    value = elem_param['value']

                    # Adjust the value of `Componement_UniqueName` as needed
                    Componement_UniqueName = value if field == 'TEXT' and name == 'UNIQUE_NAME' else None
                    params = (componentName, field, name, show, value, Componement_UniqueName, project_name, job_name, execution_date)
                    batch_insert.append(params)

                    if len(batch_insert) >= batch_size:
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
            logging.info("Database connection closed.")


def AUD_302_ALIMCONTEXTJOB(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str,local_to_dbbrut_query_results:tuple,batch_size=100 ):
    try:

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


def AUD_303_ALIMNODE(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str, batch_size=100):
    try:
        # # Step 1: Get the execution date
        # execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        # execution_date = db.get_execution_date(execution_date_query)
        # logging.info(f"Execution Date: {execution_date}")

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

        # Step 6: Prepare data batch for insertion into `aud_elementnode`
        insert_query = config.get_param('insert_queries', 'aud_elementnode')
        batch_insert = []
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

                    params = (
                        componentName, componentVersion, offsetLabelX, offsetLabelY, posX, posY,
                        Componement_UniqueName, project_name, job_name, execution_date
                    )

                    batch_insert.append(params)

                    if len(batch_insert) >= batch_size:
                        db.insert_data_batch(insert_query, 'aud_elementnode', batch_insert)
                        logging.info(f"Inserted batch of data into aud_elementnode: {len(batch_insert)} rows")
                        batch_insert.clear()

        # Insert remaining data in the batch
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_elementnode', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_elementnode: {len(batch_insert)} rows")

        # Step 7: Execute NodeJoinElementnode query
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
def AUD_303_BIGDATA_PARAMETERS(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str, batch_size=100):
    """
    Perform various database operations including retrieving execution dates,
    executing queries, deleting records, and inserting parsed XML data.

    Args:
        config (Config): Configuration instance for retrieving parameters.
        db (Database): Database instance for executing queries.
        parsed_files_data (List[Tuple[str, str, dict]]): List of parsed data from XML files.
        batch_size (int): Number of rows to insert in each batch.
    """
    try:
        # # Step 1: Get the execution date
        # execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        # execution_date = db.get_execution_date(execution_date_query)
        # logging.info(f"Execution Date: {execution_date}")

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        logging.info(f"Executing query: {local_to_dbbrut_query}")
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete output from `aud_bigdata` based on query results
        aud_bigdata_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in local_to_dbbrut_query_results
        ]
        if aud_bigdata_conditions_batch:
            db.delete_records_batch('aud_bigdata', aud_bigdata_conditions_batch)

        # Step 4: Execute `aud_bigdata` query
        aud_bigdata_query = config.get_param('queries', 'aud_bigdata')
        logging.info(f"Executing query: {aud_bigdata_query}")
        aud_bigdata_results = db.execute_query(aud_bigdata_query)
        logging.debug(f"aud_bigdata_results: {aud_bigdata_results}")

        # Step 5: Delete output from `aud_contextjob` based on query results
        aud_contextjob_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in aud_bigdata_results
        ]
        if aud_contextjob_conditions_batch:
            db.delete_records_batch('aud_contextjob', aud_contextjob_conditions_batch)

        # Step 6: Prepare batches for insertion into `aud_bigdata` and `aud_bigdata_elementvalue` tables
        aud_bigdata_batch = []
        aud_bigdata_elementvalue_batch = []

        for project_name, job_name, parsed_data in parsed_files_data:
            for param_data in parsed_data['parameters']:
                aud_bigdata_batch.append((
                    param_data['field'], param_data['name'], param_data['show'],
                    param_data['value'], project_name, job_name, execution_date
                ))

                for elementValue in param_data['elementValues']:
                    aud_bigdata_elementvalue_batch.append((
                        elementValue['elementRef'], elementValue['value'], param_data['name'],
                        project_name, job_name, execution_date
                    ))

        # Step 7: Insert data in batches
        if aud_bigdata_batch:
            insert_query = config.get_param('insert_queries', 'aud_bigdata')
            db.insert_data_batch(insert_query, 'aud_bigdata', aud_bigdata_batch)
        
        if aud_bigdata_elementvalue_batch:
            insert_query = config.get_param('insert_queries', 'aud_bigdata_elementvalue')
            db.insert_data_batch(insert_query, 'aud_bigdata_elementvalue', aud_bigdata_elementvalue_batch)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")

def AUD_304_ALIMMETADATA(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str, batch_size=100):
    """
    Perform various database operations including retrieving JDBC parameters, 
    executing queries, deleting records, and inserting data.

    Args:
        config (Config): An instance of the Config class for retrieving configuration parameters.
        db (Database): An instance of the Database class for executing database operations.
        parsed_files_data (List[Tuple[str, str, dict]]): A list where each tuple contains (project_name, job_name, parsed_data).
        batch_size (int): The number of rows to insert in each batch.
    """
    try:
        # # Step 1: Get the execution date
        # execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        # execution_date = db.get_execution_date(execution_date_query)
        # logging.info(f"Execution Date: {execution_date}")

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        logging.info(f"Executing query: {local_to_dbbrut_query}")
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete the output from aud_metadata based on the query results
        delete_conditions = []
        for result in local_to_dbbrut_query_results:
            project_name, job_name, _, _, _ = result  # Assuming result contains these fields in order
            delete_conditions.append({
                'NameProject': project_name,
                'NameJob': job_name
            })
        db.delete_records_batch('aud_metadata', delete_conditions)
        logging.info(f"Deleted records for projects/jobs: {[(d['NameProject'], d['NameJob']) for d in delete_conditions]}")

        # Step 4: Execute aud_metadata query
        aud_metadata_query = config.get_param('queries', 'aud_metadata')
        logging.info(f"Executing query: {aud_metadata_query}")
        aud_metadata_results = db.execute_query(aud_metadata_query)
        logging.debug(f"aud_metadata_results: {aud_metadata_results}")

        # Step 5: Delete records based on the aud_metadata query results
        delete_conditions = []
        for result in aud_metadata_results:
            project_name, job_name = result
            logging.debug(f"Preparing to delete records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")
            delete_conditions.append({
                'NameProject': project_name,
                'NameJob': job_name
            })
        db.delete_records_batch('aud_metadata', delete_conditions)
        logging.info(f"Deleted records for projects/jobs: {[(d['NameProject'], d['NameJob']) for d in delete_conditions]}")

        # Step 6: Collect parsed parameters data into batches
        data_batch = []
        for project_name, job_name, parsed_data in parsed_files_data:
            for node_data in parsed_data['nodes']:
                for elem_param in node_data['elementParameters']:
                    for elem_value in elem_param['elementValues']:
                        for meta in node_data['metadata']:
                            for column in meta['columns']:
                                params = (
                                    meta['connector'],
                                    meta['label'],
                                    meta['name'],
                                    column['comment'],
                                    int(column['key'] != 'false'),
                                    column['length'],
                                    column['name'],
                                    int(column['nullable'] != 'false'),
                                    column['pattern'],
                                    column['precision'],
                                    column['sourceType'],
                                    column['type'],
                                    int(column['usefulColumn'] != 'false'),
                                    column['originalLength'],
                                    column['defaultValue'],
                                    elem_value['value'],
                                    node_data['componentName'],
                                    project_name,
                                    job_name,
                                    execution_date
                                )
                                data_batch.append(params)

                                # If the batch size is reached, insert the data
                                if len(data_batch) == batch_size:
                                    db.insert_metadata('aud_metadata', data_batch)
                                    data_batch.clear()  # Clear the batch after insertion

        # Insert any remaining data that didn't fill a complete batch
        if data_batch:
            db.insert_metadata('aud_metadata', data_batch)

        # Step 7: Execute MetadataJoinElemntnode query
        metadata_join_element_node_query = config.get_param('queries', 'MetadataJoinElemntnode')
        logging.info(f"Executing query: {metadata_join_element_node_query}")
        metadata_join_element_node_results = db.execute_query(metadata_join_element_node_query)
        logging.debug(f"MetadataJoinElemntnode_results: {metadata_join_element_node_results}")

        # Step 8: Delete records based on MetadataJoinElemntnode query results
        delete_conditions = []
        for result in metadata_join_element_node_results:
            project_name, job_name = result
            logging.debug(f"Preparing to delete records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")
            delete_conditions.append({
                'NameProject': project_name,
                'NameJob': job_name
            })
        db.delete_records_batch('aud_metadata', delete_conditions)
        logging.info(f"Deleted records for projects/jobs: {[(d['NameProject'], d['NameJob']) for d in delete_conditions]}")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed")


def AUD_305_ALIMVARTABLE_XML(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str, batch_size=100):
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

        # Step 3: Delete records from aud_vartable_xml in batches
        batch_delete_conditions = []

        for result in local_to_dbbrut_query_results:
            project_name, job_name, *_ = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_vartable_xml', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_vartable_xml: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_vartable_xml', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_vartable_xml: {len(batch_delete_conditions)} rows")

        # Step 4: Execute aud_vartable_xml query
        vartableJoinElemntnode_query = config.get_param('queries', 'aud_vartable_xml')
        logging.info(f"Executing query: {vartableJoinElemntnode_query}")
        aud_vartable_xml_results = db.execute_query(vartableJoinElemntnode_query)
        logging.debug(f"aud_vartable_xml_results: {aud_vartable_xml_results}")

        # Step 5: Delete records from aud_vartable_xml in batches
        batch_delete_conditions = []
        for result in aud_vartable_xml_results:
            project_name, job_name = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_vartable_xml', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_vartable_xml: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('aud_vartable_xml', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_vartable_xml: {len(batch_delete_conditions)} rows")

        # Step 6: Insert parsed data into the `aud_vartable_xml` table in batches
        insert_query = config.get_param('insert_queries', 'aud_vartable_xml')
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
                            aud_nameVar, aud_expressionVar, aud_type, 
                            project_name, job_name, execution_date
                        )

                        batch_insert.append(params)

                        if len(batch_insert) >= insert_batch_size:
                            db.insert_data_batch(insert_query, 'aud_vartable_xml', batch_insert)
                            logging.info(f"Inserted batch of data into aud_vartable_xml: {len(batch_insert)} rows")
                            batch_insert.clear()

        # Insert remaining data in the batch
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_vartable_xml', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_vartable_xml: {len(batch_insert)} rows")

        # Step 7: Execute vartableJoinElemntnode query
        vartableJoinElemntnode_query = config.get_param('queries', 'vartableJoinElemntnode')
        logging.info(f"Executing query: {vartableJoinElemntnode_query}")
        vartableJoinElemntnode_results = db.execute_query(vartableJoinElemntnode_query)
        logging.debug(f"vartableJoinElemntnode_results: {vartableJoinElemntnode_results}")

        # Step 5: Delete records from aud_vartable_xml in batches
        batch_delete_conditions = []
        for result in vartableJoinElemntnode_results:
            project_name, job_name = result
            batch_delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(batch_delete_conditions) >= batch_size:
                db.delete_records_batch('aud_vartable_xml', batch_delete_conditions)
                logging.info(f"Batch deleted records from aud_vartable_xml: {len(batch_delete_conditions)} rows")
                batch_delete_conditions.clear()

        # Delete remaining records
        if batch_delete_conditions:
            db.delete_records_batch('vartableJoinElemntnode_results', batch_delete_conditions)
            logging.info(f"Batch deleted remaining records from aud_vartable_xml: {len(batch_delete_conditions)} rows")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()  # Ensure the database connection is closed
            logging.info("Database connection closed.")
            


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


def AUD_307_ALIMINPUTTABLE_XML(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],exec_date : str, batch_size=100):
    try:

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete records from aud_inputtable_xml in batches
        delete_conditions = []
        for result in local_to_dbbrut_query_results:
            project_name, job_name, *_ = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_inputtable_xml', delete_conditions)
                logging.info(f"Batch deleted records from aud_inputtable_xml: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_inputtable_xml', delete_conditions)
            logging.info(f"Deleted remaining records from aud_inputtable_xml: {len(delete_conditions)} rows")

        # Step 4: Execute aud_inputtable_xml query
        aud_inputtable_xml_query = config.get_param('queries', 'aud_inputtable_xml')
        aud_inputtable_xml_results = db.execute_query(aud_inputtable_xml_query)
        logging.debug(f"aud_inputtable_xml_results: {aud_inputtable_xml_results}")

        # Step 5: Delete records from aud_inputtable_xml based on query results
        delete_conditions = []
        for result in aud_inputtable_xml_results:
            project_name, job_name = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_inputtable_xml', delete_conditions)
                logging.info(f"Batch deleted records from aud_inputtable_xml: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_inputtable_xml', delete_conditions)
            logging.info(f"Deleted remaining records from aud_inputtable_xml: {len(delete_conditions)} rows")

        # Step 6: Insert parsed data into aud_inputtable_xml in batches
        insert_query = config.get_param('insert_queries', 'aud_inputtable_xml')
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
                            db.insert_data_batch(insert_query, 'aud_inputtable_xml', batch_insert)
                            logging.info(f"Inserted batch of data into aud_inputtable_xml: {len(batch_insert)} rows")
                            batch_insert.clear()

                    if aud_nameColumnInput is None:
                        logging.warning("aud_nameColumnInput is None, skipping this entry.")

        # Insert remaining data after the loop
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_inputtable_xml', batch_insert)
            logging.info(f"Inserted remaining batch of data into aud_inputtable_xml: {len(batch_insert)} rows")

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
                logging.info(f"Batch deleted records from aud_inputtable_xml: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('inputtableJoinElemntnode_results', delete_conditions)
            logging.info(f"Deleted remaining records from aud_inputtable_xml: {len(delete_conditions)} rows")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()
            logging.info("Database connection closed.")


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


def AUD_308_ALIMCONNECTIONCOMPONENT(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str, batch_size=100):
    try:
        # Step 1: Get the execution date
        # execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        # execution_date = db.get_execution_date(execution_date_query)
        # logging.info(f"Execution Date: {execution_date}")

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

        # Step 3: Delete records from aud_connectioncomponent in batches
        delete_conditions = []
        for result in local_to_dbbrut_query_results:
            project_name, job_name, *_ = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_connectioncomponent', delete_conditions)
                logging.info(f"Batch deleted records from aud_connectioncomponent: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_connectioncomponent', delete_conditions)
            logging.info(f"Deleted remaining records from aud_connectioncomponent: {len(delete_conditions)} rows")

        # Step 4: Execute aud_connectioncomponent query
        aud_connectioncomponent_query = config.get_param('queries', 'aud_connectioncomponent')
        aud_connectioncomponent_results = db.execute_query(aud_connectioncomponent_query)
        logging.debug(f"aud_connectioncomponent_results: {aud_connectioncomponent_results}")

        # Step 5: Delete records from aud_connectioncomponent based on query results
        delete_conditions = []
        for result in aud_connectioncomponent_results:
            project_name, job_name = result
            delete_conditions.append({'NameProject': project_name, 'NameJob': job_name})
            if len(delete_conditions) >= batch_size:
                db.delete_records_batch('aud_connectioncomponent', delete_conditions)
                logging.info(f"Batch deleted records from aud_connectioncomponent: {len(delete_conditions)} rows")
                delete_conditions.clear()

        if delete_conditions:
            db.delete_records_batch('aud_connectioncomponent', delete_conditions)
            logging.info(f"Deleted remaining records from aud_connectioncomponent: {len(delete_conditions)} rows")

        # Step 6: Insert parsed data into aud_connectioncomponent in batches
        insert_query = config.get_param('insert_queries', 'aud_connectioncomponent')
        logging.debug(f"Insert Query: {insert_query}")
        batch_insert = []

        for project_name, job_name, parsed_data in parsed_files_data:
            logging.info(f"Processing project: {project_name}, job: {job_name}")
            for connection in parsed_data.get('connections', []):
                aud_connectorName = connection.get('connectorName')
                aud_labelRow = connection.get('label')
                aud_lineStyle = connection.get('lineStyle')
                aud_metaname = connection.get('metaname')
                aud_offsetLabelX = connection.get('offsetLabelX')
                aud_offsetLabelY = connection.get('offsetLabelY')
                aud_sourceComponent = connection.get('source')
                aud_targetComponent = connection.get('target')
                aud_outputId = connection.get('r_outputId')

                for elem_param in connection.get('elementParameters', []):
                    aud_field = elem_param.get('field')
                    aud_name = elem_param.get('name')
                    aud_value = elem_param.get('value')
                    aud_show = 1 if elem_param.get('show')== 'true' else 0

                    params = (
                        aud_connectorName, aud_labelRow, aud_lineStyle, aud_metaname,
                        aud_offsetLabelX, aud_offsetLabelY, aud_sourceComponent, aud_targetComponent,
                        aud_outputId, aud_field, aud_name, aud_value, aud_show,
                        project_name, job_name, execution_date
                    )
                    batch_insert.append(params)

                    if len(batch_insert) >= batch_size:
                        try:
                            logging.info("Inserting batch into database.")
                            db.insert_data_batch(insert_query, 'aud_connectioncomponent', batch_insert)
                            logging.info(f"Inserted batch of data into aud_connectioncomponent: {len(batch_insert)} rows")
                        except Exception as insert_error:
                            logging.error(f"Error during batch insert: {insert_error}", exc_info=True)
                        finally:
                            batch_insert.clear()

        if batch_insert:
            try:
                logging.info("Inserting remaining batch into database.")
                db.insert_data_batch(insert_query, 'aud_connectioncomponent', batch_insert)
                logging.info(f"Inserted remaining batch of data into aud_connectioncomponent: {len(batch_insert)} rows")
            except Exception as insert_error:
                logging.error(f"Error during final batch insert: {insert_error}", exc_info=True)

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()
            logging.info("Database connection closed.")
