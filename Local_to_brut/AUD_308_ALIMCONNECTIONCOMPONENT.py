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

def AUD_308_ALIMCONNECTIONCOMPONENT(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]], batch_size=100):
    try:
        # Step 1: Get the execution date
        execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        execution_date = db.get_execution_date(execution_date_query)
        logging.info(f"Execution Date: {execution_date}")

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
