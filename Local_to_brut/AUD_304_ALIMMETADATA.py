import os
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from XML_parse import XMLParser  # Importing the XMLParser class
import time

def main():
    config_file = "configs/config.yaml"
    config = Config(config_file)
    items_directory = config.get_param('Directories', 'items_directory')

    db = None

    try:
        # Retrieve JDBC parameters and create a Database instance
        jdbc_params = config.get_jdbc_parameters()
        print("JDBC Parameters:", jdbc_params)  # Print JDBC parameters to debug

        db = Database(jdbc_params)
        db.set_jdbc_parameters(jdbc_params)  # Set JDBC parameters if needed
        db.connect_JDBC()  # Test the JDBC connection

        # Step 1: Get the execution date
        execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        execution_date = db.get_execution_date(execution_date_query)
        print("Execution Date:", execution_date)

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
        print("Executing query:", local_to_dbbrut_query)  # Print the query before execution
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        print("local_to_dbbrut_query_results:", local_to_dbbrut_query_results)

        # Step 3: Delete the output from aud_metadata based on the query results
        for result in local_to_dbbrut_query_results:
            project_name, job_name, _, _, _ = result  # Assuming result contains these fields in order
            
            # Prepare the conditions dictionary
            conditions = {
                'NameProject': project_name,
                'NameJob': job_name
            }
            
            db.delete_records('aud_metadata', **conditions)
            print(f"Deleted records for PROJECT_NAME from aud_metadata: {project_name}, JOB_NAME: {job_name}")

        # Step 4: Execute aud_metadata query
        aud_metadata_query = config.get_param('queries', 'aud_metadata')
        print("Executing query:", aud_metadata_query)  # Print the query before execution
        aud_metadata_results = db.execute_query(aud_metadata_query)
        print("aud_metadata_results:", aud_metadata_results)

        # Step 5: Delete the output from aud_contextjob based on the query results
        for result in aud_metadata_results:
            project_name, job_name = result
            conditions = {
                'NameProject': project_name,
                'NameJob': job_name
            }
            db.delete_records('aud_contextjob', **conditions)
            print(f"Deleted records for PROJECT_NAME from aud_metadata: {project_name}, JOB_NAME: {job_name}")

        # Step 6: Loop over `.item` files in the items_directory and parse them
        filenames = [f for f in os.listdir(items_directory) if f.endswith('.item')]
        
        for filename in filenames:
            file_path = os.path.join(items_directory, filename)
            parts = filename.split('.', 1)  # Split at the first dot
            project_name = parts[0]
            job_name = parts[1].replace('.item', '') if len(parts) > 1 else None
            
            print("Processing file:", filename)
            print("parts:", parts)
            print("project_name:", project_name)
            print("job_name:", job_name)
            print(f"Parsing file: {file_path}")

            xml_parser = XMLParser(file_path)
            data = xml_parser._parse_file()
            print("Parsed Data:")
            print(data)

            # Step 7: Insert parsed parameters data into the `aud_metadata` table
            insert_query = config.get_param('insert_queries', 'aud_metadata')

            for node_data in data['nodes']:
                for elem_param in node_data['elementParameters']:
                    for elem_value in elem_param['elementValues']:
                        for meta in node_data['metadata']:
                            for column in meta['columns']:
                                params = (
                                    meta['connector'],
                                    meta['label'],
                                    meta['name'],
                                    column['comment'],
                                    0 if column['key'] == 'false' else 1,
                                    column['length'],
                                    column['name'],
                                    0 if column['nullable'] == 'false' else 1,
                                    column['pattern'],
                                    column['precision'],
                                    column['sourceType'],
                                    column['type'],
                                    0 if column['usefulColumn'] == 'false' else 1,
                                    column['originalLength'],
                                    column['defaultValue'],
                                    elem_value['value'],
                                    node_data['componentName'],
                                    project_name,
                                    job_name,
                                    execution_date
                                )
                                print(params)
                                db.insert_data(insert_query, 'aud_metadata', params)
                                time.sleep(0.001)
                                print(f"Inserted parameter data into aud_metadata: {params}")

        # Step 8: Execute MetadataJoinElementNode and delete results from aud_metadata
        metadata_join_query = config.get_param('queries', 'MetadataJoinElemntnode')
        print("Executing MetadataJoinElementNode query:", metadata_join_query)
        metadata_join_results = db.execute_query(metadata_join_query)
        print("MetadataJoinElementNode Results:", metadata_join_results)

        for result in metadata_join_results:
            conditions = {
            'NameProject': result[0],
            'NameJob': result[1],
            'aud_componentValue': result[2]
        }
            db.delete_records('aud_metadata', **conditions)
            print(f"Deleted records from aud_metadata based on MetadataJoinElementNode results: {conditions}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        if db:
            db.close()  # Ensure the database connection is closed

if __name__ == "__main__":
    main()
