import os
from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py
from XML_parse import XMLParser  # Importing the XMLParser class

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

        # Step 3: Delete the output from aud_elementvaluenode based on the query results
        for result in local_to_dbbrut_query_results:
            project_name, job_name, _, _, _ = result  # Assuming result contains these fields in order
            db.delete_records(project_name, job_name)
            print(f"Deleted records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")

        # Step 4: Execute NOT_AUDITED_JOBS_QUERY
        not_audited_jobs_query = config.get_param('queries', 'NOT_AUDITED_JOBS_QUERY')
        print("Executing query:", not_audited_jobs_query)  # Print the query before execution
        not_audited_jobs_query_results = db.execute_query(not_audited_jobs_query)
        print("not_audited_jobs_query_results:", not_audited_jobs_query_results)

        # Step 5: Delete the output from aud_elementvaluenode based on the query results
        for result in not_audited_jobs_query_results:
            project_name, job_name = result
            db.delete_records(project_name, job_name)
            print(f"Deleted records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")

        # Step 6: Loop over `.item` files in the items_directory and parse them
        for filename in os.listdir(items_directory):
            if filename.endswith('.item'):
                file_path = os.path.join(items_directory, filename)
                parts = filename.split('.', 1)  # Split at the first dot
                project_name = parts[0]
                job_name = parts[1].replace('.item', '') if len(parts) > 1 else None
                print("parts:", parts)
                print("project_name:", project_name)
                print("job_name:", job_name)
                print(f"Parsing file: {file_path}")
                xml_parser = XMLParser(file_path)
                parsed_data = xml_parser.parse_nodes()
                print('parsed data:', parsed_data)
                
                # Step 7: Insert parsed data into the `aud_element_node` table
                for data in parsed_data:
                    componentName = data['componentName']
                    field = data['field']
                    name = data['name']
                    show = data['show']
                    value = data['value']
                    
                    # Adjust the value of `Componement_UniqueName` as needed, if it's derived from `componentName`
                    Componement_UniqueName = value if field == 'TEXT' and name == 'UNIQUE_NAME' else Componement_UniqueName
                    insert_query = config.get_param('insert_queries', 'aud_elementnode')
                    params = (componentName, field, name, show, value, Componement_UniqueName, project_name, job_name, execution_date)
                    db.insert_data(insert_query, 'aud_elementnode', params)
                    print(f"Inserted data for component: {data['componentName']} into aud_elementnode")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        if db:
            db.close()  # Ensure the database connection is closed

if __name__ == "__main__":
    main()
