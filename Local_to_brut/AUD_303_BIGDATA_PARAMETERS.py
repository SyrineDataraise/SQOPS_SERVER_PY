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

        # Step 3: Delete the output from aud_bigdata based on the query results
        for result in local_to_dbbrut_query_results:
            project_name, job_name, _, _, _ = result  # Assuming result contains these fields in order
            
            # Prepare the conditions dictionary
            conditions = {
                'NameProject': project_name,
                'NameJob': job_name
            }
            
            db.delete_records('aud_bigdata', **conditions)
            print(f"Deleted records for PROJECT_NAME from aud_bigdata: {project_name}, JOB_NAME: {job_name}")

        # Step 4: Execute aud_bigdata query
        aud_bigdata_query = config.get_param('queries', 'aud_bigdata')
        print("Executing query:", aud_bigdata_query)  # Print the query before execution
        aud_bigdata_results = db.execute_query(aud_bigdata_query)
        print("aud_bigdata_results:", aud_bigdata_results)

        # Step 5: Delete the output from aud_contextjob based on the query results
        for result in aud_bigdata_results:
            project_name, job_name = result
            conditions = {
                'NameProject': project_name,
                'NameJob': job_name
            }
            db.delete_records('aud_contextjob', **conditions)
            print(f"Deleted records for PROJECT_NAME from aud_bigdata: {project_name}, JOB_NAME: {job_name}")

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
                parameters_data = xml_parser.parse_parameters()
                
                # Step 7: Insert parsed parameters data into the `aud_bigdata` table
                for param_data in parameters_data:
                    field = param_data['field']
                    name = param_data['name']
                    show = param_data['show']
                    value = param_data['value']
                    
                    insert_query = config.get_param('insert_queries', 'aud_bigdata')
                    params = (field, name, show, value, project_name, job_name, execution_date)
                    db.insert_data(insert_query, 'aud_bigdata', params)
                    print(f"Inserted parameter data into aud_bigdata: {param_data}")

                    # Step 8: Insert element values into the `aud_bigdata_elementvalue` table
                    elementValues = param_data['elementValues']
                    for elementValue in elementValues:
                        elementRef = elementValue['elementRef']
                        elementRef_value = elementValue['value']
                        
                        insert_query = config.get_param('insert_queries', 'aud_bigdata_elementvalue')
                        params = (elementRef, elementRef_value, name, project_name, job_name, execution_date)
                        db.insert_data(insert_query, 'aud_bigdata_elementvalue', params)
                        print(f"Inserted element value data into aud_bigdata_elementvalue: {elementValue}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        if db:
            db.close()  # Ensure the database connection is closed

if __name__ == "__main__":
    main()
