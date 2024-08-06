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
            conditions = {
                'NameProject': project_name,
                'NameJob': job_name
            }
            db.delete_records('aud_elementvaluenode', conditions)
            print(f"Deleted records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")

        # Step 4: Execute aud_elementnode query
        aud_elementnode = config.get_param('queries', 'aud_elementnode')
        print("Executing query:", aud_elementnode)  # Print the query before execution
        aud_elementnode_results = db.execute_query(aud_elementnode)
        print("aud_elementnode_results:", aud_elementnode_results)

        # Step 5: Delete the output from aud_elementvaluenode based on the query results
        for result in aud_elementnode_results:
            project_name, job_name = result
            
            # Prepare the conditions dictionary
            conditions = {
                'NameProject': project_name,
                'NameJob': job_name
            }
            
            db.delete_records('aud_elementvaluenode', conditions)
            print(f"Deleted records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")

        # Step 6: Loop over `.item` files in the items_directory and parse them
        xml_parser = XMLParser("")
        filenames = [f for f in os.listdir(items_directory) if f.endswith('.item')]
        for filename in filenames:
            project_name,job_name,parsed_data = xml_parser.loop_parse(filename, items_directory)
                
            # Step 7: Insert parsed data into the `aud_element_node` table
            for data in parsed_data['nodes']:
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
