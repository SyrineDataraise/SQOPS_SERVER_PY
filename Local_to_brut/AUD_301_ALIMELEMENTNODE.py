from config import Config  # Assuming Config class is defined in config.py
from database import Database  # Assuming Database class is defined in database.py

def main():
    config_file = "configs/config.yaml"
    config = Config(config_file)

    try:
        # Retrieve JDBC parameters and create a Database instance
        jdbc_params = config.get_jdbc_parameters()
        print("JDBC Parameters:", jdbc_params)  # Print JDBC parameters to debug

        db = Database(jdbc_params)
        db.set_jdbc_parameters(jdbc_params)  # Set JDBC parameters if needed
        db.connect_JDBC()  # Test the JDBC connection

        # Step 1: Get the execution date
        execution_date_query = config.get_query('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        execution_date = db.get_execution_date(execution_date_query)
        print("Execution Date:", execution_date)

        # Step 2: Execute LOCAL_TO_DBBRUT_QUERY
        local_to_dbbrut_query = config.get_query('queries', 'LOCAL_TO_DBBRUT_QUERY')
        print("Executing query:", local_to_dbbrut_query)  # Print the query before execution
        local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
        print("local_to_dbbrut_query_results", local_to_dbbrut_query_results)

        # Step 3: Delete the output from aud_elementvaluenode based on the query results
        for result in local_to_dbbrut_query_results:
            project_name, job_name, _, _, _ = result  # Assuming result contains these fields in order
            db.delete_records(project_name, job_name)
            print(f"Deleted records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")

        # Step 4: Execute NOT_AUDITED_JOBS_QUERY
        not_audited_jobs_query = config.get_query('queries', 'NOT_AUDITED_JOBS_QUERY')
        print("Executing query:", not_audited_jobs_query)  # Print the query before execution
        not_audited_jobs_query_results = db.execute_query(not_audited_jobs_query)
        print("not_audited_jobs_query_results", not_audited_jobs_query_results)

        # Step 5: Delete the output from aud_elementvaluenode based on the query results
        for result in not_audited_jobs_query_results:
            project_name, job_name = result
            db.delete_records(project_name, job_name)
            print(f"Deleted records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        if db:
            db.close()  # Ensure the database connection is closed

if __name__ == "__main__":
    main()
