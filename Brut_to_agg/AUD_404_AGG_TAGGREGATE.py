from config import Config
from database import Database

def main():
    # Initialize the Config class with your configuration file
    config_file = "configs/config.yaml"
    config = Config(config_file)

    try:
        # Get MySQL database configuration and connect
        db_config = config.get_database_config()
        db = Database(db_config)

        # Connect to the database
        db.connect()

        # Test get_execution_date
        execution_date_query = config.get_query('global_queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
        execution_date = db.get_execution_date(execution_date_query)
        print("Execution Date:", execution_date)

        # Test insert_data using table name 'aud_agg_aggregate'
        table_name = 'aud_agg_aggregate'
        insert_query = config.get_query("insert_queries",table_name)
        db.insert_data(insert_query, table_name)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Close the database connection in the finally block to ensure it's always closed
        if db:
            db.close()

if __name__ == "__main__":
    main()
