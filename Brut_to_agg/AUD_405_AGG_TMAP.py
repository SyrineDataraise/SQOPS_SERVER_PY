import csv
import os
import glob
from config import Config
from database import Database
#not finished
def main():
    # Initialize the Config class with your configuration file
    config_file = "configs/config.yaml"
    config = Config(config_file)
    
    def get_csv_directory():
        try:
            context = config.config['Fichier_Rep']
            csv_directory = context['CTX_AUDIT_REP_ROOT'] + context['CTX_AUDIT_REP_WORK'] + context['CTX_AUDIT_REP_TMP']
            return csv_directory
        except KeyError:
            raise KeyError("CSV file directory information is missing in the configuration")

    try:
        # Get MySQL database configuration and connect
        db_config = config.get_database_config()
        db = Database(db_config)

        # Connect to the database
        db.connect()

        # Define the query
        query = """
        select 
            a.NameProject, 
            a.namejob, 
            a.aud_componentValue,  
            a.aud_valueElementRef as aud_valueElementRef_input, 
            b.aud_valueElementRef as aud_valueElementRef_output,
            'GROUPBY' as aud_valueElementRef_function 
        from aud_elementvaluenode a
        inner join aud_elementvaluenode b on a.aud_id = b.aud_id
        where a.aud_typeField in ('GROUPBYS') 
        and b.aud_typeField in ('GROUPBYS') 
        and a.aud_elementRef = 'INPUT_COLUMN' 
        and b.aud_elementRef = 'OUTPUT_COLUMN'
        and a.aud_componentName like 'tAggregate%'
        and b.aud_componentName like 'tAggregate%'

        union all

        select 
            a.NameProject, 
            a.namejob, 
            a.aud_componentValue,  
            a.aud_valueElementRef as aud_valueElementRef_input, 
            b.aud_valueElementRef as aud_valueElementRef_output,
            c.aud_valueElementRef as aud_valueElementRef_function 
        from aud_elementvaluenode a
        inner join aud_elementvaluenode b on a.aud_id = b.aud_id
        inner join aud_elementvaluenode c on a.aud_id = c.aud_id
        where a.aud_componentName like 'tAggregate%' 
        and a.aud_typeField = 'OPERATIONS' 
        and b.aud_componentName like 'tAggregate%' 
        and b.aud_typeField = 'OPERATIONS'
        and c.aud_componentName like 'tAggregate%' 
        and c.aud_typeField = 'OPERATIONS'
        and a.aud_elementRef = 'INPUT_COLUMN' 
        and b.aud_elementRef = 'OUTPUT_COLUMN'
        and c.aud_elementRef = 'FUNCTION'
        """

        # Execute the query
        results = db.execute_query(query)

        # Get the CSV directory from the configuration
        csv_directory = get_csv_directory()

        # Delete existing CSV files in the directory
        existing_files = glob.glob(os.path.join(csv_directory, "*.csv"))
        for file_path in existing_files:
            os.remove(file_path)

        # Define the CSV file path
        csv_file_path = os.path.join(csv_directory, "aud_inputtable.csv")

        # Write the results to a CSV file
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write the header
            writer.writerow(['NameProject', 'namejob', 'aud_componentValue', 'aud_valueElementRef_input', 'aud_valueElementRef_output', 'aud_valueElementRef_function'])
            # Write the data
            for row in results:
                writer.writerow(row)

        print(f"Data successfully written to {csv_file_path}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Close the database connection in the finally block to ensure it's always closed
        if db:
            db.close()

if __name__ == "__main__":
    main()
