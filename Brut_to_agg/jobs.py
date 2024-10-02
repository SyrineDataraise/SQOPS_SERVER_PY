from config import Config
from database import Database
import logging
import os
import csv






def delete_files_in_directory(directory_path: str, file_extension: str = None):
    """
    Delete all files in a specified directory. Optionally, only files with a specified extension are deleted.

    Args:
        directory_path (str): Path to the directory from which files should be deleted.
        file_extension (str, optional): If provided, only files with this extension will be deleted (e.g., '.txt'). Defaults to None, meaning all files will be deleted.
    
    Returns:
        None
    """
    try:
        # Check if the directory exists
        if not os.path.exists(directory_path):
            logging.error(f"The directory {directory_path} does not exist.")
            return

        # Iterate through all files in the directory
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            
            # If it's a file and matches the file extension (if specified)
            if os.path.isfile(file_path) and (file_extension is None or filename.endswith(file_extension)):
                os.remove(file_path)
                logging.info(f"Deleted file: {file_path}")
        
        logging.info("File deletion process completed.")

    except Exception as e:
        logging.error(f"An error occurred while deleting files: {str(e)}", exc_info=True)

def AUD_404_AGG_TAGGREGATE(
    config: Config,
    db: Database,
    execution_date: str,
    batch_size=100
):
    """
    Perform aggregation operations for AUD 404 data.

    This function executes a query to retrieve aggregated AUD 404 data
    from the database and inserts it into the aud_agg_aggregate table
    in batches.

    Args:
        config (Config): Configuration instance for retrieving parameters.
        db (Database): Database instance for executing queries.
        execution_date (str): The execution date to use in data insertion.
        batch_size (int, optional): Number of rows to insert in each batch. Defaults to 100.
    """
    try:
        # Execute AUD aggregation query
        aud_agg_query = config.get_param('queries', 'aud_agg')
        logging.info(f"Executing query: {aud_agg_query}")
        aud_agg_results = db.execute_query(aud_agg_query)
        logging.debug(f"AUD aggregation results: {aud_agg_results}")

        # Prepare batch insert
        batch_insert = []
        insert_query = config.get_param('insert_queries', 'aud_agg_aggregate')

        for result in aud_agg_results:
            # Unpack result tuple
            (NameProject, namejob, aud_componentValue, aud_valueElementRef_input,
             aud_valueElementRef_output, aud_valueElementRef_function) = result

            # Add result to batch insert list
            batch_insert.append(result)

            # Insert batch if batch size is reached
            if len(batch_insert) >= batch_size:
                db.insert_data_batch(insert_query, 'aud_agg_aggregate', batch_insert)
                batch_insert.clear()

        # Insert remaining data in the batch
        if batch_insert:
            db.insert_data_batch(insert_query, 'aud_agg_aggregate', batch_insert)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        if db:
            logging.info("Processing complete.")





def AUD_405_AGG_TMAP(
    config: Config,
    db: Database,
    execution_date: str,
    batch_size=100
):
    """
    This function performs the following:
    - Deletes existing files in a specified directory.
    - Executes two queries (inputtable XML and outputtable XML) and saves the results into two CSV files.
    - Joins the results from both CSVs on 'aud_componentValue', 'namejob', and 'nameproject', 
      and writes the result into the 'aud_agg_txmlmapinputinoutput' table.

    Args:
        config (Config): Configuration instance for retrieving parameters.
        db (Database): Database instance for executing queries.
        execution_date (str): The execution date to use in data insertion.
        batch_size (int, optional): Number of rows to insert in each batch. Defaults to 100.
    """

    try:
        # Step 1: Clean the directory by deleting existing files
        directory_path = config.get_param('Directories', 'delete_files')
        delete_files_in_directory(directory_path)

        # Step 2: Execute inputtable XML query and write to CSV
        inputtable_xml_query = config.get_param('queries', 'inputtable_xml')
        logging.info(f"Executing query: {inputtable_xml_query}")
        inputtable_xml_results = db.execute_query(inputtable_xml_query)
        logging.debug(f"Input table results: {inputtable_xml_results}")

        input_csv_path = os.path.join(directory_path, "aud_inputtable_xml.csv")
        input_csv_header = [
            "aud_nameColumnInput", "aud_type", "aud_xpathColumnInput", "aud_nameRowInput", 
            "aud_componentName", "aud_componentValue", "filterOutGoingConnections", 
            "lookupOutgoingConnections", "outgoingConnections", "namejob", 
            "nameproject", "exec_date", "lookupIncomingConnections", "expression", 
            "lookupMode", "matchingMode", "activateCondensedTool", 
            "activateExpressionFilter", "activateGlobalMap", "expressionFilter", 
            "filterIncomingConnections", "lookup"
        ]

        with open(input_csv_path, mode='w', newline='', encoding='utf-8') as input_csvfile:
            writer = csv.writer(input_csvfile)
            writer.writerow(input_csv_header)
            for result in inputtable_xml_results:
                writer.writerow(result)
        
        logging.info(f"Input table results written to {input_csv_path}")

        # Step 3: Execute outputtable XML query and write to CSV
        outputtable_xml_query = config.get_param('queries', 'outputtable_xml')
        logging.info(f"Executing query: {outputtable_xml_query}")
        outputtable_xml_results = db.execute_query(outputtable_xml_query)
        logging.debug(f"Output table results: {outputtable_xml_results}")

        output_csv_path = os.path.join(directory_path, "aud_outputtable_xml.csv")
        output_csv_header = [
            "aud_nameColumnInput", "aud_type", "aud_xpathColumnInput", "aud_nameRowOutput", 
            "aud_componentName", "aud_componentValue", "filterOutGoingConnections", 
            "outgoingConnections", "namejob", "nameproject", "exec_date", "expression", 
            "activateCondensedTool", "activateExpressionFilter", "expressionFilter", 
            "filterIncomingConnections"
        ]

        with open(output_csv_path, mode='w', newline='', encoding='utf-8') as output_csvfile:
            writer = csv.writer(output_csvfile)
            writer.writerow(output_csv_header)
            for result in outputtable_xml_results:
                writer.writerow(result)
        
        logging.info(f"Output table results written to {output_csv_path}")

        # Step 4: Join the two CSVs on 'aud_componentValue', 'namejob', 'nameproject'
        input_data = []
        output_data = []

        # Read input CSV
        with open(input_csv_path, mode='r', encoding='utf-8') as input_csvfile:
            reader = csv.DictReader(input_csvfile)
            input_data = list(reader)

        # Read output CSV
        with open(output_csv_path, mode='r', encoding='utf-8') as output_csvfile:
            reader = csv.DictReader(output_csvfile)
            output_data = list(reader)

        # Perform the join and extract only the relevant columns
        joined_data = []
        for input_row in input_data:
            for output_row in output_data:
                if (input_row['aud_componentValue'] == output_row['aud_componentValue'] and
                    input_row['namejob'] == output_row['namejob'] and
                    input_row['nameproject'] == output_row['nameproject']):
                    
                    # Extract only the specific columns required
                    joined_row = (
                        input_row['aud_nameColumnInput'],
                        input_row['aud_nameRowInput'],
                        input_row['aud_componentName'],
                        input_row['expression'],  # Assuming 'expression' represents 'expressionOutput'
                        output_row['aud_nameColumnInput'],  # output_aud_nameColumnInput
                        output_row['aud_nameRowOutput'],
                        output_row['namejob'],  # output_namejob
                        output_row['nameproject']  # output_nameproject
                    )
                    joined_data.append(joined_row)
        # Step 5: Insert the joined data into the database
        insert_query = config.get_param('insert_queries', 'aud_agg_txmlmapinputinoutput')
        data_batch = []
        for row in joined_data:
            data_batch.append(tuple(row.values()))  # Convert dict values to tuple for database insertion
            if len(data_batch) == batch_size:
                db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinoutput', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinoutput', data_batch)

        logging.info("Joined data successfully inserted into aud_agg_txmlmapinputinoutput table.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)



    


    
    