from config import Config
from database import Database
import logging
import os
import csv

import pandas as pd





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

def AUD_405_AGG_TXMLMAP(config: Config, db: Database, execution_date: str, batch_size=100):
    """
    This function:
    - Deletes existing files in a specified directory.
    - Executes two agg_queries (inputtable_xml XML and outputtable XML), writes results to CSVs.
    - Performs two different joins:
        - One for inserting into `aud_agg_txmlmapinputinoutput`.
        - Another for inserting into `aud_agg_txmlmapinputinfilteroutput`.

    Args:
        config (Config): Configuration instance for retrieving parameters.
        db (Database): Database instance for executing agg_queries.
        execution_date (str): Execution date used in data insertion.
        batch_size (int, optional): Number of rows to insert in each batch. Defaults to 100.
    """
    
    try:

        # ==============================================================================================
        #     Write into aud_inputtable_xml.csv & aud_outputtable_xml.csv  
        # ==============================================================================================

        # Step 1: Clean the directory by deleting existing files
        directory_path = config.get_param('Directories', 'delete_files')
        delete_files_in_directory(directory_path)

        # Step 2: Execute inputtable_xml XML query and write to CSV
        aud_inputtable_xml_query = config.get_param('agg_queries', 'aud_inputtable_xml')
        logging.info(f"Executing query: {aud_inputtable_xml_query}")
        aud_inputtable_xml_results = db.execute_query(aud_inputtable_xml_query)

        input_csv_path = os.path.join(directory_path, "inputtable_xml.csv")
        input_csv_header = [
            'aud_nameColumnInput', 'aud_type', 'aud_xpathColumnInput', 'aud_nameRowInput', 
            'aud_componentName', 'aud_componentValue', 'filterOutGoingConnections', 
            'lookupOutgoingConnections', 'outgoingConnections', 'namejob', 'nameproject', 
            'exec_date', 'lookupIncomingConnections', 'expression', 'lookupMode', 
            'matchingMode', 'activateCondensedTool', 'activateExpressionFilter', 
            'activateGlobalMap', 'expressionFilter', 'filterIncomingConnections', 'lookup'
        ]

        with open(input_csv_path, mode='w', newline='', encoding='utf-8') as input_csvfile:
            writer = csv.writer(input_csvfile)
            writer.writerow(input_csv_header)
            writer.writerows(aud_inputtable_xml_results)

        logging.info(f"Input table results written to {input_csv_path}")

        # Step 3: Execute outputtable XML query and write to CSV
        aud_outputtable_xml_query = config.get_param('agg_queries', 'aud_outputtable_xml')
        logging.info(f"Executing query: {aud_outputtable_xml_query}")
        aud_outputtable_xml_results = db.execute_query(aud_outputtable_xml_query)

        output_csv_path = os.path.join(directory_path, "outputtable_xml.csv")
        output_csv_header = [
            'aud_nameColumnInput', 'aud_type', 'aud_xpathColumnInput', 'aud_nameRowOutput', 
            'aud_componentName', 'aud_componentValue', 'filterOutGoingConnections', 
            'outgoingConnections', 'namejob', 'nameproject', 'exec_date', 'expression', 
            'activateCondensedTool', 'activateExpressionFilter', 'expressionFilter', 
            'filterIncomingConnections'
        ]

        with open(output_csv_path, mode='w', newline='', encoding='utf-8') as output_csvfile:
            writer = csv.writer(output_csvfile)
            writer.writerow(output_csv_header)
            writer.writerows(aud_outputtable_xml_results)

        logging.info(f"Output table results written to {output_csv_path}")


        # Step 4: Read the CSVs
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

        # ==============================================================================================
        # Join `aud_inputtable_xml.csv` & `aud_outputtable_xml.csv` for `aud_agg_txmlmapinputinoutput`
        # ==============================================================================================

        joined_data_output_table = []

        for input_row in input_data:
            for output_row in output_data:
                if (input_row['aud_componentValue'] == output_row['aud_componentValue'] and
                    input_row['NameJob'] == output_row['NameJob'] and
                    input_row['NameProject'] == output_row['NameProject']):
                    
                    # Apply the rule: !Relational.ISNULL(output.expression) && routines.Utils.containsElementExpression(output.expression, input.aud_nameRowInput + "." + input.aud_nameColumnInput) == 1
                    search_string = input_row['NameRowInput'] + "." + input_row['aud_nameColumnInput']
                    if output_row['expression'] is not None and search_string in output_row['expression']:
                        
                        # Prepare `expression`: replace newlines if expression is not NULL
                        expression = output_row['expression'].replace("\n", " ") if output_row['expression'] else None

                        # Apply the rule for `expressionJoin`: !Relational.ISNULL(row['expressionJoin']) && !row['expressionJoin'].isEmpty()
                        if output_row['expressionJoin'] is not None and output_row['expressionJoin'].strip():
                            
                            # Check if `expressionJoin` contains `rowName.NameRowInput`
                            search_string = input_row['rowName'] + "." + input_row['NameRowInput']
                            if search_string in output_row['expressionJoin']:
                                # Prepare `expressionJoin`: replace newlines if expressionJoin is not NULL
                                expression_join = output_row['expressionJoin'].replace("\n", " ") if output_row['expressionJoin'] else None
                                
                                # Prepare the row for insertion into the first table
                                joined_row = (
                                    input_row['aud_nameColumnInput'],
                                    input_row['NameRowInput'],
                                    input_row['aud_componentName'],
                                    expression,  # Assuming 'expression' represents 'expressionOutput'
                                    output_row['aud_nameColumnInput'],
                                    output_row['aud_nameRowOutput'],
                                    output_row['NameJob'],
                                    output_row['NameProject']
                                )
                                joined_data_output_table.append(joined_row)

        # Insert joined data into `aud_agg_txmlmapinputinoutput`
        insert_query_output_table = config.get_param('insert_agg_queries', 'aud_agg_txmlmapinputinoutput')
        data_batch = []
        for row in joined_data_output_table:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                db.insert_data_batch(insert_query_output_table, 'aud_agg_txmlmapinputinoutput', data_batch)
                data_batch.clear()

        # Insert remaining rows
        if data_batch:
            db.insert_data_batch(insert_query_output_table, 'aud_agg_txmlmapinputinoutput', data_batch)

        logging.info("Joined data inserted into `aud_agg_txmlmapinputinoutput`.")


        # ==============================================================================================
        # Join `aud_inputtable_xml.csv` & unique `aud_outputtable_xml.csv` for `aud_agg_txmlmapinputinfilteroutput`
        # ==============================================================================================

        filtered_joined_data = []
        unique_rows = set()  # Track uniqueness based on (NameJob, NameProject, composant, aud_componentValue)

        for input_row in input_data:
            for output_row in output_data:
                if (input_row['composant'] == output_row['aud_componentValue'] and
                    input_row['NameJob'] == output_row['NameJob'] and
                    input_row['NameProject'] == output_row['NameProject']):
                    
                    # Check expression filter
                    search_string = input_row['NameRowInput'] + "." + input_row['aud_nameColumnInput']
                    if output_row['expression'] is not None and search_string in output_row['expression']:
                        # Prepare `expression`: replace newlines if expression is not None
                        expression = output_row['expression'].replace("\n", " ") if output_row['expression'] else None
                        
                        # Check if column is joined (is_columnjoined condition)
                        is_columnjoined = (output_row['aud_xpathColumnInput'] is not None and 
                                        search_string in output_row['aud_xpathColumnInput'])

                        # Prepare the row key and ensure uniqueness
                        row_key = (output_row['NameJob'], output_row['NameProject'], input_row['composant'], output_row['aud_componentValue'])
                        if row_key not in unique_rows:
                            # Apply the condition to prepare `expressionFilterOutput`
                            expressionFilterOutput = (
                                output_row['expression'].replace("\n", " ") if output_row['expression'] is not None else None
                            )

                            # Prepare the joined row for insertion
                            joined_row = (
                                input_row['NameRowInput'],
                                input_row['aud_nameColumnInput'],
                                input_row['aud_componentName'],
                                expressionFilterOutput,
                                output_row['aud_nameRowOutput'],
                                output_row['NameProject'],
                                output_row['NameJob'],
                            )
                            filtered_joined_data.append(joined_row)
                            unique_rows.add(row_key)

        # Insert joined data into `aud_agg_txmlmapinputinfilteroutput`
        insert_query_filter_output_table = config.get_param('insert_agg_queries', 'aud_agg_txmlmapinputinfilteroutput')
        data_batch = []
        for row in filtered_joined_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                db.insert_data_batch(insert_query_filter_output_table, 'aud_agg_txmlmapinputinfilteroutput', data_batch)
                data_batch.clear()

        # Insert remaining rows if any
        if data_batch:
            db.insert_data_batch(insert_query_filter_output_table, 'aud_agg_txmlmapinputinfilteroutput', data_batch)

        logging.info("Filtered joined data inserted into `aud_agg_txmlmapinputinfilteroutput`.")

        # ==============================================================================================
        # Join `aud_inputtable_xml.csv` with `aud_inputtable_xml.csv` for `aud_agg_txmlmapinputinjoininput`
        # Filtering rows based on certain conditions and inserting filtered data into the database
        # ==============================================================================================

        # Step 1: Read the input CSV file
        input_data = []
        with open(input_csv_path, mode='r', encoding='utf-8') as input_csvfile:
            reader = csv.DictReader(input_csvfile)
            input_data = list(reader)

        # Step 2: Initialize a list to hold the filtered rows for insertion
        filtered_data = []

        # Step 3: Apply filtering and transformation rules
        for row in input_data:
            # Rule: Ensure aud_xpathColumnInput is not NULL or empty
            if row['aud_xpathColumnInput'] and row['aud_xpathColumnInput'].strip():
                
                # Rule: Check if the search string exists in aud_xpathColumnInput
                search_string = f"{row['aud_nameRowInput']}.{row['aud_nameColumnInput']}"
                if search_string in row['aud_xpathColumnInput']:
                    
                    # Replace newlines in 'expression' if it is not NULL
                    expression = row['expression'].replace("\n", " ") if row['expression'] else None
                    
                    # Determine is_columnjoined value
                    is_columnjoined = (
                        row['aud_xpathColumnInput'] is not None and
                        search_string in row['aud_xpathColumnInput']
                    )
                    
                    # Prepare the row for insertion
                    filtered_row = (
                        row['aud_nameRowInput'],                 # rowName
                        row['aud_nameColumnInput'],              # NameRowInput
                        row['aud_componentName'],                # Component Name
                        expression,                              # Processed expression for Join
                        row['NameProject'],                      # NameProject
                        row['NameJob'],                          # NameJob
                        is_columnjoined,                         # Column joined flag
                        row['aud_xpathColumnInput'],             # XPath Column Input
                        row['aud_type']                          # Type
                    )
                    
                    # Add the filtered row to the list
                    filtered_data.append(filtered_row)

        # Step 4: Insert filtered data into `aud_agg_txmlmapinputinjoininput`
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapinputinjoininput')
        data_batch = []

        # Insert data in batches
        for row in filtered_data:
            data_batch.append(row)

            # When the batch reaches the defined size, insert the data into the database
            if len(data_batch) >= batch_size:
                try:
                    logging.info(f"Inserting batch of size {len(data_batch)} into `aud_agg_txmlmapinputinjoininput`.")
                    db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinjoininput', data_batch)
                    logging.info(f"Successfully inserted batch of {len(data_batch)} rows.")
                except Exception as e:
                    logging.error(f"Error inserting batch: {str(e)}")
                finally:
                    data_batch.clear()  # Clear the batch after insertion

        # Insert any remaining data that didn't fill the last batch
        if data_batch:
            try:
                logging.info(f"Inserting remaining batch of size {len(data_batch)} into `aud_agg_txmlmapinputinjoininput`.")
                db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinjoininput', data_batch)
                logging.info(f"Successfully inserted remaining batch of {len(data_batch)} rows.")
            except Exception as e:
                logging.error(f"Error inserting remaining batch: {str(e)}")

        logging.info("Data successfully inserted into `aud_agg_txmlmapinputinjoininput` table.")

        # ==============================================================================================
        # insert aud_inputtable_xml.csv in `aud_agg_txmlmapinputinfilterinput`
        # ==============================================================================================
    # Initialize a list to hold the rows to be inserted
        filtered_data = []

        # Step 3: Apply rules to filter and format the data
        for row in input_data:
            # Apply the rule: !Relational.ISNULL(row9.expressionFilter) && !row9.expressionFilter.isEmpty()
            if row['expressionFilter'] is not None and row['expressionFilter'].strip():
                
                # Apply the rule: routines.Utils.containsElementExpression(row9.expressionFilter, row9.rowName + "." + row9.NameColumnInput) == 1
                search_string = row['rowName'] + "." + row['NameRowInput']
                if search_string in row['expressionFilter']:
                    
                    # Prepare the expressionFilter: replace newlines if expressionFilter is not NULL
                    expression_filter = row['expressionFilter'].replace("\n", " ") if row['expressionFilter'] else None
                    
                    # Prepare the data for insertion
                    filtered_row = (
                        row['rowName'],  # rowName
                        row['NameRowInput'],  # NameColumnInput
                        expression_filter,  # expressionFilter
                        row['composant'],  # composant
                        row['NameProject'],  # NameProject
                        row['NameJob']  # NameJob
                    )
                    
                    # Add the row to the list
                    filtered_data.append(filtered_row)

        # Step 4: Insert filtered data into 'aud_agg_txmlmapinputinfilterinput'
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapinputinfilterinput')
        data_batch = []
        for row in filtered_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                # Insert data in batches
                db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinfilterinput', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinfilterinput', data_batch)

        logging.info("Data successfully inserted into `aud_agg_txmlmapinputinfilterinput` table.")
        # ==============================================================================================
    # Join aud_inputtable_xml.csv & aud_vartable_xml for `aud_agg_txmlmapinputinvar`
    # ==============================================================================================

    # Step 1: Execute aud_vartable_xml query
    aud_vartable_xml_query = config.get_param('agg_queries', 'aud_vartable_xml')
    logging.info(f"Executing query: {aud_vartable_xml_query}")
    aud_vartable_xml_results = db.execute_query(aud_vartable_xml_query)

    logging.info(f"aud_vartable_xml_results: {aud_vartable_xml_results}")

    # Step 2: Load aud_outputtable_xml.csv
    directory_path = config.get_param('Directories', 'delete_files')
    output_csv_path = os.path.join(directory_path, "aud_outputtable_xml.csv")

    output_data = []
    with open(output_csv_path, mode='r', encoding='utf-8') as output_csvfile:
        reader = csv.DictReader(output_csvfile)
        output_data = list(reader)

    # Step 4: Join aud_inputtable_xml.csv with aud_vartable_xml based on (NameJob, NameProject, aud_componentValue)
    filtered_joined_data = []

    # Iterate over each row from aud_vartable_xml results
    for var_row in aud_vartable_xml_results:
        for input_row in input_data:
            # Join condition based on NameJob, NameProject, aud_componentValue
            if (var_row[8] == input_row['NameJob'] and             # NameJob match (index 8)
                var_row[7] == input_row['NameProject'] and         # NameProject match (index 7)
                var_row[1] == input_row['composant']):             # aud_componentValue match (index 1)

                # Check if vartable.aud_expressionVar is not null and not empty
                if var_row[5] is not None and var_row[5].strip():  # index 5 for aud_expressionVar
                    # Create search_string
                    search_string = f"{var_row[2]}.{var_row[4]}"   # index 2 for aud_Var and index 4 for aud_nameVar

                    # Check if search_string exists in aud_expressionVar
                    if search_string in var_row[5]:  
                        # Replace newlines in 'aud_expressionVar'
                        aud_expressionVar = var_row[5].replace("\n", " ")

                        # Prepare the row for insertion into the aud_agg_txmlmapinputinvar table
                        joined_row = (
                            input_row['rowName'],                     # NameRowInput
                            input_row['NameRowInput'],
                            input_row['composant'],                   # composant
                            aud_expressionVar,                        # expressionFilterOutput (aud_expressionVar after newline replacement)
                            f"{var_row[2]}.{var_row[4]}",            # nameColumnOutput (aud_Var.aud_nameVar)
                            input_row['NameProject'],                 # NameProject
                            input_row['NameJob']                      # NameJob
                        )

                        # Append to filtered data list
                        filtered_joined_data.append(joined_row)

    # Step 5: Insert the filtered and joined data into the aud_agg_txmlmapinputinvar table
    insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapinputinvar')
    data_batch = []

    for row in filtered_joined_data:
        data_batch.append(row)
        if len(data_batch) >= batch_size:
            # Insert data in batches
            db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinvar', data_batch)
            data_batch.clear()

    # Insert any remaining data
    if data_batch:
        db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinvar', data_batch)

    logging.info("Filtered joined data successfully inserted into aud_agg_txmlmapinputinvar table.")

      
# ===================================================================================================
# Catching lookup inner join reject for `aud_agg_txmlmapcolumunused`
# ===================================================================================================

    # Function to load SQL table data
    def load_sql_table(query, column_names):
        """Executes a SQL query and returns the result as a pandas DataFrame."""
        try:
            data = db.execute_query(query)
            df = pd.DataFrame(data, columns=column_names)
            return df
        except Exception as e:
            logging.error(f"Error loading data from query: {query}. Error: {str(e)}")
            return pd.DataFrame(columns=column_names)

    # Function for left anti join to mimic Talend's 'catch lookup rejects'
    def left_anti_join(left_df, right_df, join_columns):
        logging.info(f"Performing left anti join on columns: {join_columns}")
        merged_df = pd.merge(left_df, right_df, on=join_columns, how='left', indicator=True)
        unmatched_rows = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
        logging.info(f"Found {len(unmatched_rows)} unmatched rows after join.")
        return unmatched_rows

    # SQL Queries and column mappings
    sql_tables = {
        'aud_agg_txmlmapinputinoutput': "SELECT DISTINCT rowName, aud_nameColumnInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinoutput",
        'aud_agg_txmlmapinputinfilteroutput': "SELECT DISTINCT rowName, NameRowInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinfilteroutput",
        'aud_agg_txmlmapinputinjoininput': "SELECT DISTINCT rowName, NameColumnInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinjoininput",
        'aud_agg_txmlmapinputinfilterinput': "SELECT DISTINCT rowName, NameColumnInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinfilterinput",
        'aud_agg_txmlmapinputinvar': "SELECT DISTINCT rowName, NameRowInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinvar"
    }

    column_mapping = {
        'aud_agg_txmlmapinputinoutput': ['rowName', 'aud_nameColumnInput', 'composant', 'NameProject', 'NameJob'],
        'aud_agg_txmlmapinputinfilteroutput': ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'],
        'aud_agg_txmlmapinputinjoininput': ['rowName', 'NameColumnInput', 'composant', 'NameProject', 'NameJob'],
        'aud_agg_txmlmapinputinfilterinput': ['rowName', 'NameColumnInput', 'composant', 'NameProject', 'NameJob'],
        'aud_agg_txmlmapinputinvar': ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']
    }

    # Load input CSV
    input_df = pd.read_csv(input_csv_path, usecols=[ 'NameRowInput','aud_nameRowInput', 'aud_componentName', 'NameProject', 'NameJob'])

    # Load SQL tables into DataFrames
    sql_dataframes = {table: load_sql_table(query, column_mapping[table]) for table, query in sql_tables.items()}

    # Perform left anti joins
    unmatched_df = input_df.copy()
    for table, right_df in sql_dataframes.items():
        logging.info(f"Joining input_df with table: {table}")
        join_columns = [col for col in column_mapping[table] if col in unmatched_df.columns and col in right_df.columns]
        
        if join_columns:
            unmatched_df = left_anti_join(unmatched_df, right_df, join_columns)
        else:
            logging.warning(f"No common columns to join for table {table}")

    # Insert unmatched rows if any
    if not unmatched_df.empty:
        logging.info(f"Inserting {len(unmatched_df)} unmatched rows into the database.")
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapcolumunused')
        relevant_columns = ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']
        insert_data = [tuple(row[col] for col in relevant_columns) for row in unmatched_df.to_dict(orient='records') if all(row[col] is not None for col in relevant_columns)]
        
        batch_size = 1000
        for i in range(0, len(insert_data), batch_size):
            batch = insert_data[i:i + batch_size]
            try:
                db.insert_data_batch(insert_query, 'aud_agg_txmlmapcolumunused', batch)
                logging.info(f"Inserted batch {i // batch_size + 1} of size {len(batch)}.")
            except Exception as e:
                logging.error(f"Error inserting batch: {str(e)}")

    # Execute additional aggregation query and insert results
    agg_aud_inputtable_xml_query = config.get_param('agg_queries', 'aud_inputtable_xml_nb')
    agg_aud_inputtable_xml_results = db.execute_query(agg_aud_inputtable_xml_query)
    insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapinput')

    batch_insert = []
    for result in agg_aud_inputtable_xml_results:
        batch_insert.append(result)
        if len(batch_insert) >= batch_size:
            try:
                db.insert_data_batch(insert_query, 'aud_agg_txmlmapinput', batch_insert)
                logging.info(f"Inserted batch of size {len(batch_insert)}.")
            except Exception as e:
                logging.error(f"Error inserting batch: {str(e)}")
            batch_insert.clear()

    # Insert any remaining data
    if batch_insert:
        try:
            db.insert_data_batch(insert_query, 'aud_agg_txmlmapinput', batch_insert)
            logging.info(f"Inserted final batch of size {len(batch_insert)}.")
        except Exception as e:
            logging.error(f"Error inserting final batch: {str(e)}")


        except Exception as e:
            logging.error(f"An error occurred: {str(e)}", exc_info=True)

  # # ==============================================================================================
        # #  Join aud_vartable_xml &  aud_outputtable_xml.csv for `aud_agg_txmlmapvarinoutput`
        # # ==============================================================================================
        # filtered_joined_data = []

        # # Iterate over each row from aud_vartable_xml_results
        # for var_row in aud_vartable_xml_results:
        #     for output_row in output_data:  # output_data corresponds to aud_outputtable_xml.csv
        #         # Join condition based on NameJob, NameProject, aud_componentValue
        #         if (var_row[5] == output_row['NameJob'] and            # NameJob match
        #             var_row[4] == output_row['NameProject'] and        # NameProject match
        #             var_row[2] == output_row['aud_componentValue']):   # aud_componentValue match

        #             # Check if aud_expressionFilterOutput is not null and contains the appropriate string
        #             if output_row['aud_expressionFilterOutput'] and output_row['aud_expressionFilterOutput'].strip() != "":
        #                 search_string = var_row[1] + "." + var_row[2]  # Combine aud_Var + "." + aud_nameVar
                        
        #                 # Check if search_string is contained in aud_expressionFilterOutput
        #                 if search_string in output_row['aud_expressionFilterOutput']:
        #                     # Replace newlines in aud_expressionFilterOutput with spaces
        #                     expression_filter = output_row['aud_expressionFilterOutput'].replace("\n", " ")
                            
        #                     # Prepare the row for insertion into aud_agg_txmlmapvarinfilter table
        #                     joined_row = (
        #                         search_string,        # NameRowInput
        #                         var_row[2],       # composant
        #                         expression_filter,       # expressionFilterOutput
        #                         output_row['aud_nameRowOutput'],        # OutputName
        #                         output_row['aud_reject'],               # reject
        #                         output_row['aud_rejectInnerJoin'],      # rejectInnerJoin
        #                         var_row[4],  # NameProject
        #                         var_row[5]                   # NameJob
        #                     )
                            
        #                     # Append the joined row to the filtered data list
        #                     filtered_joined_data.append(joined_row)

        # # Step 7: Insert the filtered and joined data into the aud_agg_txmlmapvarinfilter table
        # insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapvarinfilter')
        # data_batch = []

        # for row in filtered_joined_data:
        #     data_batch.append(row)
        #     if len(data_batch) >= batch_size:
        #         # Insert data in batches
        #         db.insert_data_batch(insert_query, 'aud_agg_txmlmapvarinfilter', data_batch)
        #         data_batch.clear()

        # # Insert any remaining data
        # if data_batch:
        #     db.insert_data_batch(insert_query, 'aud_agg_txmlmapvarinfilter', data_batch)

        # logging.info("Filtered joined data successfully inserted into aud_agg_txmlmapvarinfilter table.")

        # # ==============================================================================================
        # #  Join aud_vartable_xml &  aud_outputtable_xml.csv for `aud_agg_txmlmapvarinfilter`
        # # ==============================================================================================
        # # Step 3: Initialize a list to hold the filtered and joined data
        # filtered_joined_data = []

        # # Step 4: Join with aud_outputtable_xml.csv based on (NameJob, NameProject, aud_componentValue)
        # for var_row in aud_vartable_xml_results:
        #     for output_row in output_data:
        #         if (var_row[2] == output_row['aud_componentValue'] and  # index 2 for aud_nameVar
        #             var_row[5] == output_row['NameJob'] and           # index 5 for NameJob
        #             var_row[4] == output_row['NameProject']):         # index 4 for NameProject
                    
        #             # Apply the rule: !Relational.ISNULL(row5.aud_expressionFilterOutput)
        #             if output_row['aud_expressionFilterOutput'] is not None:
                        
        #                 # Apply the rule: routines.Utils.containsElementExpression
        #                 search_string = var_row[1] + "." + var_row[2]  # index 1 for aud_Var and index 2 for aud_nameVar
        #                 if search_string in output_row['aud_expressionFilterOutput']:
                            
        #                     # Prepare the row for insertion into the aud_agg_txmlmapvarinfilter table
        #                     filtered_row = (
        #                         search_string,  # NameRowInput (index 2)
        #                         var_row[2],  # composant (aud_nameVar)
        #                         output_row['aud_expressionFilterOutput'],  # expressionFilterOutput
        #                         output_row['aud_nameRowOutput'],  # OutputName
        #                         output_row['aud_reject'],  # reject
        #                         output_row['aud_rejectInnerJoin'],  # rejectInnerJoin
        #                         var_row[4],  # NameProject
        #                         var_row[5]   # NameJob
        #                     )
                            
        #                     # Add the row to the list
        #                     filtered_joined_data.append(filtered_row)

        # # Step 5: Insert filtered and joined data into aud_agg_txmlmapvarinfilter
        # insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapvarinfilter')
        # data_batch = []
        # for row in filtered_joined_data:
        #     data_batch.append(row)
        #     if len(data_batch) >= batch_size:
        #         # Insert data in batches
        #         db.insert_data_batch(insert_query, 'aud_agg_txmlmapvarinfilter', data_batch)
        #         data_batch.clear()

        # # Insert any remaining data
        # if data_batch:
        #     db.insert_data_batch(insert_query, 'aud_agg_txmlmapvarinfilter', data_batch)

        # logging.info("Data successfully inserted into `aud_agg_txmlmapvarinfilter` table.")