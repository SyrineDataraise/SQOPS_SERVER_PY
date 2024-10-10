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
        db (Database): Database instance for executing agg_queries.
        execution_date (str): The execution date to use in data insertion.
        batch_size (int, optional): Number of rows to insert in each batch. Defaults to 100.
    """
    try:
        # Execute AUD aggregation query
        aud_agg_query = config.get_param('agg_queries', 'aud_agg')
        logging.info(f"Executing query: {aud_agg_query}")
        aud_agg_results = db.execute_query(aud_agg_query)
        logging.debug(f"AUD aggregation results: {aud_agg_results}")

        # Prepare batch insert
        batch_insert = []
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_aggregate')

        for result in aud_agg_results:
            # Unpack result tuple
            (NameProject, NameJob, aud_componentValue, aud_valueElementRef_input,
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















    
def AUD_405_AGG_TMAP(config: Config, db: Database, execution_date: str, batch_size=100):
    """
    This function:
    - Deletes existing files in a specified directory.
    - Executes two agg_queries (inputtable XML and outputtable XML), writes results to CSVs.
    - Performs two different joins:
        - One for inserting into `aud_agg_tmapinputinoutput`.
        - Another for inserting into `aud_agg_tmapinputinfilteroutput`.

    Args:
        config (Config): Configuration instance for retrieving parameters.
        db (Database): Database instance for executing agg_queries.
        execution_date (str): Execution date used in data insertion.
        batch_size (int, optional): Number of rows to insert in each batch. Defaults to 100.
    """
    
    try:
        # ==============================================================================================
        #     write into  aud_inputtable.csv & outputtable.csv  
        # ==============================================================================================
        # Step 1: Clean the directory by deleting existing files
        directory_path = config.get_param('Directories', 'delete_files')
        delete_files_in_directory(directory_path)

        # Step 2: Execute inputtable XML query and write to CSV
        inputtable_query = config.get_param('agg_queries', 'inputtable')
        logging.info(f"Executing query: {inputtable_query}")
        inputtable_results = db.execute_query(inputtable_query)

        input_csv_path = os.path.join(directory_path, "aud_inputtable.csv")
        input_csv_header = [
            "rowName", "NameRowInput", "expressionJoin", "expressionFilterInput", 
            "composant", "innerJoin", "NameProject", "NameJob"
        ]

        with open(input_csv_path, mode='w', newline='', encoding='utf-8') as input_csvfile:
            writer = csv.writer(input_csvfile)
            writer.writerow(input_csv_header)
            writer.writerows(inputtable_results)

        logging.info(f"Input table results written to {input_csv_path}")

        # Step 3: Execute outputtable XML query and write to CSV
        outputtable_query = config.get_param('agg_queries', 'outputtable')
        logging.info(f"Executing query: {outputtable_query}")
        outputtable_results = db.execute_query(outputtable_query)

        output_csv_path = os.path.join(directory_path, "aud_outputtable.csv")
        output_csv_header = ["aud_componentName","aud_OutputName", "aud_sizeState","aud_activateCondensedTool", "aud_reject", "aud_rejectInnerJoin", "aud_expressionOutput", "aud_nameColumnOutput", "aud_type", "aud_nullable", "aud_activateExpressionFilter", "aud_expressionFilterOutput", "aud_componentValue", "NameProject", "NameJob" ]

        with open(output_csv_path, mode='w', newline='', encoding='utf-8') as output_csvfile:
            writer = csv.writer(output_csvfile)
            writer.writerow(output_csv_header)
            writer.writerows(outputtable_results)

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
        #  Join aud_inputtable.csv & outputtable.csv for `aud_agg_tmapinputinoutput`
        # ==============================================================================================
        joined_data_output_table = []

        for input_row in input_data:
            for output_row in output_data:
                if (input_row['composant'] == output_row['aud_componentValue'] and
                    input_row['NameJob'] == output_row['NameJob'] and
                    input_row['NameProject'] == output_row['NameProject']):
                    
                    # Prepare the row for insertion into the first table
                    joined_row = (
                        input_row['rowName'],
                        input_row['NameRowInput'],
                        input_row['composant'],
                        output_row['aud_expressionOutput'],  # Assuming 'expression' represents 'expressionOutput'
                        output_row['aud_nameColumnOutput'],  # output_aud_nameColumnInput
                        output_row['aud_OutputName'],
                        output_row['aud_reject'],
                        output_row['aud_rejectInnerJoin'],
                        output_row['NameJob'],
                        output_row['NameProject']
                    )
                    joined_data_output_table.append(joined_row)

        # Insert joined data into `aud_agg_tmapinputinoutput`
        insert_query_output_table = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinoutput')
        data_batch = []
        for row in joined_data_output_table:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                db.insert_data_batch(insert_query_output_table, 'aud_agg_tmapinputinoutput', data_batch)
                data_batch.clear()

        if data_batch:
            db.insert_data_batch(insert_query_output_table, 'aud_agg_tmapinputinoutput', data_batch)

        logging.info("Joined data inserted into `aud_agg_tmapinputinoutput`.")

        # ==============================================================================================
        #  Join aud_inputtable.csv & unique outputtable.csv for `aud_agg_tmapinputinfilteroutput`
        # ==============================================================================================
        filtered_joined_data = []
        unique_rows = set()  # Track uniqueness based on (NameJob, NameProject, composant, aud_componentValue)

        for input_row in input_data:
            for output_row in output_data:
                if (input_row['composant'] == output_row['aud_componentValue'] and
                    input_row['NameJob'] == output_row['NameJob'] and
                    input_row['NameProject'] == output_row['NameProject']):
                    
                    # Check expression filter
                    search_string = input_row['NameRowInput'] + "." + input_row['rowName']
                    if search_string in (output_row['aud_expressionFilterOutput'] or "") and output_row['aud_expressionFilterOutput'] != None :
                        expression_filter_output = output_row['aud_expressionFilterOutput'].replace("\n", " ") if output_row['aud_expressionFilterOutput'] else None
                        
                        # Prepare the row for insertion into the second table
                        row_key = (output_row['NameJob'], output_row['NameProject'], input_row['composant'], output_row['aud_componentValue'])
                        if row_key not in unique_rows:
                            joined_row = (
                                input_row['rowName'],
                                input_row['NameRowInput'],
                                input_row['composant'],
                                expression_filter_output,
                                output_row['aud_nameRowOutput'],
                                output_row['innerJoin'],  # aud_reject
                                output_row['rejectInnerJoin'],  # aud_rejectInnerJoin
                                output_row['NameProject'],
                                output_row['NameJob']
                            )
                            filtered_joined_data.append(joined_row)
                            unique_rows.add(row_key)

        # Insert joined data into `aud_agg_tmapinputinfilteroutput`
        insert_query_filter_output_table = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinfilteroutput')
        data_batch = []
        for row in filtered_joined_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                db.insert_data_batch(insert_query_filter_output_table, 'aud_agg_tmapinputinfilteroutput', data_batch)
                data_batch.clear()

        if data_batch:
            db.insert_data_batch(insert_query_filter_output_table, 'aud_agg_tmapinputinfilteroutput', data_batch)

        logging.info("Filtered joined data inserted into `aud_agg_tmapinputinfilteroutput`.")

        # ==============================================================================================
        #  Join aud_inputtable.csv & aud_inputtable.csv for `aud_agg_tmapinputinjoininput`
        # ==============================================================================================

        # Step 1: Read the input CSV file
        input_data = []
        with open(input_csv_path, mode='r', encoding='utf-8') as input_csvfile:
            reader = csv.DictReader(input_csvfile)
            input_data = list(reader)

        # Step 2: Initialize a list to hold the rows to be inserted
        filtered_data = []

        # Step 3: Apply rules to filter and format the data
        for row in input_data:
            # Apply the rule: !Relational.ISNULL(row8.expressionJoin) && !row8.expressionJoin.isEmpty()
            if row['expressionJoin'] is not None and row['expressionJoin'].strip():
                
                # Apply the rule: routines.Utils.containsElementExpression(row8.expressionJoin, row7.rowName + "." + row7.NameRowInput) == 1
                search_string = row['rowName'] + "." + row['NameRowInput']
                if search_string in row['expressionJoin']:
                    
                    # Prepare the expressionJoin: replace newlines if expressionJoin is not NULL
                    expression_join = row['expressionJoin'].replace("\n", " ") if row['expressionJoin'] else None
                    
                    # Set the is_columnjoined flag based on the rule
                    is_columnjoined = 1 if row['expressionJoin'] is not None and row['expressionJoin'].strip() else 0
                    
                    # Prepare the data for insertion
                    filtered_row = (
                        row['rowName'],  # rowName
                        row['NameRowInput'],  # NameColumnInput
                        expression_join,  # expressionJoin
                        row['composant'],  # composant
                        row['innerJoin'],  # innerJoin
                        row['NameProject'],  # NameProject
                        row['NameJob'],  # NameJob
                        is_columnjoined,  # is_columnjoined
                        row['rowName_join'],  # rowName_join
                        row['NameColumnInput_join']  # NameColumnInput_join
                    )
                    
                    # Add the row to the list
                    filtered_data.append(filtered_row)

        # Step 4: Insert filtered data into 'aud_agg_tmapinputinjoininput'
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinjoininput')
        data_batch = []
        for row in filtered_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                # Insert data in batches
                db.insert_data_batch(insert_query, 'aud_agg_tmapinputinjoininput', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_tmapinputinjoininput', data_batch)

        logging.info("Data successfully inserted into `aud_agg_tmapinputinjoininput` table.")
        # ==============================================================================================
        # insert aud_inputtable.csv in `aud_agg_tmapinputinfilterinput`
        # ==============================================================================================
    # Initialize a list to hold the rows to be inserted
        filtered_data = []

        # Step 3: Apply rules to filter and format the data
        for row in input_data:
            # Apply the rule: !Relational.ISNULL(row9.expressionFilterInput) && !row9.expressionFilterInput.isEmpty()
            if row['expressionFilterInput'] is not None and row['expressionFilterInput'].strip():
                
                # Apply the rule: routines.Utils.containsElementExpression(row9.expressionFilterInput, row9.rowName + "." + row9.NameColumnInput) == 1
                search_string = row['rowName'] + "." + row['NameRowInput']
                if search_string in row['expressionFilterInput']:
                    
                    # Prepare the expressionFilterInput: replace newlines if expressionFilterInput is not NULL
                    expression_filter_input = row['expressionFilterInput'].replace("\n", " ") if row['expressionFilterInput'] else None
                    
                    # Prepare the data for insertion
                    filtered_row = (
                        row['rowName'],  # rowName
                        row['NameRowInput'],  # NameColumnInput
                        expression_filter_input,  # expressionFilterInput
                        row['composant'],  # composant
                        row['NameProject'],  # NameProject
                        row['NameJob']  # NameJob
                    )
                    
                    # Add the row to the list
                    filtered_data.append(filtered_row)

        # Step 4: Insert filtered data into 'aud_agg_tmapinputinfilterinput'
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinfilterinput')
        data_batch = []
        for row in filtered_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                # Insert data in batches
                db.insert_data_batch(insert_query, 'aud_agg_tmapinputinfilterinput', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_tmapinputinfilterinput', data_batch)

        logging.info("Data successfully inserted into `aud_agg_tmapinputinfilterinput` table.")
        # ==============================================================================================
        # Join aud_inputtable.csv &  aud_vartable for `aud_agg_tmapinputinvar`
        # ==============================================================================================
    # Step 1: Execute aud_vartable query
        aud_vartable_query = config.get_param('agg_queries', 'aud_vartable')
        logging.info(f"Executing query: {aud_vartable_query}")
        aud_vartable_results = db.execute_query(aud_vartable_query)

        logging.info(f"aud_vartable_results: {aud_vartable_results}")

        # Step 2: Load aud_outputtable.csv
        directory_path = config.get_param('Directories', 'delete_files')
        output_csv_path = os.path.join(directory_path, "aud_outputtable.csv")

        output_data = []
        with open(output_csv_path, mode='r', encoding='utf-8') as output_csvfile:
            reader = csv.DictReader(output_csvfile)
            output_data = list(reader)
        # Step 4: Join aud_inputtable.csv with aud_vartable based on (NameJob, NameProject, aud_componentValue)
        filtered_joined_data = []

        # Iterate over each row from aud_vartable results
        for var_row in aud_vartable_results:
            for input_row in input_data:
                # Join condition based on NameJob, NameProject, aud_componentValue
                if (var_row[5] == input_row['NameJob'] and            # NameJob match
                    var_row[4] == input_row['NameProject'] and        # NameProject match
                    var_row[2] == input_row['composant']):            # aud_componentValue match

                    # Check if vartable.aud_expressionVar is not null and not empty
                    if var_row[3] is not None and var_row[3].strip() != "":  # index 3 for aud_expressionVar
                        # Create search_string
                        search_string = var_row[1] + "." + var_row[2]  # index 1 for aud_Var and index 2 for aud_nameVar

                        # Check if search_string is in input_row['aud_expressionFilterOutput']
                        if search_string in (input_row['expressionFilterInput'] or ""):
                            # If aud_expressionVar is not None, replace newlines with spaces
                            aud_expressionVar_cleaned = var_row[3].replace("\n", " ") if var_row[3] else None
                            
                            # Prepare the row for insertion into the aud_agg_tmapinputinvar table
                            joined_row = (
                                input_row['rowName'],                 # NameRowInput
                                input_row['composant'],               # composant
                                aud_expressionVar_cleaned,            # expressionFilterOutput (aud_expressionVar after newline replacement)
                                input_row['rowName'],                 # OutputName (rowName used as placeholder)
                                input_row['reject'],                  # reject
                                input_row['innerJoin'],               # rejectInnerJoin (assuming innerJoin maps to rejectInnerJoin)
                                input_row['NameProject'],             # NameProject
                                input_row['NameJob']                  # NameJob
                            )
                            
                            # Append to filtered data list
                            filtered_joined_data.append(joined_row)

        # Step 5: Insert the filtered and joined data into the aud_agg_tmapinputinvar table
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinvar')
        data_batch = []

        for row in filtered_joined_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                # Insert data in batches
                db.insert_data_batch(insert_query, 'aud_agg_tmapinputinvar', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_tmapinputinvar', data_batch)

        logging.info("Filtered joined data successfully inserted into aud_agg_tmapinputinvar table.")
        # ==============================================================================================
        #  Join aud_vartable &  outputtable.csv for `aud_agg_tmapvarinoutput`
        # ==============================================================================================
        filtered_joined_data = []

        # Iterate over each row from aud_vartable_results
        for var_row in aud_vartable_results:
            for output_row in output_data:  # output_data corresponds to outputtable.csv
                # Join condition based on NameJob, NameProject, aud_componentValue
                if (var_row[5] == output_row['NameJob'] and            # NameJob match
                    var_row[4] == output_row['NameProject'] and        # NameProject match
                    var_row[2] == output_row['aud_componentValue']):   # aud_componentValue match

                    # Check if aud_expressionFilterOutput is not null and contains the appropriate string
                    if output_row['aud_expressionFilterOutput'] and output_row['aud_expressionFilterOutput'].strip() != "":
                        search_string = var_row[1] + "." + var_row[2]  # Combine aud_Var + "." + aud_nameVar
                        
                        # Check if search_string is contained in aud_expressionFilterOutput
                        if search_string in output_row['aud_expressionFilterOutput']:
                            # Replace newlines in aud_expressionFilterOutput with spaces
                            expression_filter = output_row['aud_expressionFilterOutput'].replace("\n", " ")
                            
                            # Prepare the row for insertion into aud_agg_tmapvarinfilter table
                            joined_row = (
                                search_string,        # NameRowInput
                                var_row[2],       # composant
                                expression_filter,       # expressionFilterOutput
                                output_row['aud_nameRowOutput'],        # OutputName
                                output_row['aud_reject'],               # reject
                                output_row['aud_rejectInnerJoin'],      # rejectInnerJoin
                                var_row[4],  # NameProject
                                var_row[5]                   # NameJob
                            )
                            
                            # Append the joined row to the filtered data list
                            filtered_joined_data.append(joined_row)

        # Step 7: Insert the filtered and joined data into the aud_agg_tmapvarinfilter table
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapvarinfilter')
        data_batch = []

        for row in filtered_joined_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                # Insert data in batches
                db.insert_data_batch(insert_query, 'aud_agg_tmapvarinfilter', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_tmapvarinfilter', data_batch)

        logging.info("Filtered joined data successfully inserted into aud_agg_tmapvarinfilter table.")

        # ==============================================================================================
        #  Join aud_vartable &  outputtable.csv for `aud_agg_tmapvarinfilter`
        # ==============================================================================================
        # Step 3: Initialize a list to hold the filtered and joined data
        filtered_joined_data = []

        # Step 4: Join with aud_outputtable.csv based on (NameJob, NameProject, aud_componentValue)
        for var_row in aud_vartable_results:
            for output_row in output_data:
                if (var_row[2] == output_row['aud_componentValue'] and  # index 2 for aud_nameVar
                    var_row[5] == output_row['NameJob'] and           # index 5 for NameJob
                    var_row[4] == output_row['NameProject']):         # index 4 for NameProject
                    
                    # Apply the rule: !Relational.ISNULL(row5.aud_expressionFilterOutput)
                    if output_row['aud_expressionFilterOutput'] is not None:
                        
                        # Apply the rule: routines.Utils.containsElementExpression
                        search_string = var_row[1] + "." + var_row[2]  # index 1 for aud_Var and index 2 for aud_nameVar
                        if search_string in output_row['aud_expressionFilterOutput']:
                            
                            # Prepare the row for insertion into the aud_agg_tmapvarinfilter table
                            filtered_row = (
                                search_string,  # NameRowInput (index 2)
                                var_row[2],  # composant (aud_nameVar)
                                output_row['aud_expressionFilterOutput'],  # expressionFilterOutput
                                output_row['aud_nameRowOutput'],  # OutputName
                                output_row['aud_reject'],  # reject
                                output_row['aud_rejectInnerJoin'],  # rejectInnerJoin
                                var_row[4],  # NameProject
                                var_row[5]   # NameJob
                            )
                            
                            # Add the row to the list
                            filtered_joined_data.append(filtered_row)

        # Step 5: Insert filtered and joined data into aud_agg_tmapvarinfilter
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapvarinfilter')
        data_batch = []
        for row in filtered_joined_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                # Insert data in batches
                db.insert_data_batch(insert_query, 'aud_agg_tmapvarinfilter', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_tmapvarinfilter', data_batch)

        logging.info("Data successfully inserted into `aud_agg_tmapvarinfilter` table.")
# ==========================================================================================================================================================================================================================================================================================
# Catching lookup inner join reject aud_inputtable.csv & aud_agg_tmapinputinoutput & aud_agg_tmapinputinfilteroutput & aud_agg_tmapinputinjoininput & aud_agg_tmapinputinfilterinput & aud_agg_tmapinputinvar for `aud_agg_tmapcolumunused` on rowName, NameColumnInput, composant, NameProject, NameJob
# ==========================================================================================================================================================================================================================================================================================
        # Improved Join Logic with input_df as the main DataFrame
        # Function to load SQL table data
        def load_sql_table(query, column_names):
            """
            Executes a SQL query and returns the result as a pandas DataFrame.
            
            Args:
                query (str): The SQL query to execute.
                column_names (list): List of column names for the DataFrame.
            
            Returns:
                pd.DataFrame: DataFrame containing the query results.
            """
            try:
                # Assume db.execute_query returns a list of tuples; create DataFrame directly
                data = db.execute_query(query)
                df = pd.DataFrame(data, columns=column_names)
                return df
            except Exception as e:
                logging.error(f"Error loading data from query: {query}. Error: {str(e)}")
                return pd.DataFrame(columns=column_names)

        # Function to perform the "catch lookup rejects" logic using left anti join
        def left_anti_join(left_df, right_df, join_columns):
            """
            Perform a left join and return only the rows in left_df that do not have a match in right_df.
            This mimics Talend's tMap 'catch lookup rejects' functionality.
            
            Args:
                left_df (pd.DataFrame): The left DataFrame.
                right_df (pd.DataFrame): The right DataFrame.
                join_columns (list): Columns to join on.
            
            Returns:
                pd.DataFrame: DataFrame containing unmatched rows from the left DataFrame.
            """
            logging.info(f"Performing left anti join on columns: {join_columns}")
            
            # Perform a full join with an indicator to find unmatched rows
            merged_df = pd.merge(left_df, right_df, on=join_columns, how='left', indicator=True)
            
            # Select rows that did not match in the right DataFrame
            unmatched_rows = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
            
            logging.info(f"Found {len(unmatched_rows)} unmatched rows after join.")
            return unmatched_rows

        # SQL Queries to retrieve data from SQL tables
        sql_tables = {
            'aud_agg_tmapinputinoutput': "SELECT DISTINCT rowName, NameRowInput, composant, NameProject, NameJob FROM aud_agg_tmapinputinoutput",
            'aud_agg_tmapinputinfilteroutput': "SELECT DISTINCT rowName, NameRowInput, composant, NameProject, NameJob FROM aud_agg_tmapinputinfilteroutput",
            'aud_agg_tmapinputinjoininput': "SELECT DISTINCT rowName, NameColumnInput, composant, NameProject, NameJob FROM aud_agg_tmapinputinjoininput",
            'aud_agg_tmapinputinfilterinput': "SELECT DISTINCT rowName, NameColumnInput, composant, NameProject, NameJob FROM aud_agg_tmapinputinfilterinput",
            'aud_agg_tmapinputinvar': "SELECT DISTINCT rowName, NameRowInput, composant, NameProject, NameJob FROM aud_agg_tmapinputinvar"
        }

        # Column mappings for each SQL table
        column_mapping = {
            'aud_agg_tmapinputinoutput': ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'],
            'aud_agg_tmapinputinfilteroutput': ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'],
            'aud_agg_tmapinputinjoininput': ['rowName', 'NameColumnInput', 'composant', 'NameProject', 'NameJob'],
            'aud_agg_tmapinputinfilterinput': ['rowName', 'NameColumnInput', 'composant', 'NameProject', 'NameJob'],
            'aud_agg_tmapinputinvar': ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']
        }

        # Load the input CSV file into a DataFrame (input_df is the main DataFrame)
        input_df = pd.read_csv(input_csv_path, usecols=['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'])

        # Load SQL data into DataFrames using the agg_queries and column mappings
        sql_dataframes = {table: load_sql_table(query, column_mapping[table]) for table, query in sql_tables.items()}

        # Initialize unmatched_df with input_df (this will hold unmatched rows after each join)
        unmatched_df = input_df.copy()

        # Iteratively perform left anti join with each SQL DataFrame
        for table, right_df in sql_dataframes.items():
            logging.info(f"Joining input_df with table: {table}")
            
            # Determine the join columns dynamically based on column mapping
            join_columns = [col for col in column_mapping[table] if col in unmatched_df.columns and col in right_df.columns]
            
            if join_columns:
                # Perform the left anti join to capture unmatched rows
                unmatched_df = left_anti_join(unmatched_df, right_df, join_columns)
            else:
                logging.warning(f"No common columns to join for table {table}")

        # After all joins, the remaining unmatched rows are stored in `unmatched_df`
        unused_data = unmatched_df

        # Insert unmatched rows into the database if there are any
        if not unused_data.empty:
            logging.info(f"Inserting {len(unused_data)} unused rows into the database.")
            
            insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapcolumunused')
            
            # Select only the relevant columns before insertion
            relevant_columns = ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']  # Adjust this list if necessary
            
            # Prepare the data for insertion
            insert_data = [tuple(row[col] for col in relevant_columns) for row in unused_data.to_dict(orient='records') if all(row[col] is not None for col in relevant_columns)]
            
            # Log the prepared insert data
            logging.info(f"Prepared data for insertion: {insert_data[:10]}")  # Log the first 10 rows for inspection
            
            # Insert data in batches 
            batch_insert = []

            batch_size = 1000
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                try:
                    db.insert_data_batch(insert_query, 'aud_agg_tmapcolumunused', batch)
                    logging.info(f"Inserted batch {i // batch_size + 1} of size {len(batch)}.")
                except Exception as e:
                    logging.error(f"Error inserting batch: {str(e)}")
        else:
            logging.info("No unused data to insert.")

        # Step 2: Execute agg_aud_inputtable query and write to CSV
        agg_aud_inputtable_query = config.get_param('agg_queries', 'agg_aud_inputtable')
        logging.info(f"Executing query: {agg_aud_inputtable_query}")
        agg_aud_inputtable_results = db.execute_query(agg_aud_inputtable_query)

        # Prepare and insert batch results from agg_aud_inputtable query
        batch_insert = []
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinput')

        # Log the total number of results to be processed
        logging.info(f"Processing {len(agg_aud_inputtable_results)} rows from agg_aud_inputtable query.")

        # Process results in batches
        for result in agg_aud_inputtable_results:
            batch_insert.append(result)

            # Insert data in batches
            if len(batch_insert) >= batch_size:
                try:
                    logging.info(f"Inserting batch of size {len(batch_insert)}: {batch_insert[:5]} ...")  # Log the first 5 for inspection
                    db.insert_data_batch(insert_query, 'aud_agg_tmapinput', batch_insert)
                    logging.info(f"Successfully inserted batch of size {len(batch_insert)}.")
                except Exception as e:
                    logging.error(f"Error inserting batch: {str(e)}")
                finally:
                    batch_insert.clear()  # Clear after batch insertion

        # Insert remaining data if any
        if batch_insert:
            try:
                logging.info(f"Inserting remaining batch of size {len(batch_insert)}: {batch_insert[:5]} ...")
                db.insert_data_batch(insert_query, 'aud_agg_tmapinput', batch_insert)
                logging.info(f"Successfully inserted remaining batch of size {len(batch_insert)}.")
            except Exception as e:
                logging.error(f"Error inserting remaining batch: {str(e)}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)



    
       















def AUD_405_AGG_TXMLMAP(config: Config, db: Database, execution_date: str, batch_size=100):
    """
    This function:
    - Deletes existing files in a specified directory.
    - Executes two agg_queries (inputtable XML and outputtable XML), writes results to CSVs.
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
        #     write into  aud_inputtable.csv & outputtable.csv  
        # ==============================================================================================
        # Step 1: Clean the directory by deleting existing files
        directory_path = config.get_param('Directories', 'delete_files')
        delete_files_in_directory(directory_path)

        # Step 2: Execute inputtable XML query and write to CSV
        inputtable_query = config.get_param('agg_agg_queries', 'inputtable')
        logging.info(f"Executing query: {inputtable_query}")
        inputtable_results = db.execute_query(inputtable_query)

        input_csv_path = os.path.join(directory_path, "aud_inputtable.csv")
        input_csv_header = [
            "rowName", "NameRowInput", "expressionJoin", "expressionFilterInput", 
            "composant", "innerJoin", "NameProject", "NameJob"
        ]

        with open(input_csv_path, mode='w', newline='', encoding='utf-8') as input_csvfile:
            writer = csv.writer(input_csvfile)
            writer.writerow(input_csv_header)
            writer.writerows(inputtable_results)

        logging.info(f"Input table results written to {input_csv_path}")

        # Step 3: Execute outputtable XML query and write to CSV
        outputtable_query = config.get_param('agg_agg_queries', 'outputtable')
        logging.info(f"Executing query: {outputtable_query}")
        outputtable_results = db.execute_query(outputtable_query)

        output_csv_path = os.path.join(directory_path, "aud_outputtable.csv")
        output_csv_header = ["aud_componentName","aud_OutputName", "aud_sizeState","aud_activateCondensedTool", "aud_reject", "aud_rejectInnerJoin", "aud_expressionOutput", "aud_nameColumnOutput", "aud_type", "aud_nullable", "aud_activateExpressionFilter", "aud_expressionFilterOutput", "aud_componentValue", "NameProject", "NameJob" ]

        with open(output_csv_path, mode='w', newline='', encoding='utf-8') as output_csvfile:
            writer = csv.writer(output_csvfile)
            writer.writerow(output_csv_header)
            writer.writerows(outputtable_results)

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
        #  Join aud_inputtable.csv & outputtable.csv for `aud_agg_txmlmapinputinoutput`
        # ==============================================================================================
        joined_data_output_table = []

        for input_row in input_data:
            for output_row in output_data:
                if (input_row['composant'] == output_row['aud_componentValue'] and
                    input_row['NameJob'] == output_row['NameJob'] and
                    input_row['NameProject'] == output_row['NameProject']):
                    
                    # Prepare the row for insertion into the first table
                    joined_row = (
                        input_row['rowName'],
                        input_row['NameRowInput'],
                        input_row['composant'],
                        output_row['aud_expressionOutput'],  # Assuming 'expression' represents 'expressionOutput'
                        output_row['aud_nameColumnOutput'],  # output_aud_nameColumnInput
                        output_row['aud_OutputName'],
                        output_row['aud_reject'],
                        output_row['aud_rejectInnerJoin'],
                        output_row['NameJob'],
                        output_row['NameProject']
                    )
                    joined_data_output_table.append(joined_row)

        # Insert joined data into `aud_agg_txmlmapinputinoutput`
        insert_query_output_table = config.get_param('insert_agg_agg_queries', 'aud_agg_txmlmapinputinoutput')
        data_batch = []
        for row in joined_data_output_table:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                db.insert_data_batch(insert_query_output_table, 'aud_agg_txmlmapinputinoutput', data_batch)
                data_batch.clear()

        if data_batch:
            db.insert_data_batch(insert_query_output_table, 'aud_agg_txmlmapinputinoutput', data_batch)

        logging.info("Joined data inserted into `aud_agg_txmlmapinputinoutput`.")

        # ==============================================================================================
        #  Join aud_inputtable.csv & unique outputtable.csv for `aud_agg_txmlmapinputinfilteroutput`
        # ==============================================================================================
        filtered_joined_data = []
        unique_rows = set()  # Track uniqueness based on (NameJob, NameProject, composant, aud_componentValue)

        for input_row in input_data:
            for output_row in output_data:
                if (input_row['composant'] == output_row['aud_componentValue'] and
                    input_row['NameJob'] == output_row['NameJob'] and
                    input_row['NameProject'] == output_row['NameProject']):
                    
                    # Check expression filter
                    search_string = input_row['NameRowInput'] + "." + input_row['rowName']
                    if search_string in (output_row['aud_expressionFilterOutput'] or "") and output_row['aud_expressionFilterOutput'] != None :
                        expression_filter_output = output_row['aud_expressionFilterOutput'].replace("\n", " ") if output_row['aud_expressionFilterOutput'] else None
                        
                        # Prepare the row for insertion into the second table
                        row_key = (output_row['NameJob'], output_row['NameProject'], input_row['composant'], output_row['aud_componentValue'])
                        if row_key not in unique_rows:
                            joined_row = (
                                input_row['rowName'],
                                input_row['NameRowInput'],
                                input_row['composant'],
                                expression_filter_output,
                                output_row['aud_nameRowOutput'],
                                output_row['innerJoin'],  # aud_reject
                                output_row['rejectInnerJoin'],  # aud_rejectInnerJoin
                                output_row['NameProject'],
                                output_row['NameJob']
                            )
                            filtered_joined_data.append(joined_row)
                            unique_rows.add(row_key)

        # Insert joined data into `aud_agg_txmlmapinputinfilteroutput`
        insert_query_filter_output_table = config.get_param('insert_agg_agg_queries', 'aud_agg_txmlmapinputinfilteroutput')
        data_batch = []
        for row in filtered_joined_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                db.insert_data_batch(insert_query_filter_output_table, 'aud_agg_txmlmapinputinfilteroutput', data_batch)
                data_batch.clear()

        if data_batch:
            db.insert_data_batch(insert_query_filter_output_table, 'aud_agg_txmlmapinputinfilteroutput', data_batch)

        logging.info("Filtered joined data inserted into `aud_agg_txmlmapinputinfilteroutput`.")

        # ==============================================================================================
        #  Join aud_inputtable.csv & aud_inputtable.csv for `aud_agg_txmlmapinputinjoininput`
        # ==============================================================================================

        # Step 1: Read the input CSV file
        input_data = []
        with open(input_csv_path, mode='r', encoding='utf-8') as input_csvfile:
            reader = csv.DictReader(input_csvfile)
            input_data = list(reader)

        # Step 2: Initialize a list to hold the rows to be inserted
        filtered_data = []

        # Step 3: Apply rules to filter and format the data
        for row in input_data:
            # Apply the rule: !Relational.ISNULL(row8.expressionJoin) && !row8.expressionJoin.isEmpty()
            if row['expressionJoin'] is not None and row['expressionJoin'].strip():
                
                # Apply the rule: routines.Utils.containsElementExpression(row8.expressionJoin, row7.rowName + "." + row7.NameRowInput) == 1
                search_string = row['rowName'] + "." + row['NameRowInput']
                if search_string in row['expressionJoin']:
                    
                    # Prepare the expressionJoin: replace newlines if expressionJoin is not NULL
                    expression_join = row['expressionJoin'].replace("\n", " ") if row['expressionJoin'] else None
                    
                    # Set the is_columnjoined flag based on the rule
                    is_columnjoined = 1 if row['expressionJoin'] is not None and row['expressionJoin'].strip() else 0
                    
                    # Prepare the data for insertion
                    filtered_row = (
                        row['rowName'],  # rowName
                        row['NameRowInput'],  # NameColumnInput
                        expression_join,  # expressionJoin
                        row['composant'],  # composant
                        row['innerJoin'],  # innerJoin
                        row['NameProject'],  # NameProject
                        row['NameJob'],  # NameJob
                        is_columnjoined,  # is_columnjoined
                        row['rowName_join'],  # rowName_join
                        row['NameColumnInput_join']  # NameColumnInput_join
                    )
                    
                    # Add the row to the list
                    filtered_data.append(filtered_row)

        # Step 4: Insert filtered data into 'aud_agg_txmlmapinputinjoininput'
        insert_query = config.get_param('insert_agg_agg_queries', 'aud_agg_txmlmapinputinjoininput')
        data_batch = []
        for row in filtered_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                # Insert data in batches
                db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinjoininput', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_txmlmapinputinjoininput', data_batch)

        logging.info("Data successfully inserted into `aud_agg_txmlmapinputinjoininput` table.")
        # ==============================================================================================
        # insert aud_inputtable.csv in `aud_agg_txmlmapinputinfilterinput`
        # ==============================================================================================
    # Initialize a list to hold the rows to be inserted
        filtered_data = []

        # Step 3: Apply rules to filter and format the data
        for row in input_data:
            # Apply the rule: !Relational.ISNULL(row9.expressionFilterInput) && !row9.expressionFilterInput.isEmpty()
            if row['expressionFilterInput'] is not None and row['expressionFilterInput'].strip():
                
                # Apply the rule: routines.Utils.containsElementExpression(row9.expressionFilterInput, row9.rowName + "." + row9.NameColumnInput) == 1
                search_string = row['rowName'] + "." + row['NameRowInput']
                if search_string in row['expressionFilterInput']:
                    
                    # Prepare the expressionFilterInput: replace newlines if expressionFilterInput is not NULL
                    expression_filter_input = row['expressionFilterInput'].replace("\n", " ") if row['expressionFilterInput'] else None
                    
                    # Prepare the data for insertion
                    filtered_row = (
                        row['rowName'],  # rowName
                        row['NameRowInput'],  # NameColumnInput
                        expression_filter_input,  # expressionFilterInput
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
        # Join aud_inputtable.csv &  aud_vartable for `aud_agg_txmlmapinputinvar`
        # ==============================================================================================
    # Step 1: Execute aud_vartable query
        aud_vartable_query = config.get_param('agg_queries', 'aud_vartable')
        logging.info(f"Executing query: {aud_vartable_query}")
        aud_vartable_results = db.execute_query(aud_vartable_query)

        logging.info(f"aud_vartable_results: {aud_vartable_results}")

        # Step 2: Load aud_outputtable.csv
        directory_path = config.get_param('Directories', 'delete_files')
        output_csv_path = os.path.join(directory_path, "aud_outputtable.csv")

        output_data = []
        with open(output_csv_path, mode='r', encoding='utf-8') as output_csvfile:
            reader = csv.DictReader(output_csvfile)
            output_data = list(reader)
        # Step 4: Join aud_inputtable.csv with aud_vartable based on (NameJob, NameProject, aud_componentValue)
        filtered_joined_data = []

        # Iterate over each row from aud_vartable results
        for var_row in aud_vartable_results:
            for input_row in input_data:
                # Join condition based on NameJob, NameProject, aud_componentValue
                if (var_row[5] == input_row['NameJob'] and            # NameJob match
                    var_row[4] == input_row['NameProject'] and        # NameProject match
                    var_row[2] == input_row['composant']):            # aud_componentValue match

                    # Check if vartable.aud_expressionVar is not null and not empty
                    if var_row[3] is not None and var_row[3].strip() != "":  # index 3 for aud_expressionVar
                        # Create search_string
                        search_string = var_row[1] + "." + var_row[2]  # index 1 for aud_Var and index 2 for aud_nameVar

                        # Check if search_string is in input_row['aud_expressionFilterOutput']
                        if search_string in (input_row['expressionFilterInput'] or ""):
                            # If aud_expressionVar is not None, replace newlines with spaces
                            aud_expressionVar_cleaned = var_row[3].replace("\n", " ") if var_row[3] else None
                            
                            # Prepare the row for insertion into the aud_agg_txmlmapinputinvar table
                            joined_row = (
                                input_row['rowName'],                 # NameRowInput
                                input_row['composant'],               # composant
                                aud_expressionVar_cleaned,            # expressionFilterOutput (aud_expressionVar after newline replacement)
                                input_row['rowName'],                 # OutputName (rowName used as placeholder)
                                input_row['reject'],                  # reject
                                input_row['innerJoin'],               # rejectInnerJoin (assuming innerJoin maps to rejectInnerJoin)
                                input_row['NameProject'],             # NameProject
                                input_row['NameJob']                  # NameJob
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
        # ==============================================================================================
        #  Join aud_vartable &  outputtable.csv for `aud_agg_txmlmapvarinoutput`
        # ==============================================================================================
        filtered_joined_data = []

        # Iterate over each row from aud_vartable_results
        for var_row in aud_vartable_results:
            for output_row in output_data:  # output_data corresponds to outputtable.csv
                # Join condition based on NameJob, NameProject, aud_componentValue
                if (var_row[5] == output_row['NameJob'] and            # NameJob match
                    var_row[4] == output_row['NameProject'] and        # NameProject match
                    var_row[2] == output_row['aud_componentValue']):   # aud_componentValue match

                    # Check if aud_expressionFilterOutput is not null and contains the appropriate string
                    if output_row['aud_expressionFilterOutput'] and output_row['aud_expressionFilterOutput'].strip() != "":
                        search_string = var_row[1] + "." + var_row[2]  # Combine aud_Var + "." + aud_nameVar
                        
                        # Check if search_string is contained in aud_expressionFilterOutput
                        if search_string in output_row['aud_expressionFilterOutput']:
                            # Replace newlines in aud_expressionFilterOutput with spaces
                            expression_filter = output_row['aud_expressionFilterOutput'].replace("\n", " ")
                            
                            # Prepare the row for insertion into aud_agg_txmlmapvarinfilter table
                            joined_row = (
                                search_string,        # NameRowInput
                                var_row[2],       # composant
                                expression_filter,       # expressionFilterOutput
                                output_row['aud_nameRowOutput'],        # OutputName
                                output_row['aud_reject'],               # reject
                                output_row['aud_rejectInnerJoin'],      # rejectInnerJoin
                                var_row[4],  # NameProject
                                var_row[5]                   # NameJob
                            )
                            
                            # Append the joined row to the filtered data list
                            filtered_joined_data.append(joined_row)

        # Step 7: Insert the filtered and joined data into the aud_agg_txmlmapvarinfilter table
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapvarinfilter')
        data_batch = []

        for row in filtered_joined_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                # Insert data in batches
                db.insert_data_batch(insert_query, 'aud_agg_txmlmapvarinfilter', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_txmlmapvarinfilter', data_batch)

        logging.info("Filtered joined data successfully inserted into aud_agg_txmlmapvarinfilter table.")

        # ==============================================================================================
        #  Join aud_vartable &  outputtable.csv for `aud_agg_txmlmapvarinfilter`
        # ==============================================================================================
        # Step 3: Initialize a list to hold the filtered and joined data
        filtered_joined_data = []

        # Step 4: Join with aud_outputtable.csv based on (NameJob, NameProject, aud_componentValue)
        for var_row in aud_vartable_results:
            for output_row in output_data:
                if (var_row[2] == output_row['aud_componentValue'] and  # index 2 for aud_nameVar
                    var_row[5] == output_row['NameJob'] and           # index 5 for NameJob
                    var_row[4] == output_row['NameProject']):         # index 4 for NameProject
                    
                    # Apply the rule: !Relational.ISNULL(row5.aud_expressionFilterOutput)
                    if output_row['aud_expressionFilterOutput'] is not None:
                        
                        # Apply the rule: routines.Utils.containsElementExpression
                        search_string = var_row[1] + "." + var_row[2]  # index 1 for aud_Var and index 2 for aud_nameVar
                        if search_string in output_row['aud_expressionFilterOutput']:
                            
                            # Prepare the row for insertion into the aud_agg_txmlmapvarinfilter table
                            filtered_row = (
                                search_string,  # NameRowInput (index 2)
                                var_row[2],  # composant (aud_nameVar)
                                output_row['aud_expressionFilterOutput'],  # expressionFilterOutput
                                output_row['aud_nameRowOutput'],  # OutputName
                                output_row['aud_reject'],  # reject
                                output_row['aud_rejectInnerJoin'],  # rejectInnerJoin
                                var_row[4],  # NameProject
                                var_row[5]   # NameJob
                            )
                            
                            # Add the row to the list
                            filtered_joined_data.append(filtered_row)

        # Step 5: Insert filtered and joined data into aud_agg_txmlmapvarinfilter
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapvarinfilter')
        data_batch = []
        for row in filtered_joined_data:
            data_batch.append(row)
            if len(data_batch) >= batch_size:
                # Insert data in batches
                db.insert_data_batch(insert_query, 'aud_agg_txmlmapvarinfilter', data_batch)
                data_batch.clear()

        # Insert any remaining data
        if data_batch:
            db.insert_data_batch(insert_query, 'aud_agg_txmlmapvarinfilter', data_batch)

        logging.info("Data successfully inserted into `aud_agg_txmlmapvarinfilter` table.")
# ==========================================================================================================================================================================================================================================================================================
# Catching lookup inner join reject aud_inputtable.csv & aud_agg_txmlmapinputinoutput & aud_agg_txmlmapinputinfilteroutput & aud_agg_txmlmapinputinjoininput & aud_agg_txmlmapinputinfilterinput & aud_agg_txmlmapinputinvar for `aud_agg_txmlmapcolumunused` on rowName, NameColumnInput, composant, NameProject, NameJob
# ==========================================================================================================================================================================================================================================================================================
        # Improved Join Logic with input_df as the main DataFrame
        # Function to load SQL table data
        def load_sql_table(query, column_names):
            """
            Executes a SQL query and returns the result as a pandas DataFrame.
            
            Args:
                query (str): The SQL query to execute.
                column_names (list): List of column names for the DataFrame.
            
            Returns:
                pd.DataFrame: DataFrame containing the query results.
            """
            try:
                # Assume db.execute_query returns a list of tuples; create DataFrame directly
                data = db.execute_query(query)
                df = pd.DataFrame(data, columns=column_names)
                return df
            except Exception as e:
                logging.error(f"Error loading data from query: {query}. Error: {str(e)}")
                return pd.DataFrame(columns=column_names)

        # Function to perform the "catch lookup rejects" logic using left anti join
        def left_anti_join(left_df, right_df, join_columns):
            """
            Perform a left join and return only the rows in left_df that do not have a match in right_df.
            This mimics Talend's tMap 'catch lookup rejects' functionality.
            
            Args:
                left_df (pd.DataFrame): The left DataFrame.
                right_df (pd.DataFrame): The right DataFrame.
                join_columns (list): Columns to join on.
            
            Returns:
                pd.DataFrame: DataFrame containing unmatched rows from the left DataFrame.
            """
            logging.info(f"Performing left anti join on columns: {join_columns}")
            
            # Perform a full join with an indicator to find unmatched rows
            merged_df = pd.merge(left_df, right_df, on=join_columns, how='left', indicator=True)
            
            # Select rows that did not match in the right DataFrame
            unmatched_rows = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
            
            logging.info(f"Found {len(unmatched_rows)} unmatched rows after join.")
            return unmatched_rows

        # SQL Queries to retrieve data from SQL tables
        sql_tables = {
            'aud_agg_txmlmapinputinoutput': "SELECT DISTINCT rowName, NameRowInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinoutput",
            'aud_agg_txmlmapinputinfilteroutput': "SELECT DISTINCT rowName, NameRowInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinfilteroutput",
            'aud_agg_txmlmapinputinjoininput': "SELECT DISTINCT rowName, NameColumnInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinjoininput",
            'aud_agg_txmlmapinputinfilterinput': "SELECT DISTINCT rowName, NameColumnInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinfilterinput",
            'aud_agg_txmlmapinputinvar': "SELECT DISTINCT rowName, NameRowInput, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinvar"
        }

        # Column mappings for each SQL table
        column_mapping = {
            'aud_agg_txmlmapinputinoutput': ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'],
            'aud_agg_txmlxmlmapinputinfilteroutput': ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'],
            'aud_agg_txmlmapinputinjoininput': ['rowName', 'NameColumnInput', 'composant', 'NameProject', 'NameJob'],
            'aud_agg_txmlmapinputinfilterinput': ['rowName', 'NameColumnInput', 'composant', 'NameProject', 'NameJob'],
            'aud_agg_txmlmapinputinvar': ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']
        }

        # Load the input CSV file into a DataFrame (input_df is the main DataFrame)
        input_df = pd.read_csv(input_csv_path, usecols=['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'])

        # Load SQL data into DataFrames using the agg_queries and column mappings
        sql_dataframes = {table: load_sql_table(query, column_mapping[table]) for table, query in sql_tables.items()}

        # Initialize unmatched_df with input_df (this will hold unmatched rows after each join)
        unmatched_df = input_df.copy()

        # Iteratively perform left anti join with each SQL DataFrame
        for table, right_df in sql_dataframes.items():
            logging.info(f"Joining input_df with table: {table}")
            
            # Determine the join columns dynamically based on column mapping
            join_columns = [col for col in column_mapping[table] if col in unmatched_df.columns and col in right_df.columns]
            
            if join_columns:
                # Perform the left anti join to capture unmatched rows
                unmatched_df = left_anti_join(unmatched_df, right_df, join_columns)
            else:
                logging.warning(f"No common columns to join for table {table}")

        # After all joins, the remaining unmatched rows are stored in `unmatched_df`
        unused_data = unmatched_df

        # Insert unmatched rows into the database if there are any
        if not unused_data.empty:
            logging.info(f"Inserting {len(unused_data)} unused rows into the database.")
            
            insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapcolumunused')
            
            # Select only the relevant columns before insertion
            relevant_columns = ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']  # Adjust this list if necessary
            
            # Prepare the data for insertion
            insert_data = [tuple(row[col] for col in relevant_columns) for row in unused_data.to_dict(orient='records') if all(row[col] is not None for col in relevant_columns)]
            
            # Log the prepared insert data
            logging.info(f"Prepared data for insertion: {insert_data[:10]}")  # Log the first 10 rows for inspection
            
            # Insert data in batches 
            batch_insert = []

            batch_size = 1000
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                try:
                    db.insert_data_batch(insert_query, 'aud_agg_txmlmapcolumunused', batch)
                    logging.info(f"Inserted batch {i // batch_size + 1} of size {len(batch)}.")
                except Exception as e:
                    logging.error(f"Error inserting batch: {str(e)}")
        else:
            logging.info("No unused data to insert.")

        # Step 2: Execute agg_aud_inputtable query and write to CSV
        agg_aud_inputtable_query = config.get_param('agg_queries', 'agg_aud_inputtable')
        logging.info(f"Executing query: {agg_aud_inputtable_query}")
        agg_aud_inputtable_results = db.execute_query(agg_aud_inputtable_query)

        # Prepare and insert batch results from agg_aud_inputtable query
        batch_insert = []
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmltmapinput')

        # Log the total number of results to be processed
        logging.info(f"Processing {len(agg_aud_inputtable_results)} rows from agg_aud_inputtable query.")

        # Process results in batches
        for result in agg_aud_inputtable_results:
            batch_insert.append(result)

            # Insert data in batches
            if len(batch_insert) >= batch_size:
                try:
                    logging.info(f"Inserting batch of size {len(batch_insert)}: {batch_insert[:5]} ...")  # Log the first 5 for inspection
                    db.insert_data_batch(insert_query, 'aud_agg_txmlxmltmapinput', batch_insert)
                    logging.info(f"Successfully inserted batch of size {len(batch_insert)}.")
                except Exception as e:
                    logging.error(f"Error inserting batch: {str(e)}")
                finally:
                    batch_insert.clear()  # Clear after batch insertion

        # Insert remaining data if any
        if batch_insert:
            try:
                logging.info(f"Inserting remaining batch of size {len(batch_insert)}: {batch_insert[:5]} ...")
                db.insert_data_batch(insert_query, 'aud_agg_txmlmapinput', batch_insert)
                logging.info(f"Successfully inserted remaining batch of size {len(batch_insert)}.")
            except Exception as e:
                logging.error(f"Error inserting remaining batch: {str(e)}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)

