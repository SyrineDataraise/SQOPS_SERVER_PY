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
    - Executes two agg_queries (inputtable  and outputtable ), writes results to CSVs.
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

        # Step 2: Execute inputtable  query and write to CSV
        aud_inputtable_query = config.get_param('agg_queries', 'aud_inputtable')
        logging.info(f"Executing query: {aud_inputtable_query}")
        aud_inputtable_results = db.execute_query(aud_inputtable_query)

        input_csv_path = os.path.join(directory_path, "aud_inputtable.csv")
        input_csv_header = [
            "rowName", "nameColumnInput", "expressionJoin", "expressionFilterInput", 
            "composant", "innerJoin", "NameProject", "NameJob"
        ]

        with open(input_csv_path, mode='w', newline='', encoding='utf-8') as input_csvfile:
            writer = csv.writer(input_csvfile)
            writer.writerow(input_csv_header)
            writer.writerows(aud_inputtable_results)

        logging.info(f"Input table results written to {input_csv_path}")

        # Step 3: Execute outputtable  query and write to CSV
        aud_outputtable_query = config.get_param('agg_queries', 'aud_outputtable')
        logging.info(f"Executing query: {aud_outputtable_query}")
        outputtable_results = db.execute_query(aud_outputtable_query)

        output_csv_path = os.path.join(directory_path, "aud_outputtable.csv")
        output_csv_header = ["aud_componentName","aud_OutputName", "aud_sizeState","aud_activateCondensedTool", "aud_reject", 
                             "aud_rejectInnerJoin", "aud_expressionOutput", "aud_nameColumnOutput", "aud_type", "aud_nullable",
                             "aud_activateExpressionFilter", "aud_expressionFilterOutput", "aud_componentValue", "NameProject", "NameJob" ]

        with open(output_csv_path, mode='w', newline='', encoding='utf-8') as output_csvfile:
            writer = csv.writer(output_csvfile)
            writer.writerow(output_csv_header)
            writer.writerows(outputtable_results)

        logging.info(f"Output table results written to {output_csv_path}")
        # ==============================================================================================
        #  Join aud_aud_inputtable.csv & unique outputtable.csv for `aud_agg_tmapinputinoutput`
        # ==============================================================================================
        logging.info("Reading input and output CSV files...")

        # Read CSV files
        input_df = pd.read_csv(input_csv_path, encoding='utf-8')
        output_df = pd.read_csv(output_csv_path, encoding='utf-8')
        logging.info(len(output_df))

        logging.info(f"Input DataFrame columns: {input_df.columns}")
        logging.info(f"Output DataFrame columns: {output_df.columns}")

        logging.info("Successfully read CSV files. Performing inner join...")

        # Perform inner join
        joined_df = pd.merge(
            input_df,
            output_df,
            left_on=['composant', 'NameJob', 'NameProject'],
            right_on=['aud_componentValue', 'NameJob', 'NameProject'],
            how='inner'
        )
        logging.info(f"Joined DataFrame columns: {joined_df.columns}")
        logging.info(f"Inner join resulted in {len(joined_df)} rows.")

        # Apply the condition and filter the rows before mapping
        filtered_df = joined_df[
            joined_df['aud_expressionOutput'].notna() &  # Ensure aud_expressionOutput is not NaN
            joined_df.apply(
                lambda row: isinstance(row['aud_expressionOutput'], str) and  # Check if it's a string
                            f"{row['rowName']}.{row['nameColumnInput']}" in row['aud_expressionOutput'], axis=1  # Check if expression contains the specific rowName.NameColumnInput
            )
        ]

        #Log the filtered rows
        logging.info(f"Filtered DataFrame has {len(filtered_df)} rows.")
        if len(filtered_df) > 0:
            logging.info(f"Sample of filtered rows:\n{filtered_df.head()}")

        # Map each column to its source for insertion into 'aud_agg_tmapinputinoutput' table
        mapped_df = pd.DataFrame({
            'rowName': filtered_df['rowName'],  # From input_df
            'NameRowInput': filtered_df['nameColumnInput'],  # From input_df
            'composant': filtered_df['composant'],  # From input_df
            'expressionOutput': filtered_df['aud_expressionOutput'].apply(lambda x: x.replace("\n", " ") if pd.notna(x) else x),  # From output_df
            'nameColumnOutput': filtered_df['aud_nameColumnOutput'],  # From output_df
            'OutputName': filtered_df['aud_OutputName'],  # From output_df
            'reject': filtered_df['aud_reject'],  # From output_df
            'rejectInnerJoin': filtered_df['aud_rejectInnerJoin'].map(
                lambda x: 1 if x == 'True' else 0 if x == 'False' else None
            ),  # Mapping to bit(1)
            'NameProject': filtered_df['NameProject'],  # Common column
            'NameJob': filtered_df['NameJob']  # Common column
        })

        logging.info(f"Mapped DataFrame has {len(mapped_df)} rows.")

        # Drop rows with NaN in critical columns
        critical_columns = [
            'rowName', 'NameRowInput', 'composant', 'expressionOutput',
            'nameColumnOutput', 'OutputName', 'reject',
            'rejectInnerJoin', 'NameProject', 'NameJob'
        ]
        # Fill NaN values in critical columns with None (NULL in MySQL)
        for column in critical_columns:
            mapped_df[column] = mapped_df[column].where(pd.notna(mapped_df[column]), None)
        logging.info(f"Rows after removing NaN values: {len(mapped_df)}.")

        # Prepare rows for database insertion
        data_for_insertion = mapped_df.values.tolist()
        logging.info(f"Prepared {len(data_for_insertion)} rows for database insertion.")

        # Insert rows into the database in batches
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinoutput')
        data_batch = []


        for row in data_for_insertion:
            data_batch.append(row)
            # logging.debug(f"Adding row to batch: {row}")
            if len(data_batch) == batch_size:
                try:
                    db.insert_data_batch(insert_query, 'aud_agg_tmapinputinoutput', data_batch)
                    logging.info(f"Inserted batch of {batch_size} rows.")
                except Exception as e:
                    logging.warning(f"Error inserting batch: {str(e)}")
                data_batch.clear()

   

        # Insert remaining rows
        if data_batch:
            try:
                db.insert_data_batch(insert_query, 'aud_agg_tmapinputinoutput', data_batch)
                logging.info(f"Inserted final batch of {len(data_batch)} rows.")
            except Exception as e:
                logging.warning(f"Error inserting remaining batch: {str(e)}")

        logging.info("Data successfully inserted into `aud_agg_tmapinputinoutput` table.")


        # ==============================================================================================
        #  Join aud_aud_inputtable.csv & unique outputtable.csv for `aud_agg_tmapinputinfilteroutput`
        # ==============================================================================================
        # Ensure unique rows in output_df based on the combination of columns
        # Drop duplicate rows based on the specified columns, keeping only the first occurrence
        unique_output_df = output_df.drop_duplicates(subset=['aud_OutputName', 'aud_componentValue', 'NameProject', 'NameJob'], keep='first')

        # Print or inspect the unique DataFrame
        logging.info("Unique rows in output DataFrame:")
        logging.info(len(unique_output_df))
        logging.info(unique_output_df.head(200))
        # Perform inner join
        joined_df = pd.merge(
            input_df,
            unique_output_df,
            left_on=['composant', 'NameJob', 'NameProject'],
            right_on=['aud_componentValue', 'NameJob', 'NameProject'],
            how='inner'
        )
        logging.info(f"Joined DataFrame columns: {joined_df.columns}")
        logging.info(f"Inner join resulted in {len(joined_df)} rows.")

        # Apply the condition and filter the rows before mapping
        filtered_df = joined_df[
            joined_df['aud_expressionFilterOutput'].notna() &  # Ensure aud_expressionFilterOutput is not NaN
            joined_df.apply(
                lambda row: isinstance(row['aud_expressionFilterOutput'], str) and  # Check if it's a string
                            f"{row['rowName']}.{row['nameColumnInput']}" in row['aud_expressionFilterOutput'], axis=1  # Check if expression contains the specific rowName.NameColumnInput
            )
        ]

        # Log the filtered rows
        logging.info(f"Filtered DataFrame has {len(filtered_df)} rows.")
        if len(filtered_df) > 0:
            logging.info(f"Sample of filtered rows:\n{filtered_df.head(5)}")

        # Map each column to its source for insertion into 'aud_agg_tmapinputinoutput' table
        mapped_df = pd.DataFrame({
            'rowName': filtered_df['rowName'],  # From input_df
            'NameRowInput': filtered_df['nameColumnInput'],  # From input_df
            'composant': filtered_df['composant'],  # From input_df
            'expressionFilterOutput': filtered_df['aud_expressionFilterOutput'].apply(lambda x: x.replace("\n", " ") if pd.notna(x) else x),  # From output_df
            'OutputName': filtered_df['aud_OutputName'],  # From output_df
            'reject': filtered_df['aud_reject'],  # From output_df
            'rejectInnerJoin': filtered_df['aud_rejectInnerJoin'],  # Mapping to bit(1)
            'NameProject': filtered_df['NameProject'],  # Common column
            'NameJob': filtered_df['NameJob']  # Common column
        })

        logging.info(f"Mapped DataFrame has {len(mapped_df)} rows.")

        # Drop rows with NaN in critical columns
        critical_columns = [
            'rowName', 'NameRowInput', 'composant', 'expressionFilterOutput',
             'OutputName', 'reject',
            'rejectInnerJoin', 'NameProject', 'NameJob'
        ]
        # Fill NaN values in critical columns with None (NULL in MySQL)
        for column in critical_columns:
            mapped_df[column] = mapped_df[column].where(pd.notna(mapped_df[column]), None)
        logging.info(f"Rows after removing NaN values: {len(mapped_df)}.")

        # Prepare rows for database insertion
        data_for_insertion = mapped_df.values.tolist()
        logging.info(f"Prepared {len(data_for_insertion)} rows for database insertion.")

        # Insert rows into the database in batches
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinfilteroutput')
        data_batch = []


        for row in data_for_insertion:
            data_batch.append(row)
            # logging.debug(f"Adding row to batch: {row}")
            if len(data_batch) == batch_size:
                try:
                    db.insert_data_batch(insert_query, 'aud_agg_tmapinputinfilteroutput', data_batch)
                    logging.info(f"Inserted batch of {batch_size} rows.")
                except Exception as e:
                    logging.warning(f"Error inserting batch: {str(e)}")
                data_batch.clear()

   

        # Insert remaining rows
        if data_batch:
            try:
                db.insert_data_batch(insert_query, 'aud_agg_tmapinputinfilteroutput', data_batch)
                logging.info(f"Inserted final batch of {len(data_batch)} rows.")
            except Exception as e:
                logging.warning(f"Error inserting remaining batch: {str(e)}")

        logging.info("Data successfully inserted into `aud_agg_tmapinputinfilteroutput` table.")




#         # ==============================================================================================
#         #  Join aud_aud_inputtable.csv & aud_aud_inputtable.csv for `aud_agg_tmapinputinjoininput`
#         # ==============================================================================================
        # Filter the input DataFrame for valid rows where expressionJoin is not NaN or empty
        filtered_input_df = input_df[
        input_df['expressionJoin'].notna() &  # Ensure expressionJoin is not NaN
        input_df['expressionJoin'].apply(lambda x: isinstance(x, str) and len(x.strip()) != 0)  # Ensure expressionJoin is not empty
    ]


        logging.info(f"Filtered input DataFrame has {len(filtered_input_df)} rows before merging.")


         # Merge the input DataFrames
        joined_df = pd.merge(
            input_df,
            filtered_input_df,
            left_on=['composant', 'NameJob', 'NameProject'],
            right_on=['composant', 'NameJob', 'NameProject'],
            how='inner'
        )
        logging.info(f"Joined DataFrame columns: {joined_df.columns}")
        logging.info(f"Inner join resulted in {len(joined_df)} rows.")

        # Apply the condition and filter the rows before mapping
        filtered_df = joined_df[
            joined_df['expressionJoin_x'].notna() &  # Ensure expressionJoin_x is not NaN
            joined_df.apply(
                lambda row: isinstance(row['expressionJoin_x'], str) and len(row['expressionJoin_x']) != 0 , axis=1  # Corrected check
            )
        ]

        # Log the filtered rows
        logging.info(f"Filtered DataFrame has {len(filtered_df)} rows.")
        if len(filtered_df) > 0:
            logging.info(f"Sample of filtered rows:\n{filtered_df.head()}")

        # Map each column to its source for insertion into 'aud_agg_tmapinputinoutput' table
        mapped_df = pd.DataFrame({
            'rowName': filtered_df['rowName_x'],  # From input_df
            'NameColumnInput': filtered_df['nameColumnInput_x'],  # From input_df
            'expressionJoin': filtered_df['expressionJoin_x'].apply(lambda x: x.replace("\n", " ") if pd.notna(x) else x),  # From output_df
            'composant': filtered_df['composant'],  # From input_df
            'InnerJoin': filtered_df['innerJoin_x'],  # From output_df
            'NameProject': filtered_df['NameProject'],  # Common column
            'NameJob': filtered_df['NameJob'],  # Common column
            'is_columnjoined': filtered_df.apply(
                lambda row: 1 if pd.notna(row['expressionJoin_y']) and f"{row['rowName_x']}.{row['nameColumnInput_x']}" in row['expressionJoin_y'] else 0, axis=1
            ),  # Conditional check for column join based on expressionJoin_x
            'rowName_join': filtered_df['rowName_y'], 
            'NameColumnInput_join': filtered_df['nameColumnInput_y'],  
        })

        logging.info(f"Mapped DataFrame has {len(mapped_df)} rows.")

        # Drop rows with NaN in critical columns
        critical_columns = [
            'rowName', 'NameColumnInput', 'expressionJoin', 'composant',
            'InnerJoin', 'NameProject', 'NameJob',
            'is_columnjoined', 'rowName_join', 'NameColumnInput_join'
        ]

        # Fill NaN values in critical columns with None (NULL in MySQL)
        for column in critical_columns:
            mapped_df[column] = mapped_df[column].where(pd.notna(mapped_df[column]), None)
        logging.info(f"Rows after removing NaN values: {len(mapped_df)}.")

        # Prepare rows for database insertion
        data_for_insertion = mapped_df.values.tolist()
        logging.info(f"Prepared {len(data_for_insertion)} rows for database insertion.")

        # Insert rows into the database in batches
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinjoininput')
        data_batch = []

        for row in data_for_insertion:
            data_batch.append(row)
            # logging.debug(f"Adding row to batch: {row}")
            if len(data_batch) == batch_size:
                try:
                    db.insert_data_batch(insert_query, 'aud_agg_tmapinputinjoininput', data_batch)
                    logging.info(f"Inserted batch of {len(data_batch)} rows.")
                except Exception as e:
                    logging.warning(f"Error inserting batch: {str(e)}")
                data_batch.clear()

        # Insert remaining rows
        if data_batch:
            try:
                db.insert_data_batch(insert_query, 'aud_agg_tmapinputinjoininput', data_batch)
                logging.info(f"Inserted final batch of {len(data_batch)} rows.")
            except Exception as e:
                logging.warning(f"Error inserting remaining batch: {str(e)}")
        logging.info("Data successfully inserted into `aud_agg_tmapinputinjoininput` table.")

#         # ==============================================================================================
#         # insert aud_aud_inputtable.csv in `aud_agg_tmapinputinfilterinput`
#         # ==============================================================================================
        # Filter the input DataFrame for valid rows where expressionFilterInput is not NaN or empty
        filtered_input_df = input_df[
            input_df['expressionFilterInput'].notna() &  # Ensure expressionFilterInput is not NaN
            input_df.apply(
                lambda row: f"{row['rowName']}.{row['nameColumnInput']}" in str(row['expressionFilterInput']), axis=1
            )  # Ensure expressionFilterInput contains the required pattern
        ]

        logging.info(f"Filtered input DataFrame has {len(filtered_input_df)} rows before merging.")

        # Apply the condition and filter rows for mapping
        filtered_df = filtered_input_df[
            filtered_input_df['expressionFilterInput'].notna() &  # Ensure expressionFilterInput is not NaN
            filtered_input_df['expressionFilterInput'].apply(
                lambda x: isinstance(x, str) and len(x.strip()) > 0  # Ensure expressionFilterInput is not empty
            )
        ]

        logging.info(f"Filtered DataFrame has {len(filtered_df)} rows.")
        if len(filtered_df) > 0:
            logging.info(f"Sample of filtered rows:\n{filtered_df.head()}")

        # Map each column to its source for insertion into the target table
        mapped_df = pd.DataFrame({
            'rowName': filtered_df['rowName'],  # From input_df
            'NameColumnInput': filtered_df['nameColumnInput'],  # From input_df
            'expressionFilterInput': filtered_df['expressionFilterInput'].apply(
                lambda x: x.replace("\n", " ") if pd.notna(x) else x
            ),  # Clean up line breaks
            'composant': filtered_df['composant'],  # From input_df
            'NameProject': filtered_df['NameProject'],  # Common column
            'NameJob': filtered_df['NameJob'],  # Common column
        })

        logging.info(f"Mapped DataFrame has {len(mapped_df)} rows.")

        # Drop rows with NaN in critical columns
        critical_columns = [
            'rowName', 'NameColumnInput', 'expressionFilterInput', 'composant',
            'NameProject', 'NameJob'
        ]

        for column in critical_columns:
            mapped_df[column] = mapped_df[column].where(pd.notna(mapped_df[column]), None)

        # Prepare rows for database insertion
        data_for_insertion = mapped_df.values.tolist()
        logging.info(f"Prepared {len(data_for_insertion)} rows for database insertion.")

        # Insert rows into the database in batches
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinfilterinput')
        data_batch = []

        for row in data_for_insertion:
            data_batch.append(row)
            # logging.debug(f"Adding row to batch: {row}")
            if len(data_batch) == batch_size:
                try:
                    db.insert_data_batch(insert_query, 'aud_agg_tmapinputinfilterinput', data_batch)
                    logging.info(f"Inserted batch of {len(data_batch)} rows.")
                except Exception as e:
                    logging.warning(f"Error inserting batch: {str(e)}")
                data_batch.clear()

        # Insert remaining rows
        if data_batch:
            try:
                db.insert_data_batch(insert_query, 'aud_agg_tmapinputinfilterinput', data_batch)
                logging.info(f"Inserted final batch of {len(data_batch)} rows.")
            except Exception as e:
                logging.warning(f"Error inserting remaining batch: {str(e)}")

        logging.info("Data successfully inserted into `aud_agg_tmapinputinfilterinput` table.")
  # ==============================================================================================
#         # Join aud_aud_inputtable.csv &  aud_vartable for aud_agg_tmapinputinvar
#         # ==============================================================================================
        # Step 1: Execute aud_vartable query to retrieve data
        aud_vartable_query = config.get_param('agg_queries', 'aud_vartable')
        logging.info(f"Executing query: {aud_vartable_query}")
        aud_vartable_results = db.execute_query(aud_vartable_query)

        # Convert the query results into a DataFrame
        vartable_df = pd.DataFrame(aud_vartable_results, columns=[
            'aud_componentValue', 'aud_Var', 'aud_nameVar', 'aud_expressionVar', 'NameProject', 'NameJob'
        ])
        logging.info(f"Retrieved {len(vartable_df)} rows from aud_vartable.")
        logging.debug(f"Sample from aud_vartable DataFrame:\n{vartable_df.head()}")

        

        # Step 3: Perform an inner join between input_df and vartable_df
        joined_df = pd.merge(
            input_df,
            vartable_df,
            left_on=['composant', 'NameJob', 'NameProject'],
            right_on=['aud_componentValue', 'NameJob', 'NameProject'],
            how='inner'
        )
        logging.info(f"Joined DataFrame has {len(joined_df)} rows.")
        logging.debug(f"Sample of joined DataFrame:\n{joined_df.head()}")

        # Step 4: Apply additional filtering on the joined DataFrame
        filtered_df = joined_df[
            joined_df['aud_expressionVar'].notna() &
            joined_df.apply(
                lambda row: f"{row['rowName']}.{row['nameColumnInput']}" in str(row['aud_expressionVar']),
                axis=1
            )
        ]
        logging.info(f"Filtered DataFrame has {len(filtered_df)} rows.")
        if not filtered_df.empty:
            logging.debug(f"Sample of filtered rows:\n{filtered_df.head()}")

        # Map each column for insertion into the 'aud_agg_tmapinputinvar' table
        mapped_df = pd.DataFrame({
            'rowName': filtered_df['rowName'],
            'NameColumnInput': filtered_df['nameColumnInput'],
            'composant': filtered_df['composant'],
            'expressionOutput': filtered_df['aud_expressionVar'].apply(lambda x: x.replace("\n", " ") if pd.notna(x) else x),
            'nameColumnOutput': filtered_df.apply(lambda row: f"{row['aud_Var']}.{row['aud_nameVar']}", axis=1),
            'NameProject': filtered_df['NameProject'],
            'NameJob': filtered_df['NameJob']
        })
        logging.info(f"Mapped DataFrame has {len(mapped_df)} rows.")

        # Handle NaN values in critical columns
        critical_columns = ['rowName', 'NameColumnInput', 'composant', 'expressionOutput', 'nameColumnOutput', 'NameProject', 'NameJob']
        for column in critical_columns:
            mapped_df[column] = mapped_df[column].where(pd.notna(mapped_df[column]), None)
        logging.info(f"Rows after handling NaN values: {len(mapped_df)}.")

        # Prepare rows for database insertion
        data_for_insertion = mapped_df.values.tolist()
        logging.info(f"Prepared {len(data_for_insertion)} rows for database insertion.")

        # Batch insertion into the database
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinputinvar')
        data_batch = []
        for row in data_for_insertion:
            data_batch.append(row)
            # logging.debug(f"Adding row to batch: {row}")
            if len(data_batch) == batch_size:
                try:
                    db.insert_data_batch(insert_query, 'aud_agg_tmapinputinvar', data_batch)
                    logging.info(f"Inserted batch of {len(data_batch)} rows.")
                except Exception as e:
                    logging.warning(f"Error inserting batch: {str(e)}")
                data_batch.clear()

        # Insert remaining rows
        if data_batch:
            try:
                db.insert_data_batch(insert_query, 'aud_agg_tmapinputinvar', data_batch)
                logging.info(f"Inserted final batch of {len(data_batch)} rows.")
            except Exception as e:
                logging.warning(f"Error inserting remaining batch: {str(e)}")

        logging.info("Filtered and joined data successfully inserted into the 'aud_agg_tmapinputinvar' table.")

#         # ==============================================================================================
#         #  Join aud_vartable &  outputtable.csv for `aud_agg_tmapvarinoutput`
#         # ==============================================================================================
        # Step 3: Perform an inner join between output_df and vartable_df
        joined_df = pd.merge(
            output_df,
            vartable_df,
            left_on=['aud_componentValue', 'NameJob', 'NameProject'],
            right_on=['aud_componentValue', 'NameJob', 'NameProject'],
            how='inner'
        )
        logging.info(f"Joined DataFrame has {len(joined_df)} rows.")
        logging.debug(f"Sample of joined DataFrame:\n{joined_df.head()}")

        # Step 4: Apply additional filtering on the joined DataFrame
        filtered_df = joined_df[
            joined_df['aud_expressionOutput'].notna() & 
            joined_df.apply(
                lambda row: f"{row['aud_Var']}.{row['aud_nameVar']}" in str(row['aud_expressionOutput']),
                axis=1
            )
        ]
        logging.info(f"Filtered DataFrame has {len(filtered_df)} rows.")
        if not filtered_df.empty:
            logging.debug(f"Sample of filtered rows:\n{filtered_df.head()}")

        # Map each column for insertion into the 'aud_agg_tmapvarinoutput' table
        mapped_df = pd.DataFrame({
            'NameRowInput': filtered_df.apply(lambda row: f"{row['aud_Var']}.{row['aud_nameVar']}", axis=1),
            'composant': filtered_df['aud_componentValue'],
            'expressionOutput': filtered_df['aud_expressionOutput'],
            'nameColumnOutput': filtered_df['aud_nameColumnOutput'],
            'OutputName': filtered_df['aud_OutputName'],
            'reject': filtered_df['aud_reject'],
            'rejectInnerJoin': filtered_df['aud_rejectInnerJoin'],
            'NameProject': filtered_df['NameProject'],
            'NameJob': filtered_df['NameJob']
        })
        logging.info(f"Mapped DataFrame has {len(mapped_df)} rows.")

        # Handle NaN values in critical columns
        critical_columns = ['NameRowInput','composant', 'expressionOutput',  'nameColumnOutput','OutputName', 'reject', 'rejectInnerJoin', 'NameProject', 'NameJob']
        for column in critical_columns:
            mapped_df[column] = mapped_df[column].where(pd.notna(mapped_df[column]), None)
        logging.info(f"Rows after handling NaN values: {len(mapped_df)}.")

        # Prepare rows for database insertion
        data_for_insertion = mapped_df.values.tolist()
        logging.info(f"Prepared {len(data_for_insertion)} rows for database insertion.")

        # Batch insertion into the database
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapvarinoutput')
        data_batch = []

        for row in data_for_insertion:
            data_batch.append(row)
            # logging.debug(f"Adding row to batch: {row}")
            if len(data_batch) == batch_size:
                try:
                    db.insert_data_batch(insert_query, 'aud_agg_tmapvarinoutput', data_batch)
                    logging.info(f"Inserted batch of {len(data_batch)} rows.")
                except Exception as e:
                    logging.warning(f"Error inserting batch: {str(e)}")
                data_batch.clear()

        # Insert remaining rows
        if data_batch:
            try:
                db.insert_data_batch(insert_query, 'aud_agg_tmapvarinoutput', data_batch)
                logging.info(f"Inserted final batch of {len(data_batch)} rows.")
            except Exception as e:
                logging.warning(f"Error inserting remaining batch: {str(e)}")

        logging.info("Filtered joined data successfully inserted into aud_agg_tmapvarinoutput table.")


#         # ==============================================================================================
#         #  Join aud_vartable &  outputtable.csv for `aud_agg_tmapvarinfilter`
#         # ==============================================================================================
#      # Step 4: Apply additional filtering on the joined DataFrame
        filtered_df = joined_df[
            joined_df['aud_expressionFilterOutput'].notna() & 
            joined_df.apply(
                lambda row: f"{row['aud_Var']}.{row['aud_nameVar']}" in str(row['aud_expressionFilterOutput']),
                axis=1
            )
        ]
        logging.info(f"Filtered DataFrame has {len(filtered_df)} rows.")
        if not filtered_df.empty:
            logging.debug(f"Sample of filtered rows:\n{filtered_df.head()}")

        # Map each column for insertion into the 'aud_agg_tmapvarinoutput' table
        mapped_df = pd.DataFrame({
            'NameRowInput': filtered_df.apply(lambda row: f"{row['aud_Var']}.{row['aud_nameVar']}", axis=1),
            'composant': filtered_df['aud_componentValue'],
            'expressionFilterOutput': filtered_df['aud_expressionFilterOutput'],
            'OutputName': filtered_df['aud_OutputName'],
            'reject': filtered_df['aud_reject'],
            'rejectInnerJoin': filtered_df['aud_rejectInnerJoin'],
            'NameProject': filtered_df['NameProject'],
            'NameJob': filtered_df['NameJob']
        })
        logging.info(f"Mapped DataFrame has {len(mapped_df)} rows.")

        # Handle NaN values in critical columns
        critical_columns = ['NameRowInput','composant', 'expressionFilterOutput', 'OutputName',  'reject', 'rejectInnerJoin', 'NameProject', 'NameJob']
        for column in critical_columns:
            mapped_df[column] = mapped_df[column].where(pd.notna(mapped_df[column]), None)
        logging.info(f"Rows after handling NaN values: {len(mapped_df)}.")

        # Prepare rows for database insertion
        data_for_insertion = mapped_df.values.tolist()
        logging.info(f"Prepared {len(data_for_insertion)} rows for database insertion.")

        # Batch insertion into the database
        insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapvarinfilter')
        data_batch = []

        for row in data_for_insertion:
            data_batch.append(row)
            # logging.debug(f"Adding row to batch: {row}")
            if len(data_batch) == batch_size:
                try:
                    db.insert_data_batch(insert_query, 'aud_agg_tmapvarinfilter', data_batch)
                    logging.info(f"Inserted batch of {len(data_batch)} rows.")
                except Exception as e:
                    logging.warning(f"Error inserting batch: {str(e)}")
                data_batch.clear()

        # Insert remaining rows
        if data_batch:
            try:
                db.insert_data_batch(insert_query, 'aud_agg_tmapvarinfilter', data_batch)
                logging.info(f"Inserted final batch of {len(data_batch)} rows.")
            except Exception as e:
                logging.warning(f"Error inserting remaining batch: {str(e)}")


        logging.info("Data successfully inserted into `aud_agg_tmapvarinfilter` table.")
# =========================================================================================================================
# Description:
# This script processes several tables and queries for detecting lookup inner join rejects and aggregations.
# Each query retrieves specific data, which is then converted into DataFrames for further processing.
#
# Tables and Queries:
# 1. **aud_aud_inputtable.csv**: Input table for the main process.
# 2. **aud_agg_tmapinputinoutput**: Query to extract mappings between input and output tables, focusing on column expressions.
# 3. **aud_agg_tmapinputinfilteroutput**: Query to extract filter expressions applied on input-to-output mappings.
# 4. **aud_agg_tmapinputinjoininput**: Query to capture join conditions and relationships between input tables.
# 5. **aud_agg_tmapinputinfilterinput**: Query for filtering input columns.
# 6. **aud_agg_tmapinputinvar**: Query to extract variable expressions and mappings from the aggregation.
#
# Key Columns:
# - `rowName`: Row name identifier.
# - `NameColumnInput`: Name of the input column.
# - `composant`: Component associated with the mapping or join.
# - `NameProject`: Name of the project.
# - `NameJob`: Name of the job.
# =========================================================================================================================
    
        # Step 1: Execute aud_agg_tmapinputinoutput query to retrieve data
        aud_agg_tmapinputinoutput_query = config.get_param('agg_queries', 'aud_agg_tmapinputinoutput')
        logging.info(f"Executing query: {aud_agg_tmapinputinoutput_query}")
        aud_agg_tmapinputinoutput_results = db.execute_query(aud_agg_tmapinputinoutput_query)

        # Convert the query results into a DataFrame
        aud_agg_tmapinputinoutput_df = pd.DataFrame(
            aud_agg_tmapinputinoutput_results,
            columns=['rowName', 'NameRowInput', 'composant', 'NameJob','NameProject']
        )
        logging.info(f"Retrieved {len(aud_agg_tmapinputinoutput_df)} rows from aud_agg_tmapinputinoutput.")
        logging.debug(f"Sample from aud_agg_tmapinputinoutput DataFrame:\n{aud_agg_tmapinputinoutput_df.head()}")

        # Step 2: Execute aud_agg_tmapinputinfilteroutput query to retrieve data
        aud_agg_tmapinputinfilteroutput_query = config.get_param('agg_queries', 'aud_agg_tmapinputinfilteroutput')
        logging.info(f"Executing query: {aud_agg_tmapinputinfilteroutput_query}")
        aud_agg_tmapinputinfilteroutput_results = db.execute_query(aud_agg_tmapinputinfilteroutput_query)

        # Convert the query results into a DataFrame
        aud_agg_tmapinputinfilteroutput_df = pd.DataFrame(
            aud_agg_tmapinputinfilteroutput_results,
            columns=['rowName', 'NameRowInput', 'composant', 'NameJob','NameProject']
        )
        logging.info(f"Retrieved {len(aud_agg_tmapinputinfilteroutput_df)} rows from aud_agg_tmapinputinfilteroutput.")
        logging.debug(f"Sample from aud_agg_tmapinputinfilteroutput DataFrame:\n{aud_agg_tmapinputinfilteroutput_df.head()}")

        # Step 3: Execute aud_agg_tmapinputinjoininput query to retrieve data
        aud_agg_tmapinputinjoininput_query = config.get_param('agg_queries', 'aud_agg_tmapinputinjoininput')
        logging.info(f"Executing query: {aud_agg_tmapinputinjoininput_query}")
        aud_agg_tmapinputinjoininput_results = db.execute_query(aud_agg_tmapinputinjoininput_query)

        # Convert the query results into a DataFrame
        aud_agg_tmapinputinjoininput_df = pd.DataFrame(
            aud_agg_tmapinputinjoininput_results,
            columns=['rowName', 'NameRowInput', 'composant', 'NameJob','NameProject']
        )
        logging.info(f"Retrieved {len(aud_agg_tmapinputinjoininput_df)} rows from aud_agg_tmapinputinjoininput.")
        logging.debug(f"Sample from aud_agg_tmapinputinjoininput DataFrame:\n{aud_agg_tmapinputinjoininput_df.head()}")

        # Step 4: Execute aud_agg_tmapinputinfilterinput query to retrieve data
        aud_agg_tmapinputinfilterinput_query = config.get_param('agg_queries', 'aud_agg_tmapinputinfilterinput')
        logging.info(f"Executing query: {aud_agg_tmapinputinfilterinput_query}")
        aud_agg_tmapinputinfilterinput_results = db.execute_query(aud_agg_tmapinputinfilterinput_query)

        # Convert the query results into a DataFrame
        aud_agg_tmapinputinfilterinput_df = pd.DataFrame(
            aud_agg_tmapinputinfilterinput_results,
            columns=['rowName', 'NameRowInput', 'composant', 'NameJob','NameProject']
        )
        logging.info(f"Retrieved {len(aud_agg_tmapinputinfilterinput_df)} rows from aud_agg_tmapinputinfilterinput.")
        logging.debug(f"Sample from aud_agg_tmapinputinfilterinput DataFrame:\n{aud_agg_tmapinputinfilterinput_df.head()}")

        # Step 5: Execute aud_agg_tmapinputinvar query to retrieve data
        aud_agg_tmapinputinvar_query = config.get_param('agg_queries', 'aud_agg_tmapinputinvar')
        logging.info(f"Executing query: {aud_agg_tmapinputinvar_query}")
        aud_agg_tmapinputinvar_results = db.execute_query(aud_agg_tmapinputinvar_query)

        # Convert the query results into a DataFrame
        aud_agg_tmapinputinvar_df = pd.DataFrame(
            aud_agg_tmapinputinvar_results,
            columns=['rowName', 'NameRowInput', 'composant', 'NameJob','NameProject']
        )
        logging.info(f"Retrieved {len(aud_agg_tmapinputinvar_df)} rows from aud_agg_tmapinputinvar.")
        logging.debug(f"Sample from aud_agg_tmapinputinfilterinput DataFrame:\n{aud_agg_tmapinputinvar_df.head()}")
 

      

        def catch_inner_join_rejects(left_df, right_df, left_join_keys, right_join_keys):
            """
            Detects rows rejected from both left and right DataFrames during an inner join.

            Parameters:
                left_df (pd.DataFrame): The left DataFrame for the join.
                right_df (pd.DataFrame): The right DataFrame for the join.
                left_join_keys (list or str): The columns to use as join keys from the left DataFrame.
                right_join_keys (list or str): The columns to use as join keys from the right DataFrame.

            Returns:
                pd.DataFrame: A DataFrame containing all rejected rows (left and right).
            """
            logging.info("Starting inner join rejection detection.")
            logging.debug(f"Left DataFrame shape: {left_df.shape}")
            logging.debug(f"Right DataFrame shape: {right_df.shape}")
            
            # Perform a left join to detect left rejects
            logging.info("Performing left join to detect rows rejected from the left DataFrame.")
            left_join_df = pd.merge(
                left_df,
                right_df,
                left_on=left_join_keys,
                right_on=right_join_keys,
                how='left',
                indicator=True  # Adds a '_merge' column to indicate join status
            )
            left_rejects = left_join_df[left_join_df['_merge'] == 'left_only'].drop(columns=['_merge'])
            logging.info(f"Detected {len(left_rejects)} left rejects.")
            logging.debug(f"Left rejects sample:\n{left_rejects.head()}")

            # Perform a right join to detect right rejects
            logging.info("Performing right join to detect rows rejected from the right DataFrame.")
            right_join_df = pd.merge(
                left_df,
                right_df,
                left_on=left_join_keys,
                right_on=right_join_keys,
                how='right',
                indicator=True  # Adds a '_merge' column to indicate join status
            )
            right_rejects = right_join_df[right_join_df['_merge'] == 'right_only'].drop(columns=['_merge'])
            logging.info(f"Detected {len(right_rejects)} right rejects.")
            logging.debug(f"Right rejects sample:\n{right_rejects.head()}")

            # Combine left and right rejects
            logging.info("Combining left and right rejects for overall analysis.")
            all_rejects = pd.concat([left_rejects, right_rejects], ignore_index=True)

            # Optionally, remove duplicates if you expect overlaps
            all_rejects = all_rejects.drop_duplicates()

            logging.debug(f"Total rejects detected: {len(all_rejects)}.")
            logging.debug(f"Combined rejects sample:\n{all_rejects.head()}")

            logging.info("Finished inner join rejection detection.")
            return all_rejects

 # =========================================================================================================================
# input_df + aud_agg_tmapinputinoutput_df --> rejects + aud_agg_tmapinputinfilteroutput_df --> rejects 
# + aud_agg_tmapinputinjoininput_df --> rejects + aud_agg_tmapinputinfilterinput_df --> rejects 
# + aud_agg_tmapinputinvar_df --> insert in aud_agg_tmapcolumunused
# =========================================================================================================================

        # Step 1: Prepare input DataFrame
        input_df = input_df[['rowName', 'nameColumnInput', 'composant', 'NameJob', 'NameProject']]
        input_df = input_df.rename(columns={"nameColumnInput": "NameRowInput"})

        # Step 2: Sequential joins to detect rejects
        logging.info("Starting the sequential reject detection process.")
        df = catch_inner_join_rejects(
            input_df, aud_agg_tmapinputinoutput_df, 
            ['rowName', 'NameRowInput', 'composant', 'NameJob', 'NameProject'], 
            ['rowName', 'NameRowInput', 'composant', 'NameJob', 'NameProject']
        )
        logging.info(f"First join completed. Rejects: {len(df)} rows.")

        df1 = catch_inner_join_rejects(
            df, aud_agg_tmapinputinfilteroutput_df, 
            ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'], 
            ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']
        )
        logging.info(f"Second join completed. Rejects: {len(df1)} rows.")

        df2 = catch_inner_join_rejects(
            df1, aud_agg_tmapinputinjoininput_df, 
            ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'], 
            ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']
        )
        logging.info(f"Third join completed. Rejects: {len(df2)} rows.")

        df3 = catch_inner_join_rejects(
            df2, aud_agg_tmapinputinfilterinput_df, 
            ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'], 
            ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']
        )
        logging.info(f"Fourth join completed. Rejects: {len(df3)} rows.")

        final = catch_inner_join_rejects(
            df3, aud_agg_tmapinputinvar_df, 
            ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob'], 
            ['rowName', 'NameRowInput', 'composant', 'NameProject', 'NameJob']
        )
        logging.info(f"Final join completed. Rejects: {len(final)} rows.")

        # Step 3: Insert rejects into aud_agg_tmapcolumunused
        if final.empty:
            logging.info("No data to insert into aud_agg_tmapcolumunused.")
        else:
            try:
                # Adjust insert_query as needed
                insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapcolumunused')
                # Convert final DataFrame to list of tuples for batch insertion
                data_batch = final.to_records(index=False).tolist()
                
                db.insert_data_batch(insert_query, 'aud_agg_tmapcolumunused', data_batch)
                logging.info(f"Inserted final batch of {len(data_batch)} rows into aud_agg_tmapcolumunused.")
            except Exception as e:
                logging.warning(f"Error inserting final batch into aud_agg_tmapcolumunused: {str(e)}")
# =========================================================================================================================
# Execute aud_inputtable_nb query and insert into aud_agg_tmapinput
# =========================================================================================================================

        # Step 1: Execute the query
        aud_inputtable_nb_query = config.get_param('agg_queries', 'aud_inputtable_nb')
        logging.info(f"Executing query: {aud_inputtable_nb_query}")

        try:
            # Execute the query and fetch results
            aud_inputtable_nb_results = db.execute_query(aud_inputtable_nb_query)
            logging.info(f"Query executed successfully. Number of rows retrieved: {len(aud_inputtable_nb_results)}")
        except Exception as e:
            logging.error(f"Error executing query aud_inputtable_nb: {str(e)}")
            raise

        # Step 2: Insert query results into aud_agg_tmapinput
        if len(aud_inputtable_nb_results)==0 :
            logging.info("No data to insert into aud_agg_tmapinput.")
        else:
            try:
                # Fetch the insert query dynamically
                insert_query = config.get_param('insert_agg_queries', 'aud_agg_tmapinput')
                logging.info(f"Using insert query: {insert_query}")

                # Convert the query results to a list of tuples for batch insertion
                
                # Perform batch insertion
                db.insert_data_batch(insert_query, 'aud_agg_tmapinput', aud_inputtable_nb_results)
                logging.info(f"Inserted {len(aud_inputtable_nb_results)} rows into aud_agg_tmapinput successfully.")
            except Exception as e:
                logging.error(f"Error inserting data into aud_agg_tmapinput: {str(e)}")

            

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)



























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
        # delete_files_in_directory(directory_path)

        # Step 2: Execute inputtable_xml XML query and write to CSV
        aud_inputtable_xml_query = config.get_param('agg_queries', 'aud_inputtable_xml')
        logging.info(f"Executing query: {aud_inputtable_xml_query}")
        aud_inputtable_xml_results = db.execute_query(aud_inputtable_xml_query)

        input_csv_path = os.path.join(directory_path, "inputtable_xml.csv")
        input_csv_header = [
            'aud_nameColumnInput', 'aud_type', 'aud_xpathColumnInput', 'rowName', 
            'aud_componentName', 'aud_componentValue', 'filterOutGoingConnections', 
            'lookupOutgoingConnections', 'outgoingConnections', 'NameJob', 'NameProject', 
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
            'outgoingConnections', 'NameJob', 'NameProject', 'exec_date', 'expression', 
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
                    
                    # Apply the rule: !Relational.ISNULL(output.expression) && routines.Utils.containsElementExpression(output.expression, input.rowName + "." + input.aud_nameColumnInput) == 1
                    search_string = input_row['rowName'] + "." + input_row['aud_nameColumnInput']
                    if output_row['expression'] is not None and search_string in output_row['expression']:
                        
                        # Prepare `expression`: replace newlines if expression is not NULL
                        expression = output_row['expression'].replace("\n", " ") if output_row['expression'] else None

                        # Apply the rule for `expression`: !Relational.ISNULL(row['expression']) && !row['expressionJoin'].isEmpty()
                        if output_row['expression'] is not None and output_row['expression'].strip():
                            
                            # Check if `expression` contains  rowName+"."+input.aud_nameColumnInput
                            search_string =  input_row['rowName'] + "." + input_row['aud_nameColumnInput'] 
                            if search_string in output_row['expression']:
                                # Prepare `expression=`: replace newlines if expressionJoin is not NULL
                                expression_join = output_row['expression'].replace("\n", " ") if output_row['expression'] else None
                                
                                # Prepare the row for insertion into the first table
                                joined_row = (
                                    input_row['aud_nameColumnInput'],
                                    input_row['rowName'],
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
        unique_rows = set()  # Track uniqueness based on (NameJob, NameProject, aud_componentName, aud_componentValue)

        for input_row in input_data:
            for output_row in output_data:
                if (input_row['aud_componentName'] == output_row['aud_componentValue'] and
                    input_row['NameJob'] == output_row['NameJob'] and
                    input_row['NameProject'] == output_row['NameProject']):
                    
                    # Check expression filter
                    search_string = input_row['rowName'] + "." + input_row['aud_nameColumnInput']
                    if output_row['expression'] is not None and search_string in output_row['expression']:
                        # Prepare `expression`: replace newlines if expression is not None
                        expression = output_row['expression'].replace("\n", " ") if output_row['expression'] else None
                        
                        # Check if column is joined (is_columnjoined condition)
                        is_columnjoined = (output_row['aud_xpathColumnInput'] is not None and 
                                        search_string in output_row['aud_xpathColumnInput'])

                        # Prepare the row key and ensure uniqueness
                        row_key = (output_row['NameJob'], output_row['NameProject'], input_row['aud_componentName'], output_row['aud_componentValue'])
                        if row_key not in unique_rows:
                            # Apply the condition to prepare `expressionFilterOutput`
                            expressionFilterOutput = (
                                output_row['expression'].replace("\n", " ") if output_row['expression'] is not None else None
                            )

                            # Prepare the joined row for insertion
                            joined_row = (
                                input_row['rowName'],
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
                search_string = f"{row['rowName']}.{row['aud_nameColumnInput']}"
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
                        row['rowName'],                 # rowName
                        row['aud_nameColumnInput'],              # aud_nameColumnInput
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
                        row['rowName'],  # NameColumnInput
                        expression_filter,  # expressionFilter
                        row['aud_componentName'],  # aud_componentName
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
        # directory_path = config.get_param('Directories', 'delete_files')
        # output_csv_path = os.path.join(directory_path, "aud_outputtable_xml.csv")

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
                    var_row[1] == input_row['aud_componentName']):             # aud_componentValue match (index 1)

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
                                input_row['rowName'],                     # rowName
                                input_row['aud_nameColumnInput'],
                                input_row['aud_componentName'],                   # aud_componentName
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

        # Function to load SQL table data
        def load_sql_table(query, db, column_count):
            """Executes a SQL query and returns the result as a pandas DataFrame with the specified number of columns."""
            try:
                data = db.execute_query(query)
                column_names = [f"col_{i}" for i in range(column_count)]
                df = pd.DataFrame(data, columns=column_names)
                return df
            except Exception as e:
                logging.error(f"Error loading data from query: {query}. Error: {str(e)}")
                return pd.DataFrame(columns=[f"col_{i}" for i in range(column_count)])

        # Function to display the head of each DataFrame
        def display_head(df, table_name):
            logging.info(f"Displaying head for table '{table_name}':")
            logging.info("\n" + str(df.head()))

        # Function for left anti join to mimic Talend's 'catch lookup rejects' using column indexes
        def left_anti_join(left_df, right_df, join_columns_index):
            logging.info(f"Performing left anti join on column indexes: {join_columns_index}")
            
            # Select columns by index from both DataFrames
            left_subset = left_df.iloc[:, join_columns_index]
            right_subset = right_df.iloc[:, join_columns_index]
            
            # Rename columns temporarily to perform merge
            left_subset.columns = right_subset.columns = [f"col_{i}" for i in join_columns_index]
            
            # Perform an outer merge and filter for left-only entries
            merged_df = pd.merge(left_subset, right_subset, how='left', indicator=True)
            unmatched_rows = left_df[merged_df['_merge'] == 'left_only'].reset_index(drop=True)
            
            logging.info(f"Found {len(unmatched_rows)} unmatched rows after join.")
            return unmatched_rows

        # SQL Queries and column mappings (adjust column counts as needed)
        sql_tables = {
            'aud_agg_txmlmapinputinoutput': ("SELECT distinct  aud_nameColumnInput,aud_nameRowInput, aud_componentName, output_nameproject, output_namejob FROM aud_agg_txmlmapinputinoutput", 5),
            'aud_agg_txmlmapinputinfilteroutput': ("SELECT distinct NameRowInput,rowName,  aud_componentName, expressionFilterOutput, NameProject, NameJob FROM aud_agg_txmlmapinputinfilteroutput", 6),
            'aud_agg_txmlmapinputinjoininput': ("SELECT distinct  NameColumnInput,rowName, aud_componentName, NameProject, NameJob FROM aud_agg_txmlmapinputinjoininput", 5),
            'aud_agg_txmlmapinputinfilterinput': ("SELECT DISTINCT  NameColumnInput,rowName, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinfilterinput", 5),
            'aud_agg_txmlmapinputinvar': ("SELECT DISTINCT  NameRowInput,rowName, composant, NameProject, NameJob FROM aud_agg_txmlmapinputinvar", 5)
        }

        # Load input CSV by specified column names and adjust their order if needed
        input_df = pd.read_csv(input_csv_path, usecols=['aud_nameColumnInput', 'rowName', 'aud_componentName', 'NameProject', 'NameJob'])

        # Rename columns to standardize across joins
        input_df.columns = [f"col_{i}" for i in range(5)]  



        # Display the head of the input DataFrame
        # display_head(input_df, "Input CSV")

        # Load SQL tables into DataFrames and display their heads for verification
        sql_dataframes = {}
        for table, (query, column_count) in sql_tables.items():
            sql_dataframes[table] = load_sql_table(query, db, column_count)
            # display_head(sql_dataframes[table], table)

        # Perform left anti joins
        unmatched_df = input_df.copy()
        for table, right_df in sql_dataframes.items():
            logging.info(f"Joining input_df with table: {table}")
            
            # Define the column indexes to match on
            join_columns_index = [0, 1, 2, 3, 4]  # Adjust as per your column structure
            
            # Perform left anti join by index
            unmatched_df = left_anti_join(unmatched_df, right_df, join_columns_index)

        # Insert unmatched rows if any
        if not unmatched_df.empty:
            logging.info(f"Inserting {len(unmatched_df)} unmatched rows into the database.")
            
            insert_query = config.get_param('insert_agg_queries', 'aud_agg_txmlmapcolumunused')
            relevant_columns = [1,0,  2, 3, 4]  # Use column indexes for insertion

            # Prepare data for insertion by row and column index
            insert_data = [
                tuple(row[i] for i in relevant_columns) 
                for row in unmatched_df.itertuples(index=False)
            ]
            
            # Batch insert logic
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

        # Insert aggregation results in batches
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



    
       















