# Import necessary modules
import jaydebeapi
from config import Config  # Assuming Config class is defined in config.py
import logging
import csv
# import csv
# import os
# import glob
# import yaml
# import pymysql
# import psycopg2
# import sqlite3


class Database:
    def __init__(self, db_config):
        """
        Initializes the Database object with the provided database configuration.

        Args:
        - db_config (dict): Dictionary containing database connection parameters.
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        self.jdbc_params = None  # Initialize jdbc_params

    def set_jdbc_parameters(self, jdbc_params):
        self.jdbc_params = jdbc_params


    def connect_JDBC(self):
        """
        Establishes a JDBC connection to the database based on the provided configuration.

        Raises:
        - Exception: If there is an error connecting to the database or if any JDBC parameter is missing.
        """
        if not self.jdbc_params:
            raise ValueError("JDBC parameters are not set")

        jdbc_params = self.jdbc_params
        try:
            jdbc_driver = jdbc_params.get('AUDIT_JDBC_connection_driverClass')
            jdbc_url = jdbc_params.get('AUDIT_JDBC_connection_jdbcUrl')
            jdbc_user = jdbc_params.get('AUDIT_JDBC_connection_userPassword_userId')
            jdbc_password = jdbc_params.get('AUDIT_JDBC_connection_userPassword_password')
            jdbc_jar = jdbc_params.get('AUDIT_JDBC_drivers')

            if not all([jdbc_driver, jdbc_url, jdbc_user, jdbc_password, jdbc_jar]):
                raise ValueError("Missing one or more JDBC parameters")

            # Print JDBC parameters for debugging
            print(f"JDBC Driver: {jdbc_driver}")
            print(f"JDBC URL: {jdbc_url}")
            print(f"JDBC User: {jdbc_user}")
            print(f"JDBC JAR: {jdbc_jar}")

            # Connect to the database
            self.connection = jaydebeapi.connect(jdbc_driver, jdbc_url, [jdbc_user, jdbc_password], jdbc_jar)
            self.connection.jconn.setAutoCommit(False)
            self.cursor = self.connection.cursor()


        except ValueError as e:
            print(f"Configuration error: {e}")
            raise
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
        
   
    def insert_metadata(self, table_name, data_batch):
            """
        Insert data into the specified table in batches.

        Args:
            table_name (str): The name of the table where data will be inserted.
            data_batch (list of tuples): A list of tuples containing the data to be inserted.
        """
        # SQL query with ON DUPLICATE KEY UPDATE
            insert_query = f"""
            INSERT INTO {table_name} (
                aud_connector, aud_labelConnector, aud_nameComponentView, aud_comment, aud_key, aud_length,
                aud_columnName, aud_nullable, aud_pattern, aud_precision, aud_sourceType, aud_type,
                aud_usefulColumn, aud_originalLength, aud_defaultValue, aud_componentValue, aud_componentName,
                NameProject, NameJob, exec_date
            ) VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?
            ) ON DUPLICATE KEY UPDATE
                aud_connector = VALUES(aud_connector),
                aud_labelConnector = VALUES(aud_labelConnector),
                aud_nameComponentView = VALUES(aud_nameComponentView),
                aud_comment = VALUES(aud_comment),
                aud_key = VALUES(aud_key),
                aud_length = VALUES(aud_length),
                aud_columnName = VALUES(aud_columnName),
                aud_nullable = VALUES(aud_nullable),
                aud_pattern = VALUES(aud_pattern),
                aud_precision = VALUES(aud_precision),
                aud_sourceType = VALUES(aud_sourceType),
                aud_type = VALUES(aud_type),
                aud_usefulColumn = VALUES(aud_usefulColumn),
                aud_originalLength = VALUES(aud_originalLength),
                aud_defaultValue = VALUES(aud_defaultValue),
                aud_componentValue = VALUES(aud_componentValue),
                aud_componentName = VALUES(aud_componentName),
                exec_date = VALUES(exec_date);
        """

            try:
                with self.connection.cursor() as cursor:
                    for row in data_batch:
                        try:
                            cursor.execute(insert_query, row)
                        except Exception as e:
                            logging.warning(f"Skipping row due to error: {e}, row data: {row}")
                    self.connection.commit()  # Ensure the changes are committed
                    logging.info(f"Batch inserted data into {table_name}: {len(data_batch)} rows.")
            except Exception as e:
                self.connection.rollback()  # Rollback in case of a major error
                logging.error(f"Error during batch insert into {table_name}: {e}", exc_info=True)

                
    def execute_query(self, query, params=None):
        """
        Executes a SELECT SQL query and returns the results.

        Args:
        - query (str): SQL SELECT query to be executed.
        - params (tuple, optional): Parameters to be used with the query, if applicable. Defaults to None.

        Returns:
        - list: List of tuples containing the query results.

        Raises:
        - ValueError: If the database connection is not established.
        - Exception: If an error occurs during query execution.
        """
        if not self.connection:
            raise ValueError("Database connection is not established. Call connect() method first.")

        try:
            self.cursor.execute(query, params or ())
            return self.cursor.fetchall()

        except Exception as e:
            print(f"Error executing SELECT query: {e}")
            raise  # Re-raise the exception for further handling or debugging



    def delete_records_batch(self, table_name, conditions_batch):
        try:
            with self.connection.cursor() as cursor:
                for conditions in conditions_batch:
                    condition_clauses = " AND ".join([f"{column} = '{value}'" for column, value in conditions.items()])
                    sql = f"DELETE FROM {table_name} WHERE {condition_clauses}"
                    
 
                    cursor.execute(sql)
                    
            self.connection.commit()
            
            # Logging successful batch delete
            logging.info(f"Successfully deleted records from {table_name} for {len(conditions_batch)} conditions.")
        
        except Exception as e:
            # Logging error during batch delete
            logging.error(f"Error during batch delete from {table_name}: {e}")
            
            self.connection.rollback()



    

    def get_execution_date(self, query,params=None):    
        """
        Retrieves the last execution date using the provided query.

        Args:
        - query (str): SQL query to retrieve the execution date.

        Returns:
        - str or None: Execution date as a string if available, None if no results found.
        """
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result:
                return result[0]  # Assuming the first column contains the date
            else:
                return None  # Handle case where no results are returned
        except Exception as e:
            print(f"Error executing query to get execution date: {str(e)}")
            return None

            print(f"Error committing transaction: {str(e)}")


    def insert_data_batch(self, insert_query, table_name, data_batch):
            """
            Insert data into the specified table in batches.

            Args:
                insert_query (str): The SQL insert query.
                table_name (str): The name of the table where data will be inserted.
                data_batch (list of tuples): A list of tuples containing the data to be inserted.
            """
            try:
                with self.connection.cursor() as cursor:
                    for row in data_batch:
                        try:
                            cursor.execute(insert_query, row)
                        except Exception as e:
                            logging.warning(f"Skipping row due to error: {e}, row data: {row}")
                    self.connection.commit()  # Ensure the changes are committed
                    logging.info(f"Batch inserted data into {table_name}: {len(data_batch)} rows.")
            except Exception as e:
                self.connection.rollback()  # Rollback in case of a major error
                logging.error(f"Error during batch insert into {table_name}: {e}", exc_info=True)

    def insert_from_csv_batch(self, csv_file_path, table_name, batch_size):
        """
        Reads data from a CSV file and inserts it into the specified database table in batches.

        Args:
        - csv_file_path (str): Path to the CSV file.
        - table_name (str): Name of the database table to insert data into.
        - batch_size (int): Number of rows per batch for insertion. Defaults to 1000.
        """
        try:
            with open(csv_file_path, 'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                headers = next(csv_reader)  # Read the header row from the CSV
                data_batch = []
                
                insert_query = f"""
                INSERT INTO {table_name} ({', '.join(headers)}) 
                VALUES ({', '.join(['?' for _ in headers])}) ;                
                """

                for row in csv_reader:
                    data_batch.append(tuple(row))
                    if len(data_batch) == batch_size:
                        self.insert_data_batch(insert_query, table_name, data_batch)
                        data_batch.clear()  # Clear the batch after inserting

                # Insert any remaining data if the last batch is smaller than batch_size
                if data_batch:
                    self.insert_data_batch(insert_query, table_name, data_batch)
                    logging.info(f"Inserted remaining {len(data_batch)} rows from CSV into {table_name}.")

        except FileNotFoundError as e:
            logging.error(f"CSV file not found: {e}")
        except Exception as e:
            logging.error(f"Error inserting data from CSV to {table_name}: {e}", exc_info=True)

    def truncate_table(self, table_name):
        """
        Truncates the specified table, removing all rows and resetting any auto-increment counters.

        Args:
        - table_name (str): Name of the table to truncate.
        """
        try:
            truncate_query = f"TRUNCATE TABLE {table_name}"
            with self.connection.cursor() as cursor:
                cursor.execute(truncate_query)
            self.connection.commit()  # Commit the transaction
            logging.info(f"Table {table_name} has been truncated.")
        except Exception as e:
            logging.error(f"Error truncating table {table_name}: {e}", exc_info=True)
            self.connection.rollback()  # Rollback in case of an error
                     
    def close(self):
        """
        Closes the cursor and database connection.
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            print(f"Error closing database connection: {str(e)}")
    


    

    
        # def connect(self):
        #     """
        #     Establishes a connection to the database based on the configured database type.

        #     Raises:
        #     - ValueError: If the configured database type is not supported.
        #     """
        #     db_type = self.db_config['type']
        #     if db_type == 'mysql':
        #         self.connection = pymysql.connect(
        #             host=self.db_config['host'],
        #             port=self.db_config['port'],
        #             user=self.db_config['user'],
        #             password=self.db_config['password'],
        #             database=self.db_config['dbname']
        #         )
        #     elif db_type == 'postgresql':
        #         self.connection = psycopg2.connect(
        #             host=self.db_config['host'],
        #             port=self.db_config['port'],
        #             user=self.db_config['user'],
        #             password=self.db_config['password'],
        #             database=self.db_config['dbname']
        #         )
        #     elif db_type == 'sqlite':
        #         self.connection = sqlite3.connect(self.db_config['filepath'])
        #     else:
        #         raise ValueError(f"Unsupported database type: {db_type}")

        #     self.cursor = self.connection.cursor()
