# Import necessary modules
import jaydebeapi
from config import Config  # Assuming Config class is defined in config.py
import logging
import csv
import os
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
                    
                    # Logging the SQL query
                    logging.debug(f"Executing SQL: {sql}")
                    
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
    


    

    # def insert_data(self, query, table, params=None):
    #     """
    #     Executes an insert query on the connected database.

    #     Args:
    #     - query (str): SQL insert query.
    #     - table (str): Name of the table into which data is being inserted.

    #     Prints:
    #     - Success message upon successful insertion.
    #     - Error message upon insertion failure.
    #     """
    #     try:
    #         self.cursor.execute(query,params or ())
    #         self.connection.commit()
    #     except Exception as e:
    #         print(f"Error inserting data into {table}: {str(e)}")
    #         try:
    #             self.connection.rollback()
    #         except Exception as rollback_e:
    #             print(f"Error rolling back transaction: {rollback_e}")

        # def delete_records(self, table, **conditions):
        #     """
        #     Delete records from the specified table based on the given conditions.
            
        #     :param table: The name of the table.
        #     :param conditions: A dictionary where keys are column names and values are the values to match.
        #     """
        #     condition_clauses = " AND ".join([f"{column} = '{value}'" for column, value in conditions.items()])
        #     delete_query = f"DELETE FROM {table} WHERE {condition_clauses}"

        #     try:
        #         self.cursor.execute(delete_query)
        #         self.connection.commit()
        #     except Exception as e:
        #         print(f"Error deleting records from {table} with conditions {conditions}: {e}")
        #         raise

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
