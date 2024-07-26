# Import necessary modules
import jaydebeapi
import csv
import os
import glob
import yaml
import pymysql
import psycopg2
import sqlite3
from config import Config  # Assuming Config class is defined in config.py

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

            print("Successfully connected to the database")

        except ValueError as e:
            print(f"Configuration error: {e}")
            raise
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
        
    def connect(self):
        """
        Establishes a connection to the database based on the configured database type.

        Raises:
        - ValueError: If the configured database type is not supported.
        """
        db_type = self.db_config['type']
        if db_type == 'mysql':
            self.connection = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['dbname']
            )
        elif db_type == 'postgresql':
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['dbname']
            )
        elif db_type == 'sqlite':
            self.connection = sqlite3.connect(self.db_config['filepath'])
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        self.cursor = self.connection.cursor()

    def insert_data(self, query, table):
        """
        Executes an insert query on the connected database.

        Args:
        - query (str): SQL insert query.
        - table (str): Name of the table into which data is being inserted.

        Prints:
        - Success message upon successful insertion.
        - Error message upon insertion failure.
        """
        try:
            self.cursor.execute(query)
            self.connection.commit()
            print(f"Data successfully inserted into {table}")
        except Exception as e:
            print(f"Error inserting data into {table}: {str(e)}")


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
            print(f"Executing SELECT query: {query}")

            self.cursor.execute(query, params or ())
            return self.cursor.fetchall()

        except Exception as e:
            print(f"Error executing SELECT query: {e}")
            raise  # Re-raise the exception for further handling or debugging

    def delete_records(self, project_name, job_name):
        delete_query = f"""
        DELETE FROM aud_elementnode
        WHERE NameProject = '{project_name}' AND NameJob = '{job_name}'
        """
        try:
            # Use parameterized queries to avoid SQL injection
            self.cursor.execute(delete_query)
            self.connection.commit()
            print(f"Successfully deleted records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}")
        except Exception as e:
            print(f"Error deleting records for PROJECT_NAME: {project_name}, JOB_NAME: {job_name}: {e}")
            raise

    def get_execution_date(self, query):    
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