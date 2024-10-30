import yaml
import logging

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Changed to DEBUG to capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Ensure the file is overwritten each time for clean logs
)

class Config:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.load_config()
        # logging.info("Loaded configuration: %s", self.config)

    def load_config(self):
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)
        return config

    def get_database_config(self):
        """
        Retrieves the database configuration based on the specified type from the configuration file.

        Returns:
        - dict: A dictionary containing the database configuration with the type included.

        Raises:
        - KeyError: If the database type is not specified or the configuration for the type is not found.
        """
        try:
            db_type = self.config['database']['type']
            logging.debug("Database type: %s", db_type)
        except KeyError:
            logging.error("No database type specified in configuration")
            raise KeyError("No database type specified in configuration")

        try:
            # Retrieve the database configuration for the specified type
            db_config = self.config['database'][db_type.lower()]
            logging.debug("Original Database config: %s", db_config)
            db_config['type'] = db_type  # Add the type to the config
            logging.debug("Modified Database config with type: %s", db_config)
            return db_config
        except KeyError:
            logging.error(f"No configuration found for database type '{db_type}'")
            raise KeyError(f"No configuration found for database type '{db_type}'")

    def get_jdbc_parameters(self):
        """
        Retrieves JDBC parameters from the configuration file.

        Returns:
        - dict: A dictionary containing JDBC parameters.

        Raises:
        - KeyError: If the JDBC configuration is not found in the configuration file.
        """
        try:
            jdbc_params = self.config['Audit_JDBC']
            logging.debug("JDBC Parameters: %s", jdbc_params)  # Debug log to check the contents of jdbc_params
            return jdbc_params
        except KeyError:
            logging.error("No JDBC configuration found in the configuration file")
            raise KeyError("No JDBC configuration found in the configuration file")

    def get_param(self, key, value):
        try:
            parameter = self.config[key][value]
            # logging.debug("Parameter: %s -> %s", value, parameter)
            return parameter
        except KeyError:
            logging.error(f"No parameter found with name '{value}'")
            raise KeyError(f"No parameter found with name '{value}'")

    def get_audit_jdbc_config(self):
        return self.config.get('Audit_JDBC', {})
