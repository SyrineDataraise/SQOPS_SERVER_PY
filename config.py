import yaml

class Config:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.load_config()
        print("Loaded configuration:", self.config)

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
            print("Database type:", db_type)
        except KeyError:
            raise KeyError("No database type specified in configuration")

        try:
            # Retrieve the database configuration for the specified type
            db_config = self.config['database'][db_type.lower()]
            print("Original Database config:", db_config)
            db_config['type'] = db_type  # Add the type to the config
            print("Modified Database config with type:", db_config)
            return db_config
        except KeyError:
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
            print("JDBC Parameters:", jdbc_params)  # Debug print to check the contents of jdbc_params
            return jdbc_params
        except KeyError:
            raise KeyError("No JDBC configuration found in the configuration file")

    def get_param(self, key , value):
        try:
            parameter = self.config[key][value]
            print("parameter:", value, "->", parameter)
            return parameter
        except KeyError:
            raise KeyError(f"No parameter found with name '{value}'")
    def get_audit_jdbc_config(self):
            return self.config.get('Audit_JDBC', {})