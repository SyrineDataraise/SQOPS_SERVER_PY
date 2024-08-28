import xml.etree.ElementTree as ET
import os
import logging


class XMLParser:
    def __init__(self):
        """Initialize the XMLParser without a specific file path."""
        self.file_path = ""

    def _parse_file(self):
        """Parse the XML file and return a list of data from nodes, contexts, and parameters."""
        nodes_data = self._parse_nodes()
        contexts_data = self._parse_contexts()
        parameters_data = self._parse_parameters()

        # Return combined data as a list of dictionaries
        return {
            'nodes': nodes_data,
            'contexts': contexts_data,
            'parameters': parameters_data
        }

    def _parse_nodes(self):
        """Parse and return data from `node` elements."""
        parsed_data = []

        for node in self.root.iter('node'):
            comp_data = {
                'componentName': node.get('componentName'),
                'componentVersion': node.get('componentVersion'),
                'offsetLabelX': node.get('offsetLabelX'),
                'offsetLabelY': node.get('offsetLabelY'),
                'posX': node.get('posX'),
                'posY': node.get('posY'),
                'elementParameters': [],
                'metadata': []
            }

            for elem_param in node.findall('.//elementParameter'):
                elem_data = {
                    'field': elem_param.get('field'),
                    'name': elem_param.get('name'),
                    'show': elem_param.get('show'),
                    'value': elem_param.get('value'),
                    'elementValues': []
                }

                for elem_value in elem_param.findall('.//elementValue'):
                    value_data = {
                        'elementRef': elem_value.get('elementRef'),
                        'value': elem_value.get('value')
                    }
                    elem_data['elementValues'].append(value_data)

                comp_data['elementParameters'].append(elem_data)

            for metadata in node.findall('.//metadata'):
                meta_data = {
                    'connector': metadata.get('connector'),
                    'label': metadata.get('label'),
                    'name': metadata.get('name'),
                    'columns': []
                }

                for column in metadata.findall('.//column'):
                    column_data = {
                        'comment': column.get('comment'),
                        'key': column.get('key'),
                        'length': column.get('length'),
                        'name': column.get('name'),
                        'nullable': column.get('nullable'),
                        'pattern': column.get('pattern'),
                        'precision': column.get('precision'),
                        'sourceType': column.get('sourceType'),
                        'type': column.get('type'),
                        'usefulColumn': column.get('usefulColumn'),
                        'originalLength': column.get('originalLength'),
                        'defaultValue': column.get('defaultValue'),
                        'additionalField': column.find('.//additionalField').get('value') if column.find('.//additionalField') else None,
                        'additionalProperties': column.find('.//additionalProperties').get('value') if column.find('.//additionalProperties') else None
                    }
                    meta_data['columns'].append(column_data)

                comp_data['metadata'].append(meta_data)

            parsed_data.append(comp_data)

        return parsed_data

    def _parse_contexts(self):
        """Parse `context` elements and store the data."""
        context_data = []

        for context in self.root.iter('context'):
            context_entry = {
                'confirmationNeeded': context.get('confirmationNeeded'),
                'name': context.get('name'),
                'contextParameters': []
            }

            for context_param in context.findall('contextParameter'):
                param_data = {
                    'comment': context_param.get('comment'),
                    'name': context_param.get('name'),
                    'prompt': context_param.get('prompt'),
                    'promptNeeded': context_param.get('promptNeeded'),
                    'type': context_param.get('type'),
                    'value': context_param.get('value'),
                    'repositoryContextId': context_param.get('repositoryContextId')
                }
                context_entry['contextParameters'].append(param_data)

            context_data.append(context_entry)

        return context_data

    def _parse_parameters(self):
        """Parse `parameters` elements and store the data."""
        parameters_data = []

        for parameters in self.root.findall('.//parameters'):
            for elementParameter in parameters.findall('.//elementParameter'):
                param_data = {
                    'field': elementParameter.get('field'),
                    'name': elementParameter.get('name'),
                    'show': elementParameter.get('show'),
                    'value': elementParameter.get('value'),
                    'elementValues': []
                }

                for elementValue in elementParameter.findall('.//elementValue'):
                    value_data = {
                        'elementRef': elementValue.get('elementRef'),
                        'value': elementValue.get('value')
                    }
                    param_data['elementValues'].append(value_data)

                parameters_data.append(param_data)

        return parameters_data

    def get_data(self):
        """Return all parsed data from the XML file."""
        return self._parse_file()

    def loop_parse(self, items_directory):
            """
            Parses XML files from the specified directory and extracts relevant data.

            Args:
                items_directory (str): The directory containing XML files to be parsed.

            Returns:
                list of tuples: A list where each tuple contains (project_name, job_name, parsed_data).
            """
            parsed_files_data = []
            for root, dirs, files in os.walk(items_directory):
                for filename in files:
                    if filename.endswith('.item'):
                        file_path = os.path.join(root, filename)  # Use 'root' to construct the full file path
                        logging.info(f"Processing file: {file_path}")
                        try:
                            self.tree = ET.parse(file_path)
                            self.root = self.tree.getroot()
                            parsed_data = self._parse_file()
                            # Extract project_name and job_name
                            parts = filename.split('.', 1)
                            project_name = parts[0]
                            job_name = parts[1].replace('.item', '') if len(parts) > 1 else None
                            parsed_files_data.append((project_name, job_name, parsed_data))
                        except FileNotFoundError:
                            logging.error(f"File not found: {file_path}")
                        except ET.ParseError:
                            logging.error(f"Error parsing file: {file_path}")
                        except Exception as e:
                            logging.error(f"Unexpected error with file {file_path}: {e}", exc_info=True)
            return parsed_files_data


