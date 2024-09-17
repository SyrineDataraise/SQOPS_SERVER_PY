import xml.etree.ElementTree as ET
import os
import logging


class XMLParser:
    def __init__(self):
        """Initialize the XMLParser without a specific file path."""
        self.file_path = ""

    def _parse_file(self):
        """Parse the XML file and return a list of data from nodes, contexts, parameters, and connections."""
        nodes_data = self._parse_nodes()
        contexts_data = self._parse_contexts()
        parameters_data = self._parse_parameters()
        connection_data = self._parse_connection()
        subjobs_data = self._parse_subjob()

        # Return combined data as a dictionary
        return {
            'nodes': nodes_data,
            'contexts': contexts_data,
            'parameters': parameters_data,
            'connections': connection_data,
            'subjobs' : subjobs_data
        }

    def _parse_connection(self):
        # Parse `connection` elements
        data = []

        for connection in self.root.findall('.//connection'):
            connection_data = {
                'connectorName': connection.get('connectorName'),
                'label': connection.get('label'),
                'lineStyle': connection.get('lineStyle'),
                'metaname': connection.get('metaname'),
                'offsetLabelX': connection.get('offsetLabelX'),
                'offsetLabelY': connection.get('offsetLabelY'),
                'source': connection.get('source'),
                'target': connection.get('target'),
                'r_outputId': connection.get('r_outputId'),
                'elementParameters': []
            }

            for elem_param in connection.findall('.//elementParameter'):
                elem_param_data = {
                    'field': elem_param.get('field'),
                    'name': elem_param.get('name'),
                    'value': elem_param.get('value'),
                    'show': elem_param.get('show'),
                    'elementValues': []
                }

                for elem_value in elem_param.findall('.//elementValue'):
                    elem_value_data = {
                        'elementRef': elem_value.get('elementRef'),
                        'value': elem_value.get('value')
                    }
                    elem_param_data['elementValues'].append(elem_value_data)

                connection_data['elementParameters'].append(elem_param_data)

            data.append(connection_data)

        return data
    def _parse_subjob(self):
            # Parse `connection` elements
            data = []

            for subjob in self.root.findall('.//subjob'):
                subjob_data = {
                    'elementParameters': []

                }

                for elem_param in subjob.findall('.//elementParameter'):
                    elem_param_data = {
                        'field': elem_param.get('field'),
                        'name': elem_param.get('name'),
                        'value': elem_param.get('value'),
                        'show': elem_param.get('show'),
                    }
            
                    subjob_data['elementParameters'].append(elem_param_data)

                data.append(subjob_data)

            return data

            
    def _parse_nodes(self):

        """Parse and return data from `node` elements, including additional parameters."""
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
                'metadata': [],
                'nodeData': [],
                'connection' : []
            }

            # Parse `elementParameters`
            for elem_param in node.findall('.//elementParameter'):
                elem_data = {
                    'field': elem_param.get('field'),
                    'name': elem_param.get('name'),
                    'show': elem_param.get('show'),
                    'value': elem_param.get('value'),
                    'elementValue': []
                }

                for elem_value in elem_param.findall('.//elementValue'):
                    value_data = {
                        'elementRef': elem_value.get('elementRef'),
                        'value': elem_value.get('value')
                    }
                    elem_data['elementValue'].append(value_data)

                comp_data['elementParameters'].append(elem_data)

            # Parse `metadata`
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

            # Parse `nodeData` elements
            for node_data in node.findall('.//nodeData'):
                ui_propefties = node_data.find('.//uiPropefties')
                var_tables = node_data.find('.//varTables')
                output_tables = node_data.find('.//outputTables')
                input_tables = node_data.find('.//inputTables')

                node_data_info = {
                    'type': node_data.get('{http://www.w3.org/2001/XMLSchema-instance}type'),
                    'uiPropefties': {
                        'shellMaximized': ui_propefties.get('shellMaximized') if ui_propefties is not None else None
                    },
                    'varTables': {
                        'name': var_tables.get('name') if var_tables is not None else None,
                        'sizeState': var_tables.get('sizeState') if var_tables is not None else None
                    },
                    'mapperTableEntries': [],
                    'outputTables': {
                        'activateExpressionFilter': output_tables.get('activateExpressionFilter') if output_tables is not None else None,
                        'expressionFilter': output_tables.get('expressionFilter') if output_tables is not None else None,
                        'name': output_tables.get('name') if output_tables is not None else None,
                        'sizeState': output_tables.get('sizeState') if output_tables is not None else None,
                        'activateCondensedTool': output_tables.get('activateCondensedTool') if output_tables is not None else None,
                        'reject': output_tables.get('reject') if output_tables is not None else None,
                        'rejectInnerJoin': output_tables.get('rejectInnerJoin') if output_tables is not None else None,
                        'mapperTableEntries': []
                    },
                    'inputTables': {
                        'lookupMode': input_tables.get('lookupMode') if input_tables is not None else None,
                        'matchingMode': input_tables.get('matchingMode') if input_tables is not None else None,
                        'name': input_tables.get('name') if input_tables is not None else None,
                        'sizeState': input_tables.get('sizeState') if input_tables is not None else None,
                        'activateCondensedTool': input_tables.get('activateCondensedTool') if input_tables is not None else None,
                        'activateExpressionFilter': input_tables.get('activateExpressionFilter') if input_tables is not None else None,
                        'innerJoin': input_tables.get('innerJoin') if input_tables is not None else None,
                        'expressionFilter': input_tables.get('expressionFilter') if input_tables is not None else None,
                        'persistent': input_tables.get('persistent') if input_tables is not None else None,
                        'mapperTableEntries': []
                    }
                }

                for mapper_entry in node_data.findall('.//inputTables/mapperTableEntries'):
                    mapper_entry_info = {
                        'expression': mapper_entry.get('expression'),
                        'name': mapper_entry.get('name'),
                        'type': mapper_entry.get('type'),
                        'nullable': mapper_entry.get('nullable'),
                        'operator': mapper_entry.get('operator')
                    }
                    node_data_info['inputTables']['mapperTableEntries'].append(mapper_entry_info)

                for mapper_entry in node_data.findall('.//outputTables/mapperTableEntries'):
                    mapper_entry_info = {
                        'expression': mapper_entry.get('expression'),
                        'name': mapper_entry.get('name'),
                        'type': mapper_entry.get('type'),
                        'nullable': mapper_entry.get('nullable')
                    }
                    node_data_info['outputTables']['mapperTableEntries'].append(mapper_entry_info)

                comp_data['nodeData'].append(node_data_info)

           


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
                    'elementValues': [],
                    'routinesParameters': []  # Added field to store routinesParameter data
                }

                # Parse elementValues
                for elementValue in elementParameter.findall('.//elementValue'):
                    value_data = {
                        'elementRef': elementValue.get('elementRef'),
                        'value': elementValue.get('value')
                    }
                    param_data['elementValues'].append(value_data)
                

                # Parse routinesParameter elements
                for routinesParameter in parameters.findall('.//routinesParameter'):
                    routines_param_data = {
                        'id': routinesParameter.get('id'),
                        'name': routinesParameter.get('name')
                    }
                    param_data['routinesParameters'].append(routines_param_data)
                


                parameters_data.append(param_data)

        # Log total parameters parsed
        logging.debug(f"Total parameters parsed: {len(parameters_data)}")

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


