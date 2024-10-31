import xml.etree.ElementTree as ET
import os
import logging
import base64
from io import BytesIO
from PIL import Image  # Pillow library for image handling

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Overwrite the file each time for clean logs
)
class XMLParser:
    def __init__(self):
        """Initialize the XMLParser without a specific file path."""
        self.file_path = ""

    def _parse_file_items(self):
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
            'subjobs' : subjobs_data ,
        }
    
    def _parse_file_properties(self):
        TalendProperties_data = self._parse_Properties()
        return {'TalendProperties' : TalendProperties_data}

    def _parse_file_screenshots(self):
        Screenshots_data = self._parse_screenshot()
        return{'screenshots' :Screenshots_data }


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
        def parse_children(element):
            """ Recursively parse `children` elements, capturing nested structures. """
            children_data = {
                'name': element.get('name'),
                'type': element.get('type'),
                'xpath': element.get('xpath'),
                'nodeType': element.get('nodeType'),
                'main': element.get('main'),
                'defaultValue': element.get('defaultValue'),
                'filterOutGoingConnections': element.get('filterOutGoingConnections'),
                'lookupOutgoingConnections': element.get('lookupOutgoingConnections'),
                'outgoingConnections': element.get('outgoingConnections'),
                'lookupIncomingConnections': element.get('lookupIncomingConnections'),
                'expression': element.get('expression'),
                'children': []
            }

            # Recursively parse any nested `children` elements
            for child in element.findall('./children'):
                children_data['children'].append(parse_children(child))

            return children_data

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
                # Find various sub-elements within nodeData
                ui_propefties = node_data.find('.//uiPropefties')
                var_tables = node_data.find('.//varTables')
                output_tables = node_data.find('.//outputTables')
                input_tables = node_data.find('.//inputTables')
                inputTrees = node_data.find('.//inputTrees')
                outputTrees = node_data.find('.//outputTrees')
                varTables = node_data.find('.//varTables')
                connections = node_data.find('.//connections')

                # Initialize `node_data_info` structure
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
                    },
                    'inputTrees': [],
                    'outputTrees': [],
                    'connections': []
                }

                # Parse `mapperTableEntries` for `inputTables`
                for mapper_entry in node_data.findall('.//inputTables/mapperTableEntries'):
                    mapper_entry_info = {
                        'expression': mapper_entry.get('expression'),
                        'name': mapper_entry.get('name'),
                        'type': mapper_entry.get('type'),
                        'nullable': mapper_entry.get('nullable'),
                        'operator': mapper_entry.get('operator')
                    }
                    node_data_info['inputTables']['mapperTableEntries'].append(mapper_entry_info)

                # Parse `mapperTableEntries` for `outputTables`
                for mapper_entry in node_data.findall('.//outputTables/mapperTableEntries'):
                    mapper_entry_info = {
                        'expression': mapper_entry.get('expression'),
                        'name': mapper_entry.get('name'),
                        'type': mapper_entry.get('type'),
                        'nullable': mapper_entry.get('nullable')
                    }
                    node_data_info['outputTables']['mapperTableEntries'].append(mapper_entry_info)

                # Parse `inputTrees`
                for input_tree in node_data.findall('.//inputTrees'):
                    input_tree_data = {
                        'name': input_tree.get('name'),
                        'matchingMode': input_tree.get('matchingMode'),
                        'lookupMode': input_tree.get('lookupMode'),
                        'activateCondensedTool': input_tree.get('activateCondensedTool'),
                        'activateExpressionFilter': input_tree.get('activateExpressionFilter'),
                        'activateGlobalMap': input_tree.get('activateGlobalMap'),
                        'expressionFilter': input_tree.get('expressionFilter'),
                        'filterIncomingConnections': input_tree.get('filterIncomingConnections'),
                        'lookup': input_tree.get('lookup'),
                        'children': []
                    }

                    # Parse `nodes` elements within each `inputTrees`
                    for node_item in input_tree.findall('.//nodes'):
                        node_item_data = {
                            'name': node_item.get('name'),
                            'expression': node_item.get('expression'),
                            'type': node_item.get('type'),
                            'xpath': node_item.get('xpath'),
                            'filterOutGoingConnections': node_item.get('filterOutGoingConnections'),
                            'lookupOutgoingConnections': node_item.get('lookupOutgoingConnections'),
                            'outgoingConnections': node_item.get('outgoingConnections'),
                            'lookupIncomingConnections': node_item.get('lookupIncomingConnections'),
                            'expression': node_item.get('expression'),
                            'children': []
                        }

                        # Parse `children` elements within each `node_item`
                        for child in node_item.findall('./children'):
                            node_item_data['children'].append(parse_children(child))

                        input_tree_data['children'].append(node_item_data)

                    node_data_info['inputTrees'].append(input_tree_data)


                # Parse `outputTrees`
                for output_tree in node_data.findall('.//outputTrees'):
                    output_tree_data = {
                        'name': output_tree.get('name'),
                        'expression': output_tree.get('expression'),
                        'type': output_tree.get('type'),
                        'nullable': output_tree.get('nullable')
                    }
                    node_data_info['outputTrees'].append(output_tree_data)

                # Parse `connections`
                for connection in node_data.findall('.//connections'):
                    connection_data = {
                        'source': connection.get('source'),
                        'target': connection.get('target'),
                        'type': connection.get('type')
                    }
                    node_data_info['connections'].append(connection_data)

                comp_data['nodeData'].append(node_data_info)

            parsed_data.append(comp_data)

        return parsed_data




    def _parse_Properties(self):
        """Parse and return data from `xmi:XMI` elements and their nested `TalendProperties:Property` and `additionalProperties` elements."""
        # logging.info("Starting to parse context group data from `xmi:XMI` elements.")
        parsed_context_data = []

        # Iterate over `xmi:XMI` elements
        for properties in self.root.iter('{http://www.omg.org/XMI}XMI'):
            # logging.debug(f"Found `xmi:XMI` element: {ET.tostring(properties, encoding='unicode')[:200]}")  # Print snippet

            context_data = {
                'properties': []
            }

            # Parse `TalendProperties:Property` elements
            for prop in properties.findall('.//{http://www.talend.org/properties}Property'):
                # logging.debug(f"Found `TalendProperties:Property` element: {ET.tostring(prop, encoding='unicode')[:200]}")  # Print snippet

                property_data = {
                    'id': prop.get('{http://www.omg.org/XMI}id'),
                    'label': prop.get('label'),
                    'purpose': prop.get('purpose'),
                    'description' : prop.get('description'),
                    'version': prop.get('version'),
                    'statusCode': prop.get('statusCode'),
                    'item': prop.get('item'),
                    'displayName': prop.get('displayName'),
                    'additionalProperties': []
                }

                # Parse `additionalProperties` inside the `TalendProperties:Property`
                for add_prop in prop.findall('.//{http://www.talend.org/properties}additionalProperties'):
                    # logging.debug(f"Found `additionalProperties` with Key: {add_prop.get('key')} and Value: {add_prop.get('value')}")
                    additional_property_data = {
                        'key': add_prop.get('key'),
                        'value': add_prop.get('value')
                    }
                    property_data['additionalProperties'].append(additional_property_data)

                # Add the property data to the context group data
                context_data['properties'].append(property_data)

            # Log completed parsing for `TalendProperties:Property`
            # logging.info(f"Completed parsing of `TalendProperties:Property` with {len(context_data['properties'])} properties.")

            # Append the context data to the final parsed context data list
            parsed_context_data.append(context_data)

        # logging.info(f"Completed parsing context group data. Total parsed `xmi:XMI` elements: {len(parsed_context_data)}.")
        return parsed_context_data




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

        # # Log total parameters parsed
        # logging.debug(f"Total parameters parsed: {len(parameters_data)}")

        return parameters_data



    def loop_parse_items(self, items_directory):
            """
            Parses XML files from the specified directory and extracts relevant data.

            Args:
                items_directory (str): The directory containing XML files to be parsed.

            Returns:
                list of tuples: A list where each tuple contains (project_name, job_name, parsed_data).
            """
            parsed_files_data = []
            i=0
            for root, dirs, files in os.walk(items_directory):
                for filename in files:
                    if filename.endswith('.item'):
                        i+=1
                        file_path = os.path.join(root, filename)  # Use 'root' to construct the full file path
                        # logging.info(f"Processing file: {file_path}")
                        try:
                            self.tree = ET.parse(file_path)
                            self.root = self.tree.getroot()
                            parsed_data = self._parse_file_items()
                            # Extract project_name and job_name
                            parts = filename.split('.', 1)
                            project_name = str(parts[0])
                            job_name = str(parts[1]).replace('.item', '') if len(parts) > 1 else None
                            parsed_files_data.append((project_name, job_name, parsed_data))
                        except FileNotFoundError:
                            logging.error(f"File not found: {file_path}")
                        except ET.ParseError:
                            logging.error(f"Error parsing file: {file_path}")
                        except Exception as e:
                            logging.error(f"Unexpected error with file {file_path}: {e}", exc_info=True)
            logging.info(f"Processed {i} files ")
               
            return parsed_files_data

    def loop_parse_properties(self, items_directory):
            """
            Parses XML files from the specified directory and extracts relevant data.

            Args:
                items_directory (str): The directory containing XML files to be parsed.

            Returns:
                list of tuples: A list where each tuple contains (project_name, job_name, parsed_data).
            """
            parsed_files_data = []
            i=0
            for root, dirs, files in os.walk(items_directory):
                for filename in files:
                    if filename.endswith('.properties'):
                        i+=1
                        file_path = os.path.join(root, filename)  # Use 'root' to construct the full file path
                        # logging.info(f"Processing file: {file_path}")
                        try:
                            self.tree = ET.parse(file_path)
                            self.root = self.tree.getroot()
                            parsed_data = self._parse_file_properties()
                            # Extract project_name and job_name
                            parts = filename.split('.', 1)
                            project_name = str(parts[0])
                            job_name = str(parts[1]).replace('.properties', '') if len(parts) > 1 else None
                            parsed_files_data.append((project_name, job_name, parsed_data))
                        except FileNotFoundError:
                            logging.error(f"File not found: {file_path}")
                        except ET.ParseError:
                            logging.error(f"Error parsing file: {file_path}")
                        except Exception as e:
                            logging.error(f"Unexpected error with file {file_path}: {e}", exc_info=True)
            logging.info(f"Processed {i} files ")
                
            return parsed_files_data


    def loop_parse_screenshots(self, screenshots_directory):
        """
        Parses XML files related to screenshots from the specified directory and all its subdirectories.
        
        Args:
            screenshots_directory (str): The directory containing screenshot XML files to be parsed.
        
        Returns:
            list of tuples: A list where each tuple contains (project_name, job_name, parsed_data).
        """
        parsed_screenshots_data = []
        i=0
        
        for root, dirs, files in os.walk(screenshots_directory):  # Traverse directories and subdirectories
            for filename in files:
                if filename.endswith('.screenshot'):  # Process only files with `.screenshot` extension
                    i+=1
                    file_path = os.path.join(root, filename)
                    # logging.info(f"Processing screenshot file: {file_path}")

                    try:
                        # Parse XML file
                        self.tree = ET.parse(file_path)
                        self.root = self.tree.getroot()
                        parsed_data = self._parse_file_screenshots()

                        # Extract project_name and job_name from the filename
                        parts = filename.split('.', 1)
                        project_name = str(parts[0])
                        job_name =str(parts[1]).replace('.screenshot', '') if len(parts) > 1 else None

                        # Append the result to the list
                        parsed_screenshots_data.append((project_name, job_name, parsed_data))

                    except ET.ParseError as e:
                        logging.error(f"Failed to parse screenshot XML file: {file_path}. Error: {e}")
                    except Exception as e:
                        logging.error(f"An error occurred while processing the screenshot file: {file_path}. Error: {e}")
        logging.info(f"Processed {i} files ")
        return parsed_screenshots_data
    

    def _parse_screenshot(self):
        """
        Parse and return data from the `talendfile:ScreenshotsMap` XML element, decode base64 image data,
        and capture image resolution.
        """
        screenshot_data = []
        # Define the namespace for the ScreenshotsMap element
        ns = {'talendfile': 'platform:/resource/org.talend.model/model/TalendFile.xsd'}

        # Iterate over `talendfile:ScreenshotsMap` elements
        for screenshot in self.root.findall('talendfile:ScreenshotsMap', ns):
            # logging.debug(f"Found `talendfile:ScreenshotsMap` element: {screenshot.attrib}")


            # Extract the base64 string from the `value` attribute
            base64_string = screenshot.get('value')

            if base64_string is not None:
                try:
                    # Decode the base64 string into bytes
                    image_bytes = base64.b64decode(base64_string)

                    # Use with statement for BytesIO
                    with BytesIO(image_bytes) as image_stream:
                        # Open the image using PIL (Pillow)
                        image = Image.open(image_stream)

                        # Get the image resolution (width and height)
                        width, height = image.size
                        logging.info(f"Image resolution: {width} x {height}")

                

                    # Add the screenshot data, including the resolution
                    data = {
                        'key': screenshot.get('key'),
                        'value': base64_string,  # Truncate base64 string for logging
                        'resolution': f"{width}x{height}",
                        'width' : width,
                        'height' : height
                    }
                    screenshot_data.append(data)

                except Exception as e:
                    logging.error(f"Error decoding base64 or processing image for screenshot: {e}")

            else:
                logging.warning("No base64 string found in the screenshot element.")

        return screenshot_data

