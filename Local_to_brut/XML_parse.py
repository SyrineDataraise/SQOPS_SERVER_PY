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
    def _parse_context_file_items(self):
        Contexts_items_data = self._parse_context_items()
        return{'contexts' :Contexts_items_data }
    
    def parse_context_properties_file(self):
        Contexts_properties_data = self._parse_context_properties()
        return{'contexts' : Contexts_properties_data }
    

    def _parse_context_items(self):
        """
        Parses 'context' elements from the loaded XML tree.

        Returns:
            list of dict: A list of dictionaries where each dictionary contains the details of a `ContextType` element
                        and its associated `contextParameter` elements.
        """
        if not self.root:
            raise ValueError("XML tree root is not initialized. Please load an XML file first.")

        data = []
        namespace = {'xmi': "http://www.omg.org/XMI", 'talendfile': "platform:/resource/org.talend.model/model/TalendFile.xsd"}

        # Check if the root element is the 'talendfile:ContextType'
        if self.root.tag != f"{{{namespace['talendfile']}}}ContextType":
            # If the root is not a 'ContextType', adjust the search or behavior accordingly
            context_elements = self.root.findall(".//talendfile:ContextType",namespace)
        else:
            context_elements = [self.root]

        # Find all ContextType elements (either in the root or sub-elements)
        for context in context_elements:
            context_data = {
                "id": context.get(f"{{{namespace['xmi']}}}id"),
                "environment_name": context.get("name"),
                "confirmationNeeded": context.get("confirmationNeeded"),
                "parameters": []
            }

            # Find all contextParameter elements within the current ContextType
            for param in context.findall(".//contextParameter"):
                parameter_data = {
                    "id": param.get(f"{{{namespace['xmi']}}}id"),
                    "name": param.get("name"),
                    "type": param.get("type"),
                    "value": param.get("value"),
                    "prompt": param.get("prompt"),
                    "promptNeeded": param.get("promptNeeded"),
                    "comment": param.get("comment"),
                }
                context_data["parameters"].append(parameter_data)

            data.append(context_data)

        return data


    def _parse_context_properties(self):
        """
        Parses 'context' elements in TalendProperties:Property from the loaded XML tree.

        Returns:
            list of dict: A list of dictionaries where each dictionary represents a context and its associated details.
        """
        parsed_contexts = []

        # Ensure that the XML tree is loaded
        if not self.root:
            raise ValueError("No XML tree loaded. Ensure the XML file is parsed before calling this method.")

        logging.debug("Starting to parse 'TalendProperties:Property' elements.")

        # Find all 'TalendProperties:Property' elements
        properties = self.root.findall(".//TalendProperties:Property", namespaces={
            "TalendProperties": "http://www.talend.org/properties"
        })

        logging.debug(f"Found {len(properties)} 'TalendProperties:Property' elements.")

        for property_elem in properties:
            # Extract attributes of the Property
            property_id = property_elem.get("id")
            label = property_elem.get("label")
            version = property_elem.get("version")
            display_name = property_elem.get("displayName")
            purpose = property_elem.get("purpose")
            description = property_elem.get("description")
            item = property_elem.get("item")
            statusCode = property_elem.get("statusCode")

            logging.debug(f"Parsing Property element with ID: {property_id}, Label: {label}, Version: {version}")

            # Build the context entry
            context_data = {
                "property_id": property_id,
                "label": label,
                "version": version,
                "display_name": display_name,
                "purpose": purpose,
                "description": description,
                "item": item,
                "statusCode": statusCode
            }

            logging.debug(f"Extracted context data: {context_data}")

            parsed_contexts.append(context_data)

        logging.info(f"Parsed {len(parsed_contexts)} 'TalendProperties:Property' elements successfully.")

        return parsed_contexts





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
                'outputId': connection.get('outputId'),
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
            """Recursively parse `children` elements, capturing nested structures."""
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
                'connection': []
            }

            # Parse `elementParameters`
            skip_node = False
            for elem_param in node.findall('.//elementParameter'):
                elem_data = {
                    'field': elem_param.get('field'),
                    'name': elem_param.get('name'),
                    'show': elem_param.get('show'),
                    'value': elem_param.get('value'),
                    'elementValue': []
                }

                # Check the condition to skip further processing
                if (
                    elem_data['field'] == 'CHECK'
                    and elem_data['name'] == 'ACTIVATE'
                    and elem_data['value'] == 'false'
                ):
                    skip_node = True
                    break  # Exit the loop and skip this node

                for elem_value in elem_param.findall('.//elementValue'):
                    value_data = {
                        'elementRef': elem_value.get('elementRef'),
                        'value': elem_value.get('value')
                    }
                    elem_data['elementValue'].append(value_data)

                comp_data['elementParameters'].append(elem_data)

            if skip_node:
                continue  # Skip processing the rest of this node
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

            # Loop through nodes and parse `nodeData` elements
            for node_data in node.findall('.//nodeData'):
                # Find various sub-elements within nodeData
                ui_propefties = node_data.find('.//uiPropefties')
                var_tables = node_data.find('.//varTables')

                # Initialize `node_data_info` structure for each `nodeData`
                node_data_info = {
                    'type': node_data.get('{http://www.w3.org/2001/XMLSchema-instance}type'),
                    'uiPropefties': {
                        'shellMaximized': ui_propefties.get('shellMaximized') if ui_propefties is not None else None
                    },
                    'varTables': {
                        'name': var_tables.get('name') if var_tables is not None else None,
                        'sizeState': var_tables.get('sizeState') if var_tables is not None else None,
                        'mapperTableEntries': []  # Initialize empty list for each `varTables`
                    },
                    'inputTables': [],
                    'outputTables': [],
                    'inputTrees': [],
                    'outputTrees': [],
                    'connections': []
                }

                # Check if 'varTables' exists and parse `mapperTableEntries` if present
                if var_tables is not None:
                    # Parse `mapperTableEntries`
                    for mapperTableEntry in var_tables.findall('.//mapperTableEntries'):
                        # Extract the attributes for `mapperTableEntry`
                        entry_data = {
                            'name': mapperTableEntry.get('name'),
                            'expression': mapperTableEntry.get('expression'),
                            'type': mapperTableEntry.get('type')
                        }

                        # Debug logging to check the entry being added
                        # logging.debug(f"Adding mapperTableEntry: {entry_data}")

                        # Append each entry to `mapperTableEntries`
                        node_data_info['varTables']['mapperTableEntries'].append(entry_data)
                
                # Optional: Debug log to check the final structure of node_data_info
                # logging.debug(f"Final node_data_info for node: {node_data_info}")


                # Parse multiple `inputTables`
                for input_table in node_data.findall('.//inputTables'):
                    input_table_info = {
                        'lookupMode': input_table.get('lookupMode'),
                        'matchingMode': input_table.get('matchingMode'),
                        'name': input_table.get('name'),
                        'sizeState': input_table.get('sizeState'),
                        'activateCondensedTool': input_table.get('activateCondensedTool'),
                        'activateExpressionFilter': input_table.get('activateExpressionFilter'),
                        'innerJoin': input_table.get('innerJoin'),
                        'expressionFilter': input_table.get('expressionFilter'),
                        'persistent': input_table.get('persistent'),
                        'mapperTableEntries': []
                    }
                    
                    # Parse `mapperTableEntries` for each `inputTable`
                    for mapper_entry in input_table.findall('.//mapperTableEntries'):
                        mapper_entry_info = {
                            'expression': mapper_entry.get('expression'),
                            'name': mapper_entry.get('name'),
                            'type': mapper_entry.get('type'),
                            'nullable': mapper_entry.get('nullable'),
                            'operator': mapper_entry.get('operator')
                        }
                        input_table_info['mapperTableEntries'].append(mapper_entry_info)

                    node_data_info['inputTables'].append(input_table_info)

                # Parse multiple `outputTables`
                for output_table in node_data.findall('.//outputTables'):
                    output_table_info = {
                        'activateExpressionFilter': output_table.get('activateExpressionFilter'),
                        'expressionFilter': output_table.get('expressionFilter'),
                        'name': output_table.get('name'),
                        'sizeState': output_table.get('sizeState'),
                        'activateCondensedTool': output_table.get('activateCondensedTool'),
                        'reject': output_table.get('reject'),
                        'rejectInnerJoin': output_table.get('rejectInnerJoin'),
                        'mapperTableEntries': []
                    }
                    
                    # Parse `mapperTableEntries` for each `outputTable`
                    for mapper_entry in output_table.findall('.//mapperTableEntries'):
                        mapper_entry_info = {
                            'expression': mapper_entry.get('expression'),
                            'name': mapper_entry.get('name'),
                            'type': mapper_entry.get('type'),
                            'nullable': mapper_entry.get('nullable')
                        }
                        output_table_info['mapperTableEntries'].append(mapper_entry_info)

                    node_data_info['outputTables'].append(output_table_info)

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
                        'nullable': output_tree.get('nullable'),
                        'allInOne': output_tree.get('allInOne'),
                        'activateCondensedTool': output_tree.get('activateCondensedTool'),
                        'activateExpressionFilter': output_tree.get('activateExpressionFilter'),
                        'expressionFilter' : output_tree.get('expressionFilter'),
                        'filterIncomingConnections': output_tree.get('expressionFilter'),
                        'children': []

                    }
                    # Parse `nodes` elements within each `outputTrees`
                    for node_item in output_tree.findall('.//nodes'):
                        node_item_data = {
                            'name': node_item.get('name'),
                            'expression': node_item.get('expression'),
                            'type': node_item.get('type'),
                            'xpath': node_item.get('xpath'),
                            'filterOutGoingConnections': node_item.get('filterOutGoingConnections'),
                            'lookupOutgoingConnections': node_item.get('lookupOutgoingConnections'),
                            'incomingConnections': node_item.get('incomingConnections'),
                            'lookupIncomingConnections': node_item.get('lookupIncomingConnections'),
                            'expression': node_item.get('expression'),
                            'children': []
                        }

                        # Parse `children` elements within each `node_item`
                        for child in node_item.findall('./children'):
                            node_item_data['children'].append(parse_children(child))

                        output_tree_data['children'].append(node_item_data)
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
            list of tuples: A list where each tuple contains (project_name, job_name, version, parsed_data).
        """
        parsed_files_data = []
        i = 0

        for root, dirs, files in os.walk(items_directory):
            for filename in files:
                if filename.endswith('.item'):
                    i += 1
                    file_path = os.path.join(root, filename)
                    logging.debug(f"Processing file: {file_path}")

                    try:
                        self.tree = ET.parse(file_path)
                        self.root = self.tree.getroot()
                        parsed_data = self._parse_file_items()

                        # Extract project_name, job_name, and version
                        parts = filename.split('.', 1)
                        project_name = parts[0]
                        job_name_version = parts[1].replace('.item', '') if len(parts) > 1 else None
                        job_name = '_'.join(job_name_version.split('_')[:-1])  # Exclude the version part
                        version = job_name_version.split('_')[-1]  # Last part as version
                        # logging.debug(f"Extracted: project_name={project_name}, job_name={job_name}, version={version}")

                        # Check if job_name already exists and if so, compare versions
                        existing_entry = next((entry for entry in parsed_files_data if entry[1] == job_name), None)
                        if existing_entry:
                            existing_version = existing_entry[2]  # Access existing version directly
                            # logging.debug(f"Existing entry found for job_name={job_name}: existing_version={existing_version}")

                            # Compare versions (assuming simple numeric comparison)
                            if version > existing_version:
                                # logging.debug(f"Newer version found for job_name={job_name}: replacing version {existing_version} with {version}")
                                parsed_files_data.remove(existing_entry)
                                parsed_files_data.append((project_name, job_name, version, parsed_data))
                            else:
                                # logging.debug(f"Current version for job_name={job_name} ({version}) is not newer than existing version ({existing_version}); skipping.")
                                i=0
                        else:
                            # No existing entry, so add new entry with version included
                            # logging.debug(f"No existing entry found for job_name={job_name}. Adding new entry.")
                            parsed_files_data.append((project_name, job_name, version, parsed_data))

                    except FileNotFoundError:
                        logging.error(f"File not found: {file_path}")
                    except ET.ParseError:
                        logging.error(f"Error parsing file: {file_path}")
                    except Exception as e:
                        logging.error(f"Unexpected error with file {file_path}: {e}", exc_info=True)

        logging.info(f"Processed {i} files")
        return parsed_files_data
    def loop_parse_contexts_items(self, items_directory):

        """
        Parses XML files from the specified directory and extracts relevant data.

        Args:
            items_directory (str): The directory containing XML files to be parsed.

        Returns:
            list of tuples: A list where each tuple contains (project_name, context_name, version, parsed_data).
        """
        parsed_files_data = []
        processed_file_count = 0

        for root, _, files in os.walk(items_directory):
            # Derive project_name from the directory
            # project_name = os.path.basename(root)
            project_name = "KEOLISTOURS"
            for filename in files:
                if filename.endswith('.item'):
                    processed_file_count += 1
                    file_path = os.path.join(root, filename)
                    logging.debug(f"Processing file: {file_path}")

                    try:
                        self.tree = ET.parse(file_path)
                        self.root = self.tree.getroot()
                        parsed_data = self._parse_context_file_items()

                        # Extract context_name and version from the filename
                        context_parts = filename.rsplit('.', 1)[0]  # Remove .item
                        if "_" in context_parts:
                            context_name, version = context_parts.rsplit('_', 1)
                        else:
                            context_name, version = context_parts, "unknown"

                        # Check if context_name already exists and compare versions
                        existing_entry = next((entry for entry in parsed_files_data if entry[1] == context_name), None)
                        if existing_entry:
                            existing_version = existing_entry[2]
                            if version > existing_version:  # Assuming version is comparable lexically
                                logging.debug(f"Newer version found for context_name={context_name}: replacing version {existing_version} with {version}")
                                parsed_files_data.remove(existing_entry)
                                parsed_files_data.append((project_name, context_name, version, parsed_data))
                            else:
                                logging.debug(f"Current version for context_name={context_name} ({version}) is not newer than existing version ({existing_version}); skipping.")
                        else:
                            # Add new entry
                            logging.debug(f"No existing entry found for context_name={context_name}. Adding new entry.")
                            parsed_files_data.append((project_name, context_name, version, parsed_data))

                    except FileNotFoundError:
                        logging.error(f"File not found: {file_path}")
                    except ET.ParseError:
                        logging.error(f"Error parsing file: {file_path}")
                    except Exception as e:
                        logging.error(f"Unexpected error with file {file_path}: {e}", exc_info=True)

        logging.info(f"Processed {processed_file_count} files")
        return parsed_files_data 


    def loop_parse_contexts_properties(self, properties_directory):
        """
        Parses XML and properties files from the specified directory and extracts relevant data.

        Args:
            items_directory (str): The directory containing files to be parsed.

        Returns:
            list of tuples: A list where each tuple contains (project_name, context_name, version, parsed_data).
        """
        parsed_files_data = []
        processed_file_count = 0

        for root, _, files in os.walk(properties_directory):
            # Derive project_name from the directory
            project_name = "KEOLISTOURS"
            for filename in files:
                if filename.endswith('.properties'):  # Adjust for `.properties` extension
                    processed_file_count += 1
                    file_path = os.path.join(root, filename)
                    logging.debug(f"Processing file: {file_path}")

                    try:
                        self.tree = ET.parse(file_path)
                        self.root = self.tree.getroot()
                        parsed_data = self.parse_context_properties_file()
                        # Extract context_name and version from the filename
                        context_parts = filename.rsplit('.', 1)[0]  # Remove `.properties`
                        if "_" in context_parts:
                            context_name, version = context_parts.rsplit('_', 1)
                        else:
                            context_name, version = context_parts, "unknown"

                        # Check if context_name already exists and compare versions
                        existing_entry = next((entry for entry in parsed_files_data if entry[1] == context_name), None)
                        if existing_entry:
                            existing_version = existing_entry[2]
                            if version > existing_version:  # Assuming version is comparable lexically
                                logging.debug(f"Newer version found for context_name={context_name}: replacing version {existing_version} with {version}")
                                parsed_files_data.remove(existing_entry)
                                parsed_files_data.append((project_name, context_name, version, parsed_data))
                            else:
                                logging.debug(f"Current version for context_name={context_name} ({version}) is not newer than existing version ({existing_version}); skipping.")
                        else:
                            # Add new entry
                            logging.debug(f"No existing entry found for context_name={context_name}. Adding new entry.")
                            parsed_files_data.append((project_name, context_name, version, parsed_data))

                    except FileNotFoundError:
                        logging.error(f"File not found: {file_path}")
                    except Exception as e:
                        logging.error(f"Unexpected error with file {file_path}: {e}", exc_info=True)

        logging.info(f"Processed {processed_file_count} files")
        return parsed_files_data


    def loop_parse_properties(self, items_directory):
        """
        Parses XML files from the specified directory and extracts relevant data.

        Args:
            items_directory (str): The directory containing XML files to be parsed.

        Returns:
            list of tuples: A list where each tuple contains (project_name, job_name, version, parsed_data).
        """
        parsed_files_data = []
        i = 0

        for root, dirs, files in os.walk(items_directory):
            for filename in files:
                if filename.endswith('.properties'):
                    i += 1
                    file_path = os.path.join(root, filename)
                    logging.debug(f"Processing file: {file_path}")

                    try:
                        self.tree = ET.parse(file_path)
                        self.root = self.tree.getroot()
                        parsed_data = self._parse_file_properties()

                        # Extract project_name, job_name, and version
                        parts = filename.split('.', 1)
                        project_name = parts[0]
                        job_name_version = parts[1].replace('.properties', '') if len(parts) > 1 else None
                        job_name = '_'.join(job_name_version.split('_')[:-1])  # Exclude the version part
                        version = job_name_version.split('_')[-1]  # Last part as version
                        logging.debug(f"Extracted: project_name={project_name}, job_name={job_name}, version={version}")

                        # Check if job_name already exists and if so, compare versions
                        existing_entry = next((entry for entry in parsed_files_data if entry[1] == job_name), None)
                        if existing_entry:
                            existing_version = existing_entry[2]
                            logging.debug(f"Existing entry found for job_name={job_name}: existing_version={existing_version}")

                            # Compare versions (assuming simple numeric comparison)
                            if version > existing_version:
                                logging.debug(f"Newer version found for job_name={job_name}: replacing version {existing_version} with {version}")
                                parsed_files_data.remove(existing_entry)
                                parsed_files_data.append((project_name, job_name, version, parsed_data))
                            else:
                                logging.debug(f"Current version for job_name={job_name} ({version}) is not newer than existing version ({existing_version}); skipping.")
                        else:
                            # No existing entry, so add new entry with version included
                            logging.debug(f"No existing entry found for job_name={job_name}. Adding new entry.")
                            parsed_files_data.append((project_name, job_name, version, parsed_data))

                    except FileNotFoundError:
                        logging.error(f"File not found: {file_path}")
                    except ET.ParseError:
                        logging.error(f"Error parsing file: {file_path}")
                    except Exception as e:
                        logging.error(f"Unexpected error with file {file_path}: {e}", exc_info=True)

        logging.info(f"Processed {i} files")
        return parsed_files_data
    




    def loop_parse_screenshots(self, screenshots_directory):
        """
        Parses XML files related to screenshots from the specified directory and all its subdirectories.

        Args:
            screenshots_directory (str): The directory containing screenshot XML files to be parsed.

        Returns:
            list of tuples: A list where each tuple contains (project_name, job_name, version, parsed_data).
        """
        parsed_screenshots_data = []
        i = 0

        for root, dirs, files in os.walk(screenshots_directory):
            for filename in files:
                if filename.endswith('.screenshot'):
                    i += 1
                    file_path = os.path.join(root, filename)
                    logging.debug(f"Processing screenshot file: {file_path}")

                    try:
                        # Parse XML file
                        self.tree = ET.parse(file_path)
                        self.root = self.tree.getroot()
                        parsed_data = self._parse_file_screenshots()

                        # Extract project_name, job_name, and version
                        parts = filename.split('.', 1)
                        project_name = parts[0]
                        job_name_version = parts[1].replace('.screenshot', '') if len(parts) > 1 else None
                        job_name = '_'.join(job_name_version.split('_')[:-1])  # Exclude the version part
                        version = job_name_version.split('_')[-1]  # Last part as version
                        logging.debug(f"Extracted: project_name={project_name}, job_name={job_name}, version={version}")

                        # Check if job_name already exists and if so, compare versions
                        existing_entry = next((entry for entry in parsed_screenshots_data if entry[1] == job_name), None)
                        if existing_entry:
                            existing_version = existing_entry[2]
                            logging.debug(f"Existing entry found for job_name={job_name}: existing_version={existing_version}")

                            # Compare versions (assuming simple numeric comparison)
                            if version > existing_version:
                                logging.debug(f"Newer version found for job_name={job_name}: replacing version {existing_version} with {version}")
                                parsed_screenshots_data.remove(existing_entry)
                                parsed_screenshots_data.append((project_name, job_name, version, parsed_data))
                            else:
                                logging.debug(f"Current version for job_name={job_name} ({version}) is not newer than existing version ({existing_version}); skipping.")
                        else:
                            # No existing entry, so add new entry with version included
                            logging.debug(f"No existing entry found for job_name={job_name}. Adding new entry.")
                            parsed_screenshots_data.append((project_name, job_name, version, parsed_data))

                    except ET.ParseError as e:
                        logging.error(f"Failed to parse screenshot XML file: {file_path}. Error: {e}")
                    except Exception as e:
                        logging.error(f"An error occurred while processing the screenshot file: {file_path}. Error: {e}")

        logging.info(f"Processed {i} screenshot files")
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

