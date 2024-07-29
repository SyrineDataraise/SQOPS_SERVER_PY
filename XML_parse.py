import xml.etree.ElementTree as ET

class XMLParser:
    def __init__(self, file_path):
        """Initialize the XMLParser with the path to the XML file."""
        self.file_path = file_path
        self.tree = ET.parse(file_path)
        self.root = self.tree.getroot()

import xml.etree.ElementTree as ET

class XMLParser:
    def __init__(self, file_path):
        """Initialize the XMLParser with the path to the XML file."""
        self.file_path = file_path
        self.tree = ET.parse(file_path)
        self.root = self.tree.getroot()

    def parse_nodes(self):
        """Parse and return data from `node` elements."""
        parsed_data = []

        for node in self.root.iter():
            if node.tag == 'node':
                # Extract attributes of the `node`
                comp_data = {
                    'componentName': node.get('componentName'),
                    'componentVersion': node.get('componentVersion'),
                    'offsetLabelX': node.get('offsetLabelX'),
                    'offsetLabelY': node.get('offsetLabelY'),
                    'posX': node.get('posX'),
                    'posY': node.get('posY'),
                }

                # Extract `elementParameter` data
                for elem_param in node.findall('.//elementParameter'):
                    elem_data = {
                        'componentName': comp_data['componentName'],
                        'field': elem_param.get('field'),
                        'name': elem_param.get('name'),
                        'show': elem_param.get('show'),
                        'value': elem_param.get('value'),
                        # 'elementValues': []  # Initialize empty list for element values
                    }

                    # # Extract `elementValue` data within `elementParameter`
                    # for elem_value in elem_param.findall('.//elementValue'):
                    #     value_data = {
                    #         'elementRef': elem_value.get('elementRef'),
                    #         'value': elem_value.get('value')
                    #     }
                    #     elem_data['elementValues'].append(value_data)

                    parsed_data.append(elem_data)
                    
        return parsed_data

