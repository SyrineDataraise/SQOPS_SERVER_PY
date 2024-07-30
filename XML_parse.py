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
                        'componentVersion': comp_data['componentVersion'],
                        'offsetLabelX': comp_data['offsetLabelX'],
                        'offsetLabelY': comp_data['offsetLabelY'],
                        'posX': comp_data['posX'],
                        'posY': comp_data['posY'],
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

    def parse_context(self):
        """Parse and return data from `context` elements."""
        context_data = []

        for context in self.root.iter('context'):
            context_entry = {
                'confirmationNeeded': context.get('confirmationNeeded'),
                'name': context.get('name'),
                'contextParameters': []
            }

            # Loop through context parameters
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