import xml.etree.ElementTree as ET

class XMLParser:
    def __init__(self, file_path):
        """Initialize the XMLParser with the path to the XML file."""
        self.file_path = file_path
        self.tree = ET.parse(file_path)
        self.root = self.tree.getroot()

    def parse_nodes(self):
        """Parse and print data from `node` elements."""
        for node in self.root.iter():
            if node.tag == 'node':
                # Extract and print the attributes of the `node`
                comp_data = {
                    'componentName': node.get('componentName'),
                    'componentVersion': node.get('componentVersion'),
                    'offsetLabelX': node.get('offsetLabelX'),
                    'offsetLabelY': node.get('offsetLabelY'),
                    'posX': node.get('posX'),
                    'posY': node.get('posY'),
                }
                print("Node Data:", comp_data)

                # Extract and print `elementParameter` data
                for elem_param in node.findall('.//elementParameter'):
                    elem_data = {
                        'field': elem_param.get('field'),
                        'name': elem_param.get('name'),
                        'show': elem_param.get('show'),
                        'value': elem_param.get('value')
                    }
                    print("Element Parameter Data:", elem_data)

                    # Extract and print `elementValue` data within `elementParameter`
                    for elem_value in elem_param.findall('.//elementValue'):
                        value_data = {
                            'elementRef': elem_value.get('elementRef'),
                            'value': elem_value.get('value')
                        }
                        print("Element Value Data:", value_data)

# Example usage
if __name__ == "__main__":
    # Initialize the parser with the path to the XML file
    parser = XMLParser('C:/Users/sonia/Desktop/sqops/data/tFileInputXML_tFileOutputXML_0.1.txt')
    
    # Call the method to parse and print node data
    parser.parse_nodes()
