import pydot
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

def parse_xsd(xsd_file):
    tree = ET.parse(xsd_file)
    root = tree.getroot()
    
    ns = {'xs': 'http://www.w3.org/2001/XMLSchema'}
    elements = []
    
    for elem in root.findall('.//xs:element', ns):
        name = elem.get('name')
        type_ = elem.get('type')
        elements.append((name, type_ if type_ else "ComplexType"))
    
    return elements

def visualize_xsd_pydot(xsd_file, output_file="xsd_graph.png"):
    elements = parse_xsd(xsd_file)
    
    graph = pydot.Dot(graph_type="digraph")

    for name, type_ in elements:
        node = pydot.Node(name, style="filled", fillcolor="lightblue", shape="box")
        graph.add_node(node)
        if type_:
            edge = pydot.Edge(name, type_)
            graph.add_edge(edge)

    graph.write_png(output_file)

    # Display Image
    img = mpimg.imread(output_file)
    plt.imshow(img)
    plt.axis("off")
    plt.show()

# Example Usage
xsd_file = 'example.xsd'  # Provide the path to your XSD file
visualize_xsd_pydot(xsd_file)