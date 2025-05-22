import xml.etree.ElementTree as ET
import pandas as pd

NS = {'xs': 'http://www.w3.org/2001/XMLSchema'}

def collect_complex_types(root):
    type_map = {}
    for ct in root.findall(".//xs:complexType", NS):
        name = ct.attrib.get("name")
        if name:
            type_map[name] = ct
    return type_map

def extract_children(element, path, results, type_map):
    sequence = element.find("xs:sequence", NS)
    if sequence is not None:
        for child in sequence.findall("xs:element", NS):
            name = child.attrib.get("name")
            if not name:
                continue

            xpath = f"{path}/{name}"
            min_occurs = child.attrib.get("minOccurs", "1")
            mandatory = "Mandatory" if min_occurs != "0" else "Optional"
            data_type = child.attrib.get("type", "complexType")
            range_info = ""
            pattern = ""
            length = ""

            restriction = child.find(".//xs:restriction", NS)
            if restriction is not None:
                data_type = restriction.attrib.get("base", data_type)

                # Capture min/max range constraints
                parts = []
                for tag in ["minInclusive", "maxInclusive", "minExclusive", "maxExclusive"]:
                    for val in restriction.findall(f"xs:{tag}", NS):
                        parts.append(f"{tag}={val.attrib.get('value')}")
                range_info = ", ".join(parts)

                # Capture pattern
                pattern_el = restriction.find("xs:pattern", NS)
                if pattern_el is not None:
                    pattern = pattern_el.attrib.get("value", "")

                # Capture length-related constraints
                length_parts = []
                for tag in ["length", "minLength", "maxLength"]:
                    val = restriction.find(f"xs:{tag}", NS)
                    if val is not None:
                        length_parts.append(f"{tag}={val.attrib.get('value')}")
                length = ", ".join(length_parts)

            results.append({
                "XPath": xpath,
                "Data Type": data_type,
                "Range": range_info,
                "Pattern": pattern,
                "Length": length,
                "Sample": "",
                "Mandatory/Optional": mandatory
            })

            # Recursively process nested inline complexType
            nested_complex = child.find("xs:complexType", NS)
            if nested_complex is not None:
                extract_children(nested_complex, xpath, results, type_map)

            # Process referenced type
            elif "type" in child.attrib:
                ref_type = child.attrib["type"]
                if ":" in ref_type:
                    ref_type = ref_type.split(":")[1]  # remove namespace
                if ref_type in type_map:
                    extract_children(type_map[ref_type], xpath, results, type_map)

def process_xsd(xsd_path, output_excel):
    tree = ET.parse(xsd_path)
    root = tree.getroot()
    results = []

    type_map = collect_complex_types(root)

    # Start only from /Document element
    element = root.find("xs:element[@name='Document']", NS)
    if element is None:
        print("❌ Root element 'Document' not found.")
        return

    root_name = element.attrib.get("name")
    root_xpath = f"/{root_name}"
    root_type = element.attrib.get("type", "complexType")

    results.append({
        "XPath": root_xpath,
        "Data Type": root_type,
        "Range": "",
        "Pattern": "",
        "Length": "",
        "Sample": "",
        "Mandatory/Optional": "Mandatory"
    })

    # Recurse from root complexType
    complex_type = element.find("xs:complexType", NS)
    if complex_type is not None:
        extract_children(complex_type, root_xpath, results, type_map)
    elif "type" in element.attrib:
        ref_type = element.attrib["type"]
        if ":" in ref_type:
            ref_type = ref_type.split(":")[1]
        if ref_type in type_map:
            extract_children(type_map[ref_type], root_xpath, results, type_map)

    # Save to Excel
    df = pd.DataFrame(results, columns=[
        "XPath", "Data Type", "Range", "Pattern", "Length", "Sample", "Mandatory/Optional"
    ])
    df.to_excel(output_excel, index=False)
    print(f"✅ Extraction complete. Output saved to: {output_excel}")

# Example usage
xsd_file = "your_file.xsd"         # Replace with your XSD file path
output_file = "xpaths_output.xlsx" # Desired Excel output path
process_xsd(xsd_file, output_file)