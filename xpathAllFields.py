import xml.etree.ElementTree as ET
import pandas as pd

NS = {'xs': 'http://www.w3.org/2001/XMLSchema'}

def collect_type_definitions(root):
    type_map = {}
    for t in root.findall(".//xs:complexType", NS) + root.findall(".//xs:simpleType", NS):
        name = t.attrib.get("name")
        if name:
            type_map[name] = t
    return type_map

def extract_restrictions(type_element):
    """Extract base type, pattern, length, and range info from xs:restriction."""
    data_type = ""
    pattern = ""
    length = ""
    range_info = ""

    restriction = type_element.find("xs:restriction", NS)
    if restriction is not None:
        data_type = restriction.attrib.get("base", "")

        # Range tags
        range_parts = []
        for tag in ["minInclusive", "maxInclusive", "minExclusive", "maxExclusive"]:
            for val in restriction.findall(f"xs:{tag}", NS):
                range_parts.append(f"{tag}={val.attrib.get('value')}")
        range_info = ", ".join(range_parts)

        # Pattern
        pattern_el = restriction.find("xs:pattern", NS)
        if pattern_el is not None:
            pattern = pattern_el.attrib.get("value", "")

        # Length-related tags
        length_parts = []
        for tag in ["length", "minLength", "maxLength"]:
            val = restriction.find(f"xs:{tag}", NS)
            if val is not None:
                length_parts.append(f"{tag}={val.attrib.get('value')}")
        length = ", ".join(length_parts)

    return data_type, pattern, length, range_info

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
            data_type = child.attrib.get("type", "")
            pattern = ""
            length = ""
            range_info = ""

            # 1. Inline xs:simpleType
            simple_type = child.find("xs:simpleType", NS)
            if simple_type is not None:
                data_type, pattern, length, range_info = extract_restrictions(simple_type)

            # 2. Inline xs:complexType with nested sequence
            elif child.find("xs:complexType", NS) is not None:
                results.append({
                    "XPath": xpath,
                    "Data Type": "complexType",
                    "Range": "",
                    "Pattern": "",
                    "Length": "",
                    "Sample": "",
                    "Mandatory/Optional": mandatory
                })
                extract_children(child.find("xs:complexType", NS), xpath, results, type_map)
                continue

            # 3. Referenced type
            elif data_type:
                type_name = data_type.split(":")[-1]  # remove namespace if present
                ref_type = type_map.get(type_name)
                if ref_type is not None:
                    if ref_type.tag.endswith("simpleType"):
                        data_type, pattern, length, range_info = extract_restrictions(ref_type)
                    elif ref_type.tag.endswith("complexType"):
                        results.append({
                            "XPath": xpath,
                            "Data Type": "complexType",
                            "Range": "",
                            "Pattern": "",
                            "Length": "",
                            "Sample": "",
                            "Mandatory/Optional": mandatory
                        })
                        extract_children(ref_type, xpath, results, type_map)
                        continue

            # Default row if no restrictions
            results.append({
                "XPath": xpath,
                "Data Type": data_type or "complexType",
                "Range": range_info,
                "Pattern": pattern,
                "Length": length,
                "Sample": "",
                "Mandatory/Optional": mandatory
            })

def process_xsd(xsd_path, output_excel):
    tree = ET.parse(xsd_path)
    root = tree.getroot()
    results = []

    type_map = collect_type_definitions(root)

    # Start from /Document
    element = root.find("xs:element[@name='Document']", NS)
    if element is None:
        print("❌ Root element 'Document' not found.")
        return

    root_xpath = "/Document"
    results.append({
        "XPath": root_xpath,
        "Data Type": element.attrib.get("type", "complexType"),
        "Range": "",
        "Pattern": "",
        "Length": "",
        "Sample": "",
        "Mandatory/Optional": "Mandatory"
    })

    # Process type
    if "type" in element.attrib:
        type_name = element.attrib["type"].split(":")[-1]
        ref_type = type_map.get(type_name)
        if ref_type is not None:
            extract_children(ref_type, root_xpath, results, type_map)
    else:
        ct = element.find("xs:complexType", NS)
        if ct is not None:
            extract_children(ct, root_xpath, results, type_map)

    # Save to Excel
    df = pd.DataFrame(results, columns=[
        "XPath", "Data Type", "Range", "Pattern", "Length", "Sample", "Mandatory/Optional"
    ])
    df.to_excel(output_excel, index=False)
    print(f"✅ Extraction complete. Output saved to: {output_excel}")

# Example usage
xsd_file = "your_file.xsd"           # Update this
output_file = "xpaths_output.xlsx"   # Desired output
process_xsd(xsd_file, output_file)