from lxml import etree
import pandas as pd

def extract_xpaths_from_xsd(file_path):
    tree = etree.parse(file_path)
    root = tree.getroot()

    namespaces = root.nsmap.copy()
    if None in namespaces:
        namespaces["xsd"] = namespaces.pop(None)

    # Store complexType definitions for lookup
    complex_types = {}
    for ctype in root.findall(".//xsd:complexType", namespaces):
        if "name" in ctype.attrib:
            complex_types[ctype.attrib["name"]] = ctype

    xpaths = []

    def traverse(element, current_path):
        tag_name = etree.QName(element).localname

        if tag_name == "element" and "name" in element.attrib:
            new_path = f"{current_path}/{element.attrib['name']}" if current_path else f"/{element.attrib['name']}"
            xpaths.append(new_path)

            # If this element uses a named complex type, traverse that definition
            if "type" in element.attrib:
                type_name = element.attrib["type"].split(":")[-1]  # remove namespace prefix
                if type_name in complex_types:
                    traverse(complex_types[type_name], new_path)

            # Otherwise, traverse inline children
            for child in element:
                traverse(child, new_path)

        elif tag_name in ("complexType", "sequence", "choice"):
            for child in element:
                traverse(child, current_path)

    # Start from all top-level elements
    for elem in root.findall(".//xsd:element", namespaces):
        parent_tag = etree.QName(elem.getparent()).localname
        if parent_tag == "schema":
            traverse(elem, "")

    return sorted(set(xpaths))


def compare_xsd_files(file1, file2, output_excel):
    xpaths1 = set(extract_xpaths_from_xsd(file1))
    xpaths2 = set(extract_xpaths_from_xsd(file2))

    matches = sorted(xpaths1 & xpaths2)
    only_in_file1 = sorted(xpaths1 - xpaths2)
    only_in_file2 = sorted(xpaths2 - xpaths1)

    max_len = max(len(matches), len(only_in_file1), len(only_in_file2))
    matches += [""] * (max_len - len(matches))
    only_in_file1 += [""] * (max_len - len(only_in_file1))
    only_in_file2 += [""] * (max_len - len(only_in_file2))

    df = pd.DataFrame({
        "Matching XPaths": matches,
        "Only in File 1": only_in_file1,
        "Only in File 2": only_in_file2
    })
    df.to_excel(output_excel, index=False)
    print(f"Comparison saved to {output_excel}")


# Example usage
file1 = "schema1.xsd"
file2 = "schema2.xsd"
output_excel = "xsd_comparison.xlsx"

compare_xsd_files(file1, file2, output_excel)



from lxml import etree
import pandas as pd

def extract_xpaths_from_xsd(file_path):
    """
    Extracts full XPaths from the root element to each child element in the XSD.
    """
    tree = etree.parse(file_path)
    root = tree.getroot()

    namespaces = root.nsmap.copy()
    if None in namespaces:
        namespaces["xsd"] = namespaces.pop(None)

    xpaths = []

    def traverse(element, current_path):
        tag_name = etree.QName(element).localname

        if tag_name == "element" and "name" in element.attrib:
            # Build full path
            new_path = f"{current_path}/{element.attrib['name']}" if current_path else f"/{element.attrib['name']}"
            xpaths.append(new_path)

            # Traverse deeper
            for child in element:
                traverse(child, new_path)
        else:
            # Continue through other schema constructs without adding to path
            for child in element:
                traverse(child, current_path)

    # Start traversal ONLY from global elements
    for elem in root.findall(".//xsd:element", namespaces):
        parent_tag = etree.QName(elem.getparent()).localname
        if parent_tag == "schema":  # only top-level elements
            traverse(elem, "")

    return sorted(set(xpaths))


def compare_xsd_files(file1, file2, output_excel):
    xpaths1 = set(extract_xpaths_from_xsd(file1))
    xpaths2 = set(extract_xpaths_from_xsd(file2))

    matches = sorted(xpaths1 & xpaths2)
    only_in_file1 = sorted(xpaths1 - xpaths2)
    only_in_file2 = sorted(xpaths2 - xpaths1)

    max_len = max(len(matches), len(only_in_file1), len(only_in_file2))
    matches += [""] * (max_len - len(matches))
    only_in_file1 += [""] * (max_len - len(only_in_file1))
    only_in_file2 += [""] * (max_len - len(only_in_file2))

    df = pd.DataFrame({
        "Matching XPaths": matches,
        "Only in File 1": only_in_file1,
        "Only in File 2": only_in_file2
    })
    df.to_excel(output_excel, index=False)
    print(f"Comparison saved to {output_excel}")


# Example usage
file1 = "schema1.xsd"
file2 = "schema2.xsd"
output_excel = "xsd_comparison.xlsx"

compare_xsd_files(file1, file2, output_excel)


from lxml import etree
import pandas as pd

def extract_xpaths_from_xsd(file_path):
    """
    Extracts all complete XPaths from an XSD file, including nested child elements.
    """
    tree = etree.parse(file_path)
    root = tree.getroot()

    namespaces = root.nsmap.copy()
    if None in namespaces:
        namespaces["xsd"] = namespaces.pop(None)

    xpaths = []

    def traverse(element, current_path=""):
        tag_name = etree.QName(element).localname
        if tag_name == "element" and "name" in element.attrib:
            # Append this element to the current path
            new_path = f"{current_path}/{element.attrib['name']}" if current_path else f"/{element.attrib['name']}"
            xpaths.append(new_path)

            # Traverse children with updated path
            for child in element:
                traverse(child, new_path)
        else:
            # Continue traversing non-element tags (like complexType, sequence)
            for child in element:
                traverse(child, current_path)

    traverse(root, "")
    return sorted(set(xpaths))


def compare_xsd_files(file1, file2, output_excel):
    # Extract full XPaths
    xpaths1 = set(extract_xpaths_from_xsd(file1))
    xpaths2 = set(extract_xpaths_from_xsd(file2))

    # Compare sets
    matches = sorted(xpaths1 & xpaths2)
    only_in_file1 = sorted(xpaths1 - xpaths2)
    only_in_file2 = sorted(xpaths2 - xpaths1)

    # Pad lists to same length for DataFrame
    max_len = max(len(matches), len(only_in_file1), len(only_in_file2))
    matches += [""] * (max_len - len(matches))
    only_in_file1 += [""] * (max_len - len(only_in_file1))
    only_in_file2 += [""] * (max_len - len(only_in_file2))

    # Save to Excel
    df = pd.DataFrame({
        "Matching XPaths": matches,
        "Only in File 1": only_in_file1,
        "Only in File 2": only_in_file2
    })
    df.to_excel(output_excel, index=False)
    print(f"Comparison saved to {output_excel}")


# Example usage
file1 = "schema1.xsd"
file2 = "schema2.xsd"
output_excel = "xsd_comparison.xlsx"

compare_xsd_files(file1, file2, output_excel)


from lxml import etree
import pandas as pd
from pathlib import Path

def extract_xpaths_from_xsd(file_path):
    """
    Extracts all element XPaths from an XSD file.
    """
    tree = etree.parse(file_path)
    root = tree.getroot()
    namespaces = root.nsmap
    if None in namespaces:
        namespaces["xsd"] = namespaces.pop(None)  # unify default namespace

    xpaths = []

    def traverse(element, current_path=""):
        tag_name = etree.QName(element).localname
        if tag_name == "element" and "name" in element.attrib:
            path = f"{current_path}/{element.attrib['name']}"
            xpaths.append(path)

        for child in element:
            traverse(child, current_path=current_path if tag_name != "element" else f"{current_path}/{element.attrib.get('name', '')}")

    traverse(root)
    return sorted(set(xpaths))


def compare_xsd_files(file1, file2, output_excel):
    # Extract XPaths
    xpaths1 = set(extract_xpaths_from_xsd(file1))
    xpaths2 = set(extract_xpaths_from_xsd(file2))

    # Compare
    matches = sorted(xpaths1 & xpaths2)
    only_in_file1 = sorted(xpaths1 - xpaths2)
    only_in_file2 = sorted(xpaths2 - xpaths1)

    # Prepare DataFrame
    df = pd.DataFrame({
        "Matching XPaths": matches + [""] * (max(len(only_in_file1), len(only_in_file2)) - len(matches)),
        "Only in File 1": only_in_file1 + [""] * (len(matches) - len(only_in_file1) if len(matches) > len(only_in_file1) else 0),
        "Only in File 2": only_in_file2 + [""] * (len(matches) - len(only_in_file2) if len(matches) > len(only_in_file2) else 0),
    })

    # Save to Excel
    df.to_excel(output_excel, index=False)
    print(f"Comparison saved to {output_excel}")


# Example usage
file1 = "schema1.xsd"
file2 = "schema2.xsd"
output_excel = "xsd_comparison.xlsx"

compare_xsd_files(file1, file2, output_excel)