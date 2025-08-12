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