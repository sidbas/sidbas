# Only look for element named "Document"
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
    "Sample": "",
    "Mandatory/Optional": "Mandatory"
})

# Handle inline or referenced complexType
complex_type = element.find("xs:complexType", NS)
if complex_type is not None:
    extract_children(complex_type, root_xpath, results, type_map)
elif "type" in element.attrib:
    ref_type = element.attrib["type"]
    if ":" in ref_type:
        ref_type = ref_type.split(":")[1]
    if ref_type in type_map:
        extract_children(type_map[ref_type], root_xpath, results, type_map)