import re
from lxml import etree

# -----------------------------------------------------------
# 1. Fallback: Raw existence check using regex
# -----------------------------------------------------------
def fallback_raw_exists(xml_str, tag_name):
    """
    Check if <tag> exists anywhere in the raw XML.
    Namespace prefixes are ignored.
    """
    pattern = fr"<(?:\w+:)?{tag_name}\b[^>]*>.*?</(?:\w+:)?{tag_name}>"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL))


# -----------------------------------------------------------
# 2. Fallback: Check tag under its parent using regex
# -----------------------------------------------------------
def fallback_raw_parent_child(xml_str, parent_tag, child_tag):
    """
    Detect <parent><child>...</child></parent> structure in malformed XML.
    Namespace prefixes are ignored.
    """
    pattern = fr"<(?:\w+:)?{parent_tag}\b[^>]*>.*?<(?:\w+:)?{child_tag}\b"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL))


# -----------------------------------------------------------
# 3. Enhanced existence checker for DQ rules
# -----------------------------------------------------------
def dq_xpath_exists(xml_str, xpath, ns_map):
    """
    RETURN STRUCTURED INFORMATION:
    {
        "exists": 0/1,
        "parent_exists": 0/1,
        "in_correct_location": 0/1,
        "reason": "..."
    }
    """

    # --------------------------------------------
    # PART A: Split the path for deep analysis
    # --------------------------------------------
    parts = xpath.strip("/").split("/")
    tag = parts[-1]                   # final tag
    parent = parts[-2] if len(parts) > 1 else None

    # --------------------------------------------
    # PART B: Try proper XML parsing first
    # --------------------------------------------
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        nodes = root.xpath(xpath, namespaces=ns_map)

        if nodes:
            return {
                "exists": 1,
                "parent_exists": 1,
                "in_correct_location": 1,
                "reason": "OK (XML well-formed and path matches)"
            }
        else:
            # Tag not found through XML parsing
            return {
                "exists": 0,
                "parent_exists": 1 if parent and root.xpath(f"//{parent}", namespaces=ns_map) else 0,
                "in_correct_location": 0,
                "reason": "Tag missing in well-formed XML"
            }

    except etree.XMLSyntaxError:
        # XML is malformed — fallback mode
        pass

    # --------------------------------------------
    # PART C: Fallback for malformed XML
    # --------------------------------------------

    raw_exists = fallback_raw_exists(xml_str, tag)

    if not raw_exists:
        return {
            "exists": 0,
            "parent_exists": 0,
            "in_correct_location": 0,
            "reason": "Tag not found (malformed XML, raw search failed)"
        }

    # Tag exists → check parent
    if parent:
        parent_exists = fallback_raw_exists(xml_str, parent)
        correct_location = fallback_raw_parent_child(xml_str, parent, tag)
    else:
        parent_exists = 1
        correct_location = 1

    reason = "Tag found in malformed XML"
    if not correct_location:
        reason = "Tag found but not under expected parent"

    return {
        "exists": 1,
        "parent_exists": 1 if parent_exists else 0,
        "in_correct_location": 1 if correct_location else 0,
        "reason": reason
    }
    

res = dq_xpath_exists(xml_payload, xpath_expr, ns_map)

dq_report.append({
    "path": path,
    "required": int(required),
    "exists": res["exists"],
    "parent_exists": res["parent_exists"],
    "in_correct_location": res["in_correct_location"],
    "valid": "ok" if (res["exists"] and res["in_correct_location"]) else "fail",
    "reason": res["reason"]
})


import json
from lxml import etree
import oracledb

# -------------------------------
# 1. Oracle connection
# -------------------------------
DB_USER = "YOUR_USER"
DB_PASS = "YOUR_PASS"
DB_DSN  = "YOUR_HOST:1521/YOUR_SERVICE"

conn = oracledb.connect(
    user=DB_USER,
    password=DB_PASS,
    dsn=DB_DSN
)
cursor = conn.cursor()

# -------------------------------
# 2. Load rules JSON from iso_dq_rules
# -------------------------------
rules_dict = {}
cursor.execute("SELECT xsd_name, rule_json FROM iso_dq_rules")
for xsd_name, rule_json in cursor.fetchall():
    if hasattr(rule_json, "read"):
        rules_dict[xsd_name] = json.loads(rule_json.read())
    else:
        rules_dict[xsd_name] = json.loads(str(rule_json))

# -------------------------------
# 3. Load ISO messages
# -------------------------------
messages = []
cursor.execute("SELECT msg_id, xml_payload, xsd_name FROM iso_messages")
for msg_id, xml_payload, xsd_name in cursor.fetchall():
    if hasattr(xml_payload, "read"):
        xml_str = xml_payload.read()
    else:
        xml_str = str(xml_payload)
    messages.append((msg_id, xml_str, xsd_name))

# -------------------------------
# 4. Helper: namespace-aware XPath
# -------------------------------
def check_xpath_exists(xml_str, xpath, ns_map):
    """Return True if XPath exists in XML"""
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        result = root.xpath(xpath, namespaces=ns_map)
        return len(result) > 0
    except etree.XMLSyntaxError:
        return False  # consider invalid XML as missing

# -------------------------------
# 5. Process each message
# -------------------------------
for msg_id, xml_payload, xsd_name in messages:
    dq_report = []

    # Try parsing XML first
    try:
        root = etree.fromstring(xml_payload.encode("utf-8"))
        ns_uri = root.nsmap.get(None)
        ns_map = {"ns": ns_uri} if ns_uri else {}
    except etree.XMLSyntaxError as e:
        dq_report.append({"error": f"Invalid XML: {str(e)}"})
        # Insert or update DQ report table
        cursor.execute(
            "INSERT INTO iso_message_dq_report (msg_id, dq_report) VALUES (:msg_id, :dq)",
            dq=json.dumps(dq_report),
            msg_id=msg_id
        )
        continue  # skip validation for this bad XML

    # Proceed if XML is valid
    rules = rules_dict.get(xsd_name, {}).get("rules", [])
    if not rules:
        dq_report.append({"error": f"No rules found for XSD {xsd_name}"})
    else:
        for rule in rules:
            path = rule.get("path") or rule.get("xpath")
            required = rule.get("required") or rule.get("minOccurs") or 0

            xpath_expr = path
            if ns_uri and not path.startswith("/ns:"):
                xpath_expr = "/ns:" + "/ns:".join(path.strip("/").split("/"))

            exists = check_xpath_exists(xml_payload, xpath_expr, ns_map)
            valid = "missing" if required == 1 and not exists else "ok"

            dq_report.append({
                "path": path,
                "required": int(required),
                "exists": int(exists),
                "valid": valid
            })

    # -------------------------------
    # 6. Insert into DQ table
    # -------------------------------
    cursor.execute(
        "INSERT INTO iso_message_dq_report (msg_id, dq_report) VALUES (:msg_id, :dq)",
        dq=json.dumps(dq_report),
        msg_id=msg_id
    )

# Commit all updates
conn.commit()
cursor.close()
conn.close()

print("DQ validation completed successfully (including invalid XMLs).")




import json
from lxml import etree
import oracledb

# -------------------------------
# 1. Oracle connection
# -------------------------------
DB_USER = "YOUR_USER"
DB_PASS = "YOUR_PASS"
DB_DSN  = "YOUR_HOST:1521/YOUR_SERVICE"

conn = oracledb.connect(
    user=DB_USER,
    password=DB_PASS,
    dsn=DB_DSN
)
cursor = conn.cursor()

# -------------------------------
# 2. Load rules JSON from iso_dq_rules
# -------------------------------
rules_dict = {}
cursor.execute("SELECT xsd_name, rule_json FROM iso_dq_rules")
for xsd_name, rule_json in cursor.fetchall():
    # Convert CLOB to string if necessary
    if hasattr(rule_json, "read"):
        rules_dict[xsd_name] = json.loads(rule_json.read())
    else:
        rules_dict[xsd_name] = json.loads(str(rule_json))

# -------------------------------
# 3. Load ISO messages
# -------------------------------
messages = []
cursor.execute("SELECT msg_id, xml_payload, xsd_name FROM iso_messages")
for msg_id, xml_payload, xsd_name in cursor.fetchall():
    if hasattr(xml_payload, "read"):
        xml_str = xml_payload.read()
    else:
        xml_str = str(xml_payload)
    messages.append((msg_id, xml_str, xsd_name))

# -------------------------------
# 4. Helper: namespace-aware XPath
# -------------------------------
def check_xpath_exists(xml_str, xpath, ns_map):
    """Return True if XPath exists in XML"""
    root = etree.fromstring(xml_str.encode("utf-8"))
    result = root.xpath(xpath, namespaces=ns_map)
    return len(result) > 0

# -------------------------------
# 5. Process each message
# -------------------------------
for msg_id, xml_payload, xsd_name in messages:
    rules = rules_dict.get(xsd_name, {}).get("rules", [])
    if not rules:
        dq_report = [{"error": f"No rules found for XSD {xsd_name}"}]
    else:
        # Detect default namespace
        root = etree.fromstring(xml_payload.encode("utf-8"))
        ns_uri = root.nsmap.get(None)
        ns_map = {"ns": ns_uri} if ns_uri else {}

        dq_report = []
        for rule in rules:
            # Support flexible keys
            path = rule.get("path") or rule.get("xpath")
            required = rule.get("required") or rule.get("minOccurs") or 0

            # Namespace-aware XPath
            xpath_expr = path
            if ns_uri and not path.startswith("/ns:"):
                xpath_expr = "/ns:" + "/ns:".join(path.strip("/").split("/"))

            exists = check_xpath_exists(xml_payload, xpath_expr, ns_map)
            valid = "missing" if required == 1 and not exists else "ok"

            dq_report.append({
                "path": path,
                "required": int(required),
                "exists": int(exists),
                "valid": valid
            })

    # -------------------------------
    # 6. Update dq_report column
    # -------------------------------
    cursor.execute(
        "UPDATE iso_messages SET dq_report = :dq WHERE msg_id = :msg_id",
        dq=json.dumps(dq_report),
        msg_id=msg_id
    )

# Commit all updates
conn.commit()
cursor.close()
conn.close()

print("DQ validation completed successfully.")