import json
from lxml import etree
import oracledb

# -------------------------------
# 1. Connect to Oracle
# -------------------------------
DB_USER = "YOUR_USER"
DB_PASS = "YOUR_PASS"
DB_DSN  = "YOUR_DSN"  # e.g., "host:port/service"

conn = oracledb.connect(DB_USER, DB_PASS, DB_DSN)
cursor = conn.cursor()

# -------------------------------
# 2. Load rules JSON from table
# -------------------------------
cursor.execute("SELECT xsd_name, rule_json FROM iso_dq_rules")
rules_dict = {row[0]: json.loads(row[1]) for row in cursor.fetchall()}

# -------------------------------
# 3. Load ISO messages
# -------------------------------
cursor.execute("SELECT msg_id, xml_payload, xsd_name FROM iso_messages")
messages = cursor.fetchall()

# -------------------------------
# 4. Define helper for XPath check
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
        ns_uri = root.nsmap.get(None)  # default namespace
        ns_map = {"ns": ns_uri} if ns_uri else {}

        dq_report = []
        for rule in rules:
            # Support both "path" or "xpath" keys
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
    # 6. Write result back to Oracle
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

print("DQ validation completed and results updated in iso_messages.")

