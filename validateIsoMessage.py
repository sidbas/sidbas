#!/usr/bin/env python3
"""
ISO20022 DQ validator with auto-repair for malformed XML.
Writes per-message DQ JSON into iso_message_dq_report.

Author: ChatGPT
"""

import re
import json
import traceback
from lxml import etree
import oracledb

# -------------------------
# CONFIG - edit these
# -------------------------
DB_USER = "YOUR_USER"
DB_PASS = "YOUR_PASS"
DB_DSN  = "YOUR_HOST:1521/YOUR_SERVICE"

BATCH_COMMIT = 100  # how many messages to process before commit
DRY_RUN = False     # if True, don't write to DB (useful for testing)

# -------------------------
# DB connection
# -------------------------
def get_connection():
    return oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)

# -------------------------
# Utility: read possible LOB to string
# -------------------------
def lob_to_str(maybe_lob):
    if maybe_lob is None:
        return None
    if hasattr(maybe_lob, "read"):
        return maybe_lob.read()
    return str(maybe_lob)

# -------------------------
# LEVEL 1: sanitize incoming raw XML string
# - remove BOM
# - extract first <Document>...</Document> chunk if possible
# -------------------------
def sanitize_xml(xml_str):
    if xml_str is None:
        return None
    s = xml_str.lstrip("\ufeff").strip()
    # If there's at least one <Document tag, prefer the first <Document>...</Document> block
    if "<Document" in s:
        start = s.find("<Document")
        # find last closing </Document> after start
        end = s.rfind("</Document>")
        if end != -1 and end > start:
            end = end + len("</Document>")
            s = s[start:end]
        else:
            # if no closing tag found, keep from start to end (repair step will attempt to recover)
            s = s[start:]
    return s

# -------------------------
# LEVEL 2: attempt strict parse, then recover parse if needed
# Returns: (root_element_or_None, status_string, repaired_xml_string_or_original)
# status_string: "OK", "REPAIRED", "UNRECOVERABLE"
# -------------------------
def repair_and_parse(xml_str):
    if not xml_str:
        return None, "UNRECOVERABLE: empty", None

    # Try strict parse first
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        return root, "OK", xml_str
    except etree.XMLSyntaxError:
        pass

    # Try lxml recovery mode (best-effort)
    try:
        parser = etree.XMLParser(recover=True, remove_comments=False)
        root = etree.fromstring(xml_str.encode("utf-8"), parser)
        # get a serialized repaired XML (unicode)
        repaired = etree.tostring(root, encoding="unicode")
        return root, "REPAIRED", repaired
    except Exception as e:
        return None, f"UNRECOVERABLE: {str(e)}", None

# -------------------------
# LEVEL 3: normalize default namespace (if desired)
# This function rewrites element tags to ensure consistent namespace usage.
# Note: It returns a new Element whose tags are bound to the supplied ns (if given).
# -------------------------
def normalize_namespaces(root, target_ns_uri):
    if root is None or not target_ns_uri:
        return root
    # Create new tree with same structure but tags bound to target_ns_uri
    def rewrite(elem):
        local = etree.QName(elem).localname
        new_tag = f"{{{target_ns_uri}}}{local}"
        new_elem = etree.Element(new_tag)
        # copy attributes (ignoring namespace declarations)
        for k, v in elem.attrib.items():
            new_elem.set(k, v)
        # copy text and children recursively
        if elem.text:
            new_elem.text = elem.text
        for child in elem:
            new_child = rewrite(child)
            new_elem.append(new_child)
        if elem.tail:
            new_elem.tail = elem.tail
        return new_elem

    return rewrite(root)

# -------------------------
# Fallback regex-based checks for malformed XML
# -------------------------
def fallback_raw_exists(xml_str, tag_name):
    pattern = fr"<(?:\w+:)?{re.escape(tag_name)}\b[^>]*>.*?</(?:\w+:)?{re.escape(tag_name)}>"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

def fallback_raw_parent_child(xml_str, parent_tag, child_tag):
    pattern = fr"<(?:\w+:)?{re.escape(parent_tag)}\b[^>]*>.*?<(?:\w+:)?{re.escape(child_tag)}\b"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

# -------------------------
# DQ existence checker (namespace-aware; falls back when XML malformed)
# Returns dict with detailed info
# -------------------------
def dq_xpath_exists(xml_str, xpath, ns_map):
    """
    Returns:
      {
        'exists': 0/1,
        'parent_exists': 0/1,
        'in_correct_location': 0/1,
        'reason': '...'
      }
    """
    parts = xpath.strip("/").split("/")
    tag = parts[-1] if parts else xpath
    parent = parts[-2] if len(parts) > 1 else None

    # Try proper parsing & XPath
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        nodes = root.xpath(xpath, namespaces=ns_map)
        if nodes:
            return {'exists': 1, 'parent_exists': 1, 'in_correct_location': 1, 'reason': 'OK (well-formed XML)'}
        else:
            # check parent existence
            parent_exists = 0
            if parent:
                parent_exists = 1 if root.xpath("//" + parent, namespaces=ns_map) else 0
            return {'exists': 0, 'parent_exists': parent_exists, 'in_correct_location': 0, 'reason': 'Tag missing in well-formed XML'}
    except etree.XMLSyntaxError:
        # fallback mode
        raw_exists = fallback_raw_exists(xml_str, tag)
        if not raw_exists:
            return {'exists': 0, 'parent_exists': 0, 'in_correct_location': 0, 'reason': 'Tag not found (malformed XML)'}
        parent_exists = 1 if (parent and fallback_raw_exists(xml_str, parent)) else (1 if not parent else 0)
        correct_location = 1 if (parent and fallback_raw_parent_child(xml_str, parent, tag)) else (1 if not parent else 0)
        reason = 'Tag found in malformed XML'
        if not correct_location:
            reason = 'Tag found but not under expected parent'
        return {'exists': 1, 'parent_exists': 1 if parent_exists else 0, 'in_correct_location': 1 if correct_location else 0, 'reason': reason}
    except Exception as e:
        return {'exists': 0, 'parent_exists': 0, 'in_correct_location': 0, 'reason': f'Error in XPath eval: {str(e)}'}

# -------------------------
# Helper: convert rule keys flexibly
# Accepts rules where path may be 'path' or 'xpath', and required may be 'required' or 'minOccurs'.
# -------------------------
def normalize_rule(rule):
    path = rule.get("path") or rule.get("xpath") or rule.get("element") or rule.get("field")
    # prefer explicit required boolean/int; fall back to minOccurs
    required = None
    if "required" in rule:
        required = int(bool(rule.get("required")))
    elif "minOccurs" in rule:
        try:
            required = 1 if int(rule.get("minOccurs", 0)) > 0 else 0
        except Exception:
            required = 0
    elif "mandatory" in rule:
        required = 1 if rule.get("mandatory") else 0
    else:
        required = 0
    return path, required

# -------------------------
# Main processor
# -------------------------
def process_all_messages():
    conn = get_connection()
    cur = conn.cursor()

    # ensure DQ table exists - try to create if not present (safe attempt)
    try:
        cur.execute("""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE iso_message_dq_report (
                msg_id VARCHAR2(64) PRIMARY KEY,
                dq_report CLOB,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP
            )';
        EXCEPTION WHEN OTHERS THEN
            IF SQLCODE != -955 THEN RAISE; END IF; -- ORA-00955: name already used by an existing object
        END;
        """)
        conn.commit()
    except Exception:
        # ignore create-if-exists errors
        pass

    # load rules into memory
    rules_map = {}
    cur.execute("SELECT xsd_name, rule_json FROM iso_dq_rules")
    rows = cur.fetchall()
    for xsd_name, rule_json in rows:
        s = lob_to_str(rule_json)
        try:
            rules_map[xsd_name] = json.loads(s) if s else {}
        except Exception:
            # if rule JSON malformed, keep raw text as fallback
            rules_map[xsd_name] = {}

    # fetch messages
    cur.execute("SELECT msg_id, xml_payload, xsd_name FROM iso_messages")
    processed = 0
    for msg_id, xml_payload, xsd_name in cur.fetchall():
        processed += 1
        try:
            xml_text = lob_to_str(xml_payload)
            xml_text_sanitized = sanitize_xml(xml_text)

            # Try repair & parse
            root, status, repaired_xml = repair_and_parse(xml_text_sanitized)

            # prepare namespace map for XPath (if root exists)
            ns_map = {}
            if root is not None:
                # detect default namespace (None key)
                ns_uri = root.nsmap.get(None)
                if ns_uri:
                    ns_map = {'ns': ns_uri}
                # optional: normalize tree to that namespace if you want consistent tags
                # root = normalize_namespaces(root, ns_uri)  # uncomment if needed

            dq_report = []
            dq_meta = {'xml_repair_status': status}

            # load rules for this message
            rules_json = rules_map.get(xsd_name, {})
            rules = rules_json.get("rules") if isinstance(rules_json, dict) else None
            if not rules:
                dq_report.append({'error': f'No rules found for XSD {xsd_name}'})
            else:
                for rule in rules:
                    path_raw, required = normalize_rule(rule)
                    if not path_raw:
                        continue
                    # build xpath expression: if ns_map present, use ns: prefix
                    # If path already starts with "/": keep; else ensure leading slash
                    path = path_raw
                    if ns_map and not path.startswith("/ns:"):
                        # turn /A/B -> /ns:A/ns:B
                        path = "/" + "/".join([("ns:" + p) for p in path.strip("/").split("/")])
                    elif not path.startswith("/"):
                        path = "/" + path

                    # pick which xml to use for searching: repaired_xml if available, otherwise original
                    search_xml = repaired_xml if repaired_xml is not None else xml_text_sanitized
                    res = dq_xpath_exists(search_xml, path, ns_map)

                    # construct report entry
                    entry = {
                        'path': path_raw,
                        'required': int(required),
                        'exists': int(res['exists']),
                        'parent_exists': int(res.get('parent_exists', 0)),
                        'in_correct_location': int(res.get('in_correct_location', 0)),
                        'valid': 'ok' if (res['exists'] and res.get('in_correct_location', 0)) else 'missing' if required == 1 and not res['exists'] else 'ok',
                        'reason': res.get('reason')
                    }
                    dq_report.append(entry)

            # Add meta info
            out = {
                'msg_id': msg_id,
                'xsd_name': xsd_name,
                'xml_repair_status': status,
                'dq_report': dq_report
            }
            out_json = json.dumps(out, ensure_ascii=False)

            # persist to iso_message_dq_report (upsert style: try update else insert)
            if not DRY_RUN:
                cur.execute("UPDATE iso_message_dq_report SET dq_report = :dq, created_at = SYSTIMESTAMP WHERE msg_id = :mid",
                            dq=out_json, mid=msg_id)
                if cur.rowcount == 0:
                    cur.execute("INSERT INTO iso_message_dq_report (msg_id, dq_report) VALUES (:mid, :dq)",
                                mid=msg_id, dq=out_json)

            # commit periodically
            if not DRY_RUN and (processed % BATCH_COMMIT == 0):
                conn.commit()

        except Exception as e:
            # on any message-level error, capture and store the exception in report table
            tb = traceback.format_exc()
            err_json = json.dumps({'msg_id': msg_id, 'error': str(e), 'trace': tb}, ensure_ascii=False)
            if not DRY_RUN:
                try:
                    cur.execute("UPDATE iso_message_dq_report SET dq_report = :dq, created_at = SYSTIMESTAMP WHERE msg_id = :mid",
                                dq=err_json, mid=msg_id)
                    if cur.rowcount == 0:
                        cur.execute("INSERT INTO iso_message_dq_report (msg_id, dq_report) VALUES (:mid, :dq)",
                                    mid=msg_id, dq=err_json)
                except Exception:
                    # best-effort: if even storing fails, print to console
                    print("Failed to write error for msg_id", msg_id)
                    print(err_json)
            print("Error processing msg_id", msg_id, ":", str(e))

    # final commit
    if not DRY_RUN:
        conn.commit()
    cur.close()
    conn.close()
    print(f"Processed {processed} messages. Output written to iso_message_dq_report.")

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    process_all_messages()
    
#---------------//-------------


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