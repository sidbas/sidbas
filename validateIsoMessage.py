#!/usr/bin/env python3
"""
dq_validator_root_tolerant_with_foundpath.py

ISO20022 DQ validator — strict when expected root present, tolerant otherwise,
reports detailed 'found' path when element exists but in the wrong location.
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

BATCH_COMMIT = 100
DRY_RUN = False
STRICT_STRUCTURE = False  # If True, treat mislocated required elements as missing

EXPECTED_ROOT_BY_XSD = {
    "pacs.008.001.08": "FIToFICstmrCdtTrf"
}

# -------------------------
# DB connection
# -------------------------
def get_connection():
    return oracledb.connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)

# -------------------------
# Utils
# -------------------------
def lob_to_str(maybe_lob):
    if maybe_lob is None:
        return None
    if hasattr(maybe_lob, "read"):
        return maybe_lob.read()
    return str(maybe_lob)

def sanitize_xml(xml_str):
    if xml_str is None:
        return None
    s = xml_str.lstrip("\ufeff").strip()
    if "<Document" in s:
        start = s.find("<Document")
        end = s.rfind("</Document>")
        if end != -1 and end > start:
            end = end + len("</Document>")
            s = s[start:end]
        else:
            s = s[start:]
    return s

def repair_and_parse(xml_str):
    if not xml_str:
        return None, "UNRECOVERABLE: empty", None
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        return root, "OK", xml_str
    except etree.XMLSyntaxError:
        pass
    try:
        parser = etree.XMLParser(recover=True, remove_comments=False)
        root = etree.fromstring(xml_str.encode("utf-8"), parser)
        repaired = etree.tostring(root, encoding="unicode")
        return root, "REPAIRED", repaired
    except Exception as e:
        return None, f"UNRECOVERABLE: {str(e)}", None

def fallback_raw_exists(xml_str, tag_name):
    if not tag_name:
        return False
    pattern = fr"<(?:\w+:)?{re.escape(tag_name)}\b[^>]*>.*?</(?:\w+:)?{re.escape(tag_name)}>"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

def fallback_raw_parent_child(xml_str, parent_tag, child_tag):
    if not parent_tag or not child_tag:
        return False
    pattern = fr"<(?:\w+:)?{re.escape(parent_tag)}\b[^>]*>.*?<(?:\w+:)?{re.escape(child_tag)}\b"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

def build_relaxed_localname_xpath(parts):
    if not parts:
        return None
    pieces = ["*[local-name()='" + p + "']" for p in parts]
    return "//" + "/".join(pieces)

def find_best_alternate_root(root_obj, strict_xpath, expected_root_localname, ns_map):
    if root_obj is None or not expected_root_localname:
        return None, None
    try:
        doc_children = root_obj.xpath("/*[local-name()='Document']/*")
        child_names = []
        for c in doc_children:
            ln = etree.QName(c).localname
            if ln not in child_names:
                child_names.append(ln)
    except Exception:
        child_names = []
    guess_names = ['group', 'transaction', 'batch', 'payments', 'transactions', 'envelope', 'data']
    for g in guess_names:
        if g not in child_names:
            child_names.append(g)
    for candidate in child_names:
        parts = strict_xpath.strip("/").split("/")
        new_parts = []
        replaced = False
        for p in parts:
            p_local = p.split(":")[-1]
            if (not replaced) and p_local == expected_root_localname:
                new_parts.append(candidate)
                replaced = True
            else:
                new_parts.append(p)
        if not replaced:
            continue
        variant = "/" + "/".join(new_parts)
        try:
            nodes = root_obj.xpath(variant, namespaces=ns_map)
            if nodes and len(nodes) > 0:
                return variant, candidate
        except Exception:
            continue
    return None, None

def adjust_xpath_for_missing_root_v2(root_obj, raw_xml_str, xsd_name, strict_xpath):
    expected_root = EXPECTED_ROOT_BY_XSD.get(xsd_name)
    if not expected_root:
        return strict_xpath, False, None
    major_present = False
    try:
        if root_obj is not None:
            major_present = True if root_obj.xpath("/*[local-name()='Document']/*[local-name()='" + expected_root + "']") else False
        else:
            major_present = True if fallback_raw_exists(raw_xml_str, expected_root) else False
    except Exception:
        major_present = False
    if major_present:
        return strict_xpath, False, None
    mapped_variant, mapped_to = None, None
    try:
        mapped_variant, mapped_to = find_best_alternate_root(root_obj, strict_xpath, expected_root, {})
    except Exception:
        mapped_variant, mapped_to = None, None
    if mapped_variant:
        return mapped_variant, True, {'mapped_from': expected_root, 'mapped_to': mapped_to}
    parts = [p for p in strict_xpath.strip("/").split("/") if p and p.lower() != "document" and p.split(":")[-1] != expected_root]
    if not parts:
        return strict_xpath, True, None
    relaxed = build_relaxed_localname_xpath([p.split(":")[-1] for p in parts])
    return relaxed, True, None

def normalize_rule(rule):
    path = rule.get("path") or rule.get("xpath") or rule.get("element") or rule.get("field")
    if "required" in rule:
        try:
            required = 1 if int(rule.get("required")) != 0 else 0
        except:
            required = 1 if bool(rule.get("required")) else 0
    elif "minOccurs" in rule:
        try:
            required = 1 if int(rule.get("minOccurs", 0)) > 0 else 0
        except:
            required = 0
    elif "mandatory" in rule:
        required = 1 if rule.get("mandatory") else 0
    else:
        required = 0
    return path, required

# ---------- NEW: compute a simple local-name path for a found node ----------
def build_localname_path(node):
    """
    Build a path of local-names from the document root to the given node.
    Example: /Document/group/GrpHdr/MsgId
    Works with lxml Element nodes.
    """
    segs = []
    # include the node itself
    try:
        current = node
        segs.append(etree.QName(current).localname)
        for anc in current.iterancestors():
            segs.append(etree.QName(anc).localname)
        segs.reverse()
        return "/" + "/".join(segs)
    except Exception:
        return None

# -------------------------
# Enhanced existence check using root when available, else fallback
# -------------------------
def evaluate_path_with_foundpath(search_xml, root_obj, strict_xpath, relaxed_xpath, ns_map, expected_root_localname=None):
    """
    Return a dict with:
      exists, parent_exists, in_correct_location, root_missing, reason,
      found_path (if found), location_status: 'correct'|'wrong_location'|'unknown'
    Behavior:
      - Try strict_xpath on parsed root if available
      - If strict match -> found_path = actual path -> location_status = correct
      - If not, try relaxed (local-name) search to find node(s)
      - If found via relaxed -> produce found_path (first node) and location_status wrong_location
      - If no parsed root (malformed), use regex fallback and set found_path to None (we can try to use simple parent-child regex but keep it simple)
    """
    result = {'exists':0, 'parent_exists':0, 'in_correct_location':0, 'root_missing':0, 'reason':None, 'found_path':None, 'location_status':'unknown'}

    # Prefer parsed root for XPath evaluation
    if root_obj is not None:
        try:
            # Try strict
            nodes = root_obj.xpath(strict_xpath, namespaces=ns_map)
            if nodes and len(nodes) > 0:
                # exact match found
                node = nodes[0]
                found_path = build_localname_path(node)
                result.update({'exists':1, 'parent_exists':1, 'in_correct_location':1, 'root_missing':0, 'reason':'Exact XPath match', 'found_path':found_path, 'location_status':'correct'})
                return result
            # strict failed: try relaxed local-name search for tag (descendant)
            if relaxed_xpath:
                try:
                    # relaxed_xpath is e.g. //*[local-name()='GrpHdr']/*[local-name()='MsgId'] or similar
                    rnodes = root_obj.xpath(relaxed_xpath, namespaces=ns_map)
                except Exception:
                    rnodes = []
                if rnodes and len(rnodes) > 0:
                    node = rnodes[0]
                    found_path = build_localname_path(node)
                    # parent existence: check presence of parent element local-name anywhere
                    # infer parent from strict_xpath
                    parts = strict_xpath.strip("/").split("/")
                    parent = parts[-2] if len(parts) > 1 else None
                    parent_exists = 1 if (parent and root_obj.xpath("//*[local-name()='" + parent + "']", namespaces=ns_map)) else (1 if not parent else 0)
                    # major root presence
                    major_root_exists = 1 if (expected_root_localname and root_obj.xpath("/*[local-name()='" + expected_root_localname + "']")) else 0
                    result.update({'exists':1, 'parent_exists': int(parent_exists), 'in_correct_location': 0, 'root_missing': 0 if major_root_exists else 1, 'reason':'Found via relaxed local-name search', 'found_path':found_path, 'location_status':'wrong_location'})
                    return result
            # not found
            # compute parent_exists by local-name
            parts = strict_xpath.strip("/").split("/")
            parent = parts[-2] if len(parts) > 1 else None
            parent_exists = 1 if (parent and root_obj.xpath("//*[local-name()='" + parent + "']", namespaces=ns_map)) else (1 if not parent else 0)
            major_root_exists = 1 if (expected_root_localname and root_obj.xpath("/*[local-name()='" + expected_root_localname + "']")) else 0
            result.update({'exists':0, 'parent_exists':int(parent_exists), 'in_correct_location':0, 'root_missing':0 if major_root_exists else 1, 'reason':'Tag not found'})
            return result
        except Exception as e:
            # fallback to regex if something unexpected occurred
            # continue to below
            pass

    # root_obj is None or XPath evaluation failed -> fallback to raw regex
    # try to find tag anywhere
    parts = strict_xpath.strip("/").split("/")
    tag = parts[-1] if parts else None
    parent = parts[-2] if len(parts) > 1 else None
    raw_exists = fallback_raw_exists(search_xml, tag)
    if not raw_exists:
        parent_exists = 1 if (parent and fallback_raw_exists(search_xml, parent)) else (1 if not parent else 0)
        major_root_exists = 1 if (expected_root_localname and fallback_raw_exists(search_xml, expected_root_localname)) else 0
        result.update({'exists':0, 'parent_exists':int(parent_exists), 'in_correct_location':0, 'root_missing':0 if major_root_exists else 1, 'reason':'Tag not found (raw fallback)'})
        return result
    # raw found -> we don't have a clean node to compute path; set found_path to None but set location_status
    parent_exists = 1 if (parent and fallback_raw_exists(search_xml, parent)) else (1 if not parent else 0)
    major_root_exists = 1 if (expected_root_localname and fallback_raw_exists(search_xml, expected_root_localname)) else 0
    # try parent-child raw to decide correct location
    correct_location = 1 if (parent and fallback_raw_parent_child(search_xml, parent, tag)) else (1 if not parent else 0)
    result.update({'exists':1, 'parent_exists':int(parent_exists), 'in_correct_location':int(correct_location), 'root_missing':0 if major_root_exists else 1, 'reason':'Found by raw regex', 'found_path':None, 'location_status':'correct' if correct_location else 'wrong_location'})
    return result

# -------------------------
# Main processing
# -------------------------
def process_all_messages():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
        BEGIN
            EXECUTE IMMEDIATE '
            CREATE TABLE iso_message_dq_report (
                msg_id VARCHAR2(64) PRIMARY KEY,
                dq_report CLOB,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP
            )';
        EXCEPTION WHEN OTHERS THEN
            IF SQLCODE != -955 THEN RAISE; END IF;
        END;
        """)
        conn.commit()
    except Exception:
        pass

    # load rules
    rules_map = {}
    cur.execute("SELECT xsd_name, rule_json FROM iso_dq_rules")
    for xsd_name, rule_json in cur.fetchall():
        s = lob_to_str(rule_json)
        try:
            rules_map[xsd_name] = json.loads(s) if s else {}
        except Exception:
            rules_map[xsd_name] = {}

    # fetch messages
    cur.execute("SELECT msg_id, xml_payload, xsd_name FROM iso_messages")
    processed = 0
    for msg_id, xml_payload, xsd_name in cur.fetchall():
        processed += 1
        try:
            xml_text = lob_to_str(xml_payload)
            xml_text_sanitized = sanitize_xml(xml_text)

            root, status, repaired_xml = repair_and_parse(xml_text_sanitized)
            search_xml = repaired_xml if repaired_xml is not None else xml_text_sanitized

            ns_map = {}
            if root is not None:
                ns_uri = root.nsmap.get(None)
                if ns_uri:
                    ns_map = {'ns': ns_uri}

            dq_report = []
            rules_json = rules_map.get(xsd_name, {})
            rules = rules_json.get("rules") if isinstance(rules_json, dict) else None
            if not rules:
                dq_report.append({'error': f'No rules found for XSD {xsd_name}'})
            else:
                for rule in rules:
                    path_raw, required = normalize_rule(rule)
                    if not path_raw:
                        continue

                    # strict xpath (ns-aware if possible)
                    if ns_map and not path_raw.startswith("/ns:"):
                        strict_xpath = "/" + "/".join([("ns:" + p) for p in path_raw.strip("/").split("/")])
                    elif not path_raw.startswith("/"):
                        strict_xpath = "/" + path_raw
                    else:
                        strict_xpath = path_raw

                    adjusted_xpath, was_relaxed, mapping_info = adjust_xpath_for_missing_root_v2(
                        root, xml_text_sanitized, xsd_name, strict_xpath
                    )

                    # create relaxed local-name xpath for use when needed
                    parts = [p.split(":")[-1] for p in strict_xpath.strip("/").split("/") if p]
                    relaxed_xpath = build_relaxed_localname_xpath(parts)

                    # Evaluate and get found_path if possible
                    eval_res = evaluate_path_with_foundpath(search_xml, root, adjusted_xpath, relaxed_xpath, ns_map, expected_root_localname=EXPECTED_ROOT_BY_XSD.get(xsd_name))

                    # Determine validity per your request: B (detailed) + C (treat wrong location as ok)
                    # So by default wrong_location is ok, but include details
                    valid = 'ok'
                    if required == 1 and eval_res['exists'] == 0:
                        valid = 'missing'
                    elif required == 1 and eval_res['exists'] == 1 and eval_res['in_correct_location'] == 0 and STRICT_STRUCTURE:
                        valid = 'missing'

                    entry = {
                        'path': path_raw,
                        'required': int(required),
                        'exists': int(eval_res['exists']),
                        'parent_exists': int(eval_res.get('parent_exists', 0)),
                        'in_correct_location': int(eval_res.get('in_correct_location', 0)),
                        'root_missing': int(eval_res.get('root_missing', 0)) or int(was_relaxed),
                        'location_status': eval_res.get('location_status'),
                        'expected': path_raw,
                        'found': eval_res.get('found_path'),  # may be None for raw-fallback matches
                        'mapping_info': mapping_info,
                        'valid': valid,
                        'reason': eval_res.get('reason')
                    }
                    dq_report.append(entry)

            out = {
                'msg_id': msg_id,
                'xsd_name': xsd_name,
                'xml_repair_status': status,
                'dq_report': dq_report
            }
            out_json = json.dumps(out, ensure_ascii=False)

            if not DRY_RUN:
                cur.execute("UPDATE iso_message_dq_report SET dq_report = :dq, created_at = SYSTIMESTAMP WHERE msg_id = :mid",
                            dq=out_json, mid=msg_id)
                if cur.rowcount == 0:
                    cur.execute("INSERT INTO iso_message_dq_report (msg_id, dq_report) VALUES (:mid, :dq)",
                                mid=msg_id, dq=out_json)

            if not DRY_RUN and (processed % BATCH_COMMIT == 0):
                conn.commit()

        except Exception as e:
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
                    print("Failed to write error for msg_id", msg_id)
                    print(err_json)
            print("Error processing msg_id", msg_id, ":", str(e))

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




#!/usr/bin/env python3
"""
dq_validator_root_tolerant.py

ISO20022 DQ validator — strict when expected root present, tolerant otherwise,
with mapping for non-standard containers like <group> and <transaction>.

Features:
- Auto-repair malformed XML (recover mode)
- Namespace aware XPath checks
- If expected root (per XSD) missing -> try to map to actual container (group/transaction/batch...)
- If mapping fails -> relaxed local-name descendant search
- Regex fallback for very malformed XML
- Writes DQ JSON per message into iso_message_dq_report (upsert)
- Handles Oracle CLOBs via oracledb
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

BATCH_COMMIT = 100
DRY_RUN = False
STRICT_STRUCTURE = False  # If True, treat mislocated required elements as missing

# Map XSD name (must match values in iso_messages.xsd_name / iso_dq_rules.xsd_name)
EXPECTED_ROOT_BY_XSD = {
    "pacs.008.001.08": "FIToFICstmrCdtTrf"
    # add more mappings if needed, e.g. "pacs.009.001.08": "SomeRoot"
}

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
# LEVEL 1: sanitize raw XML
# -------------------------
def sanitize_xml(xml_str):
    if xml_str is None:
        return None
    s = xml_str.lstrip("\ufeff").strip()
    if "<Document" in s:
        start = s.find("<Document")
        end = s.rfind("</Document>")
        if end != -1 and end > start:
            end = end + len("</Document>")
            s = s[start:end]
        else:
            s = s[start:]
    return s

# -------------------------
# LEVEL 2: strict parse then recover
# -------------------------
def repair_and_parse(xml_str):
    if not xml_str:
        return None, "UNRECOVERABLE: empty", None
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        return root, "OK", xml_str
    except etree.XMLSyntaxError:
        pass
    try:
        parser = etree.XMLParser(recover=True, remove_comments=False)
        root = etree.fromstring(xml_str.encode("utf-8"), parser)
        repaired = etree.tostring(root, encoding="unicode")
        return root, "REPAIRED", repaired
    except Exception as e:
        return None, f"UNRECOVERABLE: {str(e)}", None

# -------------------------
# Fallback regex based checks for malformed XML
# -------------------------
def fallback_raw_exists(xml_str, tag_name):
    if not tag_name:
        return False
    pattern = fr"<(?:\w+:)?{re.escape(tag_name)}\b[^>]*>.*?</(?:\w+:)?{re.escape(tag_name)}>"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

def fallback_raw_parent_child(xml_str, parent_tag, child_tag):
    if not parent_tag or not child_tag:
        return False
    pattern = fr"<(?:\w+:)?{re.escape(parent_tag)}\b[^>]*>.*?<(?:\w+:)?{re.escape(child_tag)}\b"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

# -------------------------
# Build a namespace-insensitive relaxed XPath using local-name()
# -------------------------
def build_relaxed_localname_xpath(parts):
    if not parts:
        return None
    # create expression like "//*[local-name()='A']//*[local-name()='B']//*[local-name()='C']"
    pieces = ["*[local-name()='" + p + "']" for p in parts]
    return "//" + "/".join(pieces)

# -------------------------
# find_best_alternate_root: try to map expected root to actual container names
# -------------------------
def find_best_alternate_root(root_obj, strict_xpath, expected_root_localname, ns_map):
    """
    Try to find an alternative container under /Document that can act as expected_root.
    Returns: (mapped_xpath, mapped_root_name) or (None, None)
    """
    if root_obj is None or not expected_root_localname:
        return None, None

    # collect direct children local-names under /Document
    try:
        doc_children = root_obj.xpath("/*[local-name()='Document']/*")
        child_names = []
        for c in doc_children:
            ln = etree.QName(c).localname
            if ln not in child_names:
                child_names.append(ln)
    except Exception:
        child_names = []

    # common guesses to try (extendable)
    guess_names = ['group', 'transaction', 'batch', 'payments', 'transactions', 'envelope', 'data']
    for g in guess_names:
        if g not in child_names:
            child_names.append(g)

    # Try each candidate by substituting expected_root in the strict xpath
    for candidate in child_names:
        parts = strict_xpath.strip("/").split("/")
        new_parts = []
        replaced = False
        for p in parts:
            p_local = p.split(":")[-1]
            if (not replaced) and p_local == expected_root_localname:
                new_parts.append(candidate)
                replaced = True
            else:
                new_parts.append(p)
        if not replaced:
            continue
        variant = "/" + "/".join(new_parts)
        try:
            nodes = root_obj.xpath(variant, namespaces=ns_map)
            if nodes and len(nodes) > 0:
                return variant, candidate
        except Exception:
            continue

    return None, None

# -------------------------
# Improved adjuster that maps or relaxes when expected root missing
# -------------------------
def adjust_xpath_for_missing_root_v2(root_obj, raw_xml_str, xsd_name, strict_xpath):
    """
    returns (adjusted_xpath, was_relaxed, mapping_info)
    mapping_info is {'mapped_from': <expected>, 'mapped_to': <candidate>} or None
    """
    expected_root = EXPECTED_ROOT_BY_XSD.get(xsd_name)
    if not expected_root:
        return strict_xpath, False, None

    # check presence of expected major root
    major_present = False
    try:
        if root_obj is not None:
            major_present = True if root_obj.xpath("/*[local-name()='Document']/*[local-name()='" + expected_root + "']") else False
        else:
            major_present = True if fallback_raw_exists(raw_xml_str, expected_root) else False
    except Exception:
        major_present = False

    if major_present:
        return strict_xpath, False, None

    # try mapping to alternate container
    mapped_variant, mapped_to = None, None
    try:
        mapped_variant, mapped_to = find_best_alternate_root(root_obj, strict_xpath, expected_root, {})
    except Exception:
        mapped_variant, mapped_to = None, None

    if mapped_variant:
        return mapped_variant, True, {'mapped_from': expected_root, 'mapped_to': mapped_to}

    # build relaxed local-name xpath by removing /Document and expected_root
    parts = [p for p in strict_xpath.strip("/").split("/") if p and p.lower() != "document" and p.split(":")[-1] != expected_root]
    if not parts:
        return strict_xpath, True, None

    relaxed = build_relaxed_localname_xpath([p.split(":")[-1] for p in parts])
    return relaxed, True, None

# -------------------------
# DQ existence checker (strict then relaxed)
# -------------------------
def dq_xpath_exists(xml_str, xpath, ns_map, search_root_localname=None, relaxed_xpath=None):
    """
    Returns dict:
      exists, parent_exists, in_correct_location, root_missing, reason
    """
    parts = xpath.strip("/").split("/") if xpath else []
    tag = parts[-1] if parts else xpath
    parent = parts[-2] if len(parts) > 1 else None
    major_root = parts[1] if len(parts) > 1 and parts[0].lower() == "document" else (parts[0] if parts else None)

    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        nodes = root.xpath(xpath, namespaces=ns_map)
        if nodes and len(nodes) > 0:
            return {'exists':1, 'parent_exists':1, 'in_correct_location':1, 'root_missing':0, 'reason':'Exact XPath match'}

        parent_exists = 0
        if parent:
            parent_exists = 1 if root.xpath("//*[local-name()='" + parent + "']", namespaces=ns_map) else 0

        major_root_exists = 1 if (major_root and root.xpath("/*[local-name()='" + major_root + "']")) else 0
        loose = root.xpath("//*[local-name()='" + tag + "']", namespaces=ns_map)
        if loose:
            return {
                'exists':1,
                'parent_exists': parent_exists,
                'in_correct_location': parent_exists if not STRICT_STRUCTURE else 0,
                'root_missing': 0 if major_root_exists else 1,
                'reason': 'Tag exists but not under expected hierarchy'
            }

        return {'exists':0, 'parent_exists':parent_exists, 'in_correct_location':0, 'root_missing':0 if major_root_exists else 1, 'reason':'Tag not found'}
    except etree.XMLSyntaxError:
        raw_exists = fallback_raw_exists(xml_str, tag)
        if not raw_exists:
            parent_exists = 1 if (parent and fallback_raw_exists(xml_str, parent)) else (1 if not parent else 0)
            major_root_exists = 1 if (search_root_localname and fallback_raw_exists(xml_str, search_root_localname)) else 0
            return {'exists':0, 'parent_exists':parent_exists, 'in_correct_location':0, 'root_missing':0 if major_root_exists else 1, 'reason':'Tag not found (malformed XML)'}
        parent_exists = 1 if (parent and fallback_raw_exists(xml_str, parent)) else (1 if not parent else 0)
        correct_location = 1 if (parent and fallback_raw_parent_child(xml_str, parent, tag)) else (1 if not parent else 0)
        major_root_exists = 1 if (search_root_localname and fallback_raw_exists(xml_str, search_root_localname)) else 0
        reason = 'Tag found in malformed XML'
        if not correct_location:
            reason = 'Tag found but not under expected parent'
        return {'exists':1, 'parent_exists':1 if parent_exists else 0, 'in_correct_location':1 if correct_location else 0, 'root_missing':0 if major_root_exists else 1, 'reason':reason}
    except Exception as e:
        return {'exists':0, 'parent_exists':0, 'in_correct_location':0, 'root_missing':0, 'reason':f'Error: {str(e)}'}

# -------------------------
# Normalize rule keys (path/xpath and required/minOccurs)
# -------------------------
def normalize_rule(rule):
    path = rule.get("path") or rule.get("xpath") or rule.get("element") or rule.get("field")
    if "required" in rule:
        try:
            required = 1 if int(rule.get("required")) != 0 else 0
        except:
            required = 1 if bool(rule.get("required")) else 0
    elif "minOccurs" in rule:
        try:
            required = 1 if int(rule.get("minOccurs", 0)) > 0 else 0
        except:
            required = 0
    elif "mandatory" in rule:
        required = 1 if rule.get("mandatory") else 0
    else:
        required = 0
    return path, required

# -------------------------
# Main processing
# -------------------------
def process_all_messages():
    conn = get_connection()
    cur = conn.cursor()

    # create report table if missing
    try:
        cur.execute("""
        BEGIN
            EXECUTE IMMEDIATE '
            CREATE TABLE iso_message_dq_report (
                msg_id VARCHAR2(64) PRIMARY KEY,
                dq_report CLOB,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP
            )';
        EXCEPTION WHEN OTHERS THEN
            IF SQLCODE != -955 THEN RAISE; END IF;
        END;
        """)
        conn.commit()
    except Exception:
        pass

    # load rules
    rules_map = {}
    cur.execute("SELECT xsd_name, rule_json FROM iso_dq_rules")
    for xsd_name, rule_json in cur.fetchall():
        s = lob_to_str(rule_json)
        try:
            rules_map[xsd_name] = json.loads(s) if s else {}
        except Exception:
            rules_map[xsd_name] = {}

    # fetch messages
    cur.execute("SELECT msg_id, xml_payload, xsd_name FROM iso_messages")
    processed = 0
    for msg_id, xml_payload, xsd_name in cur.fetchall():
        processed += 1
        try:
            xml_text = lob_to_str(xml_payload)
            xml_text_sanitized = sanitize_xml(xml_text)

            root, status, repaired_xml = repair_and_parse(xml_text_sanitized)
            search_xml = repaired_xml if repaired_xml is not None else xml_text_sanitized

            ns_map = {}
            if root is not None:
                ns_uri = root.nsmap.get(None)
                if ns_uri:
                    ns_map = {'ns': ns_uri}

            dq_report = []
            rules_json = rules_map.get(xsd_name, {})
            rules = rules_json.get("rules") if isinstance(rules_json, dict) else None
            if not rules:
                dq_report.append({'error': f'No rules found for XSD {xsd_name}'})
            else:
                for rule in rules:
                    path_raw, required = normalize_rule(rule)
                    if not path_raw:
                        continue

                    # Build strict XPath (namespace aware if possible)
                    if ns_map and not path_raw.startswith("/ns:"):
                        strict_xpath = "/" + "/".join([("ns:" + p) for p in path_raw.strip("/").split("/")])
                    elif not path_raw.startswith("/"):
                        strict_xpath = "/" + path_raw
                    else:
                        strict_xpath = path_raw

                    adjusted_xpath, was_relaxed, mapping_info = adjust_xpath_for_missing_root_v2(
                        root, xml_text_sanitized, xsd_name, strict_xpath
                    )

                    res = dq_xpath_exists(search_xml, adjusted_xpath, ns_map, search_root_localname=EXPECTED_ROOT_BY_XSD.get(xsd_name))

                    valid = 'ok'
                    if required == 1 and res['exists'] == 0:
                        valid = 'missing'
                    elif required == 1 and res['exists'] == 1 and res['in_correct_location'] == 0 and STRICT_STRUCTURE:
                        valid = 'missing'

                    entry = {
                        'path': path_raw,
                        'required': int(required),
                        'exists': int(res['exists']),
                        'parent_exists': int(res.get('parent_exists', 0)),
                        'in_correct_location': int(res.get('in_correct_location', 0)),
                        'root_missing': int(res.get('root_missing', 0)) or int(was_relaxed),
                        'mapping_info': mapping_info,
                        'valid': valid,
                        'reason': res.get('reason')
                    }
                    dq_report.append(entry)

            out = {
                'msg_id': msg_id,
                'xsd_name': xsd_name,
                'xml_repair_status': status,
                'dq_report': dq_report
            }
            out_json = json.dumps(out, ensure_ascii=False)

            if not DRY_RUN:
                cur.execute("UPDATE iso_message_dq_report SET dq_report = :dq, created_at = SYSTIMESTAMP WHERE msg_id = :mid",
                            dq=out_json, mid=msg_id)
                if cur.rowcount == 0:
                    cur.execute("INSERT INTO iso_message_dq_report (msg_id, dq_report) VALUES (:mid, :dq)",
                                mid=msg_id, dq=out_json)

            if not DRY_RUN and (processed % BATCH_COMMIT == 0):
                conn.commit()

        except Exception as e:
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
                    print("Failed to write error for msg_id", msg_id)
                    print(err_json)
            print("Error processing msg_id", msg_id, ":", str(e))

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





#!/usr/bin/env python3
"""
ISO20022 DQ validator — strict when expected root present, tolerant otherwise.

Features:
- Auto-repair malformed XML (recover mode)
- Namespace aware XPath checks
- If expected root (per XSD) missing -> relax XPaths and validate child elements anywhere
- Regex fallback for malformed XML
- Writes DQ JSON per message into iso_message_dq_report (upsert)
- Handles Oracle CLOBs via oracledb

Edit DB_USER/DB_PASS/DB_DSN and run.
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

BATCH_COMMIT = 100
DRY_RUN = False
STRICT_STRUCTURE = False  # If True, treat mislocated required elements as missing

# Map XSD name (as stored in iso_messages.xsd_name / iso_dq_rules.xsd_name)
# to the expected major root local-name used inside /Document/<ROOT>...
EXPECTED_ROOT_BY_XSD = {
    # common ISO20022 payment families - adjust to your naming convention
    "pacs.008": "FIToFICstmrCdtTrf",
    "pacs.009": "FIToFIPmtCxlReq",   # example - change if needed
    # add others as necessary
}

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
# LEVEL 1: sanitize raw XML
# -------------------------
def sanitize_xml(xml_str):
    if xml_str is None:
        return None
    s = xml_str.lstrip("\ufeff").strip()
    if "<Document" in s:
        start = s.find("<Document")
        end = s.rfind("</Document>")
        if end != -1 and end > start:
            end = end + len("</Document>")
            s = s[start:end]
        else:
            s = s[start:]
    return s

# -------------------------
# LEVEL 2: strict parse then recover
# -------------------------
def repair_and_parse(xml_str):
    if not xml_str:
        return None, "UNRECOVERABLE: empty", None
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        return root, "OK", xml_str
    except etree.XMLSyntaxError:
        pass
    try:
        parser = etree.XMLParser(recover=True, remove_comments=False)
        root = etree.fromstring(xml_str.encode("utf-8"), parser)
        repaired = etree.tostring(root, encoding="unicode")
        return root, "REPAIRED", repaired
    except Exception as e:
        return None, f"UNRECOVERABLE: {str(e)}", None

# -------------------------
# Fallback regex based checks for malformed XML
# -------------------------
def fallback_raw_exists(xml_str, tag_name):
    if not tag_name:
        return False
    pattern = fr"<(?:\w+:)?{re.escape(tag_name)}\b[^>]*>.*?</(?:\w+:)?{re.escape(tag_name)}>"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

def fallback_raw_parent_child(xml_str, parent_tag, child_tag):
    if not parent_tag or not child_tag:
        return False
    pattern = fr"<(?:\w+:)?{re.escape(parent_tag)}\b[^>]*>.*?<(?:\w+:)?{re.escape(child_tag)}\b"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

# -------------------------
# Build a namespace-insensitive relaxed XPath using local-name()
# Example: parts = ['GrpHdr','MsgId'] -> "//*[local-name()='GrpHdr']//*[local-name()='MsgId']"
# or with direct child: "//*[local-name()='GrpHdr']/*[local-name()='MsgId']"
# We'll use descendant search '//' between parts to be tolerant.
# -------------------------
def build_relaxed_localname_xpath(parts):
    if not parts:
        return None
    # create expression like "//*[local-name()='A']//*[local-name()='B']//*[local-name()='C']"
    pieces = ["//*[" + "local-name()='" + p + "'" + "]" for p in parts]
    return "//" + "/".join([p.strip("/*") for p in pieces]).replace("//", "//*")  # ensure starting //

# -------------------------
# DQ existence checker (strict then relaxed)
# Returns dict with exists,parent_exists,in_correct_location,root_missing,reason
# -------------------------
def dq_xpath_exists(xml_str, xpath, ns_map, search_root_localname=None, relaxed_xpath=None):
    """
    - xpath: strict xpath (may include ns: prefixes if ns_map used)
    - relaxed_xpath: precomputed relaxed xpath (local-name based) to use if root missing
    - search_root_localname: the major root localname (e.g. FIToFICstmrCdtTrf) for root detection
    """
    parts = xpath.strip("/").split("/") if xpath else []
    tag = parts[-1] if parts else xpath
    parent = parts[-2] if len(parts) > 1 else None
    major_root = parts[1] if len(parts) > 1 and parts[0].lower() == "document" else (parts[0] if parts else None)

    # 1) Try proper parsing & strict XPath
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        nodes = root.xpath(xpath, namespaces=ns_map)
        if nodes and len(nodes) > 0:
            return {'exists':1, 'parent_exists':1, 'in_correct_location':1, 'root_missing':0, 'reason':'Exact XPath match'}
        # If strict failed, check parent existence and loose search
        parent_exists = 0
        if parent:
            # use local-name search for parent to be namespace-insensitive for existence check
            parent_exists = 1 if root.xpath("//*[local-name()='" + parent + "']", namespaces=ns_map) else 0
        # major root exists?
        major_root_exists = 1 if (major_root and root.xpath("/*[local-name()='" + major_root + "']")) else 0
        # loose search for tag anywhere (namespace-insensitive)
        loose = root.xpath("//*[local-name()='" + tag + "']", namespaces=ns_map)
        if loose:
            return {
                'exists':1,
                'parent_exists': parent_exists,
                'in_correct_location': parent_exists if not STRICT_STRUCTURE else 0,
                'root_missing': 0 if major_root_exists else 1,
                'reason': 'Tag exists but not under expected hierarchy'
            }
        return {'exists':0, 'parent_exists':parent_exists, 'in_correct_location':0, 'root_missing':0 if major_root_exists else 1, 'reason':'Tag not found'}
    except etree.XMLSyntaxError:
        # Malformed -> fallback using regex. Use relaxed_xpath if provided to find nodes by local-name
        raw_exists = fallback_raw_exists(xml_str, tag)
        if not raw_exists:
            # determine parent/major root raw existence
            parent_exists = 1 if (parent and fallback_raw_exists(xml_str, parent)) else (1 if not parent else 0)
            major_root_exists = 1 if (search_root_localname and fallback_raw_exists(xml_str, search_root_localname)) else 0
            return {'exists':0, 'parent_exists':parent_exists, 'in_correct_location':0, 'root_missing':0 if major_root_exists else 1, 'reason':'Tag not found (malformed XML)'}
        parent_exists = 1 if (parent and fallback_raw_exists(xml_str, parent)) else (1 if not parent else 0)
        correct_location = 1 if (parent and fallback_raw_parent_child(xml_str, parent, tag)) else (1 if not parent else 0)
        major_root_exists = 1 if (search_root_localname and fallback_raw_exists(xml_str, search_root_localname)) else 0
        reason = 'Tag found in malformed XML'
        if not correct_location:
            reason = 'Tag found but not under expected parent'
        return {'exists':1, 'parent_exists':1 if parent_exists else 0, 'in_correct_location':1 if correct_location else 0, 'root_missing':0 if major_root_exists else 1, 'reason':reason}
    except Exception as e:
        return {'exists':0, 'parent_exists':0, 'in_correct_location':0, 'root_missing':0, 'reason':f'Error: {str(e)}'}

# -------------------------
# Normalize rule keys (path/xpath and required/minOccurs)
# -------------------------
def normalize_rule(rule):
    path = rule.get("path") or rule.get("xpath") or rule.get("element") or rule.get("field")
    # compute required
    if "required" in rule:
        try:
            required = 1 if int(rule.get("required")) != 0 else 0
        except:
            required = 1 if bool(rule.get("required")) else 0
    elif "minOccurs" in rule:
        try:
            required = 1 if int(rule.get("minOccurs", 0)) > 0 else 0
        except:
            required = 0
    elif "mandatory" in rule:
        required = 1 if rule.get("mandatory") else 0
    else:
        required = 0
    return path, required

# -------------------------
# Adjust XPath when expected root is missing:
# - If expected major root present -> return strict XPath (unchanged)
# - If missing -> return a relaxed, local-name based XPath which searches descendants anywhere
# -------------------------
def adjust_xpath_for_missing_root(root_obj, raw_xml_str, xsd_name, strict_xpath):
    """
    root_obj: parsed root element (or None if not parsed)
    raw_xml_str: sanitized raw XML text
    xsd_name: value from iso_messages.xsd_name (used to lookup expected root)
    strict_xpath: the original XPath (may include ns: prefixes)
    """
    # Determine expected root for this xsd_name
    expected_root = EXPECTED_ROOT_BY_XSD.get(xsd_name)
    # If we don't know expected root, do not adjust (use strict)
    if not expected_root:
        return strict_xpath, False  # False -> root not considered missing (no adjust)

    # Check presence of expected root in parsed root if available
    major_present = False
    try:
        if root_obj is not None:
            # check Document/expected_root existence by local-name
            major_present = True if root_obj.xpath("/*[local-name()='Document']/*[local-name()='" + expected_root + "']") else False
        else:
            # fallback: raw text search
            major_present = True if fallback_raw_exists(raw_xml_str, expected_root) else False
    except Exception:
        major_present = False

    if major_present:
        return strict_xpath, False
    # major root missing -> build relaxed xpath (strip /Document/<expected_root> if present)
    # build parts removing Document and expected_root
    parts = [p for p in strict_xpath.strip("/").split("/") if p and p.lower() != "document" and p != expected_root]
    if not parts:
        return strict_xpath, True
    # create relaxed local-name based xpath
    relaxed = build_relaxed_localname_xpath(parts)
    return relaxed, True

# -------------------------
# Main processing
# -------------------------
def process_all_messages():
    conn = get_connection()
    cur = conn.cursor()

    # create report table if missing
    try:
        cur.execute("""
        BEGIN
            EXECUTE IMMEDIATE '
            CREATE TABLE iso_message_dq_report (
                msg_id VARCHAR2(64) PRIMARY KEY,
                dq_report CLOB,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP
            )';
        EXCEPTION WHEN OTHERS THEN
            IF SQLCODE != -955 THEN RAISE; END IF;
        END;
        """)
        conn.commit()
    except Exception:
        pass

    # Load rules
    rules_map = {}
    cur.execute("SELECT xsd_name, rule_json FROM iso_dq_rules")
    for xsd_name, rule_json in cur.fetchall():
        s = lob_to_str(rule_json)
        try:
            rules_map[xsd_name] = json.loads(s) if s else {}
        except Exception:
            rules_map[xsd_name] = {}

    # Fetch messages
    cur.execute("SELECT msg_id, xml_payload, xsd_name FROM iso_messages")
    processed = 0
    for msg_id, xml_payload, xsd_name in cur.fetchall():
        processed += 1
        try:
            xml_text = lob_to_str(xml_payload)
            xml_text_sanitized = sanitize_xml(xml_text)

            # Attempt repair/parse
            root, status, repaired_xml = repair_and_parse(xml_text_sanitized)
            # pick search XML (prefer repaired textual serialization if available)
            search_xml = repaired_xml if repaired_xml is not None else xml_text_sanitized

            # Build ns_map if parsed root available
            ns_map = {}
            if root is not None:
                ns_uri = root.nsmap.get(None)
                if ns_uri:
                    ns_map = {'ns': ns_uri}

            dq_report = []
            rules_json = rules_map.get(xsd_name, {})
            rules = rules_json.get("rules") if isinstance(rules_json, dict) else None
            if not rules:
                dq_report.append({'error': f'No rules found for XSD {xsd_name}'})
            else:
                for rule in rules:
                    path_raw, required = normalize_rule(rule)
                    if not path_raw:
                        continue

                    # Build strict XPath for evaluation (namespace aware if ns_map exists)
                    if ns_map and not path_raw.startswith("/ns:"):
                        strict_xpath = "/" + "/".join([("ns:" + p) for p in path_raw.strip("/").split("/")])
                    elif not path_raw.startswith("/"):
                        strict_xpath = "/" + path_raw
                    else:
                        strict_xpath = path_raw

                    # Adjust xpath if expected root missing
                    adjusted_xpath, root_was_missing = adjust_xpath_for_missing_root(root, xml_text_sanitized, xsd_name, strict_xpath)

                    # Evaluate existence using dq_xpath_exists
                    res = dq_xpath_exists(search_xml, adjusted_xpath, ns_map, search_root_localname=EXPECTED_ROOT_BY_XSD.get(xsd_name), relaxed_xpath=None)

                    # Determine validity
                    valid = 'ok'
                    if required == 1 and res['exists'] == 0:
                        valid = 'missing'
                    elif required == 1 and res['exists'] == 1 and res['in_correct_location'] == 0 and STRICT_STRUCTURE:
                        valid = 'missing'

                    entry = {
                        'path': path_raw,
                        'required': int(required),
                        'exists': int(res['exists']),
                        'parent_exists': int(res.get('parent_exists', 0)),
                        'in_correct_location': int(res.get('in_correct_location', 0)),
                        'root_missing': int(res.get('root_missing', 0)) or int(root_was_missing),
                        'valid': valid,
                        'reason': res.get('reason')
                    }
                    dq_report.append(entry)

            out = {
                'msg_id': msg_id,
                'xsd_name': xsd_name,
                'xml_repair_status': status,
                'dq_report': dq_report
            }
            out_json = json.dumps(out, ensure_ascii=False)

            # Upsert into report table
            if not DRY_RUN:
                cur.execute("UPDATE iso_message_dq_report SET dq_report = :dq, created_at = SYSTIMESTAMP WHERE msg_id = :mid",
                            dq=out_json, mid=msg_id)
                if cur.rowcount == 0:
                    cur.execute("INSERT INTO iso_message_dq_report (msg_id, dq_report) VALUES (:mid, :dq)",
                                mid=msg_id, dq=out_json)

            if not DRY_RUN and (processed % BATCH_COMMIT == 0):
                conn.commit()

        except Exception as e:
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






#!/usr/bin/env python3
"""
ISO20022 DQ validator (path-tolerant).
- Repairs malformed XML where possible
- Tries exact XPath, then loose search if parent/root missing
- Writes per-message DQ JSON into iso_message_dq_report
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

BATCH_COMMIT = 100
DRY_RUN = False  # True for testing (no DB writes)
STRICT_STRUCTURE = False  # If True, treat missing parent as fail; else tolerant

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
# -------------------------
def sanitize_xml(xml_str):
    if xml_str is None:
        return None
    s = xml_str.lstrip("\ufeff").strip()
    if "<Document" in s:
        start = s.find("<Document")
        end = s.rfind("</Document>")
        if end != -1 and end > start:
            end = end + len("</Document>")
            s = s[start:end]
        else:
            s = s[start:]
    return s

# -------------------------
# LEVEL 2: attempt strict parse, then recover parse if needed
# -------------------------
def repair_and_parse(xml_str):
    if not xml_str:
        return None, "UNRECOVERABLE: empty", None
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        return root, "OK", xml_str
    except etree.XMLSyntaxError:
        pass
    try:
        parser = etree.XMLParser(recover=True, remove_comments=False)
        root = etree.fromstring(xml_str.encode("utf-8"), parser)
        repaired = etree.tostring(root, encoding="unicode")
        return root, "REPAIRED", repaired
    except Exception as e:
        return None, f"UNRECOVERABLE: {str(e)}", None

# -------------------------
# Fallback regex-based checks for malformed XML
# -------------------------
def fallback_raw_exists(xml_str, tag_name):
    if not tag_name:
        return False
    pattern = fr"<(?:\w+:)?{re.escape(tag_name)}\b[^>]*>.*?</(?:\w+:)?{re.escape(tag_name)}>"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

def fallback_raw_parent_child(xml_str, parent_tag, child_tag):
    if not parent_tag or not child_tag:
        return False
    pattern = fr"<(?:\w+:)?{re.escape(parent_tag)}\b[^>]*>.*?<(?:\w+:)?{re.escape(child_tag)}\b"
    return bool(re.search(pattern, xml_str, flags=re.DOTALL | re.IGNORECASE))

# -------------------------
# Path-tolerant DQ checker
# -------------------------
def dq_xpath_exists(xml_str, xpath, ns_map):
    """
    Returns dict:
      exists: 0/1
      parent_exists: 0/1
      in_correct_location: 0/1
      root_missing: 0/1  (if the expected major root element is missing)
      reason: text
    """
    parts = xpath.strip("/").split("/") if xpath else []
    tag = parts[-1] if parts else xpath
    parent = parts[-2] if len(parts) > 1 else None
    # detect major root (first element)
    major_root = parts[0] if len(parts) > 0 else None

    # Try proper parsing & XPath search
    try:
        root = etree.fromstring(xml_str.encode("utf-8"))
        # exact path
        nodes = root.xpath(xpath, namespaces=ns_map)
        if nodes:
            return {'exists':1, 'parent_exists':1, 'in_correct_location':1, 'root_missing':0, 'reason':'Exact XPath match'}
        # exact failed -> check parents and loose presence
        parent_exists = 0
        if parent:
            parent_exists = 1 if root.xpath("//" + parent, namespaces=ns_map) else 0
        # major root exists?
        major_root_exists = 1 if major_root and root.xpath("/*[local-name()='" + major_root + "']") else 0
        # loose search for tag anywhere
        loose = root.xpath("//*[local-name()='" + tag + "']", namespaces=ns_map)
        if loose:
            return {
                'exists':1,
                'parent_exists': parent_exists,
                'in_correct_location': parent_exists if not STRICT_STRUCTURE else 0,
                'root_missing': 0 if major_root_exists else 1,
                'reason': 'Tag exists but not under expected hierarchy'
            }
        # not found anywhere
        return {'exists':0, 'parent_exists':parent_exists, 'in_correct_location':0, 'root_missing':0 if major_root_exists else 1, 'reason':'Tag not found'}
    except etree.XMLSyntaxError:
        # malformed -> fallback regex approach
        raw_exists = fallback_raw_exists(xml_str, tag)
        if not raw_exists:
            parent_exists = 1 if (parent and fallback_raw_exists(xml_str, parent)) else (1 if not parent else 0)
            # check if major root present raw
            major_root_exists = 1 if (major_root and fallback_raw_exists(xml_str, major_root)) else 0
            return {'exists':0, 'parent_exists':parent_exists, 'in_correct_location':0, 'root_missing':0 if major_root_exists else 1, 'reason':'Tag not found (malformed XML)'}
        parent_exists = 1 if (parent and fallback_raw_exists(xml_str, parent)) else (1 if not parent else 0)
        correct_location = 1 if (parent and fallback_raw_parent_child(xml_str, parent, tag)) else (1 if not parent else 0)
        major_root_exists = 1 if (major_root and fallback_raw_exists(xml_str, major_root)) else 0
        reason = 'Tag found in malformed XML'
        if not correct_location:
            reason = 'Tag found but not under expected parent'
        return {'exists':1, 'parent_exists':1 if parent_exists else 0, 'in_correct_location':1 if correct_location else 0, 'root_missing':0 if major_root_exists else 1, 'reason':reason}
    except Exception as e:
        return {'exists':0, 'parent_exists':0, 'in_correct_location':0, 'root_missing':0, 'reason':f'Error: {str(e)}'}

# -------------------------
# Helper: normalize rule keys
# -------------------------
def normalize_rule(rule):
    path = rule.get("path") or rule.get("xpath") or rule.get("element") or rule.get("field")
    # compute required from available keys
    if "required" in rule:
        try:
            required = 1 if int(rule.get("required")) != 0 else 0
        except:
            required = 1 if bool(rule.get("required")) else 0
    elif "minOccurs" in rule:
        try:
            required = 1 if int(rule.get("minOccurs", 0)) > 0 else 0
        except:
            required = 0
    elif "mandatory" in rule:
        required = 1 if rule.get("mandatory") else 0
    else:
        required = 0
    return path, required

# -------------------------
# Main processing
# -------------------------
def process_all_messages():
    conn = get_connection()
    cur = conn.cursor()

    # create report table if missing
    try:
        cur.execute("""
        BEGIN
            EXECUTE IMMEDIATE '
            CREATE TABLE iso_message_dq_report (
                msg_id VARCHAR2(64) PRIMARY KEY,
                dq_report CLOB,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP
            )';
        EXCEPTION WHEN OTHERS THEN
            IF SQLCODE != -955 THEN RAISE; END IF;
        END;
        """)
        conn.commit()
    except Exception:
        pass

    # load rules
    rules_map = {}
    cur.execute("SELECT xsd_name, rule_json FROM iso_dq_rules")
    for xsd_name, rule_json in cur.fetchall():
        s = lob_to_str(rule_json)
        try:
            rules_map[xsd_name] = json.loads(s) if s else {}
        except Exception:
            rules_map[xsd_name] = {}

    # fetch messages
    cur.execute("SELECT msg_id, xml_payload, xsd_name FROM iso_messages")
    processed = 0
    for msg_id, xml_payload, xsd_name in cur.fetchall():
        processed += 1
        try:
            xml_text = lob_to_str(xml_payload)
            xml_text_sanitized = sanitize_xml(xml_text)

            root, status, repaired_xml = repair_and_parse(xml_text_sanitized)

            # build ns_map from repaired or original parse
            ns_map = {}
            if root is not None:
                ns_uri = root.nsmap.get(None)
                if ns_uri:
                    ns_map = {'ns': ns_uri}

            dq_report = []
            # meta
            meta = {'xml_repair_status': status}

            rules_json = rules_map.get(xsd_name, {})
            rules = rules_json.get("rules") if isinstance(rules_json, dict) else None
            if not rules:
                dq_report.append({'error': f'No rules found for XSD {xsd_name}'})
            else:
                for rule in rules:
                    path_raw, required = normalize_rule(rule)
                    if not path_raw:
                        continue
                    # create XPath for evaluation; prefer unprefixed raw path for regex fallback
                    # For namespace-aware XML, prefix with ns:
                    if ns_map and not path_raw.startswith("/ns:"):
                        xpath_eval = "/" + "/".join([("ns:" + p) for p in path_raw.strip("/").split("/")])
                    elif not path_raw.startswith("/"):
                        xpath_eval = "/" + path_raw
                    else:
                        xpath_eval = path_raw

                    search_xml = repaired_xml if repaired_xml is not None else xml_text_sanitized
                    res = dq_xpath_exists(search_xml, xpath_eval, ns_map)

                    valid = 'ok'
                    if required == 1 and res['exists'] == 0:
                        valid = 'missing'
                    elif required == 1 and res['exists'] == 1 and res['in_correct_location'] == 0 and STRICT_STRUCTURE:
                        valid = 'missing'

                    entry = {
                        'path': path_raw,
                        'required': int(required),
                        'exists': int(res['exists']),
                        'parent_exists': int(res.get('parent_exists', 0)),
                        'in_correct_location': int(res.get('in_correct_location', 0)),
                        'root_missing': int(res.get('root_missing', 0)),
                        'valid': valid,
                        'reason': res.get('reason')
                    }
                    dq_report.append(entry)

            out = {
                'msg_id': msg_id,
                'xsd_name': xsd_name,
                'xml_repair_status': status,
                'dq_report': dq_report
            }
            out_json = json.dumps(out, ensure_ascii=False)

            # upsert into report table
            if not DRY_RUN:
                cur.execute("UPDATE iso_message_dq_report SET dq_report = :dq, created_at = SYSTIMESTAMP WHERE msg_id = :mid",
                            dq=out_json, mid=msg_id)
                if cur.rowcount == 0:
                    cur.execute("INSERT INTO iso_message_dq_report (msg_id, dq_report) VALUES (:mid, :dq)",
                                mid=msg_id, dq=out_json)

            if not DRY_RUN and (processed % BATCH_COMMIT == 0):
                conn.commit()

        except Exception as e:
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
                    print("Failed to write error for msg_id", msg_id)
                    print(err_json)
            print("Error processing msg_id", msg_id, ":", str(e))

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