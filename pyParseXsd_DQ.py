#!/usr/bin/env python3
"""
Parse XSD files (ISO20022) and produce JSON metadata for DQ rules.

Outputs a JSON file per XSD, and prints a summary.

Requirements:
  pip install lxml
  (optional) pip install cx_Oracle  -- if parsing from Oracle table

Author: ChatGPT (GPT-5 Thinking mini)
"""

import os
import re
import json
import argparse
from collections import OrderedDict
from lxml import etree

# XML Schema namespace
XSD_NS = "http://www.w3.org/2001/XMLSchema"
NSMAP = {"xs": XSD_NS}

# Utility: safe get attribute with default
def _attr(el, name, default=None):
    v = el.get(name)
    return default if v is None else v

# Normalize maxOccurs text
def norm_maxocc(val):
    if val is None:
        return 1
    if val == "unbounded":
        return -1
    try:
        return int(val)
    except:
        return -1

# Parse simpleType restriction -> dict of constraints
def parse_simpletype_constraints(simpleTypeEl):
    constraints = {}
    # look for xs:restriction
    restr = simpleTypeEl.find("xs:restriction", namespaces=NSMAP)
    if restr is None:
        # could be xs:list or xs:union (rare for ISO)
        return constraints

    base = _attr(restr, "base")
    if base:
        constraints["base"] = base

    enums = []
    for e in restr.findall("xs:enumeration", namespaces=NSMAP):
        v = e.get("value")
        if v is not None:
            enums.append(v)
    if enums:
        constraints["enumeration"] = enums

    pat = restr.find("xs:pattern", namespaces=NSMAP)
    if pat is not None and pat.get("value"):
        constraints["pattern"] = pat.get("value")

    length = restr.find("xs:length", namespaces=NSMAP)
    if length is not None and length.get("value"):
        constraints["length"] = int(length.get("value"))

    minlen = restr.find("xs:minLength", namespaces=NSMAP)
    if minlen is not None and minlen.get("value"):
        constraints["minLength"] = int(minlen.get("value"))

    maxlen = restr.find("xs:maxLength", namespaces=NSMAP)
    if maxlen is not None and maxlen.get("value"):
        constraints["maxLength"] = int(maxlen.get("value"))

    minincl = restr.find("xs:minInclusive", namespaces=NSMAP)
    if minincl is not None and minincl.get("value"):
        constraints["minInclusive"] = minincl.get("value")

    maxincl = restr.find("xs:maxInclusive", namespaces=NSMAP)
    if maxincl is not None and maxincl.get("value"):
        constraints["maxInclusive"] = maxincl.get("value")

    return constraints

# Resolve a named type (simple or complex) by QName local name within schema element
def find_named_type(schema_root, type_local_name, kind="complexType"):
    # type_local_name may include prefix (like ns:Type). We only match local part
    if ":" in type_local_name:
        type_local = type_local_name.split(":")[-1]
    else:
        type_local = type_local_name

    xpath = f"xs:{kind}[@name='{type_local}']"
    res = schema_root.find(xpath, namespaces=NSMAP)
    return res

# Recursively process complexType (sequence/choice/all) and return list of element descriptors
def process_complex_type(schema_root, complexEl, path_prefix, metadata, parent_types_stack):
    """
    schema_root: root <xs:schema>
    complexEl: <xs:complexType ...> element (can be named or anonymous)
    path_prefix: current xpath prefix (like 'Document/GrpHdr')
    metadata: dict to append to
    parent_types_stack: list of type names to avoid recursion
    """
    # handle attributes on complex type
    for attr in complexEl.findall("xs:attribute", namespaces=NSMAP):
        name = _attr(attr, "name")
        if not name:
            continue
        xpath = f"{path_prefix}/@{name}"
        md = {
            "path": xpath,
            "kind": "attribute",
            "type": _attr(attr, "type"),
            "use": _attr(attr, "use", "optional")
        }
        metadata[xpath] = md

    # sequences, choices, all
    for model in ("xs:sequence", "xs:choice", "xs:all"):
        for modelEl in complexEl.findall(model, namespaces=NSMAP):
            for child in modelEl.findall("xs:element", namespaces=NSMAP):
                process_element(schema_root, child, path_prefix, metadata, parent_types_stack, in_choice = (model.endswith("choice")))

    # Also check direct element children (sometimes complexType has element directly)
    for child in complexEl.findall("xs:element", namespaces=NSMAP):
        process_element(schema_root, child, path_prefix, metadata, parent_types_stack, in_choice=False)

# Process an xs:element
def process_element(schema_root, elementEl, path_prefix, metadata, parent_types_stack, in_choice=False):
    # Determine name (or ref)
    ref = elementEl.get("ref")
    if ref:
        # if ref used, local name is after colon
        name = ref.split(":")[-1]
    else:
        name = elementEl.get("name")

    if not name:
        return

    minocc = int(elementEl.get("minOccurs")) if elementEl.get("minOccurs") is not None else 1
    maxocc = norm_maxocc(elementEl.get("maxOccurs"))

    xpath = f"{path_prefix}/{name}" if path_prefix else name

    # Prepare metadata base
    md = OrderedDict()
    md["path"] = xpath
    md["minOccurs"] = minocc
    md["maxOccurs"] = (None if maxocc == -1 else maxocc)
    md["required"] = (minocc > 0)
    md["inChoice"] = bool(in_choice)

    # If the element has a type attribute
    type_attr = elementEl.get("type")
    if type_attr:
        md["type"] = type_attr
        # try to resolve named simpleType or complexType
        # check simpleType
        simple = find_named_type(schema_root, type_attr, kind="simpleType")
        complex_ = find_named_type(schema_root, type_attr, kind="complexType")
        if simple is not None:
            md["kind"] = "simple"
            md["constraints"] = parse_simpletype_constraints(simple)
        elif complex_ is not None:
            md["kind"] = "complex"
            # avoid recursive loop
            type_local = type_attr.split(":")[-1]
            if type_local in parent_types_stack:
                md["note"] = f"recursion detected for type {type_local}"
            else:
                parent_types_stack.append(type_local)
                process_complex_type(schema_root, complex_, xpath, metadata, parent_types_stack)
                parent_types_stack.pop()
        else:
            md["kind"] = "simple"  # assume built-in simple type (xs:string etc.)
    else:
        # Check inline simpleType
        simple_inline = elementEl.find("xs:simpleType", namespaces=NSMAP)
        if simple_inline is not None:
            md["kind"] = "simple"
            md["constraints"] = parse_simpletype_constraints(simple_inline)
        else:
            # Check inline complexType
            complex_inline = elementEl.find("xs:complexType", namespaces=NSMAP)
            if complex_inline is not None:
                md["kind"] = "complex"
                process_complex_type(schema_root, complex_inline, xpath, metadata, parent_types_stack)
            else:
                # element references or missing type -> might be an element ref to global element; try to resolve
                # try finding a global element with this name
                ge = schema_root.find(f"xs:element[@name='{name}']", namespaces=NSMAP)
                if ge is not None and ge is not elementEl:
                    # avoid infinite loop
                    # fallback: if ge has type, process type
                    t = ge.get("type")
                    if t:
                        md["type"] = t
                        simple = find_named_type(schema_root, t, kind="simpleType")
                        complex_ = find_named_type(schema_root, t, kind="complexType")
                        if simple is not None:
                            md["kind"] = "simple"
                            md["constraints"] = parse_simpletype_constraints(simple)
                        elif complex_ is not None:
                            md["kind"] = "complex"
                            type_local = t.split(":")[-1]
                            if type_local not in parent_types_stack:
                                parent_types_stack.append(type_local)
                                process_complex_type(schema_root, complex_, xpath, metadata, parent_types_stack)
                                parent_types_stack.pop()
                        else:
                            md["kind"] = "simple"
                    else:
                        md["kind"] = "unknown"
                        md["note"] = "global element reference without explicit type"
                else:
                    md["kind"] = "simple"
    # record element-level documentation (if any)
    doc = None
    ann = elementEl.find("xs:annotation/xs:documentation", namespaces=NSMAP)
    if ann is not None and ann.text:
        doc = ann.text.strip()
        md["documentation"] = doc

    # Add to metadata if this entry is an actual element (we always add)
    # For complex types, child elements are added as separate keys
    metadata[xpath] = md

# Parse a schema file (lxml etree) and produce metadata dict
def parse_schema(tree, base_filename=None, schema_map=None, follow_includes=True):
    """
    tree: lxml parsed xml tree (ElementTree)
    base_filename: used to resolve includes/imports
    schema_map: dict to avoid re-parsing included/imported schemas {location: tree}
    follow_includes: if True, attempts to read included/imported schemas from local folder
    """
    root = tree.getroot()
    # build a map of global named complex/simple types for quick lookup (we use find_named_type function)
    metadata = OrderedDict()

    # Try to find the root element name (global element named Document or similar)
    # We'll iterate global xs:element elements
    globals_elems = root.findall("xs:element", namespaces=NSMAP)
    if not globals_elems:
        # maybe xsi prefix is different; fallback
        globals_elems = root.findall(".//{http://www.w3.org/2001/XMLSchema}element")

    for ge in globals_elems:
        # only consider top-level elements (those with parent xs:schema)
        parent = ge.getparent()
        if parent is not None and parent.tag == f"{{{XSD_NS}}}schema":
            # top-level element
            name = ge.get("name")
            if not name:
                continue
            # start recursion
            process_element(root, ge, "", metadata, parent_types_stack=[])
    # Optionally: parse xs:include and xs:import to merge referenced schemas
    if follow_includes:
        for inc in root.findall("xs:include", namespaces=NSMAP) + root.findall("xs:import", namespaces=NSMAP):
            schema_loc = inc.get("schemaLocation")
            if schema_loc:
                # try to read local file relative to base_filename
                if base_filename:
                    base_dir = os.path.dirname(base_filename)
                    candidate = os.path.join(base_dir, schema_loc)
                    if os.path.exists(candidate):
                        if schema_map is None:
                            schema_map = {}
                        if candidate not in schema_map:
                            try:
                                t = etree.parse(candidate)
                                schema_map[candidate] = t
                                # recursively parse included schema and merge metadata (prefix with nothing: global names unique)
                                submeta = parse_schema(t, base_filename=candidate, schema_map=schema_map, follow_includes=False)
                                # merge (but do not override existing keys)
                                for k,v in submeta.items():
                                    if k not in metadata:
                                        metadata[k] = v
                            except Exception as e:
                                metadata[f"_include_error_{schema_loc}"] = {"note": f"failed to load include/import '{schema_loc}': {str(e)}"}
                    else:
                        metadata[f"_include_missing_{schema_loc}"] = {"note": f"schemaLocation '{schema_loc}' not found relative to {base_filename}"}
                else:
                    metadata[f"_include_ref_{schema_loc}"] = {"note": f"schemaLocation '{schema_loc}' present but base file unknown"}
    return metadata

# Helper: pretty-print and save JSON
def save_metadata_json(metadata, outpath):
    with open(outpath, "w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2, ensure_ascii=False)

# Load from folder
def parse_folder(folder, outdir):
    stats = {"files": 0, "parsed": 0, "errors": 0}
    for fname in sorted(os.listdir(folder)):
        if not fname.lower().endswith(".xsd"):
            continue
        stats["files"] += 1
        path = os.path.join(folder, fname)
        try:
            tree = etree.parse(path)
            metadata = parse_schema(tree, base_filename=path)
            # save json file named after xsd
            outname = os.path.splitext(fname)[0] + ".dq.json"
            save_metadata_json(metadata, os.path.join(outdir, outname))
            print(f"Parsed {fname} -> {outname} ({len(metadata)} entries)")
            stats["parsed"] += 1
        except Exception as e:
            print(f"ERROR parsing {fname}: {e}")
            stats["errors"] += 1
    return stats

# Load from Oracle iso_xsd_repository (requires cx_Oracle)
def parse_from_db(db_user, db_pass, db_dsn, outdir, table="iso_xsd_repository"):
    try:
        import cx_Oracle
    except Exception:
        raise RuntimeError("cx_Oracle not installed. Install with: pip install cx_Oracle")
    conn = cx_Oracle.connect(db_user, db_pass, db_dsn)
    cur = conn.cursor()
    cur.execute(f"SELECT xsd_id, file_name, xsd_content FROM {table} ORDER BY xsd_id")
    rows = cur.fetchall()
    stats = {"rows": len(rows), "parsed": 0, "errors": 0}
    for xsd_id, file_name, xsd_content in rows:
        try:
            # xsd_content may be cx_Oracle LOB or string
            if hasattr(xsd_content, "read"):
                xsd_text = xsd_content.read()
            else:
                xsd_text = str(xsd_content)
            tree = etree.fromstring(xsd_text.encode("utf-8"))
            # lxml expects Element, turn into ElementTree
            tree_et = etree.ElementTree(tree)
            metadata = parse_schema(tree_et, base_filename=None)
            outname = f"{xsd_id}_{os.path.splitext(file_name)[0]}.dq.json"
            save_metadata_json(metadata, os.path.join(outdir, outname))
            print(f"Parsed DB row {xsd_id} ({file_name}) -> {outname} ({len(metadata)} entries)")
            stats["parsed"] += 1
        except Exception as e:
            print(f"ERROR parsing DB row {xsd_id} ({file_name}): {e}")
            stats["errors"] += 1
    cur.close()
    conn.close()
    return stats

# CLI
def main():
    p = argparse.ArgumentParser(description="Parse XSD files into DQ metadata JSON.")
    p.add_argument("--folder", help="Folder containing .xsd files to parse (local mode)")
    p.add_argument("--outdir", help="Folder to write JSON outputs", default="./dq_rules")
    p.add_argument("--db", action="store_true", help="Read XSDs from iso_xsd_repository in Oracle DB")
    p.add_argument("--db-user", help="DB user (for --db)")
    p.add_argument("--db-pass", help="DB pass (for --db)")
    p.add_argument("--db-dsn", help="DB dsn (for --db), e.g. host:1521/service")
    args = p.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    if args.db:
        if not (args.db_user and args.db_pass and args.db_dsn):
            print("DB mode requires --db-user, --db-pass and --db-dsn")
            return
        stats = parse_from_db(args.db_user, args.db_pass, args.db_dsn, args.outdir)
        print("DB parse stats:", stats)
    else:
        if not args.folder:
            print("Local folder mode requires --folder <path>")
            return
        stats = parse_folder(args.folder, args.outdir)
        print("Folder parse stats:", stats)

if __name__ == "__main__":
    main()

