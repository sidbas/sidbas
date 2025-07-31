# ✅ Case 1: when and then on the same line
if "when" in lower and "then" in lower:
    when_then_match = re.match(r"(.*?\bwhen\b.+?\bthen\b)\s+(.+)", line, re.IGNORECASE)
    if when_then_match:
        full = line.replace("‘", "'").replace("’", "'")
        when_part = re.search(r"\bwhen\b\s+(.+?)\s+\bthen\b", full, re.IGNORECASE)
        then_part = re.search(r"\bthen\b\s+(.+)", full, re.IGNORECASE)

        if when_part and then_part:
            condition_text = f"When {when_part.group(1).strip()} → then {then_part.group(1).strip()}"
            mapping["conditions"].append(condition_text)

            # Source field from then
            mapping["source_fields"].append(then_part.group(1).strip())

            # Fields used in condition
            fields = re.findall(r"\b\w+\.\w+(?:\.\w+)?\b", when_part.group(1))
            for f in fields:
                if f not in mapping["condition_fields"]:
                    mapping["condition_fields"].append(f)

            i += 1
            continue

# ✅ Case 2: when and then on separate lines
elif "when" in lower and i + 1 < len(lines) and "then" in lines[i + 1].lower():
    when_clause = line.replace("‘", "'").replace("’", "'")
    then_clause = lines[i + 1].replace("‘", "'").replace("’", "'")
    condition_combined = f"{when_clause} → {then_clause}"
    mapping["conditions"].append(condition_combined)

    # Extract source field from THEN clause
    then_match = re.search(r"\bthen\b\s+([\w\.\[\]]+)", then_clause, re.IGNORECASE)
    if then_match:
        mapping["source_fields"].append(then_match.group(1).strip())

    # Extract condition fields from when_clause
    fields = re.findall(r"\b\w+\.\w+(?:\.\w+)?\b", when_clause)
    for f in fields:
        if f not in mapping["condition_fields"]:
            mapping["condition_fields"].append(f)

    i += 2
    continue


if "when" in lower and i + 1 < len(lines) and "then" in lines[i + 1].lower():
    when_clause = line.replace("‘", "'").replace("’", "'")
    then_clause = lines[i + 1].replace("‘", "'").replace("’", "'")
    condition_combined = f"{when_clause} → {then_clause}"
    mapping["conditions"].append(condition_combined)

    # Extract source field from THEN clause
    then_match = re.search(r"\bthen\b\s+([\w\.\[\]]+)", then_clause, re.IGNORECASE)
    if then_match:
        field = then_match.group(1).strip()
        mapping["source_fields"].append(field)

    # Better regex: extract condition fields like Db1.Tbl1.ServiceId
    fields = re.findall(r"\b\w+\.\w+(?:\.\w+)?\b", when_clause)
    for f in fields:
        if f not in mapping["condition_fields"]:
            mapping["condition_fields"].append(f)

    i += 2
    continue

#Add a condition_fields list in extract_mapping_components()
mapping = {
    "source_fields": [],
    "condition_fields": [],
    "conditions": [],
    "join_tables": [],
    "join_conditions": [],
    "post_processing": []
}

#Extract fields from when clauses
#Update this logic inside the main loop:
if "when" in lower and i + 1 < len(lines) and "then" in lines[i + 1].lower():
    when_clause = line
    then_clause = lines[i + 1]
    condition_combined = f"{when_clause} → {then_clause}"
    mapping["conditions"].append(condition_combined)

    # 🔹 Extract source field from THEN clause
    then_match = re.search(r"\bthen\b\s+([\w\.\[\]]+)", then_clause, re.IGNORECASE)
    if then_match:
        mapping["source_fields"].append(then_match.group(1).strip())

    # 🔹 Extract all possible field references in the WHEN clause
    fields = re.findall(r"\b\w+\.\w+\b", when_clause)
    for f in fields:
        if f not in mapping["condition_fields"]:
            mapping["condition_fields"].append(f)

    i += 2
    continue

#Update format_pseudocode() to include it
#Add this block just after source fields:
if mapping.get("condition_fields"):
    output.append(f"\n🔹 **Condition Fields Used:**")
    for cf in mapping["condition_fields"]:
        output.append(f"- `{cf}`")


if mapping.get("source_fields"):
    output.append(f"🔷 **Source Fields Involved:**")
    for sf in mapping["source_fields"]:
        output.append(f"- `{sf}`")

#Updated Code Block for Table Collection:
elif "by joining" in lower:
    collecting_joins = False
    i += 1
    # Collect table names from subsequent lines
    while i < len(lines):
        next_line = lines[i].strip()
        if next_line.lower().startswith("based on the join conditions") or not next_line:
            break
        tables = re.findall(r"\b\w+\.\w+\b", next_line)
        mapping["join_tables"].extend(tables)
        i += 1
    continue

#Updated extract_mapping_components() for Multi-Condition Mappings
import re

def extract_mapping_components(text: str) -> dict:
    mapping = {
        "target_field": "",  # Optional: leave blank if multiple
        "conditions": [],
        "join_tables": [],
        "join_conditions": [],
        "post_processing": []
    }

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    collecting_joins = False
    i = 0

    while i < len(lines):
        line = lines[i].strip()
        lower = line.lower()

        # Check for "when...then..." split across 2 lines
        if "when" in lower and i + 1 < len(lines) and "then" in lines[i + 1].lower():
            when_clause = line
            then_clause = lines[i + 1]
            condition_combined = f"{when_clause} → {then_clause}"
            mapping["conditions"].append(condition_combined)

            # Optional: set target_field from 'then' if only one found
            if not mapping["target_field"]:
                then_match = re.search(r"\bthen\b\s+([\w\.\[\]]+)", then_clause, re.IGNORECASE)
                if then_match:
                    mapping["target_field"] = then_match.group(1).strip()

            i += 2
            continue

        # Join tables
        elif "by joining" in lower:
            tables = re.findall(r"\b\w+\.\w+\b", line)
            mapping["join_tables"].extend(tables)
            collecting_joins = True

        # Start of join conditions
        elif "based on the join conditions" in lower:
            collecting_joins = True
            i += 1
            continue

        # Join condition lines
        elif collecting_joins and "=" in line and "." in line:
            mapping["join_conditions"].append(line)

        # Post-processing step
        elif re.search(r"\b(trim|format|cast|clean|convert|uppercase)\b", lower):
            mapping["post_processing"].append(line)

        i += 1

    return mapping

#Update format_pseudocode() to Display Multiple Conditions
def format_pseudocode(mapping: dict) -> str:
    output = []

    if mapping.get("target_field"):
        output.append(f"🔷 **Sample Target Field:** `{mapping['target_field']}`\n")

    if mapping.get("conditions"):
        output.append("\n🔸 **Conditional Logic**")
        for cond in mapping["conditions"]:
            output.append(f"- {cond}")

    if mapping.get("join_tables"):
        output.append("\n🔸 **Join Tables**")
        for table in mapping["join_tables"]:
            output.append(f"- {table}")

    if mapping.get("join_conditions"):
        output.append("\n🔸 **Join Conditions**")
        for cond in mapping["join_conditions"]:
            output.append(f"- {cond}")

    if mapping.get("post_processing"):
        output.append("\n🔸 **Post-Processing Steps**")
        for step in mapping["post_processing"]:
            output.append(f"- {step}")

    return "\n".join(output)


import re

def extract_mapping_components(text: str) -> dict:
    mapping = {
        "target_field": "",
        "conditions": [],
        "join_tables": [],
        "join_conditions": [],
        "post_processing": []
    }

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    collecting_joins = False

    for line in lines:
        lower = line.lower()

        # Extract Target Field (after 'then' clause)
        if re.search(r"\bthen\b\s+\w+", lower):
            match = re.search(r"\bthen\b\s+([\w\.\[\]]+)", line, re.IGNORECASE)
            if match:
                mapping["target_field"] = match.group(1).strip()

        # Conditional logic like: when X then Y
        if re.search(r"\bwhen\b", lower):
            mapping["conditions"].append(line)

        # Join table list: "By joining: Db1.Tbl1, Db1.Tbl2"
        elif "by joining" in lower:
            tables = re.findall(r"\b\w+\.\w+\b", line)
            mapping["join_tables"].extend(tables)
            collecting_joins = True  # flag to start collecting join conditions

        # Join condition heading
        elif "based on the join conditions" in lower:
            collecting_joins = True
            continue

        # Join conditions: A.col = B.col
        elif collecting_joins and "=" in line and "." in line:
            mapping["join_conditions"].append(line)

        # Post-processing logic
        elif re.search(r"\b(format|uppercase|trim|cast|convert|clean)\b", lower):
            mapping["post_processing"].append(line)

    return mapping


1. In extract_mapping_components()
elif re.search(r"\bwhen\b.*\bthen\b", lower):
    conditions.append(line)

elif "by joining" in lower:
    join_tables = re.findall(r"\b\w+\.\w+\b", line)
    for jt in join_tables:
        joins.append(jt)

elif "based on the join conditions" in lower:
    continue  # just a heading, skip it

elif "=" in line and "." in line:  # likely a join condition
    joins.append(line)

2. In format_pseudocode():
if mapping.get("join_tables"):
    output.append("\n🔸 **Join Tables**")
    for jt in mapping["join_tables"]:
        output.append(f"- {jt}")

if mapping.get("join_conditions"):
    output.append("\n🔸 **Join Conditions**")
    for jc in mapping["join_conditions"]:
        output.append(f"- {jc}")


import streamlit as st
from parser import extract_mapping_components, format_pseudocode  # your functions

st.title("ETL Mapping to Pseudocode Generator")

user_input = st.text_area("Paste your mapping description here:")

if st.button("Generate Pseudocode"):
    if user_input.strip():
        mapping = extract_mapping_components(user_input)
        pseudocode_output = format_pseudocode(mapping)

        # ✅ Display with Markdown formatting
        st.markdown(pseudocode_output)
    else:
        st.warning("Please enter mapping text.")

def format_pseudocode(mapping):
    tf = mapping.get("target_field", "❓ Not found")
    output = [f"🔷 **Target Field:** `{tf}`\n"]

    if mapping.get("default_value"):
        output.append("\n🔸 **Default Assignment**")
        output.append(f"- Set `{tf}` to default value: `{mapping['default_value']}`")

    elif mapping.get("straight_move_source"):
        output.append("\n🔸 **Direct Assignment**")
        output.append(f"- Assign `{tf}` = `{mapping['straight_move_source']}`")

    if mapping.get("lookup_field"):
        output.append("\n🔸 **Lookup Logic**")
        output.append(f"- Lookup `{mapping['lookup_field']}` and assign to `{tf}`")

    if mapping.get("transformation"):
        output.append("\n🔸 **Transformation Logic**")
        output.append(f"- {mapping['transformation']}")

    if mapping.get("conditions"):
        output.append("\n🔸 **Conditional Logic**")
        for cond in mapping["conditions"]:
            output.append(f"- {cond}")

    if mapping.get("post_processing"):
        output.append("\n🔸 **Post-Processing Steps**")
        for step in mapping["post_processing"]:
            output.append(f"- {step}")

    if mapping.get("joins"):
        output.append("\n🔸 **Join Conditions**")
        for j in mapping["joins"]:
            output.append(f"- {j}")

    if mapping.get("filters"):
        output.append("\n🔸 **Filter Criteria**")
        for f in mapping["filters"]:
            output.append(f"- {f}")

    return "\n".join(output)



def format_pseudocode(mapping):
    tf = mapping.get("target_field", "❓ Not found")
    output = [f"🔷 **Target Field:** `{tf}`\n"]

    if mapping.get("default_value"):
        output.append("🔸 **Default Assignment**")
        output.append(f"Set `{tf}` to default value: `{mapping['default_value']}`")

    elif mapping.get("straight_move_source"):
        output.append("🔸 **Direct Assignment**")
        output.append(f"Assign `{tf}` = `{mapping['straight_move_source']}`")

    if mapping.get("lookup_field"):
        output.append("🔸 **Lookup Logic**")
        output.append(f"Lookup `{mapping['lookup_field']}` and assign to `{tf}`")

    if mapping.get("transformation"):
        output.append("🔸 **Transformation Logic**")
        output.append(mapping["transformation"])

    if mapping.get("conditions"):
        output.append("🔸 **Conditional Logic**")
        for cond in mapping["conditions"]:
            output.append(f"- {cond}")

    if mapping.get("post_processing"):
        output.append("🔸 **Post-Processing Steps**")
        for step in mapping["post_processing"]:
            output.append(f"- {step}")

    if mapping.get("joins"):
        output.append("🔸 **Join Conditions**")
        for j in mapping["joins"]:
            output.append(f"- {j}")

    if mapping.get("filters"):
        output.append("🔸 **Filter Criteria**")
        for f in mapping["filters"]:
            output.append(f"- {f}")

    return "\n".join(output)



def extract_mapping_components(text):
    text = normalize_quotes(text)
    doc = nlp(text)

    target_field = None
    joins = []
    filters = []
    default_value = None
    straight_move_source = None
    lookup_field = None
    transformation = None
    conditions = []
    post_processing_steps = []

    field_pattern = re.compile(r"\b\w+\.\w+\.\w+\b")
    condition_pattern = re.compile(r"\b(=|in\s*\(|like|is\s+null|>|<)\b", re.IGNORECASE)
    join_pattern = re.compile(r"\b\w+\.\w+\s*=\s*\w+\.\w+\b", re.IGNORECASE)
    default_pattern = re.compile(r"default|with value", re.IGNORECASE)

    lines = [sent.text.strip() for sent in doc.sents if sent.text.strip()]

    for line in lines:
        lower = line.lower()

        if "populate" in lower and field_pattern.search(line):
            target_field = field_pattern.search(line).group()

        elif default_pattern.search(lower):
            value_match = re.findall(r"['\"](.*?)['\"]", line)
            if value_match:
                default_value = value_match[0]

        elif "from" in lower and "to" in lower:
            matches = field_pattern.findall(line)
            if len(matches) >= 2:
                straight_move_source = matches[0]
                target_field = matches[1]

        elif "lookup" in lower:
            fields = field_pattern.findall(line)
            if fields:
                lookup_field = fields[-1]

        elif join_pattern.search(line):
            joins.append(line)

        elif condition_pattern.search(line):
            filters.append(line)

        elif "transform" in lower or "logic" in lower:
            transformation = line

        elif "if" in lower and "then" in lower:
            conditions.append(line)

        elif any(word in lower for word in ["format", "trim", "cast", "convert", "concatenate"]):
            post_processing_steps.append(line)

    return {
        "target_field": target_field,
        "joins": joins,
        "filters": filters,
        "default_value": default_value,
        "straight_move_source": straight_move_source,
        "lookup_field": lookup_field,
        "transformation": transformation,
        "conditions": conditions,
        "post_processing": post_processing_steps,
    }



import streamlit as st
from parser import extract_mapping_components, format_pseudocode

st.title("ETL Mapping → Functional Pseudocode Generator")

user_input = st.text_area("Paste your mapping description here:")

if st.button("Generate Pseudocode"):
    if user_input.strip():
        mapping = extract_mapping_components(user_input)
        pseudocode = format_pseudocode(mapping)
        st.markdown(pseudocode)
    else:
        st.warning("Please enter a mapping description.")


# parser.py

import spacy
import re

# Use lightweight spaCy model and add sentence detection
nlp = spacy.blank("en")
nlp.add_pipe("sentencizer")

def normalize_quotes(text):
    return text.replace("‘", "'").replace("’", "'").replace("“", '"').replace("”", '"')

def extract_mapping_components(text):
    text = normalize_quotes(text)
    doc = nlp(text)

    target_field = None
    joins = []
    filters = []
    default_value = None
    straight_move_source = None
    lookup_field = None
    transformation = None

    field_pattern = re.compile(r"\b\w+\.\w+\.\w+\b")
    condition_pattern = re.compile(r"\b(=|in\s*\(|like|is\s+null|>|<)\b", re.IGNORECASE)
    join_pattern = re.compile(r"\b\w+\.\w+\s*=\s*\w+\.\w+\b", re.IGNORECASE)
    default_pattern = re.compile(r"default|with value", re.IGNORECASE)

    lines = [sent.text.strip() for sent in doc.sents if sent.text.strip()]

    for line in lines:
        lower = line.lower()

        if "populate" in lower and field_pattern.search(line):
            target_field = field_pattern.search(line).group()

        elif default_pattern.search(lower):
            value_match = re.findall(r"['\"](.*?)['\"]", line)
            if value_match:
                default_value = value_match[0]

        elif "from" in lower and "to" in lower:
            matches = field_pattern.findall(line)
            if len(matches) >= 2:
                straight_move_source = matches[0]
                target_field = matches[1]

        elif "lookup" in lower:
            fields = field_pattern.findall(line)
            if fields:
                lookup_field = fields[-1]

        elif join_pattern.search(line):
            joins.append(line)

        elif condition_pattern.search(line):
            filters.append(line)

        elif "transform" in lower or "logic" in lower:
            transformation = line

    return {
        "target_field": target_field,
        "joins": joins,
        "filters": filters,
        "default_value": default_value,
        "straight_move_source": straight_move_source,
        "lookup_field": lookup_field,
        "transformation": transformation
    }

def format_pseudocode(mapping):
    tf = mapping.get("target_field", "❓ Not found")
    output = [f"🔷 **Target Field:** `{tf}`\n"]

    # 1. Default
    if mapping.get("default_value"):
        output.append("🔸 **Default Assignment**")
        output.append(f"Set `{tf}` to default value: `{mapping['default_value']}`")

    # 2. Straight Move
    elif mapping.get("straight_move_source"):
        output.append("🔸 **Direct Assignment**")
        output.append(f"Assign `{tf}` = `{mapping['straight_move_source']}`")

    # 3. Lookup
    elif mapping.get("lookup_field"):
        output.append("🔸 **Lookup Logic**")
        output.append(f"Lookup `{mapping['lookup_field']}` and assign to `{tf}`")

    # 4. Transformation logic
    if mapping.get("transformation"):
        output.append("🔸 **Transformation Logic**")
        output.append(mapping["transformation"])

    # 5. Joins
    if mapping.get("joins"):
        output.append("🔸 **Join Conditions**")
        for j in mapping["joins"]:
            output.append(f"- {j}")

    # 6. Filters
    if mapping.get("filters"):
        output.append("🔸 **Filter Criteria**")
        for f in mapping["filters"]:
            output.append(f"- {f}")

    return "\n".join(output)



import spacy

# Temporary fallback without full model
nlp = spacy.blank("en")  # Limited NLP (no POS tagging, no sentence parsing)



import spacy

try:
    nlp = spacy.load("en_core_web_sm")
    print("✅ spaCy model loaded successfully.")
except OSError:
    print("❌ spaCy model 'en_core_web_sm' is not installed.")


# parser.py (OpenAI removed)

import spacy
import re

nlp = spacy.load("en_core_web_sm")

def normalize_quotes(text):
    return text.replace("‘", "'").replace("’", "'").replace("“", '"').replace("”", '"')

def extract_mapping_components(text):
    text = normalize_quotes(text)
    doc = nlp(text)

    target_field = None
    joins, filters = [], []
    default_value, straight_move_source = None, None

    join_pattern = re.compile(r"\w+\.\w+\s*=\s*\w+\.\w+")
    filter_pattern = re.compile(r"(=|in\s*\(|like|is\s+null|>|<)", re.IGNORECASE)
    field_pattern = re.compile(r"\w+\.\w+\.\w+")

    for sent in doc.sents:
        s = sent.text.strip()
        lower = s.lower()

        if "default" in lower or "with value" in lower:
            consts = re.findall(r"['\"]([^'\"]+)['\"]", s)
            if consts:
                default_value = consts[0]

        elif "from" in lower and "populate" in lower:
            parts = re.findall(field_pattern, s)
            if len(parts) >= 2:
                target_field = parts[0]
                straight_move_source = parts[1]

        elif "populate" in lower and field_pattern.search(s):
            match = field_pattern.search(s)
            if match:
                target_field = match.group()

        elif join_pattern.search(s):
            joins.append(s)

        elif filter_pattern.search(s):
            filters.append(s)

        elif field_pattern.fullmatch(s.strip()):
            target_field = s.strip()

    return {
        "target_field": target_field,
        "joins": joins,
        "filters": filters,
        "default_value": default_value,
        "straight_move_source": straight_move_source
    }

def parse_with_fallback(text):
    return extract_mapping_components(text)

def format_pseudocode(mapping):
    output = ["📘 Functional Transformation Logic\n"]
    tf = mapping.get("target_field", "❓ Not found")
    output.append(f"**Target Field**:\n→ `{tf}`\n")

    if mapping.get("default_value"):
        output.append("**1. Default Assignment**")
        output.append(f"Populate `{tf}` with default value: `{mapping['default_value']}`\n")

    elif mapping.get("straight_move_source"):
        output.append("**1. Straight Move**")
        output.append(f"Move value from `{mapping['straight_move_source']}` to `{tf}` with no transformation.\n")

    else:
        if mapping.get("joins"):
            output.append("**1. Join Conditions**")
            for j in mapping["joins"]:
                output.append(f"- `{j}`")

        if mapping.get("filters"):
            output.append("\n**2. Filter Criteria**")
            for f in mapping["filters"]:
                output.append(f"- `{f}`")

        output.append("\n**3. Transformation Rule**")
        output.append(f"Lookup and populate `{tf}` after applying join and filter logic.")

    return "\n".join(output)




# etl_mapper_app.py

import streamlit as st
import re

st.set_page_config(page_title="ETL Mapping to Pseudocode", layout="wide")
st.title("🛠️ ETL Mapping to Functional Pseudocode")
st.markdown("Paste your plain-English ETL mapping below. This tool converts it into easy-to-understand steps for functional or business users.")


import re

import re

import re

def generate_professional_pseudocode(text):
    from textwrap import indent
    text = normalize_text(text)
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]

    joins = []
    filters = []
    target_field = None

    in_join_block = False
    in_filter_block = False

    for line in lines:
        l = line.lower()

        if "by joining" in l:
            in_join_block = True
            in_filter_block = False
            continue
        elif any(kw in l for kw in ["when", "where", "based on the conditions", "if"]):
            in_filter_block = True
            in_join_block = False
            if any(op in line.lower() for op in ["=", " in ", " like ", " is null", ">", "<"]):
                filters.append(line)
            continue
        elif "populate" in l:
            in_filter_block = False
            in_join_block = False
            continue

        if "=" in line or " in " in l or " like " in l or " is null" in l:
            if bool(re.search(r"\w+\.\w+\s*=\s*\w+\.\w+", line)):
                if in_join_block or not in_filter_block:
                    joins.append(line)
                else:
                    filters.append(line)
            else:
                filters.append(line)
        elif not target_field and re.match(r"^\w+\.\w+\.\w+$", line):
            target_field = line

    # Build output
    output = []

    output.append("📘 Functional Transformation Logic\n")

    if target_field:
        output.append(f"**Target Field**:\n→ `{target_field}`\n")
    else:
        output.append("**Target Field**:\n→ ❓ Not found\n")

    if joins:
        output.append("**1. Join Conditions**\n")
        output.append("Join the following tables using these keys:\n")
        for j in joins:
            output.append(f"- `{j}`")
        output.append("")

    if filters:
        output.append("**2. Filter Criteria**\n")
        output.append("Apply the following filters:\n")
        for f in filters:
            output.append(f"- `{f}`")
        output.append("")

    output.append("**3. Transformation Rule**\n")
    output.append(f"Lookup and populate the field `{target_field}` after applying the join and filter logic.\n")

    return "\n".join(output)



def normalize_text(text):
    # Replace curly quotes with straight quotes
    replacements = {
        '‘': "'", '’': "'",
        '“': '"', '”': '"'
    }
    for fancy, normal in replacements.items():
        text = text.replace(fancy, normal)
    return text

def generate_functional_pseudocode(text):
    if not text or not isinstance(text, str):
        return "⚠️ Invalid or empty mapping"

    text = normalize_text(text)
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
    
    joins = []
    filters = []
    target_field = None

    in_join_block = False
    in_filter_block = False

    for line in lines:
        l = line.lower()

        # Detect section headers
        if "by joining" in l:
            in_join_block = True
            in_filter_block = False
            continue
        elif any(kw in l for kw in ["when", "where", "based on the conditions", "if"]):
            in_filter_block = True
            in_join_block = False
            # Don't skip the line itself — treat as a filter
            if any(op in line.lower() for op in ["=", " in ", " like ", " is null", ">", "<"]):
                filters.append(line)
            continue
        elif "populate" in l:
            in_filter_block = False
            in_join_block = False
            continue

        # Classify line content
        if "=" in line or " in " in l or " like " in l or " is null" in l:
            if bool(re.search(r"\w+\.\w+\s*=\s*\w+\.\w+", line)):  # Looks like a join
                if in_join_block or not in_filter_block:
                    joins.append(line)
                else:
                    filters.append(line)
            else:
                filters.append(line)
        elif not target_field and re.match(r"^\w+\.\w+\.\w+$", line):
            target_field = line

    # Format output
    output = []
    step = 1

    if joins:
        output.append(f"{step}. Join tables on:")
        for j in joins:
            output.append(f"   - {j}")
        step += 1

    if filters:
        output.append(f"{step}. Apply filter conditions:")
        for f in filters:
            output.append(f"   - {f}")
        step += 1

    if target_field:
        output.append(f"{step}. Lookup and populate:")
        output.append(f"   - Target field: {target_field}")
    else:
        output.append(f"{step}. Target field: ❓ Not found")

    return "\n".join(output)



def generate_functional_pseudocode(text):
    if not text or not isinstance(text, str):
        return "⚠️ Invalid or empty mapping"

    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
    
    joins = []
    filters = []
    target_field = None

    in_join_block = False
    in_filter_block = False

    for line in lines:
        l = line.lower()

        # Detect section starts
        if "by joining" in l:
            in_join_block = True
            in_filter_block = False
            continue
        elif any(kw in l for kw in ["when", "where", "based on the conditions", "if"]):
            in_filter_block = True
            in_join_block = False
            continue
        elif "populate" in l:
            in_filter_block = False
            in_join_block = False
            continue

        # Default detection when no headers are found
        if "=" in line or " in " in l or " like " in l or " is null" in l:
            # Heuristic: if line contains multiple table aliases → treat as join
            if bool(re.search(r"\w+\.\w+\s*=\s*\w+\.\w+", line)):
                if in_join_block or not in_filter_block:
                    joins.append(line)
                else:
                    filters.append(line)
            else:
                filters.append(line)
        # Capture target field (e.g., Db1.Tbl1.Field)
        elif not target_field and re.match(r"^\w+\.\w+\.\w+$", line):
            target_field = line

    # Generate pseudocode
    output = []
    step = 1

    if joins:
        output.append(f"{step}. Join tables on:")
        for j in joins:
            output.append(f"   - {j}")
        step += 1

    if filters:
        output.append(f"{step}. Apply filter conditions:")
        for f in filters:
            output.append(f"   - {f}")
        step += 1

    if target_field:
        output.append(f"{step}. Lookup and populate:")
        output.append(f"   - Target field: {target_field}")
    else:
        output.append(f"{step}. Target field: ❓ Not found")

    return "\n".join(output)
    

def generate_functional_pseudocode(text):
    if not text or not isinstance(text, str):
        return "⚠️ Invalid or empty mapping"

    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
    
    joins = []
    filters = []
    target_field = None

    # Flags to track current parsing section
    in_join_block = False
    in_filter_block = False

    for line in lines:
        l = line.lower()

        # Detect start of JOIN or FILTER sections
        if "by joining" in l:
            in_join_block = True
            in_filter_block = False
            continue
        elif "when" in l or "based on the conditions" in l:
            in_filter_block = True
            in_join_block = False
            continue
        elif "populate" in l and not target_field:
            in_filter_block = False
            in_join_block = False
            continue  # Let the field line be picked below

        # Extract join conditions
        if in_join_block and "=" in line:
            joins.append(line)

        # Extract filter conditions
        elif in_filter_block and any(op in line.lower() for op in ["=", " in(", " in (", "like", "<", ">", "between"]):
            filters.append(line)

        # Pick target field
        elif not target_field and re.match(r"^\w+\.\w+\.\w+$", line):
            target_field = line

    # Build output
    output = []
    step = 1

    if joins:
        output.append(f"{step}. Join tables on:")
        for j in joins:
            output.append(f"   - {j}")
        step += 1

    if filters:
        output.append(f"{step}. Apply filter conditions:")
        for f in filters:
            output.append(f"   - {f}")
        step += 1

    if target_field:
        output.append(f"{step}. Lookup and populate:")
        output.append(f"   - Target field: {target_field}")
    else:
        output.append(f"{step}. Target field: ❓ Not found")

    return "\n".join(output)


import re

def generate_functional_pseudocode(text):
    if not text or not isinstance(text, str):
        return "⚠️ Invalid or empty mapping"

    text = text.strip()
    lines = text.splitlines()
    output = []

    # 1. Detect joins
    join_lines = [line.strip() for line in lines if "join" in line.lower() or "=" in line.lower()]
    join_conditions = [line for line in join_lines if re.search(r'=\s*', line)]

    if join_conditions:
        output.append("1. Join tables on:")
        for jc in join_conditions:
            output.append(f"   - {jc.strip()}")

    # 2. Detect filter conditions
    condition_lines = [line.strip() for line in lines if "when" in line.lower() or "and" in line.lower()]
    condition_lines = [line for line in condition_lines if any(op in line for op in ["=", "in", "IN"])]

    if condition_lines:
        output.append("2. Apply filter conditions:")
        for cond in condition_lines:
            output.append(f"   - {cond.strip()}")

    # 3. Detect target field to populate
    populate_line = next((line for line in lines if "populate" in line.lower()), None)
    target_field_line = next((line for line in lines if re.match(r"\w+\.\w+\.\w+", line.strip())), None)

    if target_field_line:
        output.append("3. Lookup and populate:")
        output.append(f"   - Target field: {target_field_line.strip()}")

    if not output:
        return f"⚠️ Cannot parse this mapping: {text[:60]}..."

    return "\n".join(output)



def generate_functional_pseudocode(mapping_text: str) -> str:
    lines = []

    # --- Join ---
    join_match = re.search(
        r"Populate\s+(\S+)\s+as\s+(\S+)\s+using\s+(\S+)\s+by\s+Joining\s+(.+)",
        mapping_text, re.IGNORECASE)
    if join_match:
        src_col, tgt_name, src_table, join_cond = join_match.groups()
        left, right = map(str.strip, join_cond.split("="))
        right_table = right.split(".")[0]
        lines.append(f"1. Start with data from `{src_table}`.")
        lines.append(f"2. Join `{src_table}` with `{right_table}`")
        lines.append(f"   - Match where `{left}` = `{right}`.")
        lines.append(f"3. Take `{src_col}` and rename it as `{tgt_name}`.")
        return "\n".join(lines)

    # --- Lookup ---
    lookup_match = re.search(
        r"Lookup\s+(\S+)\s+from\s+(\S+)\s+using\s+(\S+)(?:,\s*default\s+to\s+(.+))?",
        mapping_text, re.IGNORECASE)
    if lookup_match:
        src_col, ref_table, ref_key, default = lookup_match.groups()
        lines.append(f"1. Lookup value from `{ref_table}` using `{ref_key}` to match `{src_col}`.")
        if default:
            lines.append(f"2. If no match is found, use default value `{default.strip()}`.")
        return "\n".join(lines)

    # --- Type Conversion ---
    convert_match = re.search(r"Convert\s+(\S+)\s+to\s+(Decimal|Date|Integer|String)(.*)?", mapping_text, re.IGNORECASE)
    if convert_match:
        col, target_type, detail = convert_match.groups()
        lines.append(f"1. Convert `{col}` to `{target_type.upper()}{detail or ''}`.")
        return "\n".join(lines)

    # --- Conditional Logic ---
    if_match = re.search(r"If\s+(.+?),\s*set\s+(\S+)\s+to\s+(\S+),\s*else\s+(\S+)", mapping_text, re.IGNORECASE)
    if if_match:
        condition, tgt_col, val_true, val_false = if_match.groups()
        lines.append(f"1. If `{condition}`, set `{tgt_col}` to `{val_true}`.")
        lines.append(f"2. Otherwise, set `{tgt_col}` to `{val_false}`.")
        return "\n".join(lines)

    # --- Rename ---
    rename_match = re.search(r"Populate\s+(\S+)\s+as\s+(\S+)", mapping_text, re.IGNORECASE)
    if rename_match:
        src_col, tgt_name = rename_match.groups()
        lines.append(f"1. Take value from `{src_col}` and rename it to `{tgt_name}`.")
        return "\n".join(lines)

    # --- Filter ---
    filter_match = re.search(r"Include only records where (.+)", mapping_text, re.IGNORECASE)
    if filter_match:
        condition = filter_match.group(1)
        lines.append(f"1. Keep only records where `{condition}`.")
        return "\n".join(lines)

    # --- Math Calculation ---
    calc_match = re.search(r"Set\s+(\S+)\s*=\s*(\S+)\s*([\+\-\*/])\s*(\S+)", mapping_text, re.IGNORECASE)
    if calc_match:
        tgt_col, left, op, right = calc_match.groups()
        ops = {'+': 'plus', '-': 'minus', '*': 'times', '/': 'divided by'}
        lines.append(f"1. Calculate `{left}` {ops[op]} `{right}`.")
        lines.append(f"2. Store result in `{tgt_col}`.")
        return "\n".join(lines)

    # --- Aggregation ---
    agg_match = re.search(r"Group by\s+(\S+)\s+and\s+(sum|avg|count|min|max)\s+(\S+)", mapping_text, re.IGNORECASE)
    if agg_match:
        group_col, agg_fn, target_col = agg_match.groups()
        lines.append(f"1. Group records by `{group_col}`.")
        lines.append(f"2. For each group, compute `{agg_fn.upper()}` of `{target_col}`.")
        return "\n".join(lines)

    # --- Date Difference ---
    datediff_match = re.search(r"Calculate days between (\S+) and (\S+)", mapping_text, re.IGNORECASE)
    if datediff_match:
        start, end = datediff_match.groups()
        lines.append(f"1. Calculate the number of days between `{start}` and `{end}`.")
        return "\n".join(lines)

    return "⚠️ Could not parse this mapping. Try rephrasing or use a supported pattern."

# ---- Streamlit UI ---- #
sample = """Populate Source_Table.Column_1 as Acct_Id using Source_Table by Joining Source_Table.Column_Id = Reference_Table.Column_Id
Lookup Customer_Id from dim_customer using customer_key, default to -1
Convert Order_Date to Date
If Status = 'Active', set Flag to 1, else 0
Set Discount = Price * 0.1
Group by Customer_ID and sum Total_Sales
Calculate days between Start_Date and End_Date
Include only records where Country = 'US'
"""

input_text = st.text_area("✏️ Mapping Instructions:", sample, height=250)

if st.button("Generate Pseudocode"):
    st.subheader("🧾 Functional Pseudocode")
    for i, line in enumerate(input_text.strip().splitlines(), 1):
        if line.strip():
            st.markdown(f"**Mapping {i}:**")
            st.code(generate_functional_pseudocode(line), language="text")


import oracledb  # or cx_Oracle
import pandas as pd

# --- Connect to Oracle ---
connection = oracledb.connect(
    user="your_user",
    password="your_pass",
    dsn="your_host:1521/your_service"
)

cursor = connection.cursor()

# --- Fetch CLOB mapping descriptions ---
cursor.execute("SELECT mapping_id, description FROM etl_mapping")
rows = cursor.fetchall()

# --- Reuse the pseudocode generator logic ---
def generate_functional_pseudocode(mapping_text: str) -> str:
    # Insert same pattern-matching logic here from the Streamlit version
    if "lookup" in mapping_text.lower():
        return "1. Lookup value from reference table."
    elif "join" in mapping_text.lower():
        return "1. Join tables on specified keys."
    elif "convert" in mapping_text.lower():
        return "1. Convert field to required format."
    return "⚠️ Could not parse mapping."

# --- Process each row ---
results = []
for mapping_id, clob_text in rows:
    try:
        pseudocode = generate_functional_pseudocode(clob_text)
    except Exception as e:
        pseudocode = f"Error: {e}"
    results.append({
        "mapping_id": mapping_id,
        "original": clob_text,
        "pseudocode": pseudocode
    })

df = pd.DataFrame(results)
df.to_excel("etl_mappings_pseudocode.xlsx", index=False)
print("✅ Saved to etl_mappings_pseudocode.xlsx")

# (Optional) Write back to Oracle
# cursor.execute("ALTER TABLE etl_mapping ADD (pseudocode CLOB)")
# for row in results:
#     cursor.execute("UPDATE etl_mapping SET pseudocode = :1 WHERE mapping_id = :2", [row['pseudocode'], row['mapping_id']])
# connection.commit()

cursor.close()
connection.close()