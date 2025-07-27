Step 2: Basic Parsing Script

import spacy
from spacy.matcher import Matcher
import re

nlp = spacy.load("en_core_web_sm")

def normalize_quotes(text):
    return text.replace('‘', "'").replace('’', "'").replace('“', '"').replace('”', '"')

def extract_mapping_components(text):
    text = normalize_quotes(text)
    doc = nlp(text)

    target_field = None
    joins = []
    filters = []

    # Patterns for rule-based matching
    join_pattern = re.compile(r"\w+\.\w+\s*=\s*\w+\.\w+")
    filter_pattern = re.compile(r"(=|in\s*\(|like|is\s+null|>|<)", re.IGNORECASE)
    field_pattern = re.compile(r"\w+\.\w+\.\w+")

    for sent in doc.sents:
        sentence = sent.text.strip()

        if "populate" in sentence.lower() and field_pattern.search(sentence):
            match = field_pattern.search(sentence)
            if match:
                target_field = match.group()

        elif join_pattern.search(sentence):
            joins.append(sentence)

        elif filter_pattern.search(sentence):
            filters.append(sentence)

        elif field_pattern.fullmatch(sentence.strip()):
            # Standalone field line (likely target)
            target_field = sentence.strip()

    return {
        "target_field": target_field,
        "joins": joins,
        "filters": filters
    }

Step 3: Output as Clean Pseudocode
def format_pseudocode(mapping):
    output = []

    output.append("📘 Functional Transformation Logic\n")

    if mapping["target_field"]:
        output.append(f"**Target Field**:\n→ `{mapping['target_field']}`\n")
    else:
        output.append("**Target Field**:\n→ ❓ Not found\n")

    if mapping["joins"]:
        output.append("**1. Join Conditions**\nJoin the following tables using these keys:")
        for j in mapping["joins"]:
            output.append(f"- `{j.strip()}`")
        output.append("")

    if mapping["filters"]:
        output.append("**2. Filter Criteria**\nApply the following filters:")
        for f in mapping["filters"]:
            output.append(f"- `{f.strip()}`")
        output.append("")

    output.append("**3. Transformation Rule**\n")
    output.append(f"Lookup and populate the field `{mapping['target_field']}` after applying the join and filter logic.\n")

    return "\n".join(output)
    
Step 1: Modify extract_mapping_components
def extract_mapping_components(text):
    text = normalize_quotes(text)
    doc = nlp(text)

    target_field = None
    joins = []
    filters = []
    default_value = None
    straight_move_source = None

    join_pattern = re.compile(r"\w+\.\w+\s*=\s*\w+\.\w+")
    filter_pattern = re.compile(r"(=|in\s*\(|like|is\s+null|>|<)", re.IGNORECASE)
    field_pattern = re.compile(r"\w+\.\w+\.\w+")

    for sent in doc.sents:
        sentence = sent.text.strip()
        lower = sentence.lower()

        if "default" in lower or "with value" in lower or "set to" in lower:
            # Detect default value
            match = re.search(r"default\s+value\s+['\"]?(\w+)['\"]?", lower)
            if match:
                default_value = match.group(1)
            else:
                const_match = re.findall(r"['\"]([^'\"]+)['\"]", sentence)
                if const_match:
                    default_value = const_match[0]

        elif "from" in lower and "populate" in lower:
            # Detect straight move
            parts = re.findall(r"(\w+\.\w+\.\w+)", sentence)
            if len(parts) >= 2:
                target_field = parts[0]
                straight_move_source = parts[1]

        elif "populate" in lower and field_pattern.search(sentence):
            match = field_pattern.search(sentence)
            if match:
                target_field = match.group()

        elif join_pattern.search(sentence):
            joins.append(sentence)

        elif filter_pattern.search(sentence):
            filters.append(sentence)

        elif field_pattern.fullmatch(sentence.strip()):
            target_field = sentence.strip()

    return {
        "target_field": target_field,
        "joins": joins,
        "filters": filters,
        "default_value": default_value,
        "straight_move_source": straight_move_source
    }
    
Step 2: Update format_pseudocode
def format_pseudocode(mapping):
    output = []
    output.append("📘 Functional Transformation Logic\n")

    tf = mapping.get("target_field")
    if tf:
        output.append(f"**Target Field**:\n→ `{tf}`\n")
    else:
        output.append("**Target Field**:\n→ ❓ Not found\n")

    if mapping.get("default_value"):
        output.append("**1. Default Assignment**")
        output.append(f"Populate `{tf}` with default value: `{mapping['default_value']}`\n")

    elif mapping.get("straight_move_source"):
        output.append("**1. Straight Move**")
        output.append(f"Move value from `{mapping['straight_move_source']}` to `{tf}` with no transformation.\n")

    else:
        if mapping["joins"]:
            output.append("**1. Join Conditions**")
            for j in mapping["joins"]:
                output.append(f"- `{j}`")
            output.append("")

        if mapping["filters"]:
            output.append("**2. Filter Criteria**")
            for f in mapping["filters"]:
                output.append(f"- `{f}`")
            output.append("")

        output.append("**3. Transformation Rule**")
        output.append(f"Lookup and populate the field `{tf}` after applying the join and filter logic.\n")

    return "\n".join(output)

Code Sample: Fallback Strategy
def parse_with_fallback(text):
    # Try spaCy first
    components = extract_mapping_components(text)
    
    # If spaCy fails (e.g., no target or joins or filters), trigger fallback
    if not components.get("target_field") or (
        not components.get("joins") and not components.get("filters") and not components.get("straight_move_source")
    ):
        print("🔁 Fallback to AI due to insufficient rule-based match.")
        return parse_with_ai_fallback(text)  # Defined below

    return components

parse_with_ai_fallback
import openai

def parse_with_ai_fallback(text):
    prompt = f"""
You are an ETL documentation assistant. Given the following mapping, extract structured transformation details.

Mapping:
\"\"\"
{text}
\"\"\"

Return in this format:
Target Field: <field>
Join Conditions:
- <join clause>
Filter Conditions:
- <filter clause>
Transformation:
<plain-English logic>
"""

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )

    content = response.choices[0].message.content
    return parse_ai_response(content)

parse_ai_response
def parse_ai_response(content):
    lines = content.strip().splitlines()
    mapping = {"joins": [], "filters": []}
    
    current_section = None
    for line in lines:
        line = line.strip()
        if line.lower().startswith("target field:"):
            mapping["target_field"] = line.split(":", 1)[1].strip()
        elif line.lower().startswith("join conditions"):
            current_section = "joins"
        elif line.lower().startswith("filter conditions"):
            current_section = "filters"
        elif line.lower().startswith("transformation"):
            current_section = "transformation"
            mapping["transformation"] = ""
        elif current_section == "joins" and line.startswith("-"):
            mapping["joins"].append(line[1:].strip())
        elif current_section == "filters" and line.startswith("-"):
            mapping["filters"].append(line[1:].strip())
        elif current_section == "transformation":
            mapping["transformation"] += line + " "

    return mapping


Step 1: parser.py (spaCy + fallback)
# parser.py

import spacy
import re
import openai
import os

nlp = spacy.load("en_core_web_sm")

# Your OpenAI API Key
openai.api_key = os.getenv("OPENAI_API_KEY")

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

def parse_with_ai_fallback(text):
    prompt = f"""
You are an ETL documentation assistant. Given the following mapping, extract structured transformation details.

Mapping:
\"\"\"
{text}
\"\"\"

Return in this format:
Target Field: <field>
Join Conditions:
- <join clause>
Filter Conditions:
- <filter clause>
Transformation:
<plain-English logic>
"""
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    return parse_ai_response(response.choices[0].message.content)

def parse_ai_response(content):
    lines = content.strip().splitlines()
    mapping = {"joins": [], "filters": []}
    current = None

    for line in lines:
        line = line.strip()
        if line.lower().startswith("target field:"):
            mapping["target_field"] = line.split(":", 1)[1].strip()
        elif line.lower().startswith("join conditions"):
            current = "joins"
        elif line.lower().startswith("filter conditions"):
            current = "filters"
        elif line.lower().startswith("transformation"):
            current = "transformation"
            mapping["transformation"] = ""
        elif current == "joins" and line.startswith("-"):
            mapping["joins"].append(line[1:].strip())
        elif current == "filters" and line.startswith("-"):
            mapping["filters"].append(line[1:].strip())
        elif current == "transformation":
            mapping["transformation"] += line + " "

    return mapping

def parse_with_fallback(text):
    mapping = extract_mapping_components(text)

    # Determine if spaCy failed
    if not mapping.get("target_field") or (
        not mapping.get("joins") and
        not mapping.get("filters") and
        not mapping.get("straight_move_source") and
        not mapping.get("default_value")
    ):
        return parse_with_ai_fallback(text)
    return mapping

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
        logic = mapping.get("transformation", f"Lookup and populate `{tf}` after applying join and filter logic.")
        output.append(logic.strip())

    return "\n".join(output)
    
Step 2: app.py (Streamlit UI)
# app.py

import streamlit as st
from parser import parse_with_fallback, format_pseudocode

st.set_page_config(page_title="ETL Mapping Pseudocode Generator", layout="wide")

st.title("📘 ETL Mapping to Pseudocode")
st.markdown("Paste your **free-text ETL transformation mapping**, and get clean pseudocode. Handles joins, filters, defaults, and straight moves.")

input_text = st.text_area("Paste ETL mapping description here:", height=300)

if st.button("Generate Pseudocode"):
    if not input_text.strip():
        st.warning("Please paste a mapping description to proceed.")
    else:
        with st.spinner("Parsing..."):
            try:
                parsed = parse_with_fallback(input_text)
                result = format_pseudocode(parsed)
                st.markdown(result)
            except Exception as e:
                st.error(f"❌ Failed to parse: {e}")
                
Step 3: requirements.txt
streamlit
spacy
openai

Run this in the terminal to install everything:






