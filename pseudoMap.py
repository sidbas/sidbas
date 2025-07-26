# etl_mapper_app.py

import streamlit as st
import re

st.set_page_config(page_title="ETL Mapping to Pseudocode", layout="wide")
st.title("🛠️ ETL Mapping to Functional Pseudocode")
st.markdown("Paste your plain-English ETL mapping below. This tool converts it into easy-to-understand steps for functional or business users.")


import re

import re

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