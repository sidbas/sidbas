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