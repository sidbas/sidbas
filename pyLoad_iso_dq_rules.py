import os
import json
import cx_Oracle

DB_USER = "YOUR_USER"
DB_PASS = "YOUR_PASS"
DB_DSN  = "host:1521/service"

INPUT_FOLDER = "./dq_rules"

def load_rules():
    conn = cx_Oracle.connect(DB_USER, DB_PASS, DB_DSN)
    cur  = conn.cursor()

    for fname in sorted(os.listdir(INPUT_FOLDER)):
        if not fname.endswith(".dq.json"):
            continue

        xsd_name = fname.replace(".dq.json", "")
        full_path = os.path.join(INPUT_FOLDER, fname)

        with open(full_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        # Convert metadata dict to rules list
        rules = []
        for path, info in metadata.items():
            rule = {
                "path": path,
                "required": info.get("required", False),
                "datatype": info.get("type"),
                "minOccurs": info.get("minOccurs"),
                "maxOccurs": info.get("maxOccurs"),
                "constraints": info.get("constraints", {})
            }
            rules.append(rule)

        final_json = json.dumps({"rules": rules}, ensure_ascii=False)

        print(f"Loading rules for {xsd_name} ({len(rules)} rules)")

        cur.execute("""
            INSERT INTO iso_dq_rules (xsd_name, rule_json)
            VALUES (:1, :2)
        """, (xsd_name, final_json))

    conn.commit()
    cur.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    load_rules()