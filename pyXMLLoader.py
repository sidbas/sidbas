import os
import cx_Oracle

# --- CONFIG ---
DB_USER = "YOUR_USER"
DB_PASS = "YOUR_PASS"
DB_DSN  = "host:1521/service"

XML_FOLDER = r"C:\Users\your_name\Desktop\ISO_XML"   # <-- change to your folder path
XSD_NAME   = "pacs008"   # default XSD type for the batch (you can override per file)

# ----------------------------------------------
def load_xml_files():
    conn = cx_Oracle.connect(DB_USER, DB_PASS, DB_DSN)
    cur  = conn.cursor()

    for fname in sorted(os.listdir(XML_FOLDER)):
        if not (fname.lower().endswith(".xml")):
            continue

        full_path = os.path.join(XML_FOLDER, fname)

        print(f"Loading: {fname}")

        with open(full_path, "r", encoding="utf-8") as f:
            xml_text = f.read()

        # Insert into table
        cur.execute("""
            INSERT INTO iso_messages (xsd_name, xml_payload)
            VALUES (:1, :2)
        """, (XSD_NAME, xml_text))

    conn.commit()
    cur.close()
    conn.close()
    print("Done inserting XML messages!")

# ----------------------------------------------
if __name__ == "__main__":
    load_xml_files()
