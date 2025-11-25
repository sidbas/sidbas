import os
import re
import cx_Oracle

# --------------------------------------------------
# CONFIGURATION — update these for your environment
# --------------------------------------------------
FOLDER = r"C:\ISO\XSD"       # folder containing your .xsd files
DB_USER = "USER"
DB_PASS = "PASSWORD"
DB_DSN  = "HOST/ORCL"        # or HOST:PORT/SERVICE

# --------------------------------------------------
# Connect using cx_Oracle
# --------------------------------------------------
cx_Oracle.init_oracle_client()   # only needed if Instant Client is installed manually
connection = cx_Oracle.connect(DB_USER, DB_PASS, DB_DSN)
cursor = connection.cursor()

# --------------------------------------------------
# Helper: extract targetNamespace from XSD
# --------------------------------------------------
def get_namespace(xsd_text):
    match = re.search(r'targetNamespace="([^"]+)"', xsd_text)
    return match.group(1) if match else None

# --------------------------------------------------
# Helper: parse ISO filename pattern
# Example: pacs.008.001.10.xsd
# --------------------------------------------------
def parse_filename(fname):
    m = re.match(r"([a-zA-Z]+)\.(\d{3})\.(\d{3}\.\d{2})\.xsd", fname)
    if not m:
        return None, None
    msg_family = f"{m.group(1)}.{m.group(2)}"      # pacs.008
    version = m.group(3)                           # 001.10
    return msg_family, version

# --------------------------------------------------
# Main: iterate through all XSD files
# --------------------------------------------------
files_loaded = 0

for file in os.listdir(FOLDER):
    if file.lower().endswith(".xsd"):
        path = os.path.join(FOLDER, file)

        print(f"Processing: {file}")

        # Read local XSD
        with open(path, "r", encoding="utf-8") as f:
            xsd_text = f.read()

        # Extract metadata
        msg_family, version_no = parse_filename(file)
        namespace = get_namespace(xsd_text)

        if msg_family is None:
            print(f"  Skipped — filename does NOT match ISO pattern: {file}")
            continue

        # Insert into Oracle
        cursor.execute("""
            INSERT INTO iso_xsd_repository (
                msg_family,
                version_no,
                schema_namespace,
                file_name,
                xsd_content
            ) VALUES (
                :msg_family,
                :version_no,
                :schema_namespace,
                :file_name,
                XMLTYPE(:xsd_clob)
            )
        """,
        msg_family=msg_family,
        version_no=version_no,
        schema_namespace=namespace,
        file_name=file,
        xsd_clob=xsd_text )

        files_loaded += 1

# Commit
connection.commit()
cursor.close()
connection.close()

print(f"Done! Successfully loaded {files_loaded} XSD files.")




import os
import re
import oracledb

# -------------------------
# CONFIG
# -------------------------
FOLDER = r"C:\ISO\XSD"   # <--- put your folder here
DB_USER = "USER"
DB_PASS = "PASSWORD"
DB_DSN  = "HOST:PORT/SERVICE"

# -------------------------
# CONNECT TO ORACLE
# -------------------------
connection = oracledb.connect(
    user=DB_USER,
    password=DB_PASS,
    dsn=DB_DSN
)
cursor = connection.cursor()

# -------------------------
# Extract namespace from XSD text
# -------------------------
def get_namespace(xsd_text):
    match = re.search(r'targetNamespace="([^"]+)"', xsd_text)
    return match.group(1) if match else None

# -------------------------
# Extract msg_family + version from filename
# Example: pacs.008.001.10.xsd
# -------------------------
def parse_filename(fname):
    # expect: pacs.008.001.10.xsd
    m = re.match(r"([a-zA-Z]+)\.(\d{3})\.(\d{3}\.\d{2})\.xsd", fname)
    if not m:
        return None, None
    msg_family = f"{m.group(1)}.{m.group(2)}"      # pacs.008
    version = m.group(3)                           # 001.10
    return msg_family, version

# -------------------------
# MAIN LOOP
# -------------------------
files_loaded = 0

for file in os.listdir(FOLDER):
    if file.lower().endswith(".xsd"):
        path = os.path.join(FOLDER, file)

        print(f"Loading: {file}")

        # Read file
        with open(path, "r", encoding="utf-8") as f:
            xsd_text = f.read()

        # Parse metadata
        msg_family, version_no = parse_filename(file)
        namespace = get_namespace(xsd_text)

        if msg_family is None:
            print(f"  Skipped (filename does not match expected pattern): {file}")
            continue

        # Insert into Oracle
        cursor.execute("""
            INSERT INTO iso_xsd_repository (
                msg_family,
                version_no,
                schema_namespace,
                file_name,
                xsd_content
            )
            VALUES (
                :msg_family,
                :version_no,
                :schema_namespace,
                :file_name,
                XMLTYPE(:xsd_clob)
            )
        """,
        dict(
            msg_family=msg_family,
            version_no=version_no,
            schema_namespace=namespace,
            file_name=file,
            xsd_clob=xsd_text
        )
        )

        files_loaded += 1

# Commit once
connection.commit()

cursor.close()
connection.close()

print(f"Done! {files_loaded} XSD files loaded into Oracle.")
