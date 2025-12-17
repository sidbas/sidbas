import json
import oracledb
from pymongo import MongoClient, ReplaceOne

# ---------- Oracle connection ----------
oracledb.init_oracle_client()  # optional if Instant Client is configured

ora_conn = oracledb.connect(
    user="ORA_USER",
    password="ORA_PASSWORD",
    dsn="HOST:PORT/SERVICE"
)

ora_cur = ora_conn.cursor()

# ---------- MongoDB connection ----------
mongo = MongoClient("mongodb://localhost:27017")
collection = mongo.mydb.mycollection

# ---------- Oracle query ----------
sql = """
SELECT
    id,
    JSON_OBJECT(
      '_id' VALUE id,
      'data' VALUE XMLTOJSON(
          XMLELEMENT("root", xml_col1, xml_col2)
      )
      RETURNING CLOB
    ) AS json_doc
FROM your_table
"""

ora_cur.execute(sql)

# ---------- Batch insert ----------
batch = []
BATCH_SIZE = 500

for id_val, json_clob in ora_cur:
    doc = json.loads(json_clob.read())  # CLOB → string → dict
    batch.append(
        ReplaceOne(
            {"_id": doc["_id"]},
            doc,
            upsert=True
        )
    )

    if len(batch) >= BATCH_SIZE:
        collection.bulk_write(batch, ordered=False)
        batch.clear()

# Insert remaining
if batch:
    collection.bulk_write(batch, ordered=False)

# ---------- Cleanup ----------
ora_cur.close()
ora_conn.close()
mongo.close()

print("Data successfully loaded into MongoDB")

