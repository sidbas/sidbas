import json
import oracledb
from pymongo import MongoClient, ReplaceOne
from datetime import datetime

# ---------- Connections ----------
ora = oracledb.connect(
    user="ORA_USER",
    password="ORA_PWD",
    dsn="HOST:PORT/SERVICE"
)
cur = ora.cursor()

mongo = MongoClient("mongodb://localhost:27017")
col = mongo["mongo12345_ODS_POC"]["camt053_entries"]

# ---------- Query ----------
cur.execute("""
SELECT
  MsgId,
  StmtId,
  IBAN,
  AcctCcy,
  Amt,
  AmtCcy,
  DbCrInd,
  BookDt,
  ValDt,
  ntry_json
FROM camt053_entry_view
""")

ops = []
BATCH = 1000

for r in cur:
    (msg_id, stmt_id, iban, acct_ccy,
     amt, amt_ccy, dbcr, book_dt, val_dt, ntry_clob) = r

    raw = json.loads(ntry_clob.read())

    doc = {
        "_id": f"{msg_id}|{stmt_id}|{raw.get('NtryRef', '')}",
        "msg_id": msg_id,
        "stmt_id": stmt_id,
        "acct": {
            "iban": iban,
            "ccy": acct_ccy
        },
        "entry": {
            "amount": amt,
            "ccy": amt_ccy,
            "db_cr_ind": dbcr,
            "booking_date": book_dt,
            "value_date": val_dt
        },
        "raw": raw,
        "load_ts": datetime.utcnow().isoformat()
    }

    ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))

    if len(ops) >= BATCH:
        col.bulk_write(ops, ordered=False)
        ops.clear()

if ops:
    col.bulk_write(ops, ordered=False)

cur.close()
ora.close()
mongo.close()

-----------------------------




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

