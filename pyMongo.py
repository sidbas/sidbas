#database.py
from pymongo import MongoClient
from app.config import MONGO_URI, MONGO_DB

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

def get_collection(name: str):
    return db[name]

#models.py
from pydantic import BaseModel
from typing import Optional

class Entry(BaseModel):
    msg_id: str
    stmt_id: str
    iban: str
    booking_date: str
    amount: float
    currency: str
    db_cr_ind: str


#entries.py
from fastapi import APIRouter, Query
from app.database import get_collection
from app.models import Entry

router = APIRouter()
col = get_collection("camt053_entries")

@router.get("/entries", response_model=list[Entry])
def get_entries(
    iban: str = Query(...),
    from_date: str = Query(..., regex=r"\d{4}-\d{2}-\d{2}"),
    to_date: str = Query(..., regex=r"\d{4}-\d{2}-\d{2}")
):
    cursor = col.find(
        {
            "acct.iban": iban,
            "entry.booking_date": {
                "$gte": from_date,
                "$lte": to_date
            }
        },
        {
            "_id": 0,
            "msg_id": 1,
            "stmt_id": 1,
            "acct.iban": 1,
            "entry.booking_date": 1,
            "entry.amount": 1,
            "entry.ccy": 1,
            "entry.db_cr_ind": 1
        }
    )

    return [
        Entry(
            msg_id=d["msg_id"],
            stmt_id=d["stmt_id"],
            iban=d["acct"]["iban"],
            booking_date=d["entry"]["booking_date"],
            amount=d["entry"]["amount"],
            currency=d["entry"]["ccy"],
            db_cr_ind=d["entry"]["db_cr_ind"]
        )
        for d in cursor
    ]


#main.py
from fastapi import FastAPI
from app.routes.entries import router as entries_router

app = FastAPI(
    title="CAMT.053 API",
    description="Transaction access API",
    version="1.0"
)

app.include_router(entries_router)


#config.py
ORACLE_DSN = "HOST:PORT/SERVICE"
ORACLE_USER = "ORA_USER"
ORACLE_PWD = "ORA_PWD"

MONGO_URI = "mongodb://host1:27017,host2:27017/?replicaSet=rs0"
MONGO_DB = "mongo12345_ODS_POC"
MONGO_COLLECTION = "camt053_entries"

BATCH_SIZE = 1000
LOG_FILE = "logs/camt053_loader.log"

#loader.py
import json
import logging
from pathlib import Path
from datetime import datetime

import oracledb
from pymongo import MongoClient, ReplaceOne, errors

import config

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
logging.basicConfig(
    filename=config.LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def clob_to_str(val):
    """Handle CLOB or str safely."""
    if val is None:
        return None
    if hasattr(val, "read"):
        return val.read()
    return val

def load_sql(name):
    path = Path("sql") / name
    return path.read_text(encoding="utf-8")

# ------------------------------------------------------------
# Connections
# ------------------------------------------------------------
logging.info("Starting CAMT.053 load")

ora = oracledb.connect(
    user=config.ORACLE_USER,
    password=config.ORACLE_PWD,
    dsn=config.ORACLE_DSN
)
cur = ora.cursor()

mongo = MongoClient(config.MONGO_URI)
col = mongo[config.MONGO_DB][config.MONGO_COLLECTION]

# ------------------------------------------------------------
# Execute SQL
# ------------------------------------------------------------
sql = load_sql("camt053_entries.sql")
cur.execute(sql)

ops = []
row_count = 0
inserted = 0
errors_count = 0

# ------------------------------------------------------------
# Main loop
# ------------------------------------------------------------
for row in cur:
    row_count += 1

    try:
        (
            msg_id,
            stmt_id,
            iban,
            acct_ccy,
            amt,
            amt_ccy,
            dbcr,
            book_dt,
            val_dt,
            ntry_json
        ) = row

        raw = json.loads(clob_to_str(ntry_json))

        # Composite _id (stable + unique)
        ntry_ref = raw.get("NtryRef", row_count)
        doc_id = f"{msg_id}|{stmt_id}|{ntry_ref}"

        doc = {
            "_id": doc_id,
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

        ops.append(
            ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
        )

        if len(ops) >= config.BATCH_SIZE:
            result = col.bulk_write(ops, ordered=False)
            inserted += result.upserted_count + result.modified_count
            ops.clear()

    except Exception as e:
        errors_count += 1
        logging.error("Row %s failed: %s", row_count, e, exc_info=True)

# ------------------------------------------------------------
# Final flush
# ------------------------------------------------------------
if ops:
    result = col.bulk_write(ops, ordered=False)
    inserted += result.upserted_count + result.modified_count

# ------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------
cur.close()
ora.close()
mongo.close()

logging.info("Finished CAMT.053 load")
logging.info("Rows read     : %s", row_count)
logging.info("Docs upserted : %s", inserted)
logging.info("Errors        : %s", errors_count)

print("Load complete")
print(f"Rows read     : {row_count}")
print(f"Docs upserted : {inserted}")
print(f"Errors        : {errors_count}")


# ------------------------------------------------------------

#============================
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

