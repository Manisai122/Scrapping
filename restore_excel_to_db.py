import os
import io
import re
import boto3
import pandas as pd
import psycopg2
import numpy as np
from psycopg2.extras import execute_batch

# ===============================
# CONFIG
# ===============================
S3_BUCKET = "mbf-los-stg"

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ===============================
# CLIENTS
# ===============================
s3 = boto3.client("s3")

def db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# ===============================
# NORMALIZATION
# ===============================
def normalize(col):
    return re.sub(r"[^a-z0-9]", "", col.lower())

COLUMN_PRIORITY = {
    "branch_name": ["branchname", "branch", "branchnm"],
    "ifsc_code": ["ifsc", "ifscode"],
    "address": ["address", "branchaddress"],
    "city1": ["city", "district", "place", "location", "town"],
    "city2": ["centre", "center"],
    "state": ["state"],
    "std_code": ["stdcode", "std", "areacode"],
    "phone": ["phone", "telephone", "contact"],
}

LIMITS = {
    "bank_name": 100,
    "branch_name": 100,
    "ifsc_code": 20,
    "address": 100,
    "city1": 50,
    "city2": 50,
    "state": 50,
    "std_code": 50,
    "phone": 20,
}

DB_COLUMNS = [
    "bank_name",
    "branch_name",
    "ifsc_code",
    "address",
    "city1",
    "city2",
    "state",
    "std_code",
    "phone",
]

# ===============================
# HELPERS
# ===============================
def clean_digits(val, limit):
    if val is None or pd.isna(val):
        return ""
    return re.sub(r"\D", "", str(val))[:limit]

def truncate(val, limit):
    if val is None or pd.isna(val):
        return ""
    return str(val).strip()[:limit]

# ===============================
# LOAD EXCEL
# ===============================
def load_excel_from_s3(s3_path):
    key = s3_path.replace(f"s3://{S3_BUCKET}/", "")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_excel(io.BytesIO(obj["Body"].read()), dtype=str)

# ===============================
# INSERT SAFE
# ===============================
def insert_rows_safe(rows):
    if not rows:
        return 0

    conn = db_conn()
    cur = conn.cursor()

    sql = """
    INSERT INTO master_bank_details (
        bank_name,
        branch_name,
        ifsc_code,
        address,
        city1,
        city2,
        state,
        std_code,
        phone
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    execute_batch(cur, sql, rows, page_size=1000)
    conn.commit()

    inserted = cur.rowcount
    cur.close()
    conn.close()

    return inserted

# ===============================
# MAIN
# ===============================
def restore():
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT bank_name, s3_path
        FROM bank_data
        WHERE processed = true
          AND s3_path IS NOT NULL
    """)
    records = cur.fetchall()
    cur.close()
    conn.close()

    print(f"📂 Found {len(records)} Excel files")

    total = 0

    for bank_name, s3_path in records:
        try:
            print(f"⬇️ Processing: {bank_name}")

            df = load_excel_from_s3(s3_path)
            df.columns = [normalize(c) for c in df.columns]

            final = pd.DataFrame()
            final["bank_name"] = bank_name

            for target, sources in COLUMN_PRIORITY.items():
                for src in sources:
                    if src in df.columns:
                        final[target] = df[src]
                        break
                if target not in final:
                    final[target] = ""

            final = final.replace({np.nan: ""})

            # Auto-fill city2 if missing
            final["city2"] = final.apply(
                lambda r: r["city2"] if r["city2"] else r["city1"],
                axis=1
            )

            # Clean + truncate
            for col in DB_COLUMNS:
                if col in ["phone", "std_code"]:
                    final[col] = final[col].apply(lambda x: clean_digits(x, LIMITS[col]))
                else:
                    final[col] = final[col].apply(lambda x: truncate(x, LIMITS[col]))

            # Only branch is mandatory
            final = final[final["branch_name"] != ""]

            rows = final[DB_COLUMNS].values.tolist()
            inserted = insert_rows_safe(rows)

            total += inserted
            print(f"✅ Processed {inserted} rows")

        except Exception as e:
            print(f"❌ Failed {bank_name}: {e}")

    print(f"\n🎉 DONE — Total rows processed: {total}")

if __name__ == "__main__":
    restore()

