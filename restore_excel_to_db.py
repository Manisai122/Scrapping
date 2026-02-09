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
S3_PREFIX = "bank_data/"  

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
def normalize(col: str) -> str:
    # Normalize to lowercase snake-ish: BANK_NAME -> bank_name, IFSC CODE -> ifsc_code
    return re.sub(r"[^a-z0-9_]", "", str(col).strip().lower().replace(" ", "_"))


COLUMN_PRIORITY = {
    "bank_name":   ["bank_name", "bank", "bankname"],
    "branch_name": ["branch_name", "branchname", "branch", "branchnm"],
    "ifsc_code":   ["ifsc_code", "ifsc", "ifscode"],
    "address":     ["address", "branchaddress"],
    "city1":       ["city1", "city_1", "city", "district", "place", "location", "town"],
    "city2":       ["city2", "city_2", "centre", "center"],
    "state":       ["state"],
    "std_code":    ["std_code", "stdcode", "std", "areacode"],
    "phone":       ["phone", "phone_no", "phoneno", "telephone", "contact"],
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
    cleaned = re.sub(r"\D", "", str(val))
    return cleaned[:limit]

def truncate(val, limit):
    if val is None or pd.isna(val):
        return ""
    return str(val).strip()[:limit]

def list_bank_folders():
    """
    Returns: ["Woori_Bank", "Natwest_Markets_PLC", ...]
    """
    result = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX,
        Delimiter="/"
    )
    return [p["Prefix"].split("/")[1] for p in result.get("CommonPrefixes", [])]

def latest_excel_for_bank(bank_folder: str):
    """
    Picks latest .xlsx/.xls file under bank_data/<bank_folder>/
    """
    result = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=f"{S3_PREFIX}{bank_folder}/"
    )
    files = [
        obj for obj in result.get("Contents", [])
        if obj.get("Key", "").lower().endswith((".xlsx", ".xls"))
    ]
    if not files:
        return None
    latest = max(files, key=lambda x: x["LastModified"])
    return f"s3://{S3_BUCKET}/{latest['Key']}"

def load_excel_from_s3(s3_path: str) -> pd.DataFrame:
    key = s3_path.replace(f"s3://{S3_BUCKET}/", "")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    df = pd.read_excel(io.BytesIO(obj["Body"].read()), dtype=str)
    df.columns = [normalize(c) for c in df.columns]
    return df

def map_columns(df: pd.DataFrame, bank_folder_name: str) -> pd.DataFrame:
    """
    Create a normalized dataframe with DB_COLUMNS using COLUMN_PRIORITY.
    IMPORTANT: No IFSC prefix checks, no IFSC mutation, no bank_name derivation from IFSC.
    """
    out = pd.DataFrame(index=df.index)

    # Map all targets from priorities
    for target, sources in COLUMN_PRIORITY.items():
        for src in sources:
            if src in df.columns:
                out[target] = df[src]
                break
        if target not in out:
            out[target] = None

    # Fallback for bank_name ONLY if missing entirely/blank in file
    if out["bank_name"].isna().all() or (out["bank_name"].fillna("").str.strip() == "").all():
        out["bank_name"] = bank_folder_name.replace("_", " ").strip()

    # Replace NaN with None
    out = out.replace({np.nan: None})

    # Clean/truncate
    for col in DB_COLUMNS:
        if col in ["phone", "std_code"]:
            out[col] = out[col].apply(lambda x: clean_digits(x, LIMITS[col]))
        else:
            out[col] = out[col].apply(lambda x: truncate(x, LIMITS[col]))

    # IFSC cleanup (NO PREFIX VALIDATION)
    # ---------- FIX MISSING BRANCH NAME ----------
    out["branch_name"] = out["branch_name"].apply(
        lambda x: None if x is None or str(x).strip() == "" else x
    )

    out.loc[out["branch_name"].isna(), "branch_name"] = (
        out.loc[out["branch_name"].isna(), "address"]
    )

    out.loc[out["branch_name"].isna(), "branch_name"] = (
        out.loc[out["branch_name"].isna(), "city1"]
    )

    out.loc[out["branch_name"].isna(), "branch_name"] = "MAIN BRANCH"


    # Drop only truly invalid rows (Rule 4)
    out = out[
        out["ifsc_code"].notna() &
        (out["ifsc_code"] != "") 
    ]

    # De-dup in-file
    out = out.drop_duplicates(subset=["ifsc_code", "branch_name"])

    return out[DB_COLUMNS]

# ===============================
# INSERT SAFE
# ===============================
def insert_rows_safe(rows):
    if not rows:
        return 0

    conn = db_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO master_bank_details_copy (
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
        ON CONFLICT (ifsc_code, branch_name)
        DO UPDATE SET
            bank_name = EXCLUDED.bank_name,
            address   = EXCLUDED.address,
            city1     = EXCLUDED.city1,
            city2     = EXCLUDED.city2,
            state     = EXCLUDED.state,
            std_code  = EXCLUDED.std_code,
            phone     = EXCLUDED.phone;
    """
    print(f"💾 Inserting {len(rows)} rows into the database.")
    execute_batch(cur, sql, rows, page_size=1000)
    conn.commit()

    
    cur.close()
    conn.close()
    return len(rows)

# ===============================
# REMOVE DUPLICATES IN DB
# ===============================
def remove_duplicates():
    """
    Keep one row per (ifsc_code, branch_name).
    NOTE: Your previous query kept ONLY duplicates and deleted the rest (buggy).
    This one keeps the first row and deletes extra duplicates.
    """
    conn = db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            WITH ranked AS (
              SELECT ctid,
                     ROW_NUMBER() OVER (PARTITION BY ifsc_code, branch_name ORDER BY ctid) AS rn
              FROM master_bank_details_copy
            )
            DELETE FROM master_bank_details_copy
            WHERE ctid IN (SELECT ctid FROM ranked WHERE rn > 1);
        """)
        conn.commit()
        print("✅ Duplicates removed successfully.")
    except Exception as e:
        print(f"❌ Failed to remove duplicates: {e}")
    finally:
        cur.close()
        conn.close()

# ===============================
# MAIN
# ===============================
def restore():
    # Step 1: remove duplicates only (DO NOT mutate IFSC codes)
    remove_duplicates()

    bank_folders = list_bank_folders()
    print(f"📂 Found {len(bank_folders)} banks to process.")

    total_source_rows = 0
    total_processed_rows = 0
    total_inserted_rows = 0

    for bank_folder in bank_folders:
        try:
            print(f"\n📂 Processing {bank_folder}...")

            s3_path = latest_excel_for_bank(bank_folder)
            if not s3_path:
                print(f"⚠️ No Excel found for {bank_folder}. Skipping.")
                continue

            print(f"⬇️ Processing file: {s3_path}")

            df = load_excel_from_s3(s3_path)

            source_rows = len(df)
            total_source_rows += source_rows
            print(f"📊 Source Excel has {source_rows} rows.")


            # Quick visibility (optional)
            # print("Columns:", df.columns.tolist())

            final = map_columns(df, bank_folder)

            processed_rows = len(final)
            total_processed_rows += processed_rows
            print(f"🔨 Processed {processed_rows} rows after mapping and cleaning.")

            if final.empty:
                print(f"⚠️ No valid rows (need IFSC + BRANCH_NAME). Processed 0 rows for {bank_folder}")
                continue

            rows = final.values.tolist()
            inserted = insert_rows_safe(rows)
            total_inserted_rows += inserted

            print(f"✅ Processed {inserted} rows into the database for {bank_folder}")

        except Exception as e:
            print(f"❌ Failed {bank_folder}: {e}")
    print("\nSummary:")
    print(f"📊 Total rows in source Excel: {total_source_rows}")
    print(f"🔨 Total rows processed: {total_processed_rows}")
    print(f"✅ Total rows inserted: {total_inserted_rows}")        

if __name__ == "__main__":
    restore()
