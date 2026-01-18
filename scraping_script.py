import os
import time
import io
import boto3
import psycopg2
import pandas as pd
from playwright.sync_api import sync_playwright

# =========================
# CONFIG
# =========================
RBI_URL = "https://www.rbi.org.in/scripts/bs_viewcontent.aspx?Id=2009"
S3_BUCKET = "mbf-los-stg"
S3_BASE_PATH = "bank_data"

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# =========================
# DB
# =========================
def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def update_bank_metadata(bank_name, s3_path, processed):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO bank_data (bank_name, s3_path, processed, created_at, updated_at)
        VALUES (%s, %s, %s, NOW(), NOW())
        ON CONFLICT (bank_name)
        DO UPDATE SET
            s3_path = EXCLUDED.s3_path,
            processed = EXCLUDED.processed,
            updated_at = NOW();
    """, (bank_name, s3_path, processed))
    conn.commit()
    cur.close()
    conn.close()

# =========================
# S3
# =========================
s3 = boto3.client("s3")

def upload_to_s3(bank_name, binary_data):
    epoch = int(time.time())
    safe_name = bank_name.replace(" ", "_").replace(",", "")
    key = f"{S3_BASE_PATH}/{safe_name}/rbi_data_{epoch}.xlsx"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=binary_data,
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    return f"s3://{S3_BUCKET}/{key}"

# =========================
# EXCEL VALIDATION
# =========================
def is_valid_excel(data: bytes):
    # XLSX magic bytes
    return data[:4] == b"PK\x03\x04"

def count_rows(data: bytes):
    excel = pd.ExcelFile(io.BytesIO(data), engine="openpyxl")
    total = 0
    for sheet in excel.sheet_names:
        df = excel.parse(sheet).dropna(how="all")
        total += len(df)
    return total

# =========================
# MAIN
# =========================
def run_scraper():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        )
        page = context.new_page()

        print("Opening RBI page…")
        page.goto(RBI_URL, timeout=60000)

        links = page.eval_on_selector_all(
            "a[href$='.xlsx']",
            """els => els.map(e => ({
                name: e.innerText.trim(),
                href: e.href
            }))"""
        )

        print(f"Found {len(links)} Excel links")

        for item in links:
            bank_name = item["name"]
            href = item["href"]

            if not bank_name or not href:
                continue

            try:
                print(f"⬇️ Downloading Excel: {bank_name}")

                resp = context.request.get(
                    href,
                    timeout=60000,
                    headers={
                        "Referer": RBI_URL,
                        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    }
                )

                if not resp.ok:
                    raise RuntimeError(f"HTTP {resp.status}")

                data = resp.body()

                if not is_valid_excel(data):
                    raise ValueError("Downloaded file is not Excel")

                rows = count_rows(data)
                s3_path = upload_to_s3(bank_name, data)

                update_bank_metadata(bank_name, s3_path, True)
                print(f"✅ {bank_name} | {rows} rows")

            except Exception as e:
                print(f"❌ Failed {bank_name}: {e}")
                update_bank_metadata(bank_name, None, False)

        browser.close()

# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    run_scraper()

