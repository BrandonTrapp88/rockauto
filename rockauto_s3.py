import json
import time
import re
import io
import csv
import boto3
import requests
import snowflake.connector
from bs4 import BeautifulSoup
from typing import Optional, List, Dict

BUCKET_NAME = 'pdm-matillion-pipeline'
multi_result_log_csv = "multi_result_error_log.csv"
not_found_log_csv = "not_found_error_log.csv"
output_csv_file = "part_numbers_with_prices.csv"
cleaned_csv_file = "cleaned_part_numbers_with_prices.csv"

SF_USER = 'brandon'
SF_PASSWORD = ''
SF_ACCOUNT = 'HC60396.us-east-2.aws'
SF_WAREHOUSE = 'PIPELINE'
SF_ROLE = 'ACCOUNTADMIN'

# RockAuto HTTP headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

s3 = boto3.client('s3')


def clear_csv_on_s3(key: str, headers: List[str]) -> None:
    """Overwrite the S3 key with only a header row."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buf.getvalue())


def clear_all_csvs() -> None:
    """Reset all CSVs in S3 to only their header rows."""
    clear_csv_on_s3(multi_result_log_csv, ["VendorPartNumber", "Error"])
    clear_csv_on_s3(not_found_log_csv, ["VendorPartNumber", "Error"])
    clear_csv_on_s3(output_csv_file, ["SupplierPartNumber", "Partnumber", "Cost"])
    clear_csv_on_s3(cleaned_csv_file, ["SupplierPartNumber", "Partnumber", "Cost"])


def fetch_part_numbers() -> List[Dict[str, str]]:
    """Pull VENDOR_PART_NUMBER → PART_NUMBER from Snowflake."""
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT VENDOR_PART_NUMBER, PART_NUMBER
          FROM SHARED_VELOCITY.ERP_COMPLETE.PRODUCT_TO_VENDOR
         WHERE vendor_id = '70084'

    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {"vendor_part_number": r[0], "part_number": r[1]}
        for r in rows
    ]


def fetch_price(vpn: str) -> Optional[str]:
    """HTTP‐GET RockAuto search page and scrape the first price."""
    url = f"https://www.rockauto.com/en/partsearch/?partnum={vpn}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    el = soup.select_one("span[id^='dprice'] span")
    if not el:
        return None

    m = re.search(r"\d+(\.\d+)?", el.get_text())
    return m.group() if m else None


def save_csv_to_s3(rows: List[Dict[str, str]], key: str, fieldnames: List[str]) -> None:
    """Write a list of dicts to CSV and upload to S3."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buf.getvalue())


def search_part_numbers_on_rockauto(part_numbers: List[Dict[str, str]]) -> None:
    raw_results = []
    multi_errors = []
    not_found = []

    for rec in part_numbers:
        vpn = rec["vendor_part_number"]
        pnum = rec["part_number"]
        price = fetch_price(vpn)

        if price is None:
            # mark as not found
            not_found.append(vpn)
            raw_results.append({"SupplierPartNumber": vpn, "Partnumber": pnum, "Cost": "Not Found"})
        else:
            raw_results.append({"SupplierPartNumber": vpn, "Partnumber": pnum, "Cost": price})

        time.sleep(1)  # throttle

    # write out error logs
    if multi_errors:
        save_csv_to_s3(
            [{"VendorPartNumber": v, "Error": "Multiple results found"} for v in multi_errors],
            multi_result_log_csv,
            ["VendorPartNumber", "Error"]
        )
    if not_found:
        save_csv_to_s3(
            [{"VendorPartNumber": v, "Error": "No results found"} for v in not_found],
            not_found_log_csv,
            ["VendorPartNumber", "Error"]
        )

    # write raw and cleaned price CSVs
    save_csv_to_s3(raw_results, output_csv_file, ["SupplierPartNumber", "Partnumber", "Cost"])
    cleaned = []
    for row in raw_results:
        m = re.search(r"\d+(\.\d+)?", row["Cost"])
        if m:
            cleaned.append({
                "SupplierPartNumber": row["SupplierPartNumber"],
                "Partnumber": row["Partnumber"],
                "Cost": m.group()
            })
    save_csv_to_s3(cleaned, cleaned_csv_file, ["SupplierPartNumber", "Partnumber", "Cost"])


def lambda_handler(event, context):
    # clear existing CSVs before writing new data
    clear_all_csvs()

    parts = fetch_part_numbers()
    search_part_numbers_on_rockauto(parts)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "RockAuto scrape complete",
            "searched_count": len(parts)
        })
    }
