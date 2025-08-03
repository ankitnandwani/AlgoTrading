import os
import json

import gspread
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from dhanhq import dhanhq  # Dhan SDK
from google.oauth2.service_account import Credentials

# === Configuration ===
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1100519107")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU2MzM2MjE3LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDUxOTEwNyJ9.cqUqvurBhtfhaRi5mAgt63m5KhEOrZOakVhqLcIw-di71LaxkJMT5Xw9kB-W8UVBgm4vXPzQhnARyqH0RsNZhw")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL", "1VolFjhqktCaml8_TSuCvpjSIR4Yw6-Xi1MDEHVYlL6Y")
SYMBOL = os.getenv("SYMBOL", "NIFTYBEES")

# === Logging setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Scopes needed for Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_gsheet_client():
    # Uses GOOGLE_APPLICATION_CREDENTIALS env var by default if set
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
    )
    client = gspread.authorize(creds)
    return client

def fetch_orders():
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    resp = dhan.get_order_list()
    orders = resp['data']
    logging.info(f"Fetched {len(orders)} total orders from Dhan.")
    filtered = [
        o for o in orders
        if o['orderStatus'] == 'TRADED'
           and o['tradingSymbol'] == SYMBOL
    ]
    logging.info(f"Found {len(filtered)} orders matching '{SYMBOL}' with traded/complete status.")
    return filtered

def append_orders_to_excel(orders):
    columns = [
        "Order ID",
        "Exchange Time",
        "Transaction Type",
        "Order Status",
        "Close",
        "Average Price",
        "Quantity",
        "Total Holding",
        "Investment",
        "Value at close",
        "Profit/Loss",
        "Raw JSON"
    ]

    rows = []
    for o in orders:
        row = {
            "Order ID": o.get("orderId"),
            "Exchange Time": o.get("exchangeTime"),
            "Transaction Type": o.get("transactionType"),
            "Order Status": o.get("orderStatus"),
            "Average Price": o.get("averageTradedPrice") or o.get("averagePrice") or 0.0,
            "Quantity": o.get("quantity") or 0,
            "Raw JSON": json.dumps(o, default=str)
        }
        rows.append(row)


    df_new = pd.DataFrame(rows)

    # Open sheet
    client = get_gsheet_client()
    sh = client.open_by_key(GOOGLE_SHEET_URL)
    worksheet = sh.sheet1
    # Append rows to sheet (preserving header if first time)
    # If sheet is empty, write header first
    existing = worksheet.get_all_records()
    if worksheet.row_count == 0 or not existing:
        worksheet.append_row(columns)

    # Append each new row in order
    for _, row in df_new.iterrows():
        values = [row.get(col, "") for col in columns]
        worksheet.append_row(values)

    logging.info(f"Appended {len(df_new)} new row(s) to Google Sheet.")

def main():
    try:
        orders = fetch_orders()
        append_orders_to_excel(orders)
    except Exception as e:
        logging.exception(f"Job failed: {e}")

if __name__ == "__main__":
    main()
