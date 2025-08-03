import os
import json
import gspread
from datetime import datetime, time
import logging
from dhanhq import dhanhq
from google.oauth2.service_account import Credentials

# === Configuration ===
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1100519107")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU2MzM2MjE3LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDUxOTEwNyJ9.cqUqvurBhtfhaRi5mAgt63m5KhEOrZOakVhqLcIw-di71LaxkJMT5Xw9kB-W8UVBgm4vXPzQhnARyqH0RsNZhw")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL", "1VolFjhqktCaml8_TSuCvpjSIR4Yw6-Xi1MDEHVYlL6Y")
SYMBOL = os.getenv("SYMBOL", "NIFTYBEES")
SECURITY_ID = 14050  # Hardcoded ID for NIFTYBEES

# === Logging setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Scopes needed for Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_gsheet_client():
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES
    )
    client = gspread.authorize(creds)
    return client

def fetch_orders(dhan_client):
    resp = dhan_client.get_order_list()
    orders = resp['data']
    logging.info(f"Fetched {len(orders)} total orders from Dhan.")
    filtered = [
        o for o in orders
        if o['orderStatus'] == 'TRADED'
           and o['tradingSymbol'] == SYMBOL
    ]
    logging.info(f"Found {len(filtered)} orders matching '{SYMBOL}' with traded status.")
    return filtered

def get_closing_price(dhan_client):
    try:
        quote = dhan_client.get_quote(SECURITY_ID, 'NSE_EQ')
        return quote['data']['lastPrice']
    except Exception as e:
        logging.error(f"Failed to fetch closing price: {e}")
        return 0

def get_portfolio_state(worksheet):
    """Get current holdings and investment from last row of sheet"""
    records = worksheet.get_all_records()
    if not records:
        return 0, 0.0

    last_row = records[-1]
    return last_row.get("Total Holding", 0), last_row.get("Investment", 0.0)

def append_to_sheet(worksheet, data):
    """Append multiple rows efficiently"""
    if not data:
        return

    columns = [
        "Order ID", "Exchange Time", "Transaction Type", "Order Status",
        "Close", "Average Price", "Quantity", "Total Holding",
        "Investment", "Value at close", "Profit/Loss", "Raw JSON"
    ]

    # Write header if sheet is empty
    if not worksheet.get_all_records():
        worksheet.append_row(columns)

    # Prepare and append rows
    for row in data:
        worksheet.append_row([row.get(col, "") for col in columns])

def process_orders(orders, existing_ids, holding, investment):
    """Process new orders and update portfolio state"""
    new_rows = []
    today = datetime.now().date()
    has_today_order = False

    for order in sorted(orders, key=lambda x: x['exchangeTime']):
        order_id = str(order['orderId'])
        if order_id in existing_ids:
            continue

        # Parse order details
        order_date = datetime.strptime(
            order['exchangeTime'], "%Y-%m-%dT%H:%M:%S"
        ).date()
        qty = order.get("quantity", 0)
        price = order.get("averageTradedPrice") or order.get("averagePrice") or 0.0

        # Update portfolio state
        if order['transactionType'] == 'BUY':
            holding += qty
            investment += qty * price
        elif order['transactionType'] == 'SELL':
            holding = max(0, holding - qty)
            investment = holding * (investment / (holding + qty)) if holding else 0

        # Create order row
        new_rows.append({
            "Order ID": order_id,
            "Exchange Time": order['exchangeTime'],
            "Transaction Type": order['transactionType'],
            "Order Status": order['orderStatus'],
            "Average Price": price,
            "Quantity": qty,
            "Total Holding": holding,
            "Investment": investment,
            "Raw JSON": json.dumps(order, default=str)
        })

        # Check if this is today's order
        if order_date == today:
            has_today_order = True

        existing_ids.add(order_id)

    return new_rows, holding, investment, has_today_order

def create_blank_row(holding, investment, closing_price):
    """Create placeholder row for days without orders"""
    value_at_close = holding * closing_price
    return {
        "Exchange Time": datetime.combine(
            datetime.now().date(),
            time(15, 30)
        ).strftime("%Y-%m-%dT%H:%M:%S"),
        "Close": closing_price,
        "Total Holding": holding,
        "Investment": investment,
        "Value at close": value_at_close,
        "Profit/Loss": value_at_close - investment
    }

def main():
    try:
        # Initialize clients
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        gclient = get_gsheet_client()
        sheet = gclient.open_by_key(GOOGLE_SHEET_URL)
        worksheet = sheet.sheet1

        # Get current portfolio state and existing order IDs
        holding, investment = get_portfolio_state(worksheet)
        existing_ids = {str(rec["Order ID"]) for rec in worksheet.get_all_records()
                        if rec.get("Order ID")}

        # Process orders
        orders = fetch_orders(dhan)
        new_rows, holding, investment, has_today_order = process_orders(
            orders, existing_ids, holding, investment
        )

        # Add blank row if no orders today and it's a weekday
        today = datetime.now()
        if not has_today_order and today.weekday() < 5:  # Mon-Fri
            closing_price = get_closing_price(dhan)
            new_rows.append(create_blank_row(holding, investment, closing_price))
            logging.info("Added blank row with portfolio update")

        # Update Google Sheet
        append_to_sheet(worksheet, new_rows)
        logging.info(f"Updated sheet with {len(new_rows)} new rows")

    except Exception as e:
        logging.exception(f"Job failed: {e}")

if __name__ == "__main__":
    main()