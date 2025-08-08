import os
import json
import gspread
from datetime import datetime
import logging

import yfinance as yf
from dhanhq import dhanhq
from google.oauth2.service_account import Credentials

# === Configuration ===
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "YOUR_DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_DHAN_ACCESS_TOKEN")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL", "YOUR_GOOGLE_SHEET_URL")
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

def get_closing_price():
    """Fetch LTP of NIFTYBEES using yfinance"""
    try:
        # Fetch data for NIFTYBEES on NSE
        ticker = yf.Ticker(f"{SYMBOL}.NS")
        # Get last 5 days of data to ensure we capture latest close
        hist_data = ticker.history(period="5d")
        if hist_data.empty:
            logging.error("No historical data found from yfinance")
            return 0
        # Last available closing price (most recent trading day)
        close = hist_data['Close'].iloc[-1]
        logging.info(f"Fetched closing price: {close}")
        return close
    except Exception as e:
        logging.error(f"Failed to fetch closing price: {e}")
        return 0

def get_portfolio_state(worksheet):
    """Get current holdings and investment from last row of sheet"""
    records = worksheet.get_all_records()
    if not records:
        return 0, 0.0

    last_row = records[-1]
    return last_row.get("Total Holding", 0), last_row.get("Total Basic Investment", 0.0)

def append_to_sheet(worksheet, data):
    """Append multiple rows efficiently"""
    if not data:
        return

    columns = [
        "Order ID", "Exchange Time", "Transaction Type", "Order Status",
        "Close", "Average Price", "Quantity", "Total Holding",
        "Investment", "Total Basic Investment", "Value at close", "Profit/Loss", "Raw JSON"
    ]

    # Write header if sheet is empty
    if not worksheet.get_all_records():
        worksheet.append_row(columns)

    # Prepare and append rows
    for row in data:
        worksheet.append_row([row.get(col, "") for col in columns])

def process_orders(orders, existing_ids, holding, investment, closing_price):
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
            order['exchangeTime'], "%Y-%m-%d %H:%M:%S"
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

        value_at_close = holding * closing_price
        # Create order row
        new_rows.append({
            "Order ID": order_id,
            "Exchange Time": order['exchangeTime'],
            "Transaction Type": order['transactionType'],
            "Order Status": order['orderStatus'],
            "Close": closing_price,
            "Average Price": price,
            "Quantity": qty,
            "Total Holding": holding,
            "Investment": qty * price,
            "Total Basic Investment": investment,
            "Value at close": value_at_close,
            "Profit/Loss": value_at_close - investment,
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
        "Exchange Time": datetime.now().strftime("%-d-%b-%Y").lstrip("0"),
        "Close": closing_price,
        "Total Holding": holding,
        "Total Basic Investment": investment,
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
        closing_price = get_closing_price()

        # Get current portfolio state and existing order IDs
        holding, investment = get_portfolio_state(worksheet)
        existing_ids = {str(rec["Order ID"]) for rec in worksheet.get_all_records()
                        if rec.get("Order ID")}

        # Process orders
        orders = fetch_orders(dhan)
        new_rows, holding, investment, has_today_order = process_orders(
            orders, existing_ids, holding, investment, closing_price
        )

        # Add blank row if no orders today and it's a weekday
        today = datetime.now()
        if not has_today_order and today.weekday() < 5:  # Mon-Fri
            new_rows.append(create_blank_row(holding, investment, closing_price))
            logging.info("Added blank row with portfolio update")

        # Update Google Sheet
        append_to_sheet(worksheet, new_rows)
        logging.info(f"Updated sheet with {len(new_rows)} new rows")

    except Exception as e:
        logging.exception(f"Job failed: {e}")

if __name__ == "__main__":
    main()