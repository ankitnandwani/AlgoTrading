import requests
import pandas as pd
from dhanhq import dhanhq

# 🔐 Replace with your actual credentials
CLIENT_ID = '1100519107'
ACCESS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU2MzM2MjE3LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDUxOTEwNyJ9.cqUqvurBhtfhaRi5mAgt63m5KhEOrZOakVhqLcIw-di71LaxkJMT5Xw9kB-W8UVBgm4vXPzQhnARyqH0RsNZhw'
EXCEL_PATH = 'your_excel_file.xlsx'

# 🧾 Get Orders from Dhan
def get_dhan_orders():
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    print("hi ankit")
    orders = dhan.get_order_list()

    orders_data = orders['data']  # Your Dhan API response dictionary

    symbol_to_find = 'NIFTYBEES'
    completed_orders = [
        order for order in orders_data
        if order['orderStatus'] == 'COMPLETE' and order['tradingSymbol'] == symbol_to_find
    ]

    if not completed_orders:
        print(f"No completed orders found for {symbol_to_find}")
    else:
        for order in completed_orders:
            avg_price = order.get('averageTradedPrice', 0.0)
            print(f"Completed Order ID: {order['orderId']} | Average Price: ₹{avg_price}")

    return avg_price

# 🔄 Update Buy Price in Excel
def update_excel_with_buy_prices(orders, excel_path):
    df = pd.read_excel(excel_path)

    # Only consider 'BUY' filled orders
    buy_orders = [o for o in orders if o['transactionType'] == 'BUY' and o['orderStatus'] == 'COMPLETE']

    # Map symbol to average buy price
    latest_prices = {}
    for order in buy_orders:
        symbol = order['tradingSymbol']
        price = float(order['averagePrice'])
        qty = int(order['filledQuantity'])

        if symbol in latest_prices:
            # Weighted average if multiple buys
            prev_qty, prev_price = latest_prices[symbol]
            total_qty = prev_qty + qty
            avg_price = ((prev_price * prev_qty) + (price * qty)) / total_qty
            latest_prices[symbol] = (total_qty, avg_price)
        else:
            latest_prices[symbol] = (qty, price)

    # Update Excel
    for i, row in df.iterrows():
        symbol = row['Symbol']
        if symbol in latest_prices:
            df.at[i, 'Buy Price'] = latest_prices[symbol][1]

    df.to_excel(excel_path, index=False)
    print("✅ Excel updated successfully.")

# 🧩 Main flow
try:
    orders = get_dhan_orders()
    update_excel_with_buy_prices(orders, EXCEL_PATH)
except Exception as e:
    print(f"❌ Error: {e}")
