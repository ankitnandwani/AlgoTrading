import datetime
import math
import gspread
import pandas as pd
import json
from datetime import datetime, timedelta, UTC
import streamlit as st
from google.oauth2.service_account import Credentials
from vortex_api import VortexAPI, Constants

st.set_page_config(page_title="Share Genius Mall", layout="centered")
min_investment = 20000

def get_spreadsheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    gclient = gspread.authorize(creds)
    return gclient.open_by_key(st.secrets["GOOGLE_SHEET_ID"])

def google_auth():
    etf_shop = ss.worksheet("ETF shop")
    jewellers_shop = ss.worksheet("Jewellers Shop")
    top_nifty_shop = ss.worksheet("Top 10 Nifty Stocks Shop")
    log_sheet = ss.worksheet("Buy Orders Log")

    # ETF Shop
    etf_shop_all = etf_shop.col_values(2)  # if “NSE Code” is the first column
    etf_shop_cleaned = etf_shop_all[1:]
    etf_shop_cleaned = [code.replace("NSE:", "").strip() for code in etf_shop_cleaned]

    # Jewellers Shop
    jewellers_shop_all = jewellers_shop.col_values(2)  # if “NSE Code” is the first column
    jewellers_shop_cleaned = jewellers_shop_all[1:]
    jewellers_shop_cleaned = [code.replace("NSE:", "").strip() for code in jewellers_shop_cleaned]

    # top nifty
    top_nifty_shop_all = top_nifty_shop.col_values(2)  # if “NSE Code” is the first column
    top_nifty_shop_cleaned = top_nifty_shop_all[1:]
    top_nifty_shop_cleaned = [code.replace("NSE:", "").strip() for code in top_nifty_shop_cleaned]

    all_data = log_sheet.get_all_records()

    # Collect rows to keep
    rows_to_keep = []

    for row in all_data:
        if row['Instrument Key'] in existing_positions:
            rows_to_keep.append(row)

    # Clear the sheet
    log_sheet.clear()

    # Write header again
    if all_data:
        header = all_data[0].keys()
        log_sheet.append_row(list(header))

        # Append the filtered rows
        for row in rows_to_keep:
            log_sheet.append_row([row[col] for col in header])

    return etf_shop_cleaned, jewellers_shop_cleaned, top_nifty_shop_cleaned, rows_to_keep


# 🛠 Helper: Get historical closes
def get_last_n_closes(instrument_token, n=20, days_buffer=60):
    to_date = datetime.now(UTC)
    from_date = datetime.now(UTC) - timedelta(days=days_buffer)
    hist = client.historical_candles(exchange=Constants.ExchangeTypes.NSE_EQUITY, token=instrument_token, to=to_date,
                                     start=from_date, resolution=Constants.Resolutions.DAY)
    closes = hist['c']
    close_rev = closes[::-1]
    return close_rev[:n] if len(close_rev) >= n else []


def get_ltp():
    all_products = etf + jewel + nifty
    instrument_tokens = [
        f"NSE_EQ-{symbol_to_key[symbol]}"
        for symbol in all_products
        if symbol in symbol_to_key  # ensure symbol exists in mapping
    ]

    response = client.quotes(instruments=instrument_tokens, mode=Constants.QuoteModes.LTP)

    last_trade_prices = {}

    for key in instrument_tokens:
        if key in response["data"]:  # check if key exists in response
            last_trade_prices[key] = response["data"][key]["last_trade_price"]

    return last_trade_prices


# symbol to instrument key mapping
def load_symbol_to_instrument_key_map(json_file="master_nse_eq.json"):
    with open(json_file, "r") as f:
        symbol_map = json.load(f)

    return symbol_map


# ✅ Main computation
def compute_top3(shop):
    results = []

    for sym in shop:
        try:
            instrument_key = symbol_to_key.get(sym)
            if not instrument_key:
                continue

            ltp = last_trading_price["NSE_EQ-" + str(instrument_key)]
            closes = get_last_n_closes(instrument_key)
            if len(closes) < 20:
                continue

            ma20 = (sum(closes)) / 20
            dev = ((ltp - ma20) / ma20) * 100
            results.append((sym, ltp, ma20, dev, instrument_key))
        except Exception as e:
            st.warning(f"{sym} error: {e}")

    df = pd.DataFrame(results, columns=["Symbol", "LTP", "MA20", "Deviation%", "Instrument_token"])
    df = df.sort_values("Deviation%").reset_index(drop=True)
    df.index += 1  # start index from 1 for display
    return df.head(3)


def log_buy_order_to_sheet(order_details):
    log_sheet = ss.worksheet("Buy Orders Log")
    log_sheet.append_row(order_details)


def buy(instrument_key, ltp, symbol):
    quantity = max(1, math.ceil((min_investment / ltp) * 2))

    # Display order details
    st.subheader("🛒 Buy Order details")
    st.markdown(f"""
                            **Instrument Token:** `{instrument_key}`    
                            **Symbol:** `{symbol}`    
                            **Quantity:** `{quantity}`
                            **Order Value:** `₹{quantity * ltp}`  
                            **Price:** `₹{ltp}`
                            """)

    try:
        body = client.place_order(exchange=Constants.ExchangeTypes.NSE_EQUITY, token=instrument_key,
                                  transaction_type=Constants.TransactionSides.BUY, product=Constants.ProductTypes.MTF,
                                  variety=Constants.VarietyTypes.REGULAR_LIMIT_ORDER, quantity=quantity,
                                  price=ltp*1.0, trigger_price=0.0, disclosed_quantity=0,
                                  validity=Constants.ValidityTypes.FULL_DAY)
        st.info("order details : " + str(body))
        if body.get("status") == "success":
            st.success(f"✅ Order placed successfully")
            # Log the order details to Google Sheets
            order_details = [
                datetime.now().strftime('%Y-%m-%d'),
                instrument_key,  # Instrument Token
                symbol
            ]
            log_buy_order_to_sheet(order_details)
        else:
            st.error("❌ Order placement failed!")
    except Exception as e:
        st.error(f"❌ Failed to place order: {e}")


def sell(instrument_key, ltp, quantity):
    # Display order details
    st.subheader("🛒 Sell Order details")
    st.markdown(f"""
            **Instrument Token:** `{instrument_key}`  
            **LTP:** `₹{ltp}`
            **Quantity:** `{quantity}`
            **Order Value:** `₹{quantity * ltp}`
            """)

    try:
        body = client.place_order(exchange=Constants.ExchangeTypes.NSE_EQUITY, token=instrument_key,
                                  transaction_type=Constants.TransactionSides.SELL,
                                  product=Constants.ProductTypes.MTF,
                                  variety=Constants.VarietyTypes.REGULAR_LIMIT_ORDER, quantity=quantity,
                                  price=ltp, trigger_price=0.0, disclosed_quantity=0,
                                  validity=Constants.ValidityTypes.FULL_DAY)
        st.info("order details : " + str(body))
        if body.get("status") == "success":
            st.success(f"✅ Order placed successfully")
        else:
            st.error("❌ Order placement failed!")
    except Exception as e:
        st.error(f"❌ Failed to place order: {e}")


def get_current_portfolio():
    existing_pos = {item["token"] for item in positions["data"]["net"]}
    st.info("existing_pos : " + str(existing_pos))
    existing_holds = {item["token"] for item in holdings["data"]["net"]}
    st.info("existing_holds : " + str(existing_holds))
    return existing_pos + existing_holds


def filter_top3_in_holdings(top3stocks):
    for _, row in top3stocks.iterrows():
        instrument_token = row['Instrument_token']

        if instrument_token in existing_positions:
            st.info(f"Already holding: {row['Symbol']}")
        else:
            st.info(f"Buying new Stock: {row['Symbol']}")
            buy(row['Instrument_token'], row['LTP'], row['Symbol'])
            return True

    return False


def check_ceiling_and_funds():
    total_positions = len(positions["data"]["net"])
    st.info("total positions " + str(total_positions))
    if total_positions >= 14:
        st.error("Total holdings ceiling limit reached. Exiting 🚨 🚨 🚨")
        st.stop()

    funds_resp = client.funds()
    funds = funds_resp['nse']['net_available']
    st.info("funds : " + str(funds))
    if funds < min_investment * 2:
        st.error("Gareeb pase daal! Exiting 🚨 🚨 🚨")
        st.stop()


# all 5 stocks available for buy are already in portfolio
# so we will average our worst performer from the list with cmp
def sell_or_take_delivery():
    for holding in positions["data"]["net"]:
        avg_buy_price = holding["average_price"]
        instrument_key = holding["token"]
        quantity = holding["quantity"]
        symbol = holding["symbol"]
        ltp = last_trading_price["NSE_EQ-" + str(instrument_key)]
        deviation = ((ltp - avg_buy_price) / avg_buy_price) * 100

        matched_rows = [row for row in all_logs if str(row["Instrument Key"]) == str(instrument_key)]
        if matched_rows:
            order_date_str = matched_rows[0]["Order date"]  # Assuming first column header is "Timestamp"
            order_date = datetime.strptime(order_date_str, "%Y-%m-%d").date()
            days_elapsed = (datetime.now().date() - order_date).days
            st.info(symbol + f" has deviation = {deviation:.2f}% (current price {ltp} vs avg {avg_buy_price}) and "
                             f"holding days elapsed = {days_elapsed}")

            if deviation >= 3.14:
                sell(instrument_key, ltp, quantity)

            if days_elapsed >= 20:
                st.subheader("Take delivery of " + str(symbol))


# 🔐 UI Components
st.title("📊 Share Genius Mall")

API_KEY = st.secrets["API_KEY"]
APPLICATION_ID = st.secrets["APPLICATION_ID"]
auth_token = st.query_params.get("auth")

if not auth_token:
    # Show login button if user not authenticated
    login_url = f"https://flow.rupeezy.in?applicationId={APPLICATION_ID}"
    st.markdown(
        f'<a href="{login_url}" target="_blank">'
        f'<button style="padding:10px 20px;font-size:16px;">🔑 Login with Rupeezy</button>'
        f'</a>',
        unsafe_allow_html=True
    )
else:
    st.success("✅ Successfully logged in with Rupeezy")

    if st.button("🚀 Run Analysis and Trade"):
        try:
            client = VortexAPI(API_KEY, APPLICATION_ID)
            token_resp = client.exchange_token(auth_token)
            positions = client.positions()
            holdings = client.holdings()
            st.info("holdings : " + str(holdings))
            existing_positions = get_current_portfolio()
            ss = get_spreadsheet()
            etf, jewel, nifty, all_logs = google_auth()
            symbol_to_key = load_symbol_to_instrument_key_map()
            last_trading_price = get_ltp()

            sell_or_take_delivery()
            check_ceiling_and_funds()

            st.info("ETF SHOP : " + str(etf) + " count : " + str(len(etf)))
            etf3 = compute_top3(etf)
            if not etf3.empty:
                st.subheader("📈 Top 3 ETF Below MA20")
                st.dataframe(etf3)
                bought_etf = filter_top3_in_holdings(etf3)
            else:
                st.info("No qualifying ETF found.")

            st.info("Jewelery SHOP : " + str(jewel) + " count : " + str(len(jewel)))
            jewel3 = compute_top3(jewel)
            if not jewel3.empty:
                st.subheader("📈 Top 3 Jewelry Below MA20")
                st.dataframe(jewel3)
                bought_jewel = filter_top3_in_holdings(jewel3)
            else:
                st.info("No qualifying Jewelry found.")

            st.info("Nifty SHOP : " + str(nifty) + " count : " + str(len(nifty)))
            nifty3 = compute_top3(nifty)
            if not nifty3.empty:
                st.subheader("📈 Top 3 Stocks Below MA20")
                st.dataframe(nifty3)
                bought_nifty = filter_top3_in_holdings(nifty3)
            else:
                st.info("No qualifying Stocks found.")

        except Exception as e:
            st.error(f"Something went wrong: {e}")
