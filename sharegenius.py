import csv
import datetime
import math

import gspread
import pandas as pd
import upstox_client
from upstox_client.rest import ApiException
import json
from datetime import datetime, timedelta, UTC, timezone
import streamlit as st
from google.oauth2.service_account import Credentials
from vortex_api import VortexAPI, Constants

from goldbees_etf import make_api_request

st.set_page_config(page_title="Share Genius Mall", layout="centered")


def google_auth():
    # Define scope and load credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)

    # Authorize and open the sheet
    gclient = gspread.authorize(creds)
    ss = gclient.open_by_key(st.secrets["GOOGLE_SHEET_ID"])
    etf_shop = ss.worksheet("ETF shop")
    jewellers_shop = ss.worksheet("Jewellers Shop")
    top_nifty_shop = ss.worksheet("Top 10 Nifty Stocks Shop")

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
    return etf_shop_cleaned, jewellers_shop_cleaned, top_nifty_shop_cleaned

# 🛠 Helper: Get historical closes
def get_last_n_closes(instrument_token, n=20, days_buffer=60):
    to_date = datetime.now(UTC)
    from_date = datetime.now(UTC) - timedelta(days=days_buffer)
    hist = client.historical_candles(exchange=Constants.ExchangeTypes.NSE_EQUITY, token=instrument_token, to=to_date,
                                     start=from_date,
                                     resolution=Constants.Resolutions.DAY)

    #st.info("hist : " + str(hist))
    closes = hist['c']
    close_rev = closes[::-1]
    #st.info("close_rev : " + str(close_rev[:n]))
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
        except ApiException as e:
            st.warning(f"{sym} error: {e}")

    df = pd.DataFrame(results, columns=["Symbol", "LTP", "MA20", "Deviation%", "Instrument_token"])
    df = df.sort_values("Deviation%")
    df.index = df.index + 1  # start index from 1 for display
    return df.head(3)


def buy(instrument_key, ltp):
    # Get current IST time
    now_ist = datetime.now(UTC).astimezone(timezone(timedelta(hours=5, minutes=30)))
    market_close_time = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

    # Determine order type and AMO status based on current time
    if now_ist < market_close_time:
        variety = Constants.VarietyTypes.REGULAR_MARKET_ORDER
        is_amo = False
    else:
        variety = Constants.VarietyTypes.REGULAR_LIMIT_ORDER
        is_amo = True

    min_investment = 10000
    quantity = max(1, math.ceil((min_investment / ltp)*2))

    try:
        data = {
            "exchange": "NSE_EQ",
            "token": instrument_key,
            "transaction_type": "BUY",
            "product": Constants.ProductTypes.DELIVERY,
            "variety": variety,
            "quantity": quantity,
            "price": ltp,
            "trigger_price": 0.0,
            "disclosed_quantity": 0,
            "validity": "DAY",
            "is_amo": True
        }

        # Display order details
        st.subheader("🛒 Buy Order details")
        st.markdown(f"""
                        **Instrument Token:** `{instrument_key}`    
                        **Quantity:** `{quantity}`
                        **Order Type:** `{variety}`
                        **Order Value:** `₹{quantity * ltp}`  
                        **Price:** `₹{ltp}`
                        **AMO:** `{is_amo}`
                        """)

        api_response = make_api_request(token, "POST", data=data)
        st.success(f"✅ Buy order placed successfully: {api_response}")
    except ApiException as e:
        st.error(f"❌ Failed to place order: {e}")


def sell(instrument_key, ltp):
    # Get current IST time
    now_ist = datetime.now(UTC).astimezone(timezone(timedelta(hours=5, minutes=30)))
    market_close_time = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

    # Determine order type and AMO status based on current time
    if now_ist < market_close_time:
        order_type = "MARKET"
        price = 0.0
        is_amo = False
    else:
        order_type = "LIMIT"
        price = ltp
        is_amo = True

    quantity = 0

    for item in portfolio["data"]:
        nse_data = item.get("nse")

        if not nse_data:
            continue

        if nse_data.get("exchange") == "NSE_EQ" and nse_data.get("token") == instrument_key:
            quantity = item.get("total_free", 0)
            break


    # Display order details
    st.subheader("🛒 Sell Order details")
    st.markdown(f"""
            **Instrument Token:** `{instrument_key}`  
            **LTP:** `₹{ltp}`  
            **Order Type:** `{order_type}`  
            **Price:** `₹{price}`
            **Quantity:** `₹{quantity}`
            **Order Value:** `₹{quantity * ltp}`
            **AMO:** `{is_amo}`
            """)

    try:
        body = client.place_order(exchange=Constants.ExchangeTypes.NSE_EQUITY, token=instrument_key, transaction_type=Constants.TransactionSides.SELL,
                                                 product=Constants.ProductTypes.DELIVERY, variety=Constants.VarietyTypes.REGULAR_LIMIT_ORDER, quantity=quantity,
                                                 price=ltp, trigger_price=0.0, disclosed_quantity=0, validity=Constants.ValidityTypes.AFTER_MARKET)
        st.info("body : " + str(body))
        api_response = order_api.place_order(body)
        st.success(f"✅ Sell order placed successfully: {api_response}")
    except ApiException as e:
        st.error(f"❌ Failed to place order: {e}")



def get_current_portfolio():
    existing_holds = {item["nse"]["token"] for item in portfolio["data"]}

    executed_ordr_tokens = {
        order["token"]
        #order.instrument_token
        for order in existing_orders.get("orders", [])
        if order.get("status") == "EXECUTED"
    }

    return existing_holds, executed_ordr_tokens

def filter_top3_in_holdings(top3stocks):
    for _, row in top3stocks.iterrows():
        instrument_token = row['Instrument_token']
        symbol = row['Symbol']

        if instrument_token in existing_holdings:
            st.info(f"Already holding: {row['Symbol']}")
        elif instrument_token in executed_order_tokens:
            st.info(f"Order already placed for: {symbol}")
        else:
            st.info(f"Buying new ETF: {row['Symbol']}")
            buy(row['Instrument_token'], row['LTP'])
            return True

    return False

# all 5 stocks available for buy are already in portfolio
# so we will average our worst performer from the list with cmp
def averaging():
    candidates = []
    nse_data = {}

    st.info("Starting averaging")
    for holding in portfolio["data"]:
        nse_data = holding["nse"]
        print(holding["isin"], nse_data["symbol"], nse_data["token"])

    st.info("nse_data : " + str(nse_data))
    for holding in portfolio["data"]:
        nse_data = holding["nse"]
        st.info("nse_data : " + str(nse_data))
        avg_buy_price = holding.get("average_price")
        st.info("avg_buy_price : " + str(avg_buy_price))
        instrument_key = nse_data.get("token")
        st.info("instrument_key : " + str(instrument_key))
        symbol = nse_data.get("symbol")
        st.info("symbol : " + str(symbol))
        ltp = last_trading_price["NSE_EQ-" + str(instrument_key)]
        deviation = ((ltp - avg_buy_price) / avg_buy_price) * 100
        st.info(
            symbol + f" has deviation = {deviation:.2f}% (current price {ltp} vs last buy {avg_buy_price})")

        if deviation < -3.14:
            candidates.append({
                "instrument_token": instrument_key,
                "ltp": ltp,
                "symbol": symbol,
                "deviation": deviation
            })

        #if deviation > 6.28:
        sell(instrument_key, ltp)

    if not candidates:
        st.info("No eligible stock found in portfolio for averaging.")
        return

    # if bought_etf:
    #     st.info("Buy order already placed, skipping averaging")
    #     return

    best_candidate = min(candidates, key=lambda x: x["deviation"])
    buy(best_candidate['instrument_token'], best_candidate['ltp'])
    st.success(f"Averaged: {best_candidate['symbol']} @ Deviation {best_candidate['deviation']:.2f}%")


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
        unsafe_allow_html = True
    )
else:
    st.success("✅ Successfully logged in with Rupeezy")

    if st.button("🚀 Run Analysis and Trade"):
        try:
            client = VortexAPI(API_KEY, APPLICATION_ID)
            token_resp = client.exchange_token(auth_token)
            token = token_resp["data"]["access_token"]
            etf, jewel, nifty = google_auth()
            symbol_to_key = load_symbol_to_instrument_key_map()
            last_trading_price = get_ltp()
            portfolio = client.holdings()
            st.info("portfolio : " + str(portfolio))
            existing_orders = client.orders(limit=50, offset=1)
            existing_holdings, executed_order_tokens = get_current_portfolio()
            st.info("existing_holdings : " + str(existing_holdings) + " executed_order_tokens : " + str(executed_order_tokens))

            st.info("ETF SHOP : " + str(etf) + " count : " + str(len(etf)))
            # etf3 = compute_top3(etf)
            # if not etf3.empty:
            #     st.subheader("📈 Top 3 ETF Below MA20")
            #     st.dataframe(etf3)
            #     bought_etf = filter_top3_in_holdings(etf3)
            # else:
            #     st.info("No qualifying ETF found.")
            #
            # st.info("Jewelery SHOP : " + str(jewel) + " count : " + str(len(jewel)))
            # jewel3 = compute_top3(jewel)
            # if not jewel3.empty:
            #     st.subheader("📈 Top 3 Jewelry Below MA20")
            #     st.dataframe(jewel3)
            #     bought_jewel = filter_top3_in_holdings(jewel3)
            # else:
            #     st.info("No qualifying Jewelry found.")
            #
            # st.info("Nifty SHOP : " + str(nifty) + " count : " + str(len(nifty)))
            # nifty3 = compute_top3(nifty)
            # if not nifty3.empty:
            #     st.subheader("📈 Top 3 Stocks Below MA20")
            #     st.dataframe(nifty3)
            #     bought_nifty = filter_top3_in_holdings(nifty3)
            # else:
            #     st.info("No qualifying Stocks found.")

            averaging()


            config = upstox_client.Configuration()
            api_client = upstox_client.ApiClient(config)

            login_api = upstox_client.LoginApi(api_client)
            history_api = upstox_client.HistoryV3Api(api_client)
            quote_api = upstox_client.MarketQuoteV3Api(api_client)
            portfolio_api = upstox_client.PortfolioApi(api_client)
            post_trade_api = upstox_client.PostTradeApi(api_client)
            order_api = upstox_client.OrderApiV3(api_client)
            order_apiv1 = upstox_client.OrderApi(api_client)
            api_version = '2.0'


            # Global injection for helper functions
            globals().update({
                "history_api": history_api,
                "quote_api": quote_api,
                "portfolio_api": portfolio_api,
                "order_api": order_api,
                "api_version": api_version,
                "portfolio": portfolio
            })

        except Exception as e:
            st.error(f"Something went wrong: {e}")
