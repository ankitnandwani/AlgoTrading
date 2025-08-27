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

st.set_page_config(page_title="Share Genius Mall", layout="centered")


def google_auth():
    # Define scope and load credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)

    # Authorize and open the sheet
    client = gspread.authorize(creds)
    ss = client.open_by_key(st.secrets["GOOGLE_SHEET_ID"])
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
    st.info("to_date : " + str(to_date))
    from_date = datetime.now(UTC) - timedelta(days=days_buffer)
    st.info("from_date : " + str(from_date))
    hist = client.historical_candles(exchange=Constants.ExchangeTypes.NSE_EQUITY, token=instrument_token, to=to_date,
                                     start=from_date,
                                     resolution=Constants.Resolutions.DAY)
    st.info("hist : " + str(hist))
    closes = hist['c']
    st.info("closes : " + str(closes))
    closerev = closes[::-1]
    st.info("closerev : " + str(closerev))
    clos20 = closerev[:n]
    st.info("clos20 : " + str(clos20))
    return closes[:n] if len(closes) >= n else []


# 🛠 Helper: Get live LTP
def get_ltp(instrument_token):
    start = datetime.now(UTC) - timedelta(days=2)
    st.info("start : " + str(start))
    to = datetime.now(UTC) - timedelta(days=1)
    st.info("to : " + str(to))
    instrument_token2 = [f"NSE_EQ-{instrument_token}"]
    st.info("instrument_token2 : " + str(instrument_token2))

    ltp = client.quotes(instruments=instrument_token2, mode=Constants.QuoteModes.LTP)
    st.info("ltp : " + str(ltp))
    hist = client.historical_candles(exchange=Constants.ExchangeTypes.NSE_EQUITY, token=instrument_token, to=to, start=start,
                                     resolution=Constants.Resolutions.DAY)
    st.info("hist : " + str(hist))
    st.info("hist in ltp: " + str(hist))
    close_price = hist['c'][0]
    return close_price

def get_ltp2():
    all_products = etf + jewel + nifty
    st.info("all_products : " + str(all_products))
    instrument_tokens = [
        symbol_to_key[symbol]
        for symbol in all_products
        if symbol in symbol_to_key  # ensure symbol exists in mapping
    ]
    st.info("instrument_token : " + str(instrument_tokens))

    response = client.quotes(instruments=instrument_tokens, mode=Constants.QuoteModes.LTP)
    st.info("response : " + str(response))

    last_trade_prices = {}

    for key in all_products:
        if key in response["data"]:  # check if key exists in response
            ltp = response["data"][key]["last_trade_price"]
            last_trade_prices[key] = ltp
            st.info("key : " + str(key) + " ltp : " + str(ltp))


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
        st.info(f"symbol: {sym}")
        try:
            instrument_key = symbol_to_key.get(sym)
            if not instrument_key:
                continue

            ltp = price["NSE_EQ-"+instrument_key]
            st.info(" ltp : " + str(ltp))
            closes = get_last_n_closes(instrument_key)
            if len(closes) < 20:
                continue

            ma20 = (sum(closes)) / 20
            dev = ((ltp - ma20) / ma20) * 100
            st.info(" dev : " + str(dev))
            results.append((sym, ltp, ma20, dev, instrument_key))
        except ApiException as e:
            st.warning(f"{sym} error: {e}")

    df = pd.DataFrame(results, columns=["Symbol", "LTP", "MA20", "Deviation%", "Instrument_token"])
    df = df.sort_values("Deviation%")
    return df.head(3)


def buy(instrument_key, ltp):
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

    min_investment = 10000
    quantity = max(1, math.ceil(min_investment / ltp))

    # Display order details
    st.subheader("🛒 Buy Order details")
    st.markdown(f"""
            **Instrument Token:** `{instrument_key}`  
            **LTP:** `₹{ltp}`  
            **Order Type:** `{order_type}`
            **Quantity:** `{quantity}`  
            **Order Value:** `₹{quantity * ltp}`  
            **Price:** `₹{price}`  
            **AMO:** `{is_amo}`
            """)

    try:
        body = upstox_client.PlaceOrderV3Request(quantity=quantity, product="D", validity="DAY",
                                                 price=price, tag="penny_etf", instrument_token=instrument_key,
                                                 order_type=order_type, transaction_type="BUY",
                                                 disclosed_quantity=0,
                                                 trigger_price=0.0, is_amo=is_amo, slice=True)
        api_response = order_api.place_order(body)
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

    quantity=0
    for item in portfolio.data:
        if item.tradingsymbol not in etf:
            continue

        if item.instrument_token == instrument_key:
            quantity = item.quantity
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
        body = upstox_client.PlaceOrderV3Request(quantity=quantity, product="D", validity="DAY",
                                                 price=price, tag="penny_etf", instrument_token=instrument_key,
                                                 order_type=order_type, transaction_type="SELL",
                                                 disclosed_quantity=0,
                                     trigger_price=0.0, is_amo=is_amo, slice=True)
        api_response = order_api.place_order(body)
        st.success(f"✅ Sell order placed successfully: {api_response}")
    except ApiException as e:
        st.error(f"❌ Failed to place order: {e}")



def get_current_portfolio(top5stocks):
    existing_holdings = {item.instrument_token for item in portfolio.data}
    existing_orders = order_apiv1.get_order_book(api_version=api_version)
    executed_order_tokens = {
        order.instrument_token
        for order in existing_orders.data
        if order.status in {"complete"}  # relevant open statuses
    }
    for _, row in top5stocks.iterrows():
        token = row['Instrument_token']
        symbol = row['Symbol']

        if token in existing_holdings:
            st.info(f"Already holding: {row['Symbol']}")
        elif token in executed_order_tokens:
            st.info(f"Order already placed for: {symbol}")
        else:
            st.info(f"Buying new ETF: {row['Symbol']}")
            buy(row['Instrument_token'], row['LTP'])
            st.stop()

def getOrderHistory():
    today = datetime.now(UTC).date()
    one_year_ago = today - timedelta(days=365)

    start_date = one_year_ago.strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    param = {
        'segment': "EQ"
    }

    order_summary = {}

    try:
        api_response = post_trade_api.get_trades_by_date_range(start_date, end_date, 1, 1000, **param)
        orders = getattr(api_response, "data", []) or []
        buy_orders = [o for o in orders if o.transaction_type == "BUY"]
        for order in buy_orders:
            symbol = order.symbol
            if symbol not in etf:
                continue
            if symbol not in order_summary:
                order_summary[symbol] = {
                    "last_buy_price": float(order.price),
                    "buy_count": 1
                }
            else:
                order_summary[symbol]["buy_count"] += 1

    except ApiException as e:
        st.error("Exception when calling OrderApi->get trades_by_date_range: %s\n" % e.body)

    return order_summary

# all 5 stocks available for buy are already in portfolio
# so we will average our worst performer from the list with cmp
def averaging():
    order_summary = getOrderHistory()

    candidates = []

    for item in portfolio.data:
        if item.tradingsymbol not in etf:
            continue

        info = order_summary.get(item.tradingsymbol)
        last_buy_price = float(info.get("last_buy_price", 0) or 0)
        order_count = info.get("buy_count", 0)

        # Skip if quantity is 0 or avg price is 0
        if item.quantity == 0 or not last_buy_price:
            continue

        try:
            # Fetch the current LTP from market API
            current_price = get_ltp(item.instrument_token, item.tradingsymbol)
        except Exception as e:
            st.warning(f"Failed to fetch LTP for {item.trading_symbol}: {e}")
            continue

        deviation = ((current_price - last_buy_price) / last_buy_price) * 100
        st.info(
            item.trading_symbol + f" has deviation = {deviation:.2f}% (current price {current_price} vs last buy {last_buy_price})")

        if deviation < -3.14:
            candidates.append({
                "instrument_token": item.instrument_token,
                "ltp": current_price,
                "symbol": item.trading_symbol,
                "deviation": deviation,
                "order_count": order_count
            })

        if deviation > 6.28:
            sell(item.instrument_token, current_price)

    if not candidates:
        st.info("No eligible stock found in portfolio for averaging.")
        return

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
            st.info("symbol_to_key : " + str(symbol_to_key))
            price = get_ltp2()
            st.info("price : " + str(price))

            st.info("ETF SHOP : " + str(etf) + " count : " + str(len(etf)))
            etf3 = compute_top3(etf)
            if not etf3.empty:
                st.subheader("📈 Top 3 ETF Below MA20")
                st.dataframe(etf3)

            else:
                st.info("No qualifying ETF found.")

            st.info("Jewelery SHOP : " + str(jewel) + " count : " + str(len(jewel)))
            jewel3 = compute_top3(jewel)
            if not jewel3.empty:
                st.subheader("📈 Top 3 Jewelry Below MA20")
                st.dataframe(jewel3)

            else:
                st.info("No qualifying Jewelry found.")

            st.info("Nifty SHOP : " + str(nifty) + " count : " + str(len(nifty)))
            nifty3 = compute_top3(jewel)
            if not nifty3.empty:
                st.subheader("📈 Top 3 Stocks Below MA20")
                st.dataframe(nifty3)

            else:
                st.info("No qualifying Stocks found.")

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
            portfolio = portfolio_api.get_holdings(api_version)

            # Global injection for helper functions
            globals().update({
                "history_api": history_api,
                "quote_api": quote_api,
                "portfolio_api": portfolio_api,
                "order_api": order_api,
                "api_version": api_version,
            })

        except Exception as e:
            st.error(f"Something went wrong: {e}")
