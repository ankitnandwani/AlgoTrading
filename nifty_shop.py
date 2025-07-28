from urllib.parse import urlparse, parse_qs

from niftystocks import ns
import datetime
import pandas as pd
import upstox_client
from upstox_client.rest import ApiException
import json
from datetime import datetime, timedelta, UTC
import streamlit as st

# 🔐 Your Upstox app credentials
API_KEY = "YOUR_API_KEY"
API_SECRET = "YOUR_API_SECRET"
REDIRECT_URI = "https://niftyshop.streamlit.app/"  # Must match Upstox app settings

st.set_page_config(page_title="Nifty Shop", layout="centered")

# 🔁 Step 1: OAuth Login
st.title("🔐 Login to Upstox")

# OAuth login URL
login_url = (
    f"https://api.upstox.com/v2/login/authorization/dialog"
    f"?response_type=code"
    f"&client_id={API_KEY}"
    f"&redirect_uri={REDIRECT_URI}"
    f"&scope=read%20trade"
)

st.markdown(f"[Login with Upstox]({login_url})")

# Step 2: Ask user to paste full redirected URL
redirected_url = st.text_input("Paste the full redirected URL here after login:")
auth_code = None

if redirected_url:
    try:
        parsed_url = urlparse(redirected_url)
        query = parse_qs(parsed_url.query)
        auth_code = query.get("code", [None])[0]
        if not auth_code:
            st.error("Authorization code not found in URL.")
    except Exception as e:
        st.error(f"Failed to extract code: {e}")

#01406508
# Step 3: Exchange auth_code for access_token
if auth_code:
    try:
        api = upstox_client.LoginApi()
        token_resp = api.get_access_token(
            client_id=API_KEY,
            client_secret=API_SECRET,
            code=auth_code,
            redirect_uri=REDIRECT_URI,
            grant_type="authorization_code"
        )
        access_token = token_resp.access_token
        st.success("Access Token received!")
        st.code(access_token, language="bash")
    except Exception as e:
        st.error(f"Failed to get access token: {e}")


# 🛠 Helper: Get historical closes
def get_last_n_closes(instrument_key, n=19, days_buffer=30):
    to_date = datetime.now(UTC).strftime("%Y-%m-%d")
    from_date = (datetime.now(UTC) - timedelta(days=days_buffer)).strftime("%Y-%m-%d")
    resp = history_api.get_historical_candle_data1(instrument_key=instrument_key, unit="days", interval= 1, to_date=to_date, from_date=from_date)
    candles = resp.data.candles
    closes = [candle[4] for candle in candles]  # 4th index is 'close'
    return closes[-n:] if len(closes) >= n else []

# 🛠 Helper: Get live LTP
def get_ltp(instrument_key, sym):
    response = quote_api.get_ltp(instrument_key=instrument_key)
    return response.data['NSE_EQ:'+sym].last_price


#symbol to instrument key mapping
def load_symbol_to_instrument_key_map(json_file="NSE.json"):
    with open(json_file, 'r') as f:
        instruments = json.load(f)

    symbol_map = {}

    for inst in instruments:
        if (
            inst.get("segment") == "NSE_EQ" and
            inst.get("instrument_type") == "EQ" and
            "trading_symbol" in inst and
            "instrument_key" in inst
        ):
            symbol_map[inst["trading_symbol"]] = inst["instrument_key"]

    return symbol_map

# ✅ Main computation
def compute_top5_nifty_below_ma():
    results = []
    symbol_to_key = load_symbol_to_instrument_key_map("NSE.json")

    for sym in nifty50_list:
        try:
            instrument_key = symbol_to_key.get(sym)
            if not instrument_key:
                continue
            ltp = get_ltp(instrument_key, sym)
            closes = get_last_n_closes(instrument_key)
            if len(closes) < 19:
                continue

            ma20 = (sum(closes)) / 20
            dev = ((ltp - ma20) / ma20) * 100
            if ltp < ma20:
                results.append((sym, ltp, ma20, dev, instrument_key))
        except ApiException as e:
            st.warning(f"{sym} error: {e}")

    df = pd.DataFrame(results, columns=["Symbol","LTP","MA20","Deviation%", "Instrument_token"])
    df = df[df["Deviation%"] < 0].sort_values("Deviation%")
    return df.head(5)

def buy(instrument_key, ltp):
    body = upstox_client.PlaceOrderV3Request(quantity=1, product="D", validity="DAY",
                                             price=0.0, tag="nifty_shop", instrument_token=instrument_key,
                                             order_type="MARKET", transaction_type="BUY", disclosed_quantity=0,
                                             trigger_price=0.0, is_amo=False, slice=True)
    api_response = order_api.place_order(body)
    st.success(f"Buy order placed: {api_response}")

def get_current_portfolio(top5stocks):
    portfolio = portfolio_api.get_holdings(api_version)
    for _, row in top5stocks.iterrows():
        st.info(row['Instrument_token'])
        match = next((item for item in portfolio.data if item.instrument_token == row['Instrument_token']), None)
        if match:
            st.info(f"Already holding: {row['Symbol']}")
        else:
            st.info(f"Buying new stock: {row['Symbol']}")
            buy(row['Instrument_token'], row['LTP'])
            st.stop()

#all 5 stocks available for buy are already in portfolio
#so we will average our worst performer from the list with cmp
def averaging(top5stocks):
    portfolio = portfolio_api.get_holdings(api_version)
    worst_deviation = None
    stock_to_average = None
    for _, row in top5stocks.iterrows():
        match = next((item for item in portfolio.data if item.instrument_token == row['Instrument_token']), None)
        if match:
            avg_buy_price = match.average_price
            current_price = row['LTP']  # Already calculated in top5
            if avg_buy_price > 0:  # Prevent division by zero
                deviation = ((current_price - avg_buy_price) / avg_buy_price) * 100
                print(row['Symbol'] + " has deviation = " + str(deviation))
                if worst_deviation is None or deviation < worst_deviation:
                    worst_deviation = deviation
                    stock_to_average = row

    if stock_to_average and worst_deviation < 3.14:
        buy(stock_to_average['Instrument_token'], stock_to_average['LTP'])
        st.success(f"Averaged: {stock_to_average['Symbol']} @ Deviation {worst_deviation:.2f}%")
    else:
        st.info("No eligible stock found in portfolio for averaging.")


# 🔐 UI Components
st.title("📊 Nifty Shop")

if access_token and st.button("🚀 Analyze & Trade"):
    try:
        nifty50_list = ns.get_nifty50()

        config = upstox_client.Configuration()
        config.access_token = access_token
        api_client = upstox_client.ApiClient(config)

        login_api = upstox_client.LoginApi(api_client)
        history_api = upstox_client.HistoryV3Api(api_client)
        quote_api = upstox_client.MarketQuoteV3Api(api_client)
        portfolio_api = upstox_client.PortfolioApi(api_client)
        order_api = upstox_client.OrderApiV3(api_client)
        api_version = '2.0'

        # Global injection for helper functions
        globals().update({
            "nifty50_list": nifty50_list,
            "history_api": history_api,
            "quote_api": quote_api,
            "portfolio_api": portfolio_api,
            "order_api": order_api,
            "api_version": api_version,
        })

        top5 = compute_top5_nifty_below_ma()
        if not top5.empty:
            st.subheader("📈 Top 5 Nifty Stocks Below MA20")
            st.dataframe(top5)
            get_current_portfolio(top5)
            averaging(top5)
        else:
            st.info("No qualifying stocks found.")

    except Exception as e:
        st.error(f"Something went wrong: {e}")




