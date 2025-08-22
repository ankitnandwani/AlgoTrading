import json
import math
from datetime import datetime, timedelta, UTC

import streamlit as st
from vortex_api import VortexAPI, Constants

st.set_page_config(page_title="GOLDBEES ETF", layout="centered")


# symbol to instrument key mapping
def load_symbol_to_instrument_key_map(json_file="complete.json"):
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


def buy(buy_price):
    min_investment = 10000
    quantity = max(1, math.ceil(min_investment / buy_price))

    # Display order details
    st.subheader("🛒 Buy Order details")
    st.markdown(f"""
            **Instrument Token:** `{"GOLDBEES"}`    
            **Quantity:** `{quantity}`  
            **Order Value:** `₹{quantity * buy_price}`  
            **Price:** `₹{buy_price}`
            """)
    st.info("exchange= " + Constants.ExchangeTypes.NSE_EQUITY + ", token=" + str(ETF_TOKEN) + ","
                        "transaction_type=" + Constants.TransactionSides.BUY + ", product=" + Constants.ProductTypes.DELIVERY + ","
                        "variety=" + Constants.VarietyTypes.REGULAR_LIMIT_ORDER + ","
                        "quantity=" + str(quantity) + ", price=" + str(buy_price) + ", validity=" + Constants.ValidityTypes.AFTER_MARKET)

    api_response = client.place_order(exchange= Constants.ExchangeTypes.NSE_EQUITY, token=ETF_TOKEN,
                        transaction_type=Constants.TransactionSides.BUY, product=Constants.ProductTypes.MTF,
                        variety=Constants.VarietyTypes.REGULAR_LIMIT_ORDER,
                        quantity=quantity, price=buy_price,trigger_price=0.0,
                        disclosed_quantity= 0, validity=Constants.ValidityTypes.AFTER_MARKET)

    st.success(f"✅ Buy order placed successfully: {api_response}")

def get_buy_price():
    start = datetime.now(UTC) - timedelta(days=1)
    to = datetime.now(UTC)
    hist = client.historical_candles(exchange=Constants.ExchangeTypes.NSE_EQUITY, token=ETF_TOKEN, to=to, start=start,
                                     resolution=Constants.Resolutions.DAY)
    close_price = hist['c'][0]
    st.info("close_price : " + str(close_price))
    buy_p = close_price - 0.20
    st.info("buy_price : " + str(buy_p))
    return buy_p




# 🔐 UI Components
st.title("📊 GOLDBEES ETF")

API_KEY = "kdYNchen1BKmeQbK22ingVtEDmd2sph8jKcDNKzf"
APPLICATION_ID = "dev_zeHSphjh"
ETF_TOKEN = 14428

# Initialize session state
if "access_token" not in st.session_state:
    st.session_state.access_token = None

# Capture `auth` param from callback
query_params = st.query_params
auth_token = query_params.get("auth")

# If `auth` found, save and clear URL params to stop redirect loop
if auth_token and not st.session_state.access_token:
    st.session_state.access_token = auth_token

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
            token = client.exchange_token(st.session_state.access_token)
            st.info("token : " + str(token))
            orders = client.orders(limit=20, offset=1)
            buy_rate = get_buy_price()
            buy(buy_rate)



        except Exception as e:
            st.error(f"Something went wrong: {e}")
