import json
from datetime import datetime

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
            client.exchange_token(st.session_state.access_token)
            orders = client.orders(limit=20, offset=1)
            inst = client.download_master()
            st.info("inst : " + str(inst))
            st.info("orders : " + str(orders))
            start = datetime(2025, 8, 21)  # start datetime
            to = datetime(2025, 8, 21)  # end datetime
            hist = client.historical_candles(exchange=Constants.ExchangeTypes.NSE_EQUITY, token=ETF_TOKEN, to=to, start=start, resolution=Constants.Resolutions.DAY)
            st.info("hist : " + str(hist))
            close_price = hist['c'][0]
            st.info("close_price : " + str(close_price))
        except Exception as e:
            st.error(f"Something went wrong: {e}")
