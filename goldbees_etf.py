import json
import streamlit as st
from vortex_api import VortexAPI

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
CALLBACK_URL = "https://goldbeesetf.streamlit.app/callback/"

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
    login_url = f"https://flow.rupeezy.in?applicationId={APPLICATION_ID}&redirect_uri={CALLBACK_URL}"
    st.markdown(
        f'<a href="{login_url}" target="_self">'
        f'<button style="padding:10px 20px;font-size:16px;">🔑 Login with Rupeezy</button>'
        f'</a>',
        unsafe_allow_html=True
    )
else:
    st.success("✅ Successfully logged in with Rupeezy")
    st.write(f"Access Token : {st.session_state.access_token}")
    st.write(f"Auth Token : {auth_token}")

    if st.button("🚀 Run Analysis and Trade"):
        try:
            client = VortexAPI(API_KEY, APPLICATION_ID)
            client.exchange_token(auth_token)
            orders = client.orders(limit=20, offset=1)
            st.info("orders : " + str(orders))
        except Exception as e:
            st.error(f"Something went wrong: {e}")
