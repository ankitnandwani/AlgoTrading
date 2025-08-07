from niftystocks import ns
import datetime
import pandas as pd
import upstox_client
from upstox_client.rest import ApiException
import json
from datetime import datetime, timedelta, UTC, timezone
import streamlit as st

st.set_page_config(page_title="Nifty Shop", layout="centered")


# 🛠 Helper: Get historical closes
def get_last_n_closes(instrument_key, n=19, days_buffer=30):
    to_date = datetime.now(UTC).strftime("%Y-%m-%d")
    from_date = (datetime.now(UTC) - timedelta(days=days_buffer)).strftime("%Y-%m-%d")
    resp = history_api.get_historical_candle_data1(instrument_key=instrument_key, unit="days", interval=1,
                                                   to_date=to_date, from_date=from_date)
    candles = resp.data.candles
    closes = [candle[4] for candle in candles]  # 4th index is 'close'
    return closes[-n:] if len(closes) >= n else []


# 🛠 Helper: Get live LTP
def get_ltp(instrument_key, sym):
    response = quote_api.get_ltp(instrument_key=instrument_key)
    return response.data['NSE_EQ:' + sym].last_price


# symbol to instrument key mapping
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

    df = pd.DataFrame(results, columns=["Symbol", "LTP", "MA20", "Deviation%", "Instrument_token"])
    df = df[df["Deviation%"] < 0].sort_values("Deviation%")
    return df.head(5)


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

    # Display order details
    st.subheader("🛒 Buy Order details")
    st.markdown(f"""
            **Instrument Token:** `{instrument_key}`  
            **LTP:** `₹{ltp}`  
            **Order Type:** `{order_type}`  
            **Price:** `₹{price}`  
            **AMO:** `{is_amo}`
            """)

    try:
        body = upstox_client.PlaceOrderV3Request(quantity=1, product="D", validity="DAY",
                                                 price=price, tag="nifty_shop", instrument_token=instrument_key,
                                                 order_type=order_type, transaction_type="BUY",
                                                 disclosed_quantity=0,
                                                 trigger_price=0.0, is_amo=is_amo, slice=True)
        api_response = order_api.place_order(body)
        st.success(f"✅ Buy order placed successfully: {api_response}")
    except ApiException as e:
        st.error(f"❌ Failed to place order: {e}")


def get_current_portfolio(top5stocks):
    portfolio = portfolio_api.get_holdings(api_version)
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
            st.info(f"Buying new stock: {row['Symbol']}")
            buy(row['Instrument_token'], row['LTP'])
            st.stop()


# all 5 stocks available for buy are already in portfolio
# so we will average our worst performer from the list with cmp
def averaging():
    portfolio = portfolio_api.get_holdings(api_version)

    worst_deviation = None
    stock_to_average = None

    for item in portfolio.data:
        instrument_token = item.instrument_token
        avg_buy_price = item.average_price

        # Skip if quantity is 0 or avg price is 0
        if item.quantity == 0 or avg_buy_price == 0:
            continue

        try:
            # Fetch the current LTP from market API
            current_price = get_ltp(item.instrument_token, item.tradingsymbol)
        except Exception as e:
            st.warning(f"Failed to fetch LTP for {item.trading_symbol}: {e}")
            continue

        deviation = ((current_price - avg_buy_price) / avg_buy_price) * 100
        st.info(item.trading_symbol + " has deviation = " + str(deviation))

        if worst_deviation is None or deviation < worst_deviation:
            worst_deviation = deviation
            stock_to_average = {
                'instrument_token': instrument_token,
                'ltp': current_price,
                'symbol': item.trading_symbol
            }

    if stock_to_average and worst_deviation < -3.14:
        buy(stock_to_average['instrument_token'], stock_to_average['ltp'])
        st.success(f"Averaged: {stock_to_average['symbol']} @ Deviation {worst_deviation:.2f}%")
    else:
        st.info("No eligible stock found in portfolio for averaging.")


# 🔐 UI Components
st.title("📊 Nifty Shop")
access_token = st.text_input("Enter your ACCESS_TOKEN:", type="password")
run = st.button("🚀 Run Analysis and buy")

if run:
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
        order_apiv1 = upstox_client.OrderApi(api_client)
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
            averaging()
        else:
            st.info("No qualifying stocks found.")

    except Exception as e:
        st.error(f"Something went wrong: {e}")
