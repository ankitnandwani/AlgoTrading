import datetime
import math

import pandas as pd
import upstox_client
from nsepython import nsefetch
from upstox_client.rest import ApiException
import json
from datetime import datetime, timedelta, UTC, timezone
import streamlit as st
from ta.momentum import RSIIndicator

st.set_page_config(page_title="Nifty Shop RSI", layout="centered")

def get_rsiUpstox(closes):
    closes_series = pd.Series(closes[::-1])
    rsi_14 = RSIIndicator(close=closes_series, window=14)
    rsiSeries = rsi_14.rsi()
    value = rsiSeries.tail(1).iloc[0]
    return value

# 🛠 Helper: Get historical closes
def get_last_n_closes(instrument_key, n=99, days_buffer=200):
    to_date = datetime.now(UTC).strftime("%Y-%m-%d")
    from_date = (datetime.now(UTC) - timedelta(days=days_buffer)).strftime("%Y-%m-%d")
    resp = history_api.get_historical_candle_data1(instrument_key=instrument_key, unit="days", interval=1,
                                                     to_date=to_date, from_date=from_date)
    candles = resp.data.candles
    closes = [candle[4] for candle in candles]  # 4th index is 'close'
    return closes[:n] if len(closes) >= n else []


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
            rsi = get_rsiUpstox(closes)
            results.append((sym, ltp, rsi, instrument_key))
        except ApiException as e:
            st.warning(f"{sym} error: {e}")

    all_df = pd.DataFrame(results, columns=["Symbol", "LTP", "RSI", "Instrument_token"])
    below35_df = all_df[all_df["RSI"] < 35].sort_values("RSI").reset_index(drop=True)
    below35_df.index = below35_df.index + 1  # start index from 1 for display

    return all_df, below35_df


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
                                                 price=price, tag="nifty_shop", instrument_token=instrument_key,
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
        if item.tradingsymbol not in nifty50_list:
            continue
        if item.instrument_token == instrument_key:
            quantity = item.quantity
            st.info("symbol : " + str(item.tradingsymbol) + " quantity : " + str(quantity))
            break



    # Display order details
    st.subheader("🛒 Sell Order details")
    st.markdown(f"""
            **Instrument Token:** `{instrument_key}`  
            **LTP:** `₹{ltp}`  
            **Order Type:** `{order_type}`  
            **Price:** `₹{price}`  
            **AMO:** `{is_amo}`
            """)

    try:
        body = upstox_client.PlaceOrderV3Request(quantity=quantity, product="D", validity="DAY",
                                                 price=price, tag="nifty_shop", instrument_token=instrument_key,
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
            st.info(f"Buying new stock: {row['Symbol']}")
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
            if symbol not in nifty50_list:
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
        if item.tradingsymbol not in nifty50_list:
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
        st.info(item.trading_symbol + f" has deviation = {deviation:.2f}% (current price {current_price} vs last buy {last_buy_price})")

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

    candidates.sort(key=lambda x: x["deviation"])

    for stock in candidates:
        order_count = stock["order_count"]
        rsi = rsi_map.get(stock["symbol"])
        if ((order_count == 1 and rsi<30) or
            (order_count == 2 and rsi<25) or
            (order_count == 3 and rsi<20) or
            (order_count == 4 and rsi<15) or
            (order_count == 5 and rsi<10) or
            (order_count == 6 and rsi<5)):
            buy(stock['instrument_token'], stock['ltp'])
            st.success(f"Averaged: {stock['symbol']} @ Deviation {stock['deviation']:.2f}%")
            return
    else:
        st.info("No stock met RSI rules for averaging.")


# 🔐 UI Components
st.title("📊 Nifty Shop RSI")
access_token = st.text_input("Enter your ACCESS_TOKEN:", type="password")
run = st.button("🚀 Run Analysis and buy")

if run:
    try:
        nifty50_data = nsefetch("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050")
        nifty50_list = [stock['symbol'] for stock in nifty50_data['data']]
        nifty50_list = [symbol for symbol in nifty50_list if symbol != 'NIFTY 50']
        nifty_next50_data = nsefetch("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20NEXT%2050")
        nifty_next50_list = [stock['symbol'] for stock in nifty_next50_data['data']]
        nifty_next50_list = [symbol for symbol in nifty_next50_list if symbol != 'NIFTY NEXT 50']
        nifty50_list = nifty50_list + nifty_next50_list
        st.info("nifty100_list : " + str(nifty50_list))

        config = upstox_client.Configuration()
        config.access_token = access_token
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
            "nifty50_list": nifty50_list,
            "history_api": history_api,
            "quote_api": quote_api,
            "portfolio_api": portfolio_api,
            "order_api": order_api,
            "api_version": api_version,
        })

        all_rsi, rsi_below35 = compute_top5_nifty_below_ma()
        rsi_map = dict(zip(all_rsi["Symbol"], all_rsi["RSI"]))

        if not rsi_below35.empty:
            st.subheader("📈 Stocks Below 35 RSI")
            st.dataframe(rsi_below35)
            get_current_portfolio(rsi_below35)
        else:
            st.info("No qualifying stocks found.")

        getOrderHistory()
        averaging()

    except Exception as e:
        st.error(f"Something went wrong: {e}")
