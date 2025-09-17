import datetime
import math

import pandas as pd
import upstox_client
from nsepython import nsefetch
from upstox_client.rest import ApiException
import json
from datetime import datetime, timedelta, timezone
import streamlit as st
from ta.momentum import RSIIndicator

UTC = timezone.utc

st.set_page_config(page_title="Nifty Shop RSI", layout="centered")


def get_rsi_upstox(closes):
    closes_series = pd.Series(closes[::-1])
    rsi_14 = RSIIndicator(close=closes_series, window=14)
    rsi_series = rsi_14.rsi()
    value = rsi_series.tail(1).iloc[0]
    return value


# 🛠 Helper: Get historical closes
def get_last_n_closes(instrument_key, n, days_buffer):
    to_date = datetime.now(UTC).strftime("%Y-%m-%d")
    from_date = (datetime.now(UTC) - timedelta(days=days_buffer)).strftime("%Y-%m-%d")
    resp = history_api.get_historical_candle_data1(instrument_key=instrument_key, unit="days", interval=1,
                                                   to_date=to_date, from_date=from_date)
    candles = resp.data.candles
    closes = [candle[4] for candle in candles]  # 4th index is 'close'
    return closes[:n] if len(closes) >= n else []


# 🛠 Helper: Get live LTP
def get_ltp():
    instrument_tokens = [
        symbol_to_key.get(symbol)
        for symbol in all_products
        if symbol in symbol_to_key  # ensure symbol exists in mapping
    ]

    instrument_key_str = ",".join(instrument_tokens)
    response = quote_api.get_ltp(instrument_key=instrument_key_str)

    last_trade_prices = {}

    for key in all_products:
        sym = 'NSE_EQ:' + key
        if sym in response.data:  # check if key exists in response
            last_trade_prices[sym] = response.data[sym].last_price

    return last_trade_prices


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
def compute_top5_nifty_below_ma(is_rsi, stock_list):
    results = []

    for sym in stock_list:
        try:
            instrument_key = symbol_to_key.get(sym)
            if not instrument_key:
                continue

            ltp = last_trading_price["NSE_EQ:" + str(sym)]
            if is_rsi:
                closes = get_last_n_closes(instrument_key=instrument_key, n=99, days_buffer=200)
                rsi = get_rsi_upstox(closes)
                results.append((sym, ltp, rsi, instrument_key))
            else:
                closes = get_last_n_closes(instrument_key=instrument_key, n=20, days_buffer=60)
                ma20 = (sum(closes)) / 20
                dev = ((ltp - ma20) / ma20) * 100
                results.append((sym, ltp, ma20, dev, instrument_key))
        except ApiException as e:
            st.warning(f"{sym} error: {e}")

    if is_rsi:
        all_df = pd.DataFrame(results, columns=["Symbol", "LTP", "RSI", "Instrument_token"])
        below35_df = all_df[all_df["RSI"] < 35].sort_values("RSI").reset_index(drop=True)
        below35_df.index += 1  # start index from 1 for display
        return all_df, below35_df
    else:
        df = pd.DataFrame(results, columns=["Symbol", "LTP", "MA20", "Deviation%", "Instrument_token"])
        df = df.sort_values("Deviation%").reset_index(drop=True)
        df.index += 1
        return df, df.head(2)


def buy(instrument_key, ltp):
    # Get current IST time
    now_ist = datetime.now(UTC).astimezone(timezone(timedelta(hours=5, minutes=30)))
    market_close_time = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

    # Determine order type and AMO status based on current time
    if now_ist < market_close_time:
        is_amo = False
    else:
        is_amo = True

    min_investment = 20000
    quantity = max(1, math.ceil(min_investment / ltp))

    # Display order details
    st.subheader("🛒 Buy Order details")
    st.markdown(f"""
            **Instrument Token:** `{instrument_key}`  
            **LTP:** `₹{ltp}` 
            **Quantity:** `{quantity}`  
            **Order Value:** `₹{quantity * ltp}`  
            **AMO:** `{is_amo}`
            """)

    try:
        body = upstox_client.PlaceOrderV3Request(quantity=quantity, product="D", validity="DAY",
                                                 price=ltp, tag="nifty_shop", instrument_token=instrument_key,
                                                 order_type="LIMIT", transaction_type="BUY",
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
        is_amo = False
    else:
        is_amo = True

    quantity = 0
    for item in portfolio.data:
        if item.tradingsymbol not in all_products:
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
            **Quantity:** `{quantity}`
            **AMO:** `{is_amo}`
            """)

    try:
        body = upstox_client.PlaceOrderV3Request(quantity=quantity, product="D", validity="DAY",
                                                 price=ltp, tag="nifty_shop", instrument_token=instrument_key,
                                                 order_type="LIMIT", transaction_type="SELL",
                                                 disclosed_quantity=0,
                                                 trigger_price=0.0, is_amo=is_amo, slice=True)
        api_response = order_api.place_order(body)
        st.success(f"✅ Sell order placed successfully: {api_response}")
    except ApiException as e:
        st.error(f"❌ Failed to place order: {e}")


def get_current_portfolio(top5stocks):
    existing_holdings = {item.instrument_token for item in portfolio.data}

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
            return True

    return False


def get_order_history():
    today = datetime.now(UTC).date()
    one_year_ago = today - timedelta(days=365)

    start_date = one_year_ago.strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    param = {
        'segment': "EQ"
    }

    order_summ = {}

    try:
        api_response = post_trade_api.get_trades_by_date_range(start_date, end_date, 1, 1000, **param)
        orders = getattr(api_response, "data", []) or []
        buy_orders = [o for o in orders if o.transaction_type == "BUY"]
        for order in buy_orders:
            symbol = order.symbol
            if symbol not in all_products:
                continue
            if symbol not in order_summ:
                order_summ[symbol] = {
                    "last_buy_price": float(order.price),
                    "buy_count": 1
                }
            else:
                order_summ[symbol]["buy_count"] += 1

    except ApiException as e:
        st.error("Exception when calling OrderApi->get trades_by_date_range: %s\n" % e.body)

    return order_summ


# all 5 stocks available for buy are already in portfolio
# so we will average our worst performer from the list with cmp
def averaging(stock_list, is_buy_done, is_rsi):
    candidates = []

    for item in portfolio.data:
        if item.tradingsymbol not in stock_list:
            continue

        info = order_summary.get(item.tradingsymbol)
        last_buy_price = float(info.get("last_buy_price", 0) or 0)
        order_count = info.get("buy_count", 0)

        # Skip if quantity is 0 or avg price is 0
        if item.quantity == 0 or not last_buy_price:
            continue

        ltp = last_trading_price["NSE_EQ:" + str(item.tradingsymbol)]
        deviation = ((ltp - last_buy_price) / last_buy_price) * 100
        st.info(
            item.trading_symbol + f" has deviation = {deviation:.2f}% (current price {ltp} vs last buy {last_buy_price})")

        if deviation < -3.14:
            candidates.append({
                "instrument_token": item.instrument_token,
                "ltp": ltp,
                "symbol": item.trading_symbol,
                "deviation": deviation,
                "order_count": order_count
            })

        if deviation > 6.28:
            sell(item.instrument_token, ltp)

    if not candidates:
        st.info("No eligible stock found in portfolio for averaging.")
        return

    if (is_rsi and is_buy_done) or (not is_rsi and is_buy_done):
        st.info("Buy order already placed, skipping averaging")
        return

    if is_rsi:
        candidates.sort(key=lambda x: x["deviation"])

        for stock in candidates:
            order_count = stock["order_count"]
            rsi = rsi_map.get(stock["symbol"])
            st.info("stock : " + str(stock) + " order_count : " + str(order_count) + " rsi : " + str(rsi))
            if ((order_count == 1 and rsi < 30) or
                    (order_count == 2 and rsi < 25) or
                    (order_count == 3 and rsi < 20) or
                    (order_count == 4 and rsi < 15) or
                    (order_count == 5 and rsi < 10) or
                    (order_count == 6 and rsi < 5)):
                buy(stock['instrument_token'], stock['ltp'])
                st.success(f"Averaged: {stock['symbol']} @ Deviation {stock['deviation']:.2f}%")
            else:
                st.info("No stock met RSI rules for averaging.")
    else:
        best_candidate = min(candidates, key=lambda x: x["deviation"])
        buy(best_candidate['instrument_token'], best_candidate['ltp'])
        st.success(f"Averaged: {best_candidate['symbol']} @ Deviation {best_candidate['deviation']:.2f}%")


def get_access_token():
    api_response = login_api.token(api_version, code=code, client_id=client_id, client_secret=client_secret,
                                   redirect_uri=redirect_uri, grant_type="authorization_code")
    access_token = api_response.access_token
    return access_token


def get_stock_list():
    exclude = {"NIFTY 50", "HEROMOTOCO", "INDUSINDBK", "NIFTY NEXT 50", "DABUR", "ICICIPRULI", "SWIGGY"}
    # Fallback lists in case NSE fetch fails
    fallback_nifty50 = ['RELIANCE', 'HDFCBANK', 'TCS', 'BHARTIARTL', 'ICICIBANK', 'SBIN', 'HINDUNILVR', 'INFY', 'BAJFINANCE', 'ITC', 'LT', 'MARUTI', 'M&M', 'KOTAKBANK', 'HCLTECH', 'SUNPHARMA', 'ULTRACEMCO', 'AXISBANK', 'TITAN', 'BAJAJFINSV', 'NTPC', 'ETERNAL', 'ONGC', 'ADANIPORTS', 'BEL', 'POWERGRID', 'ADANIENT', 'JSWSTEEL', 'WIPRO', 'TATAMOTORS', 'BAJAJ-AUTO', 'ASIANPAINT', 'COALINDIA', 'NESTLEIND', 'TATASTEEL', 'JIOFIN', 'TRENT', 'GRASIM', 'SBILIFE', 'EICHERMOT', 'HINDALCO', 'HDFCLIFE', 'TECHM', 'CIPLA', 'APOLLOHOSP', 'SHRIRAMFIN', 'HEROMOTOCO', 'TATACONSUM', 'DRREDDY', 'INDUSINDBK']
    fallback_nifty_next50 = ['ABB', 'ADANIENSOL', 'ADANIGREEN', 'ADANIPOWER', 'AMBUJACEM', 'DMART', 'BAJAJHLDNG', 'BAJAJHFL', 'BANKBARODA', 'BPCL', 'BOSCHLTD', 'BRITANNIA', 'CGPOWER', 'CANBK', 'CHOLAFIN', 'DLF', 'DABUR', 'DIVISLAB', 'GAIL', 'GODREJCP', 'HAVELLS', 'HAL', 'HYUNDAI', 'ICICIGI', 'ICICIPRULI', 'INDHOTEL', 'IOC', 'IRFC', 'NAUKRI', 'INDIGO', 'JSWENERGY', 'JINDALSTEL', 'LTIM', 'LICI', 'LODHA', 'PIDILITIND', 'PFC', 'PNB', 'RECLTD', 'MOTHERSON', 'SHREECEM', 'SIEMENS', 'SWIGGY', 'TVSMOTOR', 'TATAPOWER', 'TORNTPHARM', 'UNITDSPR', 'VBL', 'VEDL', 'ZYDUSLIFE']
    try:
        nifty50_data = nsefetch("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050")
        nifty50_list = [stock['symbol'] for stock in nifty50_data['data']]
        nifty50_list = [symbol for symbol in nifty50_list if symbol not in exclude]
    except Exception as e:
        st.error(f"Failed to fetch NIFTY 50 data from NSE: {e}")
        nifty50_list = [symbol for symbol in fallback_nifty50 if symbol not in exclude]

    try:
        nifty_next50_data = nsefetch("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20NEXT%2050")
        nifty_next50_list = [stock['symbol'] for stock in nifty_next50_data['data']]
        nifty_next50_list = [symbol for symbol in nifty_next50_list if symbol not in exclude]
    except Exception as e:
        st.error(f"Failed to fetch NIFTY NEXT 50 data from NSE: {e}")
        nifty_next50_list = [symbol for symbol in fallback_nifty_next50 if symbol not in exclude]

    nifty100_lst = nifty50_list + nifty_next50_list
    penny_etf_lst = ['TATAGOLD', 'TATSILV', 'METALIETF', 'ABSLPSE', 'GROWWNIFTY', 'GROWWPOWER', 'GROWWLOVOL']
    return nifty100_lst, penny_etf_lst


# 🔐 UI Components
st.title("📊 Nifty Shop + Penny ETF")

login_api = upstox_client.LoginApi()
api_version = '2.0'
code = st.query_params.get("code")
client_id = st.secrets["CLIENT_ID"]
client_secret = st.secrets["CLIENT_SECRET"]
redirect_uri = st.secrets["REDIRECT_URI"]

if not code:
    # Show login button if user not authenticated
    login_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
    st.markdown(
        f'<a href="{login_url}" target="_blank">'
        f'<button style="padding:10px 20px;font-size:16px;">🔑 Login with Upstox</button>'
        f'</a>',
        unsafe_allow_html = True
    )
else:
    st.success("✅ Successfully logged in with Upstox")
    if st.button("🚀 Run Analysis and Trade"):
        try:
            nifty100_list, penny_etf_list = get_stock_list()
            st.info("nifty100_list : " + str(nifty100_list) + " count : " + str(len(nifty100_list)))
            all_products = nifty100_list + penny_etf_list

            config = upstox_client.Configuration()
            config.access_token = get_access_token()
            api_client = upstox_client.ApiClient(config)

            history_api = upstox_client.HistoryV3Api(api_client)
            quote_api = upstox_client.MarketQuoteV3Api(api_client)
            portfolio_api = upstox_client.PortfolioApi(api_client)
            post_trade_api = upstox_client.PostTradeApi(api_client)
            order_api = upstox_client.OrderApiV3(api_client)
            order_apiv1 = upstox_client.OrderApi(api_client)
            user_api = upstox_client.UserApi(api_client)

            funds_resp = user_api.get_user_fund_margin(api_version)
            st.info("funds_resp : " + str(funds_resp))
            fundsdata = funds_resp.data
            st.info("fundsdata : " + str(fundsdata))
            fundsdataequity = funds_resp.data['equity']
            st.info("fundsdataequity : " + str(fundsdataequity))
            funds = funds_resp.data['equity']['available_margin']
            st.info("Available funds : " + str(funds))

            portfolio = portfolio_api.get_holdings(api_version)
            existing_orders = order_apiv1.get_order_book(api_version=api_version)
            order_summary = get_order_history()
            symbol_to_key = load_symbol_to_instrument_key_map()
            last_trading_price = get_ltp()

            # Global injection for helper functions
            globals().update({
                "nifty100_list": nifty100_list,
                "history_api": history_api,
                "quote_api": quote_api,
                "portfolio_api": portfolio_api,
                "order_api": order_api,
                "api_version": api_version,
            })

            is_rsi_algo = True
            all_rsi, rsi_below35 = compute_top5_nifty_below_ma(is_rsi_algo, nifty100_list)
            rsi_map = dict(zip(all_rsi["Symbol"], all_rsi["RSI"]))

            is_nifty_buy_done = False
            if not rsi_below35.empty:
                st.subheader("📈 Stocks Below 35 RSI")
                st.dataframe(rsi_below35)
                is_nifty_buy_done = get_current_portfolio(rsi_below35)
            else:
                st.info("No qualifying stocks found.")

            averaging(nifty100_list, is_nifty_buy_done, is_rsi_algo)

            is_rsi_algo = False
            st.info("penny_etf_list : " + str(penny_etf_list) + " count : " + str(len(penny_etf_list)))
            all_rsi, top5 = compute_top5_nifty_below_ma(is_rsi_algo, penny_etf_list)
            is_etf_buy_done = False
            if not top5.empty:
                st.subheader("📈 Top 2 Penny ETF Below MA20")
                st.dataframe(top5)
                is_etf_buy_done = get_current_portfolio(top5)
            else:
                st.info("No qualifying ETF found.")

            averaging(penny_etf_list, is_etf_buy_done, is_rsi_algo)

        except Exception as e:
            st.error(f"Something went wrong: {e}")
