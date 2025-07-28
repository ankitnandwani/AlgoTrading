from niftystocks import ns
import datetime
import pandas as pd
import upstox_client
from upstox_client.rest import ApiException
import json
from datetime import datetime, timedelta, UTC

#get current list for Nifty50 stocks
nifty50_list = ns.get_nifty50()
print(nifty50_list)



# 🔐 CONFIG: Add your credentials
API_KEY = "YOUR_API_KEY"
API_SECRET = "YOUR_API_SECRET"
REDIRECT_URI = "https://google.com"
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"  # obtain using OAuth flow once


# 📦 Setup API client
config = upstox_client.Configuration()
config.access_token = ACCESS_TOKEN
api_client = upstox_client.ApiClient(config)

login_api = upstox_client.LoginApi(api_client)
history_api = upstox_client.HistoryV3Api(api_client)
quote_api = upstox_client.MarketQuoteV3Api(api_client)
portfolio_api = upstox_client.PortfolioApi(api_client)
order_api = upstox_client.OrderApiV3(api_client)
api_version = '2.0'

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
            print('NSE_EQ|'+sym)
            instrument_key = symbol_to_key.get(sym)
            print(instrument_key)
            ltp = get_ltp(instrument_key, sym)
            print("ltp = " + str(ltp))
            closes = get_last_n_closes(instrument_key)
            print("closes = " + str(closes))
            if len(closes) < 19:
                continue

            ma20 = (sum(closes)) / 20
            print("ma20 = " + str(ma20))
            dev = ((ltp - ma20) / ma20) * 100
            if ltp < ma20:
                results.append((sym, ltp, ma20, dev, instrument_key))
        except ApiException as e:
            print(f"{sym} error:", e)

    df = pd.DataFrame(results, columns=["Symbol","LTP","MA20","Deviation%", "Instrument_token"])
    df = df[df["Deviation%"] < 0].sort_values("Deviation%")
    return df.head(5)

def buy(instrument_key, ltp):
    body = upstox_client.PlaceOrderV3Request(quantity=1, product="D", validity="DAY",
                                             price=ltp, tag="nifty_shop", instrument_token=instrument_key,
                                             order_type="LIMIT", transaction_type="BUY", disclosed_quantity=0,
                                             trigger_price=0.0, is_amo=True, slice=True)
    api_response = order_api.place_order(body)
    print(api_response)

def get_current_portfolio(top5stocks):
    portfolio = portfolio_api.get_holdings(api_version)
    for _, row in top5stocks.iterrows():
        print(row['Instrument_token'])
        match = next((item for item in portfolio.data if item.instrument_token == row['Instrument_token']), None)
        if match:
            print("Match found:", match)
            print("continue searching")
        else:
            print("No match found.")
            #Buy stock and exit
            buy(row['Instrument_token'], row['LTP'])
            exit("Buy successful. Bought : " + row['Instrument_token'] + " - " + row['Symbol'])

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

    if stock_to_average is not None:
        print(f"Averaging stock: {stock_to_average['Symbol']} with deviation: {worst_deviation:.2f}%")
        if worst_deviation<3.14:
            buy(stock_to_average['Instrument_token'])
        exit("Averaging successful. Bought more of: " + stock_to_average['Symbol'])
    else:
        print("No eligible stock found in portfolio for averaging.")


if __name__ == "__main__":
    top5 = compute_top5_nifty_below_ma()
    print(top5.to_string(index=False))
    get_current_portfolio(top5)
    averaging(top5)




