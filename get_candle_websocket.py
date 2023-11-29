import websocket
import json
import pprint
import rel
from datetime import datetime
import botfunction as func
from botclass import BinanceExchangeInfo as symbolClass
from botclass import BinanceCandlePrice as candleClass

rel.safe_read()

# websocket.enableTrace(True)
dbCursor = None


def on_close(ws, close_status_code, close_msg):
    print(f"closed connection:{close_msg}")


def on_error(ws, error):
    print(f"closed: {error}")


def on_open(ws):
    print("opened connection")


def on_message(ws, message):
    global dbCursor
    response = json.loads(message)
    data = response['data']
    # pprint.pprint(listTicker)
    isClosedCandle = data["k"]["x"]
    if isClosedCandle:
        candle = candleClass()
        rec = data["k"]
        # Binance timestamp 13 hane uzunluğunda yani milliseconds olarak geliyor.
        # Çevirme işleminden önce 1000 e bölmek gerekiyor.
        milliSeconds = rec["t"]
        candle.start_time = datetime.fromtimestamp(milliSeconds / 1000)  # .strftime("%Y-%m-%d %H:%M:%S")
        milliSeconds = rec["T"]
        candle.close_time = datetime.fromtimestamp(milliSeconds / 1000)  # .strftime("%Y-%m-%d %H:%M:%S")
        candle.symbol = rec["s"]
        candle.candle_interval = rec["i"]
        candle.first_trade_id = rec["f"]
        candle.last_trade_id = rec["L"]
        candle.open_price = float(rec["o"])
        candle.close_price = float(rec["c"])
        candle.high_price = float(rec["h"])
        candle.low_price = float(rec["l"])
        candle.base_asset_volume = rec["v"]
        candle.number_of_trades = rec["n"]
        candle.quote_asset_volume = rec["q"]
        candle.taker_buy_base_asset_volume = rec["V"]
        candle.taker_buy_quote_asset_volume = rec["Q"]
        candle.ignore_info = rec["B"]
        candle.dbCursor = dbCursor
        candle.addData()


#
# Binance websocket ten mum verilerini çekme
#
def main():
    # Connect database
    global dbCursor
    db = func.connectDB()
    dbCursor = db.cursor()

    # Read binance section from config.ini
    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    streamName = "@kline_5m"
    symbol = symbolClass()
    symbol.dbCursor = dbCursor
    symbolRows = symbol.readAllSymbol()
    symbolCount = len(symbolRows)
    if (symbolRows is not None) and (symbolCount > 0):
        streams = "stream?streams="
        ix = 0
        for symbolRow in symbolRows:
            coin = json.loads(symbolRow[0])
            coinName = coin["symbol"].lower()
            streams = streams + f"{coinName}{streamName}"
            ix = ix + 1
            if ix < symbolCount:
                streams = streams + "/"

        baseUrl = binanceConfig["websocket_base"]
        socketUrl = f"{baseUrl}/{streams}"
        ws = websocket.WebSocketApp(socketUrl,
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
        rel.signal(2, rel.abort)
        rel.dispatch()


main()
