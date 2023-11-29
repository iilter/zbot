import websocket
import json
import pprint
import rel
from datetime import datetime
import botfunction as func
from botclass import BinanceExchangeInfo as symbolClass

rel.safe_read()

ORAN = 3.0


# websocket.enableTrace(True)


def on_close(ws, close_status_code, close_msg):
    print(f"closed connection:{close_msg}")


def on_error(ws, error):
    print(f"closed: {error}")


def on_open(ws):
    print("opened connection")


def on_message(ws, message):
    global ORAN
    response = json.loads(message)
    # print(json_data)
    data = response['data']
    # pprint.pprint(listTicker)

    symbol = data["s"]
    candle = data["k"]
    isClosedCandle = candle["x"]
    if isClosedCandle:
        closePrice = float(candle["c"])
        openPrice = float(candle["o"])
        if closePrice > openPrice:
            oran = (closePrice - openPrice) * 100 / openPrice
            if oran > ORAN:
                line = f"saat:{datetime.now()} symbol:{symbol} open price: {openPrice} close price:{closePrice} oran: {oran}"
                #                print(line)
                with open("artancoin.txt", "a") as fp:
                    fp.write(line)
                    fp.write("\n")


def main():
    # Connect database
    db = func.connectDB()
    dbCursor = db.cursor()

    # Read binance section from config.ini
    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    streamName = "@kline_3m"
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
