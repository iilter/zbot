import websocket
import json
import pprint
import rel
import time

rel.safe_read()
# websocket.enableTrace(True)

adet = 0
start_time = time.time()


def on_close(ws, close_status_code, close_msg):
    print(f"closed connection:{close_msg}")


def on_error(ws, error):
    print(f"closed: {error}")


def on_open(ws):
    global start_time
    start_time = time.time()
    print("opened connection")


def on_message(ws, message):
    global adet
    global start_time

    adet = adet + 1
    json_message = json.loads(message)
    # print("received message")
    # pprint.pprint(json_message)

    if adet > 300:
        print(f"Toplam süre: {time.time() - start_time} saniye")
        ws.close()

        print("Bitti")


def main():
    streams = [
        "btcusdt@aggTrade", "ftmusdt@kline_1m", "xrpusdt@trade", "runeusdt@kline_5m",
        "crvusdt@miniTicker", "vetusdt@ticker", "ethusdt@bookTicker"
    ]
    # streams = [
    #     # "btcusdt@trade"
    #     # "!ticker@arr"
    #     "ftmusdt@kline_1m"
    # ]

    # socketUrl = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ftmusdt@kline_1m"
    baseUrl = "wss://stream.binance.com:9443/"
    prm = "stream?streams="
    lenStream = len(streams)
    ix = 0
    for stream in streams:
        prm = prm + f"{stream}"
        ix = ix + 1
        if ix < lenStream:
            prm = prm + "/"

    socketUrl = f"{baseUrl}{prm}"
    ws = websocket.WebSocketApp(socketUrl,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever(dispatcher=rel)  # Set dispatcher to automatic reconnection
    rel.signal(2, rel.abort)
    rel.dispatch()


main()
