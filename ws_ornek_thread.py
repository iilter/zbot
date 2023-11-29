import time
import websocket
import json
import pprint
import rel

rel.safe_read()
# websocket.enableTrace(True)

adet = 0
start_time = time.time()


def on_close(ws, close_status_code, close_msg):
    print(f"closed connection:{close_msg}")


def on_error(ws, error):
    print(f"closed: {error}")


def on_open(ws):
    print("opened connection")


def on_message(ws, message):
    global adet
    global start_time

    adet = adet + 1
    # json_message = json.loads(message)
    # print("received message")
    # pprint.pprint(json_message)
    if adet > 300:
        print(f"Toplam süre: {time.time() - start_time} saniye")
        ws.close()

        print("Bitti")


def main():
    # socket = "wss://stream.binance.com:9443/ws/ftmusdt@ticker"
    baseEndPoint = "wss://stream.binance.com:9443/ws/"
    for event in ["ftmusdt@ticker", "btcusdt@trade"]:
        streamAddress = f"{baseEndPoint}{event}"
        ws = websocket.WebSocketApp(streamAddress,
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.run_forever(dispatcher=rel)
    rel.signal(2, rel.abort)
    rel.dispatch()


main()
