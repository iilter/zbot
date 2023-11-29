import websocket
import json
import pprint
import rel

rel.safe_read()
websocket.enableTrace(True)


def on_close(ws):
    print("closed connection")


def on_error(ws, close_status_code, close_msg):
    print("closed")


def on_open(ws):
    print("opened connection")


def on_message(ws, message):
    print("received message")
    json_message = json.loads(message)
    pprint.pprint(json_message)
    # eventType = json_message['e']
    # print(f"Event Type: {eventType}")
    # if eventType == "kline":
    #     candle = json_message['k']
    #     is_candle_closed = candle['x']
    #     if is_candle_closed:
    #         pprint.pprint(json_message)
    # else:
    #     pprint.pprint(json_message)


def ws_thread(*args):
    pass


def main():
    # socket = "wss://stream.binance.com:9443/ws/ftmusdt@ticker"
    socket = "wss://stream.binance.com:9443/ws/!ticker@arr"
    ws = websocket.WebSocketApp(socket,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever(dispatcher=rel)
    rel.signal(2, rel.abort)
    rel.dispatch()


main()
