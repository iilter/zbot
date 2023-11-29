import time
from threading import Thread

BTC = 100

def buy():
    while True:
        print(f"BUY BTC:{BTC}", flush=True)
        time.sleep(1)


def sell(session=None, dbCursor=None):
    global BTC
    BTC = 100
    while True:
        BTC += 1

        print(f"SELL - session:{session} dbCursor: {dbCursor}", flush=True)
        time.sleep(1)

def main():
    print("main")
    thread_sell = Thread(target=sell,
                         args=("ses","cursor"),
                         daemon=True)
    # thread_buy = Thread(target=buy)
    thread_sell.start()
    # thread_buy.start()
    buy()

main()