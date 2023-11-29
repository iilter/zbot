import sys
import json
import botfunction as func
from botclass import Test as testClass


def main():
    TEST_ID = 4
    RSI_LOW_LEVEL = 10
    LR_LOW_LEVEL = 10

    db = func.connectDB()
    dbCursor = db.cursor()

    test = testClass()
    testRows = test.fetchAllAlarm(dbCursor=dbCursor, testId=TEST_ID, rsiLevel=RSI_LOW_LEVEL, lrLevel=LR_LOW_LEVEL)
    if (testRows is None) or (len(testRows) <= 0):
        sys.exit(1)

    for testRow in testRows:
        row = json.loads(testRow[0])
        testId = row["test_id"]
        coin = row["symbol"]
        candleStartTime = row["candle_start_time"]
        candleCloseTime = row["candle_close_time"]
        openPrice = row["open_price"]
        closePrice = row["close_price"]
        rsi = row["rsi"]
        lr = row["lr"]
        lowLR = row["low_lr"]

        # SIMULATION tablosuna status=BUY olarak yazılıp,
        # sonra alım yerinden itibaren 1m mum verileri ile ilerleyerek
        # her adımda kontroller yapılarak işlemin sona ermesi sağlanacak.
        # İşlem sona erince SIMULATION tablosu status=SELL ve satış değerleri ile update edilecek.


main()
