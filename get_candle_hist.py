import json
import sys
import time
from datetime import datetime
import pprint

import mariadb
from numpy.compat import long

import botfunction as func
from botclass import BinanceExchangeInfo as symbolClass
from botclass import BinanceCandlePrice as candleClass


#
# Gemişteki mum verilerini binance borsasından çekip, CANDLE_PRICE tablosuna yazar.
#
def main():
    db = func.connectDB()
    dbCursor = db.cursor()
    # Read binance section from config.ini
    binanceConfig = func.readConfig(filename="config.ini", section="binance")

    url_candle = binanceConfig["url_historical"] + binanceConfig["url_candle_historical"]
    CANDLE_INTERVAL = "1h"
    LIMIT = 1000

    baslangic_zamani = time.time()

    candle = candleClass()
    candle.dbCursor = dbCursor

    # Mum verileri çekilecek coinlerin sembolleri (kodları) okunur
    symbol = symbolClass()
    symbol.dbCursor = dbCursor
    symbolRows = symbol.readAllSymbol()
    if (symbolRows is not None) and (len(symbolRows) > 0):
        for symbolRow in symbolRows:
            start_time = time.time()
            coin = json.loads(symbolRow[0])
            coinSymbol = coin["symbol"]

            # Dataların en son kaldığı yerden çekilmesi için, CANDLE_PRICE tablosundaki
            # coin ve intervale ait son kayıt okunur.
            # Son kaydın close_time verisi startingTime olarak alınır.
            # Kayıt yoksa 2021-01-01 startingTime olarak kullanılır.
            maxCloseTime = candle.fetchMaxCloseTime(symbol=coinSymbol, candleInterval=CANDLE_INTERVAL)
            if maxCloseTime is None:
                # Binance da timestamp 13 hane (milisaniye olarak) kullanıldığı için 1000 ile çarpılır.
                startingTime = datetime.strptime('2021-01-01 00:00:00.00000', '%Y-%m-%d %H:%M:%S.%f').timestamp() * 1000
                startingTime = long(round(startingTime))
            else:
                startingTime = long(round(maxCloseTime.timestamp() * 1000))

            # Coin için mum verilerini getirir
            candleRows = candle.getData(url=url_candle,
                                        symbol=coinSymbol,
                                        candleInterval=CANDLE_INTERVAL,
                                        startTime=startingTime,
                                        limit=LIMIT)
            if candleRows is None:
                rowCount = 0
            else:
                rowCount = len(candleRows)

            if rowCount <= 0:
                continue  # sonraki coin

            while rowCount > 0:
                for candleRow in candleRows:
                    # pprint.pprint(candleRow[0])
                    candle.symbol = coinSymbol
                    candle.candle_interval = CANDLE_INTERVAL

                    startTimestamps = candleRow[0]
                    candle.start_time = datetime.fromtimestamp(startTimestamps / 1000)

                    candle.open_price = candleRow[1]
                    candle.high_price = candleRow[2]
                    candle.low_price = candleRow[3]
                    candle.close_price = candleRow[4]
                    candle.base_asset_volume = candleRow[5]

                    closeTimestamps = candleRow[6]
                    candle.close_time = datetime.fromtimestamp(closeTimestamps / 1000)

                    candle.quote_asset_volume = candleRow[7]
                    candle.number_of_trades = candleRow[8]
                    candle.taker_buy_base_asset_volume = candleRow[9]
                    candle.taker_buy_quote_asset_volume = candleRow[10]
                    candle.ignore_info = candleRow[11]

                    currentTimestamps = long(round(time.time() * 1000))
                    # Mumun kapanış saati şu anki zamandan büyükse mum henüz kapanmamış demektir.
                    if currentTimestamps > closeTimestamps:
                        candle.addData()  # insert candle_price table

                startingTime = closeTimestamps   # en son mum kapanış zamanı atanır.
                candleRows = candle.getData(url=url_candle,
                                            symbol=coinSymbol,
                                            candleInterval=CANDLE_INTERVAL,
                                            startTime=startingTime,
                                            limit=LIMIT)
                if candleRows is None:
                    rowCount = 0
                else:
                    rowCount = len(candleRows)

            print(f"{coinSymbol} - {CANDLE_INTERVAL} toplam: {time.time() - start_time} saniye ")

    print(f"==={CANDLE_INTERVAL} toplam: {time.time() - baslangic_zamani} saniye===")


main()
