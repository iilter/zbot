import json
import sys
import time

import mariadb

import botfunction as func
from botclass import BinanceExchangeInfo as symbolClass
from botclass import BinanceSymbolPriceTicker as priceTickerClass


#
# Anlık coin fiyatlarını çekme
#
def main():
    db = func.connectDB()
    dbCursor = db.cursor()

    # Read binance section from config.ini
    binanceConfig = func.readConfig(filename="config.ini", section="binance")

    url_price_ticker = binanceConfig["url_base"] + binanceConfig["url_price_ticker"]

    priceTicker = priceTickerClass()
    priceTicker.dbCursor = dbCursor

    start_time = time.time()

    symbol = symbolClass()
    symbol.dbCursor = dbCursor
    symbolRows = symbol.readAllSymbol()
    if (symbolRows is not None) and (len(symbolRows) > 0):
        for symbolRow in symbolRows:
            coin = json.loads(symbolRow[0])
            priceTicker.symbol = coin["symbol"]
            # coin için anlık fiyat bilgisi getirilir
            priceRecord = priceTicker.getData(url_price_ticker)
            priceTicker.symbol = priceRecord["symbol"]
            priceTicker.price = priceRecord["price"]
            priceTicker.addData()

    print(f"Toplam süre: {time.time() - start_time} saniye")
    print(f"Çekilen kayıt sayısı: {len(symbolRows)}")


main()
