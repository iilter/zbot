import json
import time
from decimal import Decimal

import botfunction as func
from botclass import BinanceSymbolPriceTicker as priceTickerClass
from botclass import TriangularPair as pairClass


#
# Uclu arbitraj icin anlik fiyatları çekme
# Silinecek arbitraj_01 yazildi bunun yerine
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
    toplam = 0
    adet = 0
    pair = pairClass()
    pair.dbCursor = dbCursor
    pairRows = pair.readAllPair(exchangeId=1, refSymbol="USDT")
    if (pairRows is not None) and (len(pairRows) > 0):
        for pairRow in pairRows:
            rec = json.loads(pairRow[0])
            aPairSymbol = rec["pair_a_symbol"]
            bPairSymbol = rec["pair_b_symbol"]
            cPairSymbol = rec["pair_c_symbol"]
            # coin için anlık fiyat bilgisi getirilir
            priceTicker.symbol = aPairSymbol
            priceRecord = priceTicker.getData(url_price_ticker)
            aPairPrice = Decimal(priceRecord["price"])

            priceTicker.symbol = bPairSymbol
            priceRecord = priceTicker.getData(url_price_ticker)
            bPairPrice = Decimal(priceRecord["price"])

            priceTicker.symbol = cPairSymbol
            priceRecord = priceTicker.getData(url_price_ticker)
            cPairPrice = Decimal(priceRecord["price"])

            caprazOran = (1/aPairPrice) * (1/bPairPrice) * cPairPrice

            if caprazOran > (1 + Decimal(0.00225)):
                toplam = toplam + caprazOran - (1 + Decimal(0.0025))
                adet = adet + 1
                print(f"aPair: {aPairSymbol} bPair: {bPairSymbol} cPair: {cPairSymbol} oran: {caprazOran} "
                      f"-> Toplam tutar: {toplam} adet: {adet}")
            # priceTicker.symbol = priceRecord["symbol"]
            # priceTicker.price = priceRecord["price"]
            # priceTicker.addData()

    print(f"Toplam süre: {time.time() - start_time} saniye")
    print(f"Toplam tutar: {toplam} adet: {adet}")
#    print(f"Çekilen kayıt sayısı: {len(symbolRows)}")


main()
