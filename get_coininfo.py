# BINANCE borsasında işlem gören coin bilgilerini çeker.
# Tarih: 06.03.2022
import sys
import mariadb
import click as click

import botfunction as func
from botclass import BinanceExchangeInfo as symbolClass


#
#  Binance borsasındaki işlem gören coin listesini çeker COIN tablosuna yazar
#
def main():
    db = func.connectDB()
    dbCursor = db.cursor()
    binanceConfig = func.readConfig(filename="config.ini", section="binance")

    url_api = binanceConfig["url_base"] + binanceConfig["url_exchangeinfo"]

    api = symbolClass()
    api.dbCursor = dbCursor
    records = api.getData(url_api)
    kayitSayisi = 0
    if (records is not None) and (len(records) > 0):
        # print(f"{records}")
        # print(f"{len(records)}")
        for record in records:
            if (record["status"] == 'TRADING') and (record["isSpotTradingAllowed"]):
                if 'MARKET' in record["orderTypes"]:
                    api.symbol = record["symbol"]
                    api.baseAsset = record["baseAsset"]
                    api.quoteAsset = record["quoteAsset"]
                    api.baseAssetName = record["baseAsset"]
                    api.addData()
                    ++kayitSayisi

    print(f"Kayıt Sayısı: {kayitSayisi}")

    db.commit()
    db.close()


main()
