import botfunction as func
from btcturkclass import ExchangeInfo as symbolClass


#
#  BTCTURK (3) borsasındaki işlem gören coin listesini çeker ve COIN tablosuna yazar
#
def main():
    db = func.connectDB()
    dbCursor = db.cursor()
    btcturkConfig = func.readConfig(filename="config.ini", section="btcturk")

    url_api = btcturkConfig["url_base"] + btcturkConfig["url_exchangeinfo"]

    api = symbolClass()
    api.dbCursor = dbCursor
    records = api.getData(url_api)
    kayitSayisi = 0
    if (records is not None) and (len(records) > 0):
        # print(f"{records}")
        # print(f"{len(records)}")
        for record in records:
            if record["status"] == 'TRADING':
                if 'MARKET' in record["orderMethods"]:
                    api.symbol = record["name"]
                    api.baseAsset = record["numerator"]
                    api.quoteAsset = record["denominator"]
                    api.baseAssetName = record["numerator"]
                    api.addData()
                    kayitSayisi = kayitSayisi + 1

    print(f"Kayıt Sayısı: {kayitSayisi}")

    db.commit()
    db.close()


main()
