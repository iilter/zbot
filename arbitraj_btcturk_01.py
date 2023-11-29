import json
import time
from decimal import Decimal

import botfunction as func
from botclass import TriangularPair as pairClass
from btcturkclass import OrderBook as tickerClass

#
# Uclu arbitraj bot BTCTURK
#
def main():
    db = func.connectDB()
    dbCursor = db.cursor()

    # Read binance section from config.ini
    btcturkConfig = func.readConfig(filename="config.ini", section="btcturk")

    url_book_ticker = btcturkConfig["url_base"] + btcturkConfig["url_orderbook"]

    ticker = tickerClass()
    ticker.dbCursor = dbCursor

    start_time = time.time()
    toplam = 0
    adet = 0
    komisyon = Decimal(0.0015)
    pair = pairClass()
    pair.dbCursor = dbCursor
    pairRows = pair.readAllPair(exchangeId=3, refSymbol="TRY")
    if (pairRows is not None) and (len(pairRows) > 0):
        while True:
            #print("*****************************")
            for pairRow in pairRows:
                rec = json.loads(pairRow[0])
                aSymbol = rec["pair_a_symbol"]
                bSymbol = rec["pair_b_symbol"]
                cSymbol = rec["pair_c_symbol"]

                ticker.symbol = aSymbol
                tickerRow = ticker.getData(url=url_book_ticker)
                price = tickerRow['bids'][0][0]
                aBidPrice = Decimal(price)
                qty = tickerRow['bids'][0][1]
                aBidQty = Decimal(qty)
                price = tickerRow['asks'][0][0]
                aAskPrice = Decimal(price)
                qty = tickerRow['asks'][0][1]
                aAskQty = Decimal(qty)

                ticker.symbol = bSymbol
                tickerRow = ticker.getData(url=url_book_ticker)
                price = tickerRow['bids'][0][0]
                bBidPrice = Decimal(price)
                qty = tickerRow['bids'][0][1]
                bBidQty = Decimal(qty)
                price = tickerRow['asks'][0][0]
                bAskPrice = Decimal(price)
                qty = tickerRow['asks'][0][1]
                bAskQty = Decimal(qty)

                ticker.symbol = cSymbol
                tickerRow = ticker.getData(url=url_book_ticker)
                price = tickerRow['bids'][0][0]
                cBidPrice = Decimal(price)
                qty = tickerRow['bids'][0][1]
                cBidQty = Decimal(qty)
                price = tickerRow['asks'][0][0]
                cAskPrice = Decimal(price)
                qty = tickerRow['asks'][0][1]
                cAskQty = Decimal(qty)

                AlAlSatOran = (1/aAskPrice) * (1/bAskPrice) * cBidPrice
                AlSatSatOran = (1/cAskPrice) * bBidPrice * aBidPrice

                # print(f"aPair: {aSymbol} bPair: {bSymbol} cPair: {cSymbol} AL_AL_SAT: {AlAlSatOran} "
                #       f"AL_SAT_SAT: {AlSatSatOran}")

                arbitrajSabiti = 1 + komisyon

                if AlAlSatOran > arbitrajSabiti:
                    toplam = toplam + AlAlSatOran - arbitrajSabiti
                    adet = adet + 1
                    print(f"aPair: {aSymbol} bPair: {bSymbol} cPair: {cSymbol} AL_AL_SAT: {AlAlSatOran} "
                          f"-> Toplam tutar: {toplam} Adet: {adet} *****")

                if AlSatSatOran > arbitrajSabiti:
                    toplam = toplam + AlSatSatOran - arbitrajSabiti
                    adet = adet + 1
                    print(f"aPair: {aSymbol} bPair: {bSymbol} cPair: {cSymbol} AL_SAT_SAT: {AlSatSatOran} "
                          f"-> Toplam tutar: {toplam} Adet: {adet} *****")

                time.sleep(3)
    print(f"Toplam süre: {(time.time() - start_time) / 60} dakika ")
    print(f"Toplam tutar: {toplam} adet: {adet}")


#    print(f"Çekilen kayıt sayısı: {len(symbolRows)}")


main()
