import json
import time
from datetime import datetime
from decimal import Decimal

import botfunction as func
from botclass import BinanceBookTicker as tickerClass
from botclass import TriangularPair as pairClass
from botclass import TestArbitraj as testClass

"""
Uclu arbitraj bot
"""

def main():
    db = func.connectDB()
    dbCursor = db.cursor()

    # Read binance section from config.ini
    binanceConfig = func.readConfig(filename="config.ini", section="binance")

    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    test = testClass()

    ticker = tickerClass()
    ticker.dbCursor = dbCursor

    start_time = time.time()
    bakiye = Decimal(1000)
    toplam = 0
    adet = 0
    komisyon = Decimal(0.00225)
    arbitrajSabiti = 1 + komisyon

    pair = pairClass()
    pair.dbCursor = dbCursor
    pairRows = pair.readAllPair(exchangeId=1, refSymbol="BUSD")
    if (pairRows is not None) and (len(pairRows) > 0):
        while True:
            #print("*****************************")
            for pairRow in pairRows:
                rec = json.loads(pairRow[0])
                aSymbol = rec["pair_a_symbol"]
                bSymbol = rec["pair_b_symbol"]
                cSymbol = rec["pair_c_symbol"]
                symbols = [aSymbol, bSymbol, cSymbol]
                ticker.symbols = symbols
                while True:
                    tickerRows = ticker.getData(url=url_book_ticker)
                    for tickerRow in tickerRows:
                        if aSymbol == tickerRow["symbol"]:
                            aBidPrice = Decimal(tickerRow["bidPrice"])
                            aBidQty = Decimal(tickerRow["bidQty"])
                            aAskPrice = Decimal(tickerRow["askPrice"])
                            aAskQty = Decimal(tickerRow["askQty"])

                        if bSymbol == tickerRow["symbol"]:
                            bBidPrice = Decimal(tickerRow["bidPrice"])
                            bBidQty = Decimal(tickerRow["bidQty"])
                            bAskPrice = Decimal(tickerRow["askPrice"])
                            bAskQty = Decimal(tickerRow["askQty"])

                        if cSymbol == tickerRow["symbol"]:
                            cBidPrice = Decimal(tickerRow["bidPrice"])
                            cBidQty = Decimal(tickerRow["bidQty"])
                            cAskPrice = Decimal(tickerRow["askPrice"])
                            cAskQty = Decimal(tickerRow["askQty"])

                    AlAlSatOran = (1/aAskPrice) * (1/bAskPrice) * cBidPrice
                    AlSatSatOran = (1/cAskPrice) * bBidPrice * aBidPrice

                    # print(f"aPair: {aSymbol} bPair: {bSymbol} cPair: {cSymbol} AL_AL_SAT: {AlAlSatOran} "
                    #       f"AL_SAT_SAT: {AlSatSatOran}")

                    if AlAlSatOran > arbitrajSabiti:
                        aMaxPrice = aAskPrice * aAskQty
                        bMaxPrice = bAskPrice * aAskPrice * bAskQty
                        cMaxPrice = cBidPrice * cBidQty
                        islemTutar = min(aMaxPrice, bMaxPrice, cMaxPrice)

                        toplam = toplam + AlAlSatOran - arbitrajSabiti
                        adet = adet + 1
                        print(f"{aSymbol} - {bSymbol} - {cSymbol} AL_AL_SAT: {AlAlSatOran} "
                              f" Kar: {(AlAlSatOran - arbitrajSabiti)} tutar: {islemTutar} Toplam: {toplam} Adet: {adet} "
                              f"Saat: {datetime.now().strftime('%H:%M:%S.%f')} Sn: {time.time()} ")

                        test.borsa_id = 1
                        test.apair = aSymbol
                        test.abase = aSymbol
                        test.aquote = aSymbol
                        test.abid_price = aBidPrice
                        test.aask_price = aAskPrice
                        test.abid_qty = aBidQty
                        test.aask_qty = aAskQty
                        test.bpair = bSymbol
                        test.bbase = bSymbol
                        test.bquote = bSymbol
                        test.bbid_price = bBidPrice
                        test.bask_price = bAskPrice
                        test.bbid_qty = bBidQty
                        test.bask_qty = bAskQty
                        test.cpair = cSymbol
                        test.cbase = cSymbol
                        test.cquote = cSymbol
                        test.cbid_price = cBidPrice
                        test.cask_price = cAskPrice
                        test.cbid_qty = cBidQty
                        test.cask_qty = cAskQty
                        test.yontem = "AL_AL_SAT"
                        test.capraz = AlAlSatOran
                        test.islemTutar = islemTutar
                        test.komisyon = arbitrajSabiti
                        test.kar = AlAlSatOran - arbitrajSabiti
                        test.prg = "arbitraj_01"
                        test.islzaman = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        test.zamanstr = datetime.now().strftime('%H:%M:%S.%f')
                        test.addData(dbCursor=dbCursor)


                    if AlSatSatOran > arbitrajSabiti:
                        cMaxPrice = cAskPrice * cAskQty
                        bMaxPrice = bBidPrice * aBidPrice * bBidQty
                        aMaxPrice = aBidPrice * aBidQty

                        islemTutar = min(aMaxPrice, bMaxPrice, cMaxPrice)

                        toplam = toplam + AlSatSatOran - arbitrajSabiti
                        adet = adet + 1
                        print(f"{aSymbol} - {bSymbol} - {cSymbol} AL_SAT_SAT: {AlSatSatOran} "
                              f" Kar: {(AlSatSatOran - arbitrajSabiti)} tutar: {islemTutar} Toplam: {toplam} Adet: {adet} "
                              f"Saat: {datetime.now().strftime('%H:%M:%S.%f') } Sn: {time.time()} ")

                        test.borsa_id = 1
                        test.apair = aSymbol
                        test.abase = aSymbol
                        test.aquote = aSymbol
                        test.abid_price = aBidPrice
                        test.aask_price = aAskPrice
                        test.abid_qty = aBidQty
                        test.aask_qty = aAskQty
                        test.bpair = bSymbol
                        test.bbase = bSymbol
                        test.bquote = bSymbol
                        test.bbid_price = bBidPrice
                        test.bask_price = bAskPrice
                        test.bbid_qty = bBidQty
                        test.bask_qty = bAskQty
                        test.cpair = cSymbol
                        test.cbase = cSymbol
                        test.cquote = cSymbol
                        test.cbid_price = cBidPrice
                        test.cask_price = cAskPrice
                        test.cbid_qty = cBidQty
                        test.cask_qty = cAskQty
                        test.yontem = "AL_SAT_SAT"
                        test.capraz = AlSatSatOran
                        test.islemTutar = islemTutar
                        test.komisyon = arbitrajSabiti
                        test.kar = AlSatSatOran - arbitrajSabiti
                        test.prg = "arbitraj_01"
                        test.islzaman = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        test.zamanstr = datetime.now().strftime('%H:%M:%S.%f')
                        test.addData(dbCursor=dbCursor)

                    # sonraki kaydı oku, aksi takdirde aynı kaydı oku arbitraj devam ediyor mu kontrol et
                    if (AlAlSatOran <= arbitrajSabiti) and (AlSatSatOran <= arbitrajSabiti):
                        break

    print(f"Toplam süre: {(time.time() - start_time) / 60} dakika ")
    print(f"Toplam tutar: {toplam} adet: {adet}")


#    print(f"Çekilen kayıt sayısı: {len(symbolRows)}")


main()
