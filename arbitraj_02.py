import json
import os
import time
from datetime import datetime

import botfunction as func
from botclass import BinanceBookTicker as tickerClass
from botclass import TriangularPair as pairClass
from botclass import TestArbitraj as testClass
from botclass import Counter as counterClass
from botclass import CounterType
from botclass import Cuzdan

from binance_api import Order as orderClass
import winsound

def sinyal():
    frequency = 2000
    duration = 1000
    winsound.Beep(frequency, duration)

#
# Uclu arbitraj bot
#
def main():
    start_time = time.time()
    maxBakiye = float(100.0)
    minIslemTutar = float(5.0)
    toplam = float(0.0)
    adet = 0
    komisyon = float(0.00225)
    arbitrajSabiti = 1 + komisyon
    aAskPrice = float(0.0)
    aAskQty = float(0.0)
    aBidPrice = float(0.0)
    aBidQty = float(0.0)
    bAskPrice = float(0.0)
    bAskQty = float(0.0)
    bBidPrice = float(0.0)
    bBidQty = float(0.0)
    cAskPrice = float(0.0)
    cAskQty = float(0.0)
    cBidPrice = float(0.0)
    cBidQty = float(0.0)

    db = func.connectDB()
    dbCursor = db.cursor()

    # Read binance section from config.ini
    binanceConfig = func.readConfig(filename="config.ini", section="binance")

    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    test = testClass()

    test_api_key = 'MrMWYq9dTdLtPrDG7rc8zfZWKKBUVwLwiQoETOIO3AHddfKaEju4gohvqylXoJZn'
    test_api_secret = 'JRzClTKk5fGC5G4vLhrezfPvP1adr7qZ5paKnkjor1EGMaN9tRVBYO3BO8o4j4cn'
    api_key = test_api_key
    api_secret = test_api_secret
    # api_key = os.environ.get('BinanceApiGet')
    # api_secret = os.environ.get('BinanceSecretGet')

    order = orderClass(api_key=api_key, api_secret=api_secret, testnet=True)
    """ Borsadan hesap bakiyeleri getirilir """
    account = order.get_account()

    res = order.order_market_buy(symbol='BTCUSDT', quoteOrderQty=100)

    """
    Arbitraj işlemi boyunca CUZDAN bilgileri bu ID ile kaydedilir.
    Her bir arbitraj işlemi sonrası ise cuzdanSira bir arttırılır
        (id + sira + coin) 
    """
    counter = counterClass()
    cuzdanId = counter.getCounter(dbCursor=dbCursor, counterType=CounterType['CUZDAN_COUNTER'])

    """
    Başlanıçtaki hesap bakiyeleri ile arbitraj işlemi sonrası bakiyeleri karşılaştırmak için;
    Arbitraj işlemine başlamadan önce hesap bakiyeleri CUZDAN tablosuna kaydedilir.
    Her arbitraj işlemi sonrası cuzdanSira bir arttırılır ve yeni cuzdan bakiyeleri kaydedilir.
    """
    cuzdanSira = 1
    cuzdan = Cuzdan()
    cuzdan.id = cuzdanId
    cuzdan.sira = cuzdanSira
    for balance in account['balances']:
        cuzdan.coin = balance['asset']
        cuzdan.kullanilabilir = float(balance['free'])
        cuzdan.bloke = float(balance['locked'])
        cuzdan.addData(dbCursor=dbCursor)

    ticker = tickerClass(dbCursor=dbCursor)

    # rsp = trade.order_market_buy(symbol='BUSDUSDT', quoteOrderQty=10.0)
    # if (rsp is not None) and (len(rsp) > 0) and (rsp['status'] == 'FILLED'):
    #     print("alındı")

    """ 
    Mümkün olan Üçlü arbitraj çiftlerinin hepsi TRIANGULAR_PAIR tablosundan okunur.
    pairRows : Okunan bilgilerinin saklandığı değişken
    refSymbol : Arbitrajın ana coin bilgisi
    """
    pair = pairClass(dbCursor=dbCursor)
    pairRows = pair.readAllPair(exchangeId=1, refSymbol="USDT")
    if (pairRows is not None) and (len(pairRows) > 0):
        while True:
            for pairRow in pairRows:
                rec = json.loads(pairRow[0])
                aSymbol = rec["pair_a_symbol"]
                bSymbol = rec["pair_b_symbol"]
                cSymbol = rec["pair_c_symbol"]
                symbols = [aSymbol, bSymbol, cSymbol]
                ticker.symbols = symbols

                """
                Arbitraj işlemi devam ettiği sürece aynı kayıtta kalmak için
                """
                while True:
                    """ 
                    Sıradaki üçlü çiftin anlık order book ticker (tahta) bilgileri okunur 
                    Örnek; aPair: BTCUSDT - bPair: LTCBTC - cPair: LTCUSDT
                    """
                    islemTutar = float(0.0)
                    tickerRows = ticker.getData(url=url_book_ticker)
                    for tickerRow in tickerRows:
                        if aSymbol == tickerRow["symbol"]:
                            aBidPrice = float(tickerRow["bidPrice"])
                            aBidQty = float(tickerRow["bidQty"])
                            aAskPrice = float(tickerRow["askPrice"])
                            aAskQty = float(tickerRow["askQty"])

                        if bSymbol == tickerRow["symbol"]:
                            bBidPrice = float(tickerRow["bidPrice"])
                            bBidQty = float(tickerRow["bidQty"])
                            bAskPrice = float(tickerRow["askPrice"])
                            bAskQty = float(tickerRow["askQty"])

                        if cSymbol == tickerRow["symbol"]:
                            cBidPrice = float(tickerRow["bidPrice"])
                            cBidQty = float(tickerRow["bidQty"])
                            cAskPrice = float(tickerRow["askPrice"])
                            cAskQty = float(tickerRow["askQty"])

                    """
                    İki tür arbitraj işleminin varlığı kontrol edilir
                    1- AL_AL_SAT: AlAlSatOran > 1 + (komisyon * 3) ise arbitraj imkanı var
                    2- AL_SAT_SAT: AlSatSatOran > 1 + (komisyon *3) ise arbitraj imkanı var demektir
                    """
                    try:
                        AlAlSatOran = (1 / aAskPrice) * (1 / bAskPrice) * cBidPrice
                    except ZeroDivisionError:
                        AlAlSatOran = 0.0

                    try:
                        AlSatSatOran = (1 / cAskPrice) * bBidPrice * aBidPrice
                    except ZeroDivisionError:
                        AlSatSatOran = 0.0

                    # print(f"aPair: {aSymbol} bPair: {bSymbol} cPair: {cSymbol} AL_AL_SAT: {AlAlSatOran} "
                    #       f"AL_SAT_SAT: {AlSatSatOran}")
                    # sinyal()

                    if AlAlSatOran > arbitrajSabiti:
                        """
                        Her üç işlem çiftinin refSymbol(örn: USDT) cinsinden işlem tutarı bulunur
                        Bulunan tutarlar içinde en küçüğü ile işlem yapılmalıdır
                        """
                        aMaxPrice = aAskPrice * aAskQty
                        bMaxPrice = bAskPrice * aAskPrice * bAskQty
                        cMaxPrice = cBidPrice * cBidQty

                        islemTutar = min(aMaxPrice, bMaxPrice, cMaxPrice, maxBakiye)
                        islemTutar = round(islemTutar, 6)
                        """
                        İşlem tutarı, minimum işlem tutarından küçük ise arbitraj işlemi yapılmaz
                        """
                        if float(islemTutar) > minIslemTutar:
                            sinyal()
                            sinyal()
                            aRsp = order.order_market_buy(symbol=aSymbol, quoteOrderQty=islemTutar)
                            if (aRsp is not None) and (len(aRsp) > 0) and (aRsp['status'] == 'FILLED'):
                                bIslemAdet = float(aRsp['executedQty'])
                                bRsp = order.order_market_buy(symbol=bSymbol, quantity=bIslemAdet)
                                if (bRsp is not None) and (len(bRsp) > 0) and (bRsp['status'] == 'FILLED'):
                                    cIslemAdet = float(bRsp['executedQty'])
                                    cRsp = order.order_market_sell(symbol=cSymbol, quantity=cIslemAdet)
                                    if (cRsp is not None) and (len(cRsp) > 0) and (cRsp['status'] == 'FILLED'):
                                        """ İşlem sonrası borsadan hesap bakiyeleri okunur """
                                        account = order.get_account()

                                        cuzdan.id = cuzdanId
                                        cuzdan.sira = cuzdanSira + 1
                                        for balance in account['balances']:
                                            cuzdan.coin = balance['asset']
                                            cuzdan.kullanilabilir = float(balance['free'])
                                            cuzdan.bloke = float(balance['locked'])
                                            cuzdan.addData(dbCursor=dbCursor)

                                        toplam = toplam + AlAlSatOran - arbitrajSabiti
                                        adet = adet + 1

                                        print(f"{aSymbol} - {bSymbol} - {cSymbol} AL_AL_SAT: {AlAlSatOran} "
                                              f" Kar: {(AlAlSatOran - arbitrajSabiti)} tutar: {islemTutar} "
                                              f"Toplam: {toplam} Adet: {adet} "
                                              f"Saat: {datetime.now().strftime('%H:%M:%S.%f')} ")
                                        sinyal()
                                        sinyal()

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

                                        exit(0)

                    if AlSatSatOran > arbitrajSabiti:
                        """
                        Her üç işlem çiftinin refSymbol(örn: USDT) cinsinden işlem tutarı bulunur
                        Bulunan tutarlar içinde en küçüğü ile işlem yapılmalıdır
                        """
                        cMaxPrice = cAskPrice * cAskQty
                        bMaxPrice = bBidPrice * aBidPrice * bBidQty
                        aMaxPrice = aBidPrice * aBidQty

                        islemTutar = min(aMaxPrice, bMaxPrice, cMaxPrice, maxBakiye)
                        islemTutar = round(islemTutar, 6)

                        """
                        İşlem tutarı, minimum işlem tutarından küçük ise arbitraj işlemi yapılmaz
                        """
                        if float(islemTutar) > minIslemTutar:
                            sinyal()
                            sinyal()
                            cRsp = order.order_market_buy(symbol=cSymbol, quoteOrderQty=islemTutar)
                            if (cRsp is not None) and (len(cRsp) > 0) and (cRsp['status'] == 'FILLED'):
                                bIslemAdet = float(cRsp['executedQty'])
                                bRsp = order.order_market_sell(symbol=bSymbol, quantity=bIslemAdet)
                                if (bRsp is not None) and (len(bRsp) > 0) and (bRsp['status'] == 'FILLED'):
                                    aIslemAdet = float(bRsp['executedQty'])
                                    aRsp = order.order_market_sell(symbol=aSymbol, quantity=aIslemAdet)
                                    if (aRsp is not None) and (len(aRsp) > 0) and (aRsp['status'] == 'FILLED'):
                                        """ İşlem sonrası borsadan hesap bakiyeleri okunur """
                                        account = order.get_account()

                                        cuzdan.id = cuzdanId
                                        cuzdan.sira = cuzdanSira + 1
                                        for balance in account['balances']:
                                            cuzdan.coin = balance['asset']
                                            cuzdan.kullanilabilir = float(balance['free'])
                                            cuzdan.bloke = float(balance['locked'])
                                            cuzdan.addData(dbCursor=dbCursor)

                                        toplam = toplam + AlSatSatOran - arbitrajSabiti
                                        adet = adet + 1

                                        print(f"{aSymbol} - {bSymbol} - {cSymbol} AL_SAT_SAT: {AlSatSatOran} "
                                              f"Kar: {(AlSatSatOran - arbitrajSabiti)} tutar: {islemTutar} "
                                              f"Toplam: {toplam} Adet: {adet} "
                                              f"Saat: {datetime.now().strftime('%H:%M:%S.%f')} ")
                                        sinyal()
                                        sinyal()

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

                                        exit(0)

                    """
                    Arbitraj imkanı devam ettiği sürece aynı kayıtta işlem yapmaya devam edilir.
                    Arbitraj işlemi yoksa sonraki üçlü çifte geçilir 
                    """
                    if (((AlAlSatOran <= arbitrajSabiti) and (AlSatSatOran <= arbitrajSabiti)) or
                            islemTutar <= minIslemTutar):
                        break

    print(f"Toplam süre: {(time.time() - start_time) / 60} dakika ")
    print(f"Toplam tutar: {toplam} adet: {adet}")


#    print(f"Çekilen kayıt sayısı: {len(symbolRows)}")


main()
