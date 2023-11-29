import json
import time
import os
from decimal import Decimal
from typing import Union, Optional, Dict
from binance_api import Order as orderClass
from botclass import BinanceBookTicker as tickerClass
from botclass import BinanceSymbol as symbolClass
import botfunction as func

TEST = 15
# ALIM / SATIM örnek
def round_step_size(quantity: Union[float, Decimal], step_size: Union[float, Decimal]) -> float:
    """
    step_size adetin (miktarın) arttırılıp, azaltılabileceği minimum aralıktır.
    Adet step-size ın bir katı olmalıdır.

    Bu fonksiyon adeti step_size ın katı olarak ayarlamak (yuvarlamak) için kullanılır.

    :param quantity: Adet bilgisi (required)
    :param step_size: Adetin arttırlıp azaltılabileceği min değer (required)
    :return: float
    """
    quantity = Decimal(str(quantity))
    return float(quantity - quantity % Decimal(str(step_size)))

def main():
    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    test_api_key = 'MrMWYq9dTdLtPrDG7rc8zfZWKKBUVwLwiQoETOIO3AHddfKaEju4gohvqylXoJZn'
    test_api_secret = 'JRzClTKk5fGC5G4vLhrezfPvP1adr7qZ5paKnkjor1EGMaN9tRVBYO3BO8o4j4cn'
    api_key = test_api_key
    api_secret = test_api_secret
    # api_key = os.environ.get('BinanceApiGet')
    # api_secret = os.environ.get('BinanceSecretGet')

    order = orderClass(api_key=api_key, api_secret=api_secret, testnet=True)
    """ Borsadan hesap bakiyeleri getirilir """
    account = order.get_account()

    for balance in account['balances']:
        symbol = balance['asset']
        free = float(balance['free'])
        locked = float(balance['locked'])

        print(f"symbol: {symbol} free: {free} locked: {locked}")

    symbolId = 'BNBUSDT'

    symbol = symbolClass()
    symbolRow = symbol.readOne(dbCursor=dbCursor, exchangeId=1, symbol=symbolId)
    if (symbolRow is None) or (len(symbolRow) <= 0):
        print(f"{symbolId} okunamadı")
        exit(1)

    item = json.loads(symbolRow[0])
    coinSymbol = item['symbol']
    tickSize = float(item['tick_size'])
    stepSize = float(item['step_size'])
    minNotional = float(item['min_notional'])
    minLot = float(item['min_lot'])
    print(f"{symbolId} için minLot:{minLot} stepSize:{stepSize} minNotional:{minNotional}")

    ticker = tickerClass()
    ticker.symbols = [symbolId]

    tickerRows = ticker.getData(url=url_book_ticker)
    if tickerRows is None:
        print(f"{symbolId} ticker bilgisi çekilemedi")
        exit(1)

    for tickerRow in tickerRows:
        bidPrice = float(tickerRow["bidPrice"])
        bidQty = float(tickerRow["bidQty"])
        askPrice = float(tickerRow["askPrice"])
        askQty = float(tickerRow["askQty"])

    if (askPrice * askQty) > 100.0:
        """
        Gerçekte round yapılmayacak. Bölme işleminin sonucunda çıkan sayı step_size a göre düzenlenecek.
        float_precision(quantity=adet, step_size) yazılacak.
        """
        islemTutari = 20
        adet = (islemTutari / askPrice) # islem tutarı alınabilecek adet
        adet = round_step_size(quantity=adet, step_size=stepSize)

        adet = float(0.063)
        print(f"adet: {adet} minLot: {minLot}")
        if (adet < minLot):
            print(f"{symbolId} adet{adet} min lottan {minLot} büyük olmalıdır")
            exit(1)

        print(f"adet * fiyat: {adet * bidPrice} minNotional: {minNotional}")
        if (adet * bidPrice < minNotional):
            print(f"{symbolId} adet * fiyat {adet} {bidPrice} min notional dan {minNotional} büyük olmalıdır")
            exit(1)
        # if (adet * askPrice < minNotional):
        #     print(f"{symbolId} adet * fiyat {adet} {askPrice} min notional dan {minNotional} büyük olmalıdır")
        #     exit(1)

        ix = 0
        while ix < 5:
            # rsp = order.order_limit_buy(timeInForce=order.TIME_IN_FORCE_FOK,
            #                             symbol=symbolId,
            #                             quantity=adet,
            #                             price=askPrice)
            rsp = order.order_limit_sell(timeInForce=order.TIME_IN_FORCE_FOK,
                                        symbol=symbolId,
                                        quantity=adet,
                                        price=bidPrice)
            print(f"{rsp['status']}, {ix}")
            if rsp['status'] == 'FILLED':
                break
            time.sleep(0.5)
            ix += 1

            # print(f"{rsp}")

    account = order.get_account()

    for balance in account['balances']:
        symbol = balance['asset']
        free = float(balance['free'])
        locked = float(balance['locked'])

        print(f"\nsymbol: {symbol} free: {free} locked: {locked}")


main()