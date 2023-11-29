import pandas as pd
import json
from datetime import datetime
import requests
import time
import numpy as np
from botclass import BinanceCandlePrice as candleClass
from botclass import BinanceSymbol as symbolClass
from botclass import BinanceBookTicker as tickerClass
from botclass import Trade as tradeClass
import indicator as ind
import botfunction as func
import winsound
import matplotlib.pyplot as plt

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
CANDLE_INTERVAL = "15m"  # Okunacak mum verisi periyodu
CANDLE_COUNT = 100  # Okunacak mum verisi adeti
BOLLINGER_BAND_PERIOD = 20  # Bollinger band hesaplama uzunluğu (mum bar sayısı)
MINIMUM_BOLLINGER_HEIGHT = 4  # Bollinger alt ve üst bant arası yükseklik (yüzde olarak)
LINEAR_REGRESSION_PERIOD = 20
AO_SHORT_PERIOD = 5
AO_LONG_PERIOD = 34
STANDARD_DEVIATION = 2  # Bollinger band alt ve üst bandlar için standart sapma değeri
TARGET_DOWN_TO_CENTER = 'DOWN_TO_CENTER'
TARGET_DOWN_TO_UP = 'DOWN_TO_UP'
TARGET_CENTER_TO_UP = 'CENTER_TO_UP'
STOP_LIMIT_PERCENTAGE = 1
MAXIMUM_STOP_PERCENTAGE = float(1.5)
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP_LIMIT = 2

glbExplanation = ""

def convert_dataframe(bars=None):
    # İlk 5 beş kolon muhafaza edilir, "date", "open", "high", "low", "close"
    for line in bars:
        del line[5:]

    df = pd.DataFrame(bars, columns=['date', 'open', 'high', 'low', 'close'])
    return df

def get_candle_data(dbCursor=None, session=None, url=None, symbol=None, interval="1m", limit=100):
    candle = candleClass()
    candle.dbCursor = dbCursor
    bars = candle.getDataWithSession(session=session, url=url, symbol=symbol, candleInterval=interval, limit=limit)
    return bars

def get_ticker_info(session=None, url=None, symbol=None):
    status = False
    bidPrice = None
    bidQty = None
    askPrice = None
    askQty = None

    ticker = tickerClass()
    ticker.symbols = [symbol]
    tickerRows = ticker.getDataWithSession(session=session, url=url)
    if tickerRows is None:
        status = False
        return status, bidPrice, bidQty, askPrice, askQty

    for tickerRow in tickerRows:
        status = True
        bidPrice = float(tickerRow["bidPrice"])
        bidQty = float(tickerRow["bidQty"])
        askPrice = float(tickerRow["askPrice"])
        askQty = float(tickerRow["askQty"])
        return status, bidPrice, bidQty, askPrice, askQty

# Son muma ait bollinger band (up, center, down, price) bilgilerini döner
def get_bollinger_last(prices=None, period=None, standard_deviation=None):
    up, center, down = ind.get_bollinger_bands(prices=prices,
                                               period=period,
                                               standard_deviation=standard_deviation)

    curIndex = prices.size - 1
    band_up = round(up[curIndex], 6)
    band_center = round(center[curIndex], 6)
    band_down = round(down[curIndex], 6)
    price = float(prices[curIndex])
    return band_down, band_center, band_up

def control_bollinger_buy_signal(openPrices=None, highPrices=None, lowPrices=None, closePrices=None,
                                 down=None, center=None, up=None):
    signal = False
    sellTarget = None
    stopPrice = None

    length = lowPrices.size
    curIndex = length - 1
    prev1Index = length - 2
    prev2Index = length - 3

    currentLowPrice = round(float(lowPrices[curIndex]), 6)
    currentClosePrice = round(float(closePrices[curIndex]), 6)
    currentHighPrice = round(float(highPrices[curIndex]), 6)
    currentBandDown = round(down[curIndex], 6)
    currentBandCenter = round(center[curIndex], 6)
    currentBandUp = round(up[curIndex], 6)

    # Bollinger alt ve üst band arası MINIMUM_BOLLINGER_HEIGHT den küçük ise işlem yapılmaz
    # Örnek; %5 den küçük ise
    height = ((currentBandUp - currentBandDown) * 100) / currentBandDown
    if height < MINIMUM_BOLLINGER_HEIGHT:
        return signal, sellTarget, stopPrice

    # Trendin eğimi (closingPrices kullanılarak) bulunur.
    # Slope < 0 ise eğim aşağı, slope > 0 ise eğim yukarı
    slope = ind.get_trend_slope(prices=closePrices.to_numpy(dtype=float), period=LINEAR_REGRESSION_PERIOD)

    prev1LowPrice = round(float(lowPrices[prev1Index]), 6)
    prev1ClosePrice = round(float(closePrices[prev1Index]), 6)
    prev1HighPrice = round(float(highPrices[prev1Index]), 6)
    prev1BandDown = round(down[prev1Index], 6)
    prev1BandCenter = round(center[prev1Index], 6)
    prev1BandUp = round(up[prev1Index], 6)

    prev2LowPrice = round(float(lowPrices[prev2Index]), 6)
    prev2BandDown = round(down[prev2Index], 6)
    prev2BandCenter = round(center[prev2Index], 6)
    prev2BandUp = round(up[prev2Index], 6)

    """ Trend eğimi aşağı doğru ise """
    if slope <= 0:
        """
        2 önceki mumum low değeri bollinger alt bandının altında ise VE
        1 önceki mumun low değeri bollinger alt bandının üstünde ise VE
        mevcut mumun anlık low değeri bollinger alt bandının üstünde VE
        1 önceki mum YEŞİL kapatmış ise ALIM sinyali
        """
        if (prev2LowPrice < prev2BandDown) and (prev1LowPrice > prev1BandDown) and (currentLowPrice > currentBandDown):
            prev1OpenPrice = round(float(openPrices[prev1Index]), 6)
            prev1ClosePrice = round(float(closePrices[prev1Index]), 6)
            """ 1 önceki mum YEŞİL ise """
            if prev1ClosePrice > prev1OpenPrice:
                """ Mevcut ve 1 önceki mumun HIGH değerleri BOLLINGER ORTA BANIDIN üzerinde değilse """
                if (currentHighPrice < currentBandCenter) and (prev1HighPrice < currentBandCenter):
                    signal = True
                    # Trend eğimi aşağı ise satış hedefi BOLLINGER ORTA BANDI yapılır
                    # TODO: Satış hedefini sellTarget a göre belirlemekten vazgeçildi.
                    #       Satış işlemi trailing stop ile stop işlemi ile yapılıyor.
                    sellTarget = TARGET_DOWN_TO_CENTER
                    # TODO: stopPrice belirlenecek
                    stopPrice = prev2LowPrice

    """ 
    Trend eğimi yukarı doğru ise 
    İki durum varsayımsal olarak beklenen iki durum kontrol edilir;
    1 - Fiyatın bollinger alt banttdan -> üst banta yükselmesi 
    2 - Fiyatın bollinger orta banttan -> üst bantta yükselmesi
    """
    if slope > 0:
        """
        1. ALT BANT -> ÜST BANT
        Önceki mumun low değeri bollinger alt bandının altında ise VE
        Mevcut mumun anlık low değeri bollinger alt bandının üstünde ise VE
        Mevcut mumun anlık değeri YEŞİL ise ALIM SİNYALİ
        """
        if (prev1LowPrice < prev1BandDown) and (currentLowPrice > currentBandDown):
            currentOpenPrice = round(float(openPrices[curIndex]), 6)
            currentClosePrice = round(float(closePrices[curIndex]), 6)
            """ Mevcut mumun anlık değeri YEŞİL ise """
            if currentClosePrice > currentOpenPrice:
                """ Mevcut mumun HIGH değeri BOLLINGER ÜST BANDINI aşmamış (Fiyat fazla artmamış) ise """
                if (currentHighPrice < currentBandUp):
                    signal = True
                    # TODO: Satış hedefini sellTarget a göre belirlemekten vazgeçildi.
                    #       Satış işlemi trailing stop ve stop işlemi ile yapılıyor.
                    sellTarget = TARGET_DOWN_TO_UP
                    # TODO: stopPrice belirlenecek
                    stopPrice = prev1LowPrice

        """ 
        1. ORTA BANT -> ÜST BANT
        Önceki mumun LOW değeri, bollinger orta bandının altında ise VE
        Mevcut mumun anlık LOW değeri, bollinger ortan bandının üstünde ise VE
        Mevcut mumun anlık değeri YEŞİL ise ALIM SİNYALİ
        """
        if (prev1LowPrice < prev1BandCenter) and (currentLowPrice > currentBandCenter):
            currentOpenPrice = round(float(openPrices[curIndex]), 6)
            currentClosePrice = round(float(closePrices[curIndex]), 6)
            """ Mevcut mumun anlık değeri YEŞİL ise """
            if currentClosePrice > currentOpenPrice:
                """ Mevcut mumun HIGH değeri BOLLINGER ÜST BANDINI aşmamış (Fiyat fazla artmamış) ise """
                if (currentHighPrice < currentBandUp):
                    signal = True
                    # TODO: Satış hedefini sellTarget a göre belirlemekten vazgeçildi.
                    #       Satış işlemi trailing stop ve stop işlemi ile yapılıyor.
                    sellTarget = TARGET_CENTER_TO_UP
                    # TODO: stopPrice belirlenecek
                    stopPrice = prev1LowPrice

    if signal is True:
        """ 
        Trend aşağı giderken bazen stopPrice > currentPrice durumları ile karşılaşıldı.
        Engellemek için aşağıdaki kontrol konuldu.
        """
        if stopPrice > currentClosePrice:
            signal = False
            sellTarget = None
            stopPrice = None
            return signal, sellTarget, stopPrice

        """ 
        stopPrice ın alındığı mum çok aşağı iğne atmış ise stop aralığı çok büyük oluyor.
        Stop aralığının çok büyük olmasını engellemek için MAXIMUM_STOP_PERCENTAGE kadar 
        olması sağlanır.
        """
        p = ((currentClosePrice - stopPrice)*100) / stopPrice
        if p > MAXIMUM_STOP_PERCENTAGE:
            stopPrice = currentClosePrice - ((currentClosePrice * MAXIMUM_STOP_PERCENTAGE)/100)

        """ 
        Potansiyel (hedeflenen) kar, stop olunduğunda yapılacak zarardan BüYÜK ise ALIM yapılmaz.
        Yani; Hedef kar > Stop loss zararı -> ise işlem yapma
        """
        # TODO: Aşağıdaki kontroller kapatıldı, yeniden bakılacak açılmalı mı?
        if (sellTarget == TARGET_DOWN_TO_CENTER):
            if (currentClosePrice - stopPrice) > (currentBandCenter - currentClosePrice):
                signal = False
                sellTarget = None
                stopPrice = None
        if (sellTarget == TARGET_DOWN_TO_UP):
            if (currentClosePrice - stopPrice) > (currentBandUp - currentClosePrice):
                signal = False
                sellTarget = None
                stopPrice = None

    return signal, sellTarget, stopPrice

def control_buy_signal(openPrices=None, highPrices=None, lowPrices=None, closePrices=None):
    buySignal = False
    sellTarget = None
    stopPrice = None

    """ Bollinger band bilgileri okunur """
    bandUp, bandCenter, bandDown = ind.get_bollinger_bands(prices=closePrices,
                                                           period=BOLLINGER_BAND_PERIOD,
                                                           standard_deviation=STANDARD_DEVIATION)

    buySignal, sellTarget, stopPrice = control_bollinger_buy_signal(openPrices=openPrices,
                                                                    highPrices=highPrices,
                                                                    lowPrices=lowPrices,
                                                                    closePrices=closePrices,
                                                                    down=bandDown,
                                                                    center=bandCenter,
                                                                    up=bandUp)
    curIndex = bandDown.size - 1
    curDown = round(bandDown[curIndex], 6)
    curCenter = round(bandCenter[curIndex], 6)
    curUp = round(bandUp[curIndex], 6)
    # TODO: glbexplanation sonra silinecek (aşağıdaki kısım)
    global glbExplanation
    glbExplanation= f"down: {curDown} center: {curCenter} up: {curUp}"

    return buySignal, sellTarget, stopPrice

def control_awesome_signal(ao=None):
    signal = False
    signalType = None

    if ao is None:
        return signal, signalType

    oscillator = ao['ao']
    length = oscillator.size - 1
    current = round(oscillator[length], 6)
    prev1 = round(oscillator[length - 1], 6)
    prev2 = round(oscillator[length - 2], 6)

    short = round(ao['short'][length], 6)
    long = round(ao['long'][length], 6)

    if (current < 0) and (prev1 < 0) and (prev2 < 0): #and (short > long):
        signal = True
        signalType = 'AL'

    if (current > 0) and (prev1 > 0) and (prev2 > 0): #and (long > short):
        signal = True
        signalType = 'SAT'

    return signal, signalType

def control_awesome_signal_for_bollinger(ao=None, bollingerTradeType=None):
    signal = False
    signalType = None

    if ao is None:
        return signal, signalType

    oscillator = ao['ao']
    length = oscillator.size - 1
    current = round(oscillator[length], 6)
    prev1 = round(oscillator[length-1], 6)
    prev2 = round(oscillator[length-2], 6)

    short = round(ao['short'][length], 6)
    long = round(ao['long'][length], 6)

    if bollingerTradeType == 'AL':
        if (current < 0) and (prev1 < 0) and (prev2 < 0): #and (short > long):
            signal = True
            signalType = 'AL'

    if bollingerTradeType == 'SAT':
        if (current > 0) and (prev1 > 0) and (prev2 > 0): #and (long > short):
            signal = True
            signalType = 'SAT'

    return signal, signalType

def control_ao_saucer_signal(ao=None):
    signal = False
    signalType = None

    if ao is None:
        return signal, signalType

    oscillator = ao['ao']
    length = oscillator.size - 1
    current = round(oscillator[length], 6)
    prev1 = round(oscillator[length - 1], 6)
    prev2 = round(oscillator[length - 2], 6)
    prev3 = round(oscillator[length - 3], 6)

    if current > 0: # Birinci SIFIRDAN büyük
        if current > prev1: # Birinci YEŞİL ve SIFIRDAN büyük
            if (prev3 > prev2) and (prev2 > prev1):
                signal = True
                signalType = 'AL'

    if current < 0: # birinci SIFIRDAN küçük
        if current < prev1: # Birinci KIRMIZI ve SIFIRDAN küçük
            if (prev3 < prev2) and (prev2 < prev1):
                signal = True
                signalType = 'SAT'

    return signal, signalType

def position_existence_control(dbCursor=None, symbol=None, status=None):
    trade = tradeClass()
    row = trade.readTrade(dbCursor=dbCursor, symbol=symbol, status=status)
    if (row is None) or (len(row) <= 0):
        return False, row
    return True, row

def insert_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, stopPrice=None, sellTarget=None,
                       period=None, periodTime=None, signalName=None, explanation=None):
    trade = tradeClass()

    trade.symbol = symbol
    trade.period = period
    trade.period_time = periodTime
    trade.explanation = explanation
    trade.buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    trade.buy_price = price
    trade.stop_price = stopPrice
    trade.stop_height = price - stopPrice
    trade.buy_signal_name = signalName
    trade.sell_target = sellTarget
    trade.status = STATUS_BUY

    trade.addTrade(dbCursor=dbCursor)

    # TODO: Aşağıdaki satırlar silinecek
    print(f"{buySell} {symbol} {explanation} stop: {stopPrice} height:{trade.stop_height}")
    alarm()

def update_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, signalName=None,
                       oldStatus=None, newStatus=None):
    trade = tradeClass()

    trade.symbol = symbol
    if buySell == SIDE_BUY:
        trade.buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        trade.buy_price = price
        trade.buy_signal_name = signalName
    if buySell == SIDE_SELL:
        trade.sell_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        trade.sell_price = price
        trade.sell_signal_name = signalName

    trade.updateTrade(dbCursor=dbCursor, symbol=symbol, oldStatus=oldStatus, newStatus=newStatus)
    alarm()

def update_stop_price(dbCursor=None, symbol=None, stopPrice=None):
    trade = tradeClass()
    trade.updateStopPrice(dbCursor=dbCursor, symbol=symbol, stopPrice=stopPrice, status=STATUS_BUY)

def stop_control(currentPrice=None, stopPrice=None):
    if currentPrice < stopPrice:
        return True

    return False

def trailing_stop_control(stopHeight=None, currentPrice=None, buyPrice=None, stopPrice=None):
    stopChange = False
    newStopPrice = None

    if (currentPrice > stopPrice):
        difference = round((currentPrice - stopPrice), 8)
        if difference > stopHeight:
            newStopPrice = currentPrice - stopHeight
            newStopPrice = round(newStopPrice, 8)
            stopChange = True

    return stopChange, newStopPrice

def sell_control(currentPrice=None, buyPrice=None, sellTarget=None, down=None, center=None, up=None):
    sellPrice = None
    if sellTarget == TARGET_DOWN_TO_CENTER:
        if currentPrice >= center:
            sellPrice = currentPrice
            return True, sellPrice

    if sellTarget == TARGET_DOWN_TO_UP:
        if currentPrice >= up:
            sellPrice = currentPrice
            return True, sellPrice

    return False, sellPrice

def alarm():
    frequency = 2000
    duration = 1000
    winsound.Beep(frequency, duration)

def main():
    tradeSignal = False
    signalName = None

    db = func.connectDB()
    dbCursor = db.cursor()

    connSession = requests.session()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_candle = binanceConfig["url_historical"] + binanceConfig["url_candle_historical"]
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    symbol = symbolClass()
    symbolRows = symbol.readAll(dbCursor=dbCursor, exchangeId=1, quoteAsset='USDT')
    if (symbolRows is not None) and (len(symbolRows) > 0):
        while True:
            # start_time = time.time()
            for symbolRow in symbolRows:
                tradeSignal = False
                signalName = None

                item = json.loads(symbolRow[0])
                coinSymbol = item['symbol']

                # coinSymbol = 'RUNEUSDT'

                """ Coin mum verileri okunur """
                bars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle, symbol=coinSymbol,
                                       interval=CANDLE_INTERVAL,
                                       limit=CANDLE_COUNT)

                """ MUM verileri okunamamış ise işlem yapılmaz """
                if (bars is None) or (len(bars) < CANDLE_COUNT):
                    continue

                df = convert_dataframe(bars=bars)
                date_kline = df['date']
                open_prices = df['open']
                high_prices = df['high']
                low_prices = df['low']
                closing_prices = df['close']

                isPosition, positionRow = position_existence_control(dbCursor=dbCursor,
                                                                     symbol=coinSymbol,
                                                                     status=STATUS_BUY)

                """ Coine ait işlemde olan bir kayıt var ise SATIŞ işlem kontrolleri yapılır """
                if isPosition is True:
                    positionRecord = json.loads(positionRow[0])

                    """ Coin in anlık tahta fiyatı okunur """
                    tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                                       url=url_book_ticker,
                                                                                       symbol=coinSymbol)
                    if tickerStatus is False:
                        continue

                    """ STOP kontrolleri bidPrice (tahtadaki anlık alış fiyatı) ile yapılır """
                    isStop = stop_control(currentPrice=bidPrice, stopPrice=positionRecord['stop_price'])

                    if isStop is True:
                        # TODO: Burada SATIM işlemi yapılacak
                        # Satış yapılmış gibi satış kaydı update edilir.
                        update_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_SELL,
                                           price=bidPrice, oldStatus=positionRecord['status'],
                                           newStatus=STATUS_STOP_LIMIT)
                        print(f"{SIDE_SELL} {coinSymbol} Alış: {positionRecord['buy_price']} Satış: {bidPrice} STOP OLDU ***")
                        continue
                    # end if isStop is True:

                    """ Trailing Stop (iz süren stop) kontrolleri """
                    isTrailingStopChange, newStopPrice = trailing_stop_control(stopHeight=positionRecord['stop_height'],
                                                                               currentPrice=bidPrice,
                                                                               buyPrice=positionRecord['buy_price'],
                                                                               stopPrice=positionRecord['stop_price'])
                    if isTrailingStopChange is True:
                        update_stop_price(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice)
                        # TODO: print silinecek
                        print(f"  *** STOP UPDATE *** {coinSymbol} Alış: {positionRecord['buy_price']} Eski Stop: {positionRecord['stop_price']} Yeni Stop: {newStopPrice}")
                        alarm()
                        # TODO: Burada kayıt yeniden okunabilir.
                        #       Vakit kaybı olmasın diye daha önce okunan kayıtta stop_price değiştirildi.
                        positionRecord['stopPrice'] = newStopPrice
                    # end if isTrailingStopChange is True:
                # end if isPosition is True:

                """ Coine ait işlemde olan bir kayıt yok ise ALIM KONTROLLERİ yapılır """
                if isPosition is False:
                    """ ALIM sinyali olup olmadığı kontrol edilir """
                    buySignal, sellTarget, stopPrice = control_buy_signal(openPrices=open_prices,
                                                                          highPrices=high_prices,
                                                                          lowPrices=low_prices,
                                                                          closePrices=closing_prices)
                    """ ALIM sinyali yoksa sonraki COIN """
                    if buySignal is False:
                        continue

                    """ Coin in anlık tahta fiyatı okunur. """
                    tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                                       url=url_book_ticker,
                                                                                       symbol=coinSymbol)
                    if tickerStatus is False:
                        continue

                    """ ALIM işlemi yapılır. ALIM işlemi askPrice ile yapılır """
                    # TODO: Alım işlemi yapılacak
                    global glbExplanation

                    curIndex = date_kline.size - 1
                    periodTimetamps = date_kline[curIndex]
                    periodTime = datetime.fromtimestamp((periodTimetamps / 1000))

                    glbExplanation = f"price:{askPrice} target: {sellTarget} {glbExplanation}"
                    insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                                       price=askPrice, stopPrice=stopPrice, sellTarget=sellTarget,
                                       period=CANDLE_INTERVAL, periodTime=periodTime, signalName='BOLL',
                                       explanation=glbExplanation)
                # end isPosition is False:
            # end for symbolRow
            # print(f"Toplam süre: {(time.time() - start_time) / 60} dakika ")
        # end while True

main()
