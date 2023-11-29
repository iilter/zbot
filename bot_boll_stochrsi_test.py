import pandas as pd
import json
from datetime import datetime
import requests
from threading import Thread
import time
import numpy as np
from botclass import BinanceCandlePrice as candleClass
from botclass import BinanceSymbol as symbolClass
from botclass import BinanceBookTicker as tickerClass
from botclass import Trade as tradeClass
import indicator as ind
import botfunction as func
import winsound

import logging

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
CANDLE_INTERVAL = "15m"  # Okunacak mum verisi periyodu
CANDLE_COUNT = 40  # Okunacak mum verisi adeti
BOLLINGER_BAND_PERIOD = 20  # Bollinger band hesaplama uzunluğu (mum bar sayısı)
MINIMUM_BOLLINGER_HEIGHT = 5  # Bollinger alt ve üst bant arası yükseklik (yüzde olarak)
PERCENT_BOLLINGER_LOWER_THRESHOLD = 5
LINEAR_REGRESSION_PERIOD = 30
ATR_PERIOD = 14
AO_SHORT_PERIOD = 5
AO_LONG_PERIOD = 34
STOCHRSI_PERIOD = 14
STOCHRSI_SLOW_PERIOD = 3
STOCHRSI_FAST_PERIOD = 3
FASTK_THRESHOLD = 20
SLOWD_THRESHOLD = 20
STANDARD_DEVIATION = 2.0  # Bollinger band alt ve üst bandlar için standart sapma değeri
STANDARD_DEVIATION_UP = 2.0  # Trend YUKARI iken kullanılacak standart sapma değeri
STANDARD_DEVIATION_DOWN = 2.5  # Trend AŞAĞI iken kullanılacak standart sapma değeri
MFI_PERIOD = 14
MFI_LOW_THRESHOLD = 10
TARGET_DOWN_TO_CENTER = 'DOWN_TO_CENTER'
TARGET_DOWN_TO_UP = 'DOWN_TO_UP'
TARGET_CENTER_TO_UP = 'CENTER_TO_UP'
DOWN_CENTER_RATIO = float(0.15)
CENTER_UP_RATIO = float(0.05)
DOWN_UP_RATIO = float(0.10)
STOP_LIMIT_PERCENTAGE = 1
TRAILING_STOP_FACTOR_FOR_UP = float(0.7)
TRAILING_STOP_FACTOR_FOR_DOWN = float(0.4)
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP = 2

IS_LOG = True
IS_PRINT = True
IS_ALARM = False

URL_TELEGRAM = 'https://api.telegram.org/bot'
TOKEN = '5474334107:AAEceU3EUiINChLTunuTsZ6CZN-udB3e_EY'

glbExplanation = ""
logging.basicConfig(filename="debug", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )

def convert_dataframe(bars=None):
    # İlk 6 beş kolon muhafaza edilir, "date", "open", "high", "low", "close", "volume"
    for line in bars:
        del line[6:]

    df = pd.DataFrame(bars, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    return df

def get_candle_data(dbCursor=None, session=None, url=None, symbol=None, interval=None, limit=None):
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

def position_existence_control(dbCursor=None, symbol=None, status=None):
    trade = tradeClass()
    row = trade.readTrade(dbCursor=dbCursor, symbol=symbol, status=status)
    if (row is None) or (len(row) <= 0):
        return False, row
    return True, row

def insert_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, stopPrice=None, stopHeight=None,
                       sellTarget=None, period=None, periodTime=None, signalName=None, explanation=None,
                       mfi=None, fastk=None, slowd=None, pboll=None, strategy=None):
    trade = tradeClass()
    trade.symbol = symbol
    trade.period = period
    trade.period_time = periodTime
    trade.explanation = explanation
    trade.buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    trade.buy_price = price
    trade.stop_price = stopPrice
    trade.stop_height = stopHeight
    trade.stop_change_count = 0
    trade.buy_signal_name = signalName
    trade.sell_target = sellTarget
    trade.status = STATUS_BUY
    trade.mfi = mfi
    trade.fastk = fastk
    trade.slowd = slowd
    trade.pboll = pboll
    trade.strategy = strategy

    trade.addTrade(dbCursor=dbCursor)

def update_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, signalName=None, explanation=None,
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

def update_stop_price(dbCursor=None, symbol=None, stopPrice=None, stopChangeCount=None):
    trade = tradeClass()
    trade.updateStopPrice(dbCursor=dbCursor, symbol=symbol, stopPrice=stopPrice,
                          stopChangeCount=stopChangeCount,
                          status=STATUS_BUY)

def control_bollinger_buy_signal(openPrices=None, highPrices=None, lowPrices=None, closePrices=None, volumes=None,
                                 down=None, center=None, up=None):
    signal = False
    sellTarget = None
    stopPrice = None

    length = lowPrices.size
    curIndex = length - 1
    prev1Index = length - 2
    prev2Index = length - 3

    currentLowPrice = round(float(lowPrices[curIndex]), 8)
    currentClosePrice = round(float(closePrices[curIndex]), 8)
    currentHighPrice = round(float(highPrices[curIndex]), 8)
    currentBandDown = round(float(down[curIndex]), 8)
    currentBandCenter = round(float(center[curIndex]), 8)
    currentBandUp = round(float(up[curIndex]), 8)
    currentVolume = round(float(volumes[curIndex]), 8)

    # Bollinger alt ve üst band arası MINIMUM_BOLLINGER_HEIGHT den küçük ise işlem yapılmaz
    # Örnek; %5 den küçük ise
    height = ((currentBandUp - currentBandDown) * 100) / currentBandDown
    if height < MINIMUM_BOLLINGER_HEIGHT:
        return signal, sellTarget, stopPrice

    # Trendin eğimi (closingPrices kullanılarak) bulunur.
    # Slope < 0 ise eğim aşağı, slope > 0 ise eğim yukarı
    slope, average, intercept = ind.get_linear_regression_slope(prices=closePrices.to_numpy(dtype=float),
                                                                period=LINEAR_REGRESSION_PERIOD)

    prev1LowPrice = round(float(lowPrices[prev1Index]), 8)
    prev1ClosePrice = round(float(closePrices[prev1Index]), 8)
    prev1HighPrice = round(float(highPrices[prev1Index]), 8)
    prev1BandDown = round(float(down[prev1Index]), 8)
    prev1BandCenter = round(float(center[prev1Index]), 8)
    prev1BandUp = round(float(up[prev1Index]), 8)
    prev1Volume = round(float(volumes[prev1Index]), 8)


    prev2LowPrice = round(float(lowPrices[prev2Index]), 8)
    prev2BandDown = round(float(down[prev2Index]), 8)
    prev2BandCenter = round(float(center[prev2Index]), 8)
    prev2BandUp = round(float(up[prev2Index]), 8)
    prev2Volume = round(float(volumes[prev2Index]), 8)

    """ Trend eğimi aşağı doğru ise """
    if slope <= 0:
        """
        2 önceki mumum low değeri bollinger alt bandının altında ise VE
        1 önceki mumun low değeri bollinger alt bandının üstünde ise VE
        mevcut mumun anlık low değeri bollinger alt bandının üstünde VE
        1 önceki mum YEŞİL kapatmış ise ALIM sinyali
        Bir önceki mumun hacmi > İki önceki mumum hacminden büyük ise (Volume artarak geliyor)
        """
        if (prev2LowPrice < prev2BandDown) and (prev1LowPrice > prev1BandDown) and (currentLowPrice > currentBandDown):
            prev1OpenPrice = round(float(openPrices[prev1Index]), 8)
            prev1ClosePrice = round(float(closePrices[prev1Index]), 8)
            """ 1 önceki mum YEŞİL ise """
            if prev1ClosePrice > prev1OpenPrice:
                """ Mevcut ve 1 önceki mumun HIGH değerleri BOLLINGER ORTA BANIDIN üzerinde değilse """
                if (currentHighPrice < currentBandCenter) and (prev1HighPrice < currentBandCenter):
                    """ Önceki iki mumum hacmi artarak geliyorsa """
                    if (prev1Volume > prev2Volume):
                        signal = True
                        # Trend eğimi aşağı ise satış hedefi BOLLINGER ORTA BANDI yapılır
                        # TODO: Satış hedefini sellTarget a göre belirlemekten vazgeçildi.
                        #       Satış işlemi trailing stop ile stop işlemi ile yapılıyor.
                        sellTarget = TARGET_DOWN_TO_CENTER
                        # TODO: stopPrice belirlenecek
                        atr = ind.get_atr(highPrices=highPrices,
                                          lowPrices=lowPrices,
                                          closePrices=closePrices,
                                          period=ATR_PERIOD)
                        # curAtr = round(float(atr[curIndex]), 8)
                        # stopPrice = currentClosePrice - (curAtr * TRAILING_STOP_FACTOR)
                        atrIndicator = round(float(atr[curIndex]), 8)
                        stopPrice = currentClosePrice - (atrIndicator * TRAILING_STOP_FACTOR_FOR_DOWN)

                        # str = f"ATR Prev1: {round(float(atr[prev1Index]), 8)} ATR Current: {round(float(atr[curIndex]), 8)}"
                        # log(msg=str)

                        # stopPrice = prev2LowPrice

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
        Mevcut mumun anlık değeri YEŞİL ise 
        Bir önceki mumun hacmi > İki önceki mumum hacminden büyük ise (Volume artarak geliyor)
        """
        if (prev1LowPrice < prev1BandDown) and (currentLowPrice > currentBandDown):
            currentOpenPrice = round(float(openPrices[curIndex]), 8)
            currentClosePrice = round(float(closePrices[curIndex]), 8)
            """ Mevcut mumun anlık değeri YEŞİL ise """
            if currentClosePrice > currentOpenPrice:
                """ Mevcut mumun HIGH değeri BOLLINGER ÜST BANDINI aşmamış (Fiyat fazla artmamış) ise """
                if (currentHighPrice < currentBandUp):
                    """ Önceki iki mumum hacmi artarak geliyorsa """
                    if (prev1Volume > prev2Volume):
                        signal = True
                        # TODO: Satış hedefini sellTarget a göre belirlemekten vazgeçildi.
                        #       Satış işlemi trailing stop ve stop işlemi ile yapılıyor.
                        sellTarget = TARGET_DOWN_TO_UP
                        # TODO: stopPrice belirlenecek
                        atr = ind.get_atr(highPrices=highPrices,
                                          lowPrices=lowPrices,
                                          closePrices=closePrices,
                                          period=ATR_PERIOD)
                        atrIndicator = round(float(atr[curIndex]), 8)
                        stopPrice = currentClosePrice - (atrIndicator * TRAILING_STOP_FACTOR_FOR_UP)

                        # str = f"ATR Prev1: {round(float(atr[prev1Index]), 8)} ATR Current: {round(float(atr[curIndex]), 8)}"
                        # log(msg=str)

        """ 
        1. ORTA BANT -> ÜST BANT
        Önceki mumun low değeri > bollinger alt bandının üstünde ise VE
        Mevcut mumun anlık değeri < bollinger alt bandının üstünde ise 
        """
        if (prev1LowPrice > prev1BandCenter):
            if (currentClosePrice < currentBandCenter):
                signal = True
                # TODO: Satış hedefini sellTarget a göre belirlemekten vazgeçildi.
                #       Satış işlemi trailing stop ve stop işlemi ile yapılıyor.
                sellTarget = TARGET_DOWN_TO_UP
                # TODO: stopPrice belirlenecek
                atr = ind.get_atr(highPrices=highPrices,
                                  lowPrices=lowPrices,
                                  closePrices=closePrices,
                                  period=ATR_PERIOD)
                atrIndicator = round(float(atr[curIndex]), 8)
                stopPrice = currentClosePrice - (atrIndicator * TRAILING_STOP_FACTOR_FOR_UP)

                # str = f"ATR Prev1: {round(float(atr[prev1Index]), 8)} ATR Current: {round(float(atr[curIndex]), 8)}"
                # log(msg=str)
        # TODO: Kural değişikliği; Orta bant -> Üst Bant Giriş kuralı değiştirildi.
        #       Amaç mümkün olduğu kadar aşağıdan pozisyona girmek.
        #       Aşağıdaki kısım kapatıldı üstteki denenecek
        """ 
        1. ORTA BANT -> ÜST BANT
        Önceki mumun LOW değeri, bollinger orta bandının altında ise VE
        Mevcut mumun anlık LOW değeri, bollinger ortan bandının üstünde ise VE
        Mevcut mumun anlık değeri YEŞİL ise VE
        Bir önceki mumun hacmi > İki önceki mumum hacminden büyük ise (Volume artarak geliyor)
        """
        # if (prev1LowPrice < prev1BandCenter) and (currentLowPrice > currentBandCenter):
        #     currentOpenPrice = round(float(openPrices[curIndex]), 8)
        #     currentClosePrice = round(float(closePrices[curIndex]), 8)
        #     """ Mevcut mumun anlık değeri YEŞİL ise """
        #     if currentClosePrice > currentOpenPrice:
        #         """ Mevcut mumun HIGH değeri BOLLINGER ÜST BANDINI aşmamış (Fiyat fazla artmamış) ise """
        #         if (currentHighPrice < currentBandUp):
        #             """ Önceki iki mumum hacmi artarak geliyorsa """
        #             if (prev1Volume > prev2Volume):
        #                 signal = True
        #                 # TODO: Satış hedefini sellTarget a göre belirlemekten vazgeçildi.
        #                 #       Satış işlemi trailing stop ve stop işlemi ile yapılıyor.
        #                 sellTarget = TARGET_CENTER_TO_UP
        #                 # TODO: stopPrice belirlenecek
        #                 atr = ind.get_atr(highPrices=highPrices,
        #                                   lowPrices=lowPrices,
        #                                   closePrices=closePrices,
        #                                   period=ATR_PERIOD)
        #                 atrIndicator = round(float(atr[curIndex]), 8)
        #                 stopPrice = currentClosePrice - (atrIndicator * TRAILING_STOP_FACTOR_FOR_UP)

                        # str = f"ATR Prev1: {round(float(atr[prev1Index]), 8)} ATR Current: {round(float(atr[curIndex]), 8)}"
                        # log(msg=str)

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
        # p = ((currentClosePrice - stopPrice)*100) / stopPrice
        # if p > MAXIMUM_STOP_PERCENTAGE:
        #     stopPrice = currentClosePrice - ((currentClosePrice * MAXIMUM_STOP_PERCENTAGE)/100)

        """ 
        Potansiyel (hedeflenen) kar, stop olunduğunda yapılacak zarardan BüYÜK ise ALIM yapılmaz.
        Yani; Hedef kar > Stop loss zararı -> ise işlem yapma
        """
        # TODO: Aşağıdaki kontroller kapatıldı, yeniden bakılacak açılmalı mı?
        if (sellTarget == TARGET_DOWN_TO_CENTER):
            # if (currentClosePrice - stopPrice) > (currentBandCenter - currentClosePrice):
            if currentClosePrice > currentBandDown:
                heightCurrentToDown = currentClosePrice - currentBandDown
                heightCenterToDown = currentBandCenter - currentBandDown
                if (heightCurrentToDown  / heightCenterToDown) > DOWN_CENTER_RATIO:
                    signal = False
                    sellTarget = None
                    stopPrice = None
        if (sellTarget == TARGET_DOWN_TO_UP):
            # if (currentClosePrice - stopPrice) > (currentBandUp - currentClosePrice):
            if (currentClosePrice > currentBandDown):
                heightCurrentToDown = currentClosePrice - currentBandDown
                heightUpToDown = currentBandUp - currentBandDown
                if (heightCurrentToDown / heightUpToDown) > DOWN_UP_RATIO:
                    signal = False
                    sellTarget = None
                    stopPrice = None
#         if (sellTarget == TARGET_CENTER_TO_UP):
#             if currentClosePrice > currentBandCenter:
#                 heightCurrentToCenter = currentClosePrice - currentBandCenter
#                 heightUpToCenter = currentBandUp - currentBandCenter
# #                if heightCurrentToCenter > heightUpToCurrent:
#                 if (heightCurrentToCenter / heightUpToCenter) > CENTER_UP_RATIO:
#                     signal = False
#                     sellTarget = None
#                     stopPrice = None

    # end if signal is True:

    return signal, sellTarget, stopPrice

def control_percent_bollinger_buy_signal(percentBollinger=None, threshold=None):
    currentPBoll = percentBollinger[-1]
    if currentPBoll < threshold:
        return True
    return False

def control_stochastic_buy_signal(fastK=None, slowD=None):
    if (fastK < FASTK_THRESHOLD) and (slowD < SLOWD_THRESHOLD) and (slowD < fastK):
        return True
    return False

def control_stochrsi_buy_signal(fastK=None, slowD=None):
    if (fastK < FASTK_THRESHOLD) and (slowD < SLOWD_THRESHOLD) and (slowD < fastK):
        return True
    return False

def control_mfi_buy_signal(mfi=None, threshold=None):
    if mfi <= threshold:
        return True
    return False

def control_mfi_range_buy_signal(mfi=None):
    if mfi >= 40 and mfi <= 50:
        return True
    return False

def get_stop_price(high=None, low=None, close=None, tickSize=None):
    atr = ind.get_atr(highPrices=high,
                      lowPrices=low,
                      closePrices=close,
                      period=ATR_PERIOD)
    atrIndicator = round(float(atr[-1]), 8)
    currentClosePrice = round(close[-1], 8)
    stopPrice = currentClosePrice - (atrIndicator * TRAILING_STOP_FACTOR_FOR_UP)

    """ 
    Stop fiyatının coin in minumum artım miktarına göre ayarlanması.
    Minimum artım miktarından fazla olan digitler silinir.
    Örnek: Stop: 0.2346328 ise ve tick_size: 0.0001 ise stop: 0.2346 yapılır.
    """
    residualValue = stopPrice % tickSize
    stopPrice = stopPrice - residualValue

    return stopPrice

def control_buy_signal(openPrices=None, highPrices=None, lowPrices=None, closePrices=None, volumes=None, tickSize=None):
    buySignal = False
    sellTarget = None
    stopPrice = None

    percentBoll = ind.get_percent_bollinger(prices=closePrices,
                                            period=BOLLINGER_BAND_PERIOD,
                                            standard_deviation=STANDARD_DEVIATION)
    percentBoll *= 100.0

    pBollBuySignal = control_percent_bollinger_buy_signal(percentBollinger=percentBoll,
                                                          threshold=PERCENT_BOLLINGER_LOWER_THRESHOLD)
    # if pBollBuySignal is False:
    #     return buySignal, sellTarget, stopPrice

    fastK, slowD = ind.get_stochrsi(prices=closePrices.to_numpy(dtype=float),
                                    timePeriod=STOCHRSI_PERIOD,
                                    slowKPeriod=STOCHRSI_FAST_PERIOD,
                                    slowDPeriod=STOCHRSI_SLOW_PERIOD)

    stochrsiBuySignal = control_stochrsi_buy_signal(fastK=fastK[-1], slowD=slowD[-1])

    high = highPrices.to_numpy(dtype=float)
    low = lowPrices.to_numpy(dtype=float)
    close = closePrices.to_numpy(dtype=float)
    volume = volumes.to_numpy(dtype=float)

    mfi = ind.get_mfi_talib(high=high, low=low, close=close, volume=volume, period=MFI_PERIOD)

    mfiBuySignal = control_mfi_buy_signal(mfi=mfi[-1], threshold=MFI_LOW_THRESHOLD)
    mfiRangeBuySignal = control_mfi_range_buy_signal(mfi=mfi[-1])

    strategy = 0
    if (pBollBuySignal is True) and (stochrsiBuySignal is True):
        buySignal = True
        strategy = 1

    if (pBollBuySignal is True) and (mfiBuySignal is True):
        buySignal = True
        strategy += 2*10

    # if (mfiBuySignal is True) and (stochrsiBuySignal is True):
    #     buySignal = True
    #     strategy += 2*10
    #
    # if (mfiRangeBuySignal is True) and (stochrsiBuySignal is True):
    #     buySignal = True
    #     strategy += 3*100

    if buySignal is True:
        sellTarget = TARGET_DOWN_TO_UP

        stopPrice = get_stop_price(high=high, low=low, close=close, tickSize=tickSize)

        global glbExplanation
        glbExplanation= f"pBoll: {round(percentBoll[-1], 2)} mfi: {round(mfi[-1],2)} fast: {round(fastK[-1], 2)} slow: {round(slowD[-1], 2)}"

    return buySignal, sellTarget, stopPrice, mfi[-1], fastK[-1], slowD[-1], percentBoll[-1], strategy

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

def log(msg=None):
    if IS_LOG is True:
        logging.info(msg=msg)
    if IS_PRINT is True:
        print(msg, flush=True)
    if IS_ALARM is True:
        alarm()

def alarm():
    frequency = 2000
    duration = 500
    winsound.Beep(frequency, duration)

def buy(connSession=None):
    log(f"BUY Thread Start")

    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_candle = binanceConfig["url_historical"] + binanceConfig["url_candle_historical"]
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    symbol = symbolClass()
    symbolRows = symbol.readAll(dbCursor=dbCursor, exchangeId=1, quoteAsset='USDT')
    if (symbolRows is not None) and (len(symbolRows) > 0):
        while True:

            for symbolRow in symbolRows:
                item = json.loads(symbolRow[0])
                coinSymbol = item['symbol']
                tickSize = float(item['tick_size'])

                # if coinSymbol != 'NEARUSDT':
                #     continue

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
                volumes = df['volume']

                isPosition, positionRow = position_existence_control(dbCursor=dbCursor,
                                                                     symbol=coinSymbol,
                                                                     status=STATUS_BUY)

                """ Coine ait işlemde olan bir kayıt var ise satılmadan tekrar alış yapılmaz """
                if isPosition is True:
                    continue

                """ ALIM sinyali olup olmadığı kontrol edilir """
                buySignal, sellTarget, stopPrice, mfi, fastk, slowd, pboll, strategy = \
                    control_buy_signal(openPrices=open_prices,
                                       highPrices=high_prices,
                                       lowPrices=low_prices,
                                       closePrices=closing_prices,
                                       volumes=volumes,
                                       tickSize=tickSize)
                """ ALIM sinyali yoksa sonraki COIN """
                if buySignal is False:
                    continue

                stopPrice = round(stopPrice, 8)
                """ Coin in anlık tahta fiyatı okunur. """
                tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                                   url=url_book_ticker,
                                                                                   symbol=coinSymbol)
                if tickerStatus is False:
                    continue

                """ ALIM işlemi yapılır. ALIM işlemi askPrice (tahtadaki üstteki satış fiyatı) ile yapılır """
                # TODO: Alım işlemi yapılacak
                global glbExplanation

                curIndex = date_kline.size - 1
                periodTimetamps = date_kline[curIndex]
                periodTime = datetime.fromtimestamp((periodTimetamps / 1000))

                stopHeight = float(closing_prices[curIndex]) - stopPrice
                stopHeight = round(stopHeight, 8)

                glbExplanation = f"price:{askPrice} stop: {stopPrice} {glbExplanation}"
                insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                                   price=askPrice, stopPrice=stopPrice, stopHeight=stopHeight,
                                   sellTarget=sellTarget, period=CANDLE_INTERVAL, periodTime=periodTime,
                                   signalName='BOLL', explanation=glbExplanation, mfi=mfi, fastk=fastk,
                                   slowd=slowd, pboll=pboll, strategy=strategy)
                # TODO: Aşağıdaki satırlar silinecek
                log(f"{SIDE_BUY} {coinSymbol} {glbExplanation} height:{stopHeight} %{round((stopHeight*100/stopPrice), 2)} strateji: {strategy}")

            # end for symbolRow in symbolRows:
        # end while True:

def sell(connSession=None):
    log(f"SELL Thread Start")

    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    while True:
        """ Alınmış durumdaki (trade.status = 0) kayıtlar okunur """
        trade = tradeClass()
        positionRecords = trade.readTradeAll(dbCursor=dbCursor, status=STATUS_BUY)
        if (positionRecords is None):
            continue

        for item in positionRecords:
            position = json.loads(item[0])
            coinSymbol = position['symbol']
            expl = position['explanation']

            """ Coin in anlık tahta fiyatı okunur. """
            tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                               url=url_book_ticker,
                                                                               symbol=coinSymbol)
            if tickerStatus is False:
                continue

            """ STOP kontrolleri bidPrice (tahtadaki anlık alış fiyatı) ile yapılır """
            isStop = stop_control(currentPrice=bidPrice, stopPrice=position['stop_price'])

            if isStop is True:
                # TODO: Burada SATIM işlemi yapılacak
                # Satış yapılmış gibi satış kaydı update edilir.
                update_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_SELL,
                                   price=bidPrice, explanation=expl, oldStatus=position['status'],
                                   newStatus=STATUS_STOP)
                # TODO: LOG kayıtları silinecek
                if bidPrice > position['buy_price']:
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} KAR: {round((bidPrice - position['buy_price']), 8)}  >>>")
                else:
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} ZARAR: {round((bidPrice - position['buy_price']), 8)}  <<<")
                continue
            # end if isStop is True:

            """ Trailing Stop (iz süren stop) kontrolleri """
            isTrailingStopChange, newStopPrice = trailing_stop_control(stopHeight=position['stop_height'],
                                                                       currentPrice=bidPrice,
                                                                       buyPrice=position['buy_price'],
                                                                       stopPrice=position['stop_price'])
            if isTrailingStopChange is True:
                target = position['sell_target']
                buyPrice = position['buy_price']
                stopChangeCount = position['stop_change_count']

                # TODO: Stop belirlemede kural;
                #   aşağı trend ise atr * TRAILING_STOP_FACTOR_FOR_DOWN (0.5)
                #   yukarı trend ise atr * TRAILING_STOP_FACTOR_FOR_UP (1.1)
                # olacak şekilde kural değiştirildiği için aşağıdaki kısım kapatıldı.
                # Test sonuçları olumsuz olursa tekrar açılabilir.
                stopChangeCount = stopChangeCount + 1

                update_stop_price(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                  stopChangeCount=stopChangeCount)
                # TODO: print silinecek
                log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {position['buy_price']} New Stop: {newStopPrice} fark: {round((newStopPrice - position['buy_price']), 8)} %:{round((newStopPrice - position['buy_price'])/position['buy_price']*100, 3)}")
                # print(f"  *** STOP UPDATE *** {coinSymbol} Alış: {position['buy_price']} Eski Stop: {position['stop_price']} Yeni Stop: {newStopPrice}")
                # TODO: Burada kayıt yeniden okunabilir.
                #       Vakit kaybı olmasın diye daha önce okunan kayıtta stop_price değiştirildi.
                #       Aşağıdaki satır silinecek
                position['stopPrice'] = newStopPrice

            # end if isTrailingStopChange is True:

        # end for item in positionRecords:
    # end while True:

def getChatId(connSession=None):
    url = URL_TELEGRAM + TOKEN + '/getUpdates'
    response = connSession.get(url)
    r = response.json()
    chatId = r['result'][0]['message']['chat']['id']
    return str(chatId)

def sendNotification(connSession=None, notification=None):
    bot_chatID = getChatId(connSession=connSession)
    msg = f"{notification}"
    send_text = URL_TELEGRAM + TOKEN + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + msg
    response = connSession.get(send_text)
    return response.json

def readSummary(dbCursor=None):
    trade = tradeClass()
    res = trade.readProfitSummary(dbCursor=dbCursor)
    return res

def notify(connSession=None):
    db = func.connectDB()
    dbCursor = db.cursor()

    while True:
        row = readSummary(dbCursor=dbCursor)
        record = json.loads(row[0])
        kar = record["kar"]
        zarar = record["zarar"]
        fark = record["fark"]
        message = f"Hesap Özeti\n {kar} - {zarar} = *{fark}*"
        r = sendNotification(connSession=connSession, notification=message)
        time.sleep(3600)

def main():
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    str = f"========== TEST START TIME: {t} ============="
    log(msg=str)
    str = f"========== MFI + STOCHRSI ============="
    log(msg=str)
    str = f"========== STANDART DEVIATION: {STANDARD_DEVIATION} ============="
    log(msg=str)
    str = f"========== CANDLE INTERVAL: {CANDLE_INTERVAL} ============="
    log(msg=str)

    connSession = requests.session()

    thread_notify = Thread(name='notify', target = notify, args = (connSession, ), daemon=True)
    thread_sell = Thread(name='sell', target = sell, args = (connSession, ), daemon=True)
    thread_buy = Thread(name='buy', target=buy, args=(connSession,))

    thread_notify.start()
    thread_sell.start()
    thread_buy.start()

    thread_notify.join()
    thread_sell.join()
    thread_buy.join()

main()