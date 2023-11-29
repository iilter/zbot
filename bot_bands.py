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
MINIMUM_BOLLINGER_HEIGHT = 5  # Bollinger alt ve üst arası yükseklik (yüzde olarak)
LINEAR_REGRESSION_PERIOD = 20
AO_SHORT_PERIOD = 5
AO_LONG_PERIOD = 34
STANDARD_DEVIATION = 2  # Bollinger band alt ve üst bandlar için standart sapma değeri
TARGET_CENTER = 'CENTER'
TARGET_UP = 'UP'
STOP_LIMIT_PERCENTAGE = 1
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP_LIMIT = 2

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

    length = prices.size - 1
    band_up = round(up[length], 6)
    band_center = round(center[length], 6)
    band_down = round(down[length], 6)
    price = float(prices[length])
    return band_down, band_center, band_up

def control_bollinger_buy_signal(openPrices=None, highPrices=None, lowPrices=None, closePrices=None,
                                 down=None, center=None, up=None):
    signal = False
    sellTarget = None

    length = lowPrices.size
    curIndex = length - 1
    prev1Index = length - 2
    prev2Index = length - 3

    currentLowPrice = round(float(lowPrices[curIndex]), 6)
    currentBandDown = round(down[curIndex], 6)
    currentBandCenter = round(center[curIndex], 6)
    currentBandUp = round(up[curIndex], 6)

    # Bollinger alt ve üst band arası MINIMUM_BOLLINGER_HEIGHT den küçük ise işlem yapılmaz
    # Örnek; %5 den küçük ise
    height = ((currentBandUp - currentBandDown) * 100) / currentBandDown
    if height < MINIMUM_BOLLINGER_HEIGHT:
        return signal, sellTarget

    # Trendin eğimi (closingPrices kullanılarak) bulunur.
    # Slope < 0 ise eğim aşağı, slope > 0 ise eğim yukarı
    slope = ind.get_trend_slope(prices=closePrices.to_numpy(dtype=float), period=LINEAR_REGRESSION_PERIOD)

    prev1LowPrice = round(float(lowPrices[prev1Index]), 6)
    prev1BandDown = round(down[prev1Index], 6)
    prev1BandCenter = round(center[prev1Index], 6)
    prev1BandUp = round(up[prev1Index], 6)

    prev2LowPrice = round(float(lowPrices[prev2Index]), 6)
    prev2BandDown = round(down[prev2Index], 6)
    prev2BandCenter = round(center[prev2Index], 6)
    prev2BandUp = round(up[prev2Index], 6)

    # Trend eğimi aşağı doğru ise
    if slope <= 0:
        # 2 önceki mumum low değeri bollinger alt bandının altında ise VE
        # 1 önceki mumun low değeri bollinger alt bandının üstünde ise VE
        # mevcut mumun anlık low değeri bollinger alt bandının üstünde VE
        # 1 önceki mum YEŞİL kapatmış ise ALIM sinyali
        if (prev2LowPrice < prev2BandDown) and (prev1LowPrice > prev1BandDown) and (currentLowPrice > currentBandDown):
            prev1OpenPrice = round(float(openPrices[prev1Index]), 6)
            prev1ClosePrice = round(float(closePrices[prev1Index]), 6)
            # 1 önceki mum YEŞİL ise
            if prev1ClosePrice > prev1OpenPrice:
                signal = True
                # Trend eğimi aşağı ise satış hedefi BOLLINGER ORTA BANDI yapılır
                sellTarget = TARGET_CENTER

    # Trend eğimi yukarı doğru ise
    if slope > 0:
        # önceki mumun low değeri bollinger alt bandının altında ise VE
        # mevcut mumun anlık low değeri bollinger alt bandının üstünde ise VE
        # mevcut mumun anlık değeri YEŞİL ise ALIM SİNYALİ
        if (prev1LowPrice < prev1BandDown) and (currentLowPrice > currentBandDown):
            currentOpenPrice = round(float(openPrices[curIndex]), 6)
            currentClosePrice = round(float(closePrices[curIndex]), 6)
            # Mevcut mumun anlık değeri YEŞİL ise
            if currentClosePrice > currentOpenPrice:
                signal = True
                # Trend eğimi yukarı ise satış hedefi BOLLINGER ÜST BANDI yapılır
                sellTarget = TARGET_UP

    return signal, sellTarget

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

def trade_record_existence_control(dbCursor=None, symbol=None, status=None):
    trade = tradeClass()
    row = trade.readTrade(dbCursor=dbCursor, symbol=symbol, status=status)
    if (row is None) or (len(row) <= 0):
        return False, row
    return True, row

def insert_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, stopPrice=None, sellTarget=None,
                       period=None, signalName=None, explanation=None):
    trade = tradeClass()

    trade.symbol = symbol
    trade.period = period
    trade.explanation = explanation
    if buySell == SIDE_BUY:
        trade.buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        trade.buy_price = price
        trade.stop_price = stopPrice
        trade.buy_signal_name = signalName
        trade.sell_target = sellTarget
        trade.status = 0

    trade.addTrade(dbCursor=dbCursor)
    print(f"{buySell} {symbol} {explanation}")
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

def update_stop_price(dbCursor=None, symbol=None, stopPrice=None, status=None):
    trade = tradeClass()
    trade.updateStopPrice(dbCursor=dbCursor, symbol=symbol, stopPrice=stopPrice, status=status)

def stop_limit_control(price=None, buyPrice=None, stopPrice=None):
    if stopPrice is None:
        if price < buyPrice:
            stopPercent = (buyPrice - price) * 100 / price
            if stopPercent > STOP_LIMIT_PERCENTAGE:
                return True
        return False

    if price < stopPrice:
        return True

    return False

def sell_control(price=None, buyPrice=None, sellTarget=None, down=None, center=None, up=None):
    sellPrice = None
    if sellTarget == TARGET_CENTER:
        if price >= center:
            sellPrice = price
            return True, sellPrice

    if sellTarget == TARGET_UP:
        if price >= up:
            sellPrice = price
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
                # Coine ait mum verileri okunur
                bars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle, symbol=coinSymbol,
                                       interval=CANDLE_INTERVAL,
                                       limit=CANDLE_COUNT)
                # Hiç kayıt okunamamış ise veya okunan kayıt sayısı CANDLE_COUNT tan küçük ise işlem yapılmaz
                if (bars is None) or (len(bars) < CANDLE_COUNT):
                    continue

                df = convert_dataframe(bars=bars)
                date_kline = df['date']
                open_prices = df['open']
                high_prices = df['open']
                low_prices = df['low']
                closing_prices = df['close']

                # Bollinger band bilgileri okunur
                bandUp, bandCenter, bandDown = ind.get_bollinger_bands(prices=closing_prices,
                                                                       period=BOLLINGER_BAND_PERIOD,
                                                                       standard_deviation=STANDARD_DEVIATION)

                tradeSignal, sellTarget = control_bollinger_buy_signal(openPrices=open_prices,
                                                                       highPrices=high_prices,
                                                                       lowPrices=low_prices,
                                                                       closePrices=closing_prices,
                                                                       down=bandDown,
                                                                       center=bandCenter,
                                                                       up=bandUp)

                isPosition, tradeRow = trade_record_existence_control(dbCursor=dbCursor,
                                                                      symbol=coinSymbol,
                                                                      status=STATUS_BUY)

                # Coin in anlık tahta fiyatı okunur.
                tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                                   url=url_book_ticker,
                                                                                   symbol=coinSymbol)
                if tickerStatus is False:
                    continue

                if isPosition is True:
                    buyRecord = json.loads(tradeRow[0])

                    # STOP LIMIT kontrolü, daha önce alınmış coin için yapılır.
                    isStopLimit = stop_limit_control(price=bidPrice,
                                                     buyPrice=buyRecord['buy_price'],
                                                     stopPrice=buyRecord['stop_price'])
                    if isStopLimit is True:
                        # Satış yapılmış gibi satış kaydı update edilecek.
                        update_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_SELL,
                                           price=bidPrice, oldStatus=buyRecord['status'], newStatus=STATUS_STOP_LIMIT)
                        print(f"{SIDE_SELL} {coinSymbol} Alış: {buyRecord['buy_price']} Satış: {bidPrice} STOP OLDU")

                        continue  # sonraki coin

                    # SATIŞ KONTROLÜ, daha önce alınmış coin için satış şartları kontrolleri yapılır
                    length = closing_prices.size - 1
                    isSell, sellPrice = sell_control(price=bidPrice,
                                                     buyPrice=buyRecord['buy_price'],
                                                     sellTarget=buyRecord['sell_target'],
                                                     down=round(bandDown[length], 6),
                                                     center=round(bandCenter[length], 6),
                                                     up=round(bandUp[length], 6)
                                                     )
                    if isSell is True:
                        update_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_SELL,
                                           price=sellPrice, oldStatus=buyRecord['status'], newStatus=STATUS_SELL)
                        print(f"{SIDE_SELL} {coinSymbol} Alış: {buyRecord['buy_price']} Satış: {sellPrice} Satış Hedef: {buyRecord['sell_target']}")
                        continue  # sonraki coin
                # end if isPosition is True

                if tradeSignal is True:
                    # SİNYAL geldiğinde ALIM işlemi yapılır
                    # Aynı coin daha önce alındı ve hala işlemde ise başka alım yapılmaz
                    if isPosition is True:
                        continue

                    length = closing_prices.size - 1
                    curPrice = round(float(closing_prices[length]), 6)
                    curDown = round(bandDown[length], 6)
                    curCenter = round(bandCenter[length], 6)
                    curUp = round(bandUp[length], 6)
                    # Bir önceki mumum low değeri STOP PRICE olarak alınır
                    stopPrice = round(float(low_prices[length - 1]), 6)
                    strExp = f"{SIDE_BUY} target:{sellTarget} price:{curPrice} down: {curDown} center: {curCenter} up: {curUp}"
                    insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                                       price=curPrice, stopPrice=stopPrice, sellTarget=sellTarget,
                                       period=CANDLE_INTERVAL, signalName='BOLL', explanation=strExp)
                # end if tradeSignal is True

            # end for symbolRow
            # print(f"Toplam süre: {(time.time() - start_time) / 60} dakika ")
        # end while True

    # linreg = ind.get_linear_regression(prices=closing_prices.to_numpy(dtype=float), period=LINEAR_REG_PERIOD)
    #
    # GRAFİK ÇİZME Örnek
    # plt.title(coin_symbol + ' Bollinger Bands')
    # plt.xlabel('Time')
    # plt.ylabel('Closing Prices')
    # plt.plot(bollinger_up, label='Bollinger Up', c='g')
    # plt.plot(bollinger_down, label='Bollinger Down', c='r')
    # plt.plot(bollinger_center, label='Bollinger Center', c='b')
    # #    plt.plot(closing_prices, label='closing_prices', c='y')
    #
    # plt.legend()
    # plt.show()

main()
