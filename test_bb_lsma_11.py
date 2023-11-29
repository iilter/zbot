import json
import requests
import pandas as pd
from datetime import datetime
import time
import schedule
from threading import Thread
from pathlib import Path

import constant as cons
import botfunction as func
import indicator as ind
from botclass import BinanceCandlePrice as candleClass
from botclass import BinanceSymbol as symbolClass
from botclass import BinanceBookTicker as tickerClass
from botclass import Trade as tradeClass
from botclass import TickerData

import notify as tlg
import logging
import winsound

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP = 2

CANDLE_INTERVAL = "3m"     # Okunacak mum verisi periyodu
CANDLE_COUNT = 11           # Okunacak mum verisi adeti
AVERAGE_CANDLE_COUNT = 6    # Ortalaması hesaplanacak mum adeti
RATE_THRESHOLD = float(200)
MAX_AMOUNT_LIMIT = float(100)
LIMIT_FACTOR = float(5)
SMA_PERIOD = 6
BOLLINGER_PERIOD = 6
STANDARD_DEVIATION = 2
LSMA_PERIOD = 6
ATR_PERIOD = 6
ATR_STOP_FACTOR = float(0.5)
USE_PROFIT_RANGE = False

VOLUME_WEIGHT = float(30000.0)
STRATEGY_2 = 2
USE_PROFIT_RANGE_2 = True
PROFIT_STRATEGY_2 = float(1.5)

STRATEGY_4 = 4
USE_PROFIT_RANGE_4 = False
PROFIT_STRATEGY_4 = float(0.75)

STRATEGY_5 = 5
USE_PROFIT_RANGE_5 = True
PROFIT_STRATEGY_5 = float(0.60)

STRATEGY_7 = 7
USE_PROFIT_RANGE_7 = False
PROFIT_STRATEGY_7 = float(0.50)

PROFIT_PERCENTAGE = float(1.5)      # Kazanç yüzdesi
PROFIT_STRATEGY_1 = float(1.5)
PROFIT_STRATEGY_3 = float(0.75)
PROFIT_STRATEGY_6 = float(0.75)

STRATEGY_1 = 1
STRATEGY_3 = 3
STRATEGY_6 = 6

IS_LOG = True
IS_PRINT = True
IS_ALARM = False

TEST_NAME = "STRATEJI 2,4,5,7"
glbExplanation = ""
logging.basicConfig(filename="debug_test_karma", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )


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


def convert_dataframe(bars=None):
    """
    Mum verilerinin ilk 6 kolonu "date", "open", "high", "low", "close", "volume" muhafaza edilir.
    Diğer kolonlar silinir.
    DataFrame e çevrilen data geri dönülür.
    """
    for line in bars:
        del line[6:]

    df = pd.DataFrame(bars, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    return df

def calculate_average(data=None):
    dataLength = len(data)
    total = sum(data)
    average = total / dataLength
    return average

def get_candle_data(dbCursor=None, session=None, url=None, symbol=None, interval=None, limit=None):
    candle = candleClass()
    candle.dbCursor = dbCursor
    bars = candle.getDataWithSession(session=session, url=url, symbol=symbol, candleInterval=interval, limit=limit)
    return bars


def insert_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, stopPrice=None,
                       stopType=None, stopHeight=None,
                       sellTarget=None, period=None, periodTime=None, currentPeriodTime=None,
                       signalName=None, explanation=None,
                       mfi=None, fastk=None, slowd=None, pboll=None, strategy=None):
    trade = tradeClass()
    trade.symbol = symbol
    trade.period = period
    trade.period_time = periodTime
    trade.current_period_time = currentPeriodTime
    trade.explanation = explanation
    trade.buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    trade.buy_price = price
    trade.stop_price = stopPrice
    trade.stop_type = stopType
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


def update_stop_price_and_current_period(dbCursor=None, symbol=None, stopPrice=None,
                                         currentPeriodTime=None, stopChangeCount=None):
    trade = tradeClass()
    trade.updateStopPriceAndCurrentPeriod(dbCursor=dbCursor, symbol=symbol, stopPrice=stopPrice,
                                          currentPeriodTime=currentPeriodTime, stopChangeCount=stopChangeCount,
                                          status=STATUS_BUY)


def position_control(dbCursor=None, symbol=None, status=None):
    trade = tradeClass()
    row = trade.readTrade(dbCursor=dbCursor, symbol=symbol, status=status)
    if (row is None) or (len(row) <= 0):
        return False, row
    return True, row


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


def get_stop_price(stopType=None, opens=None, highs=None, lows=None, closes=None, tickSize=None):
    stopPrice = None
    stopHeight = None
    if stopType == cons.STOP_TYPE_PREVIOUS_LOW:
        stopPrice = lows[-2]
        if stopPrice > lows[-1]:
            stopPrice = lows[-1]

    if stopType == cons.STOP_TYPE_TRAILING:
        atr = ind.get_atr(highPrices=highs, lowPrices=lows, closePrices=closes, period=ATR_PERIOD)
        atrIndicator = func.round_tick_size(price=atr[-2], tick_size=tickSize)
        stopPrice = lows[-2]
        if ATR_STOP_FACTOR > 0:
            stopPrice = stopPrice - (atrIndicator * ATR_STOP_FACTOR)
        else:
            stopPrice = stopPrice - atrIndicator

        """ 
        Stop fiyatının coin in minumum artım miktarına (tick size) göre ayarlanır.
        Minimum artım miktarından fazla olan digitler silinir.
        Örnek: Stop: 0.2346328 ise ve tick_size: 0.0001 ise stop: 0.2346 yapılır.
        """
        # residualValue = stopPrice % tickSize
        # stopPrice = stopPrice - residualValue
        stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
        stopHeight = lows[-2] - stopPrice
        stopHeight = func.round_tick_size(price=stopHeight, tick_size=tickSize)
        log(f"   Stop Price: {stopPrice} Stop Height: {stopHeight} Under Low: %{round((stopHeight/stopPrice)*100, 3)}")

    return stopPrice, stopHeight


def trailing_stop_control(stopHeight=None, currentPrice=None, buyPrice=None, stopPrice=None):
    stopChange = False
    newStopPrice = None

    if currentPrice > stopPrice:
        difference = round((currentPrice - stopPrice), 8)
        if difference > stopHeight:
            newStopPrice = currentPrice - stopHeight
            newStopPrice = round(newStopPrice, 8)
            stopChange = True

    return stopChange, newStopPrice


def stop_control(currentPrice=None, stopPrice=None):
    if currentPrice < stopPrice:
        log(f"   STOP StopPrice: {stopPrice} CurrentPrice: {currentPrice}")
        return True
    return False


def profit_control(currentPrice=None, buyingPrice=None, strategy=None):
    if strategy == STRATEGY_2:
        if USE_PROFIT_RANGE_2 is True:
            if currentPrice > buyingPrice:
                profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
                if profit >= PROFIT_STRATEGY_2:
                    log(f"   PROFIT_1 STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                    return True

    if strategy == STRATEGY_4:
        if USE_PROFIT_RANGE_4 is True:
            if currentPrice > buyingPrice:
                profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
                if profit >= PROFIT_STRATEGY_4:
                    log(f"   PROFIT_1 STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                    return True

    if strategy == STRATEGY_5:
        if USE_PROFIT_RANGE_5 is True:
            if currentPrice > buyingPrice:
                profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
                if profit >= PROFIT_STRATEGY_5:
                    log(f"   PROFIT_1 STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                    return True

    if strategy == STRATEGY_7:
        if USE_PROFIT_RANGE_7 is True:
            if currentPrice > buyingPrice:
                profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
                if profit >= PROFIT_STRATEGY_7:
                    log(f"   PROFIT_1 STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                    return True

    return False

    # if USE_PROFIT_RANGE is True:
    #     profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
    #     if profit >= PROFIT_PERCENTAGE:
    #         log(f"   PROFIT STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
    #         return True
    # return False

def control_strategy_7(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None, btcTicker=None):
    data = volumes[-LSMA_PERIOD:-1]
    averageVolume = calculate_average(data=data)
    minVolumeFactor = averageVolume * 2
    maxVolumeFactor = averageVolume * 3

    curVolume = volumes[-1]
    curClose = closes[-1]
    curCandleColor = ind.get_candle_color(open=opens[-1], close=closes[-1])
    prevCandleColor = ind.get_candle_color(open=opens[-2], close=closes[-2])

    btcLsma = ind.get_lsma(data=btcTicker.closes, period=LSMA_PERIOD)
    curBtcLsma = btcLsma[-1]
    prev1BtcLsma = btcLsma[-2]
    prev2BtcLsma = btcLsma[-3]

    curBtcCandleColor = ind.get_candle_color(open=btcTicker.opens[-1], close=btcTicker.closes[-1])
    prev1BtcCandleColor = ind.get_candle_color(open=btcTicker.opens[-2], close=btcTicker.closes[-2])

    global glbExplanation
    if curBtcLsma >= prev1BtcLsma >= prev2BtcLsma:
        if (curCandleColor == cons.CANDLE_GREEN) and (curBtcCandleColor == cons.CANDLE_GREEN):
            if maxVolumeFactor >= curVolume >= minVolumeFactor:
                totalWeight = round((curVolume * curClose), 2)
                log(f"  ##### {symbol} Vol:{round(maxVolumeFactor,2)} >= {curVolume} >= "
                    f"{round(minVolumeFactor,2)} close:{curClose} Weight:{totalWeight} ")
                if totalWeight > VOLUME_WEIGHT:
                    glbExplanation = f"PUMP Vol:{round(maxVolumeFactor,2)} >= {curVolume} >= " \
                                     f"{round(minVolumeFactor,2)} Weight:{totalWeight}"
                    return True

    return False

def control_strategy_6(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None):
    percentBoll = ind.get_percent_bollinger(prices=volumes,
    period = BOLLINGER_PERIOD,
    standard_deviation = STANDARD_DEVIATION)
    if percentBoll is None:
        return False

    curCandleColor = ind.get_candle_color(open=opens[-1], close=closes[-1])
    if curCandleColor == cons.CANDLE_RED:
        return False

    global glbExplanation
    curPBOLL = percentBoll[-1]
    if curPBOLL > 1.0:
        lsma = ind.get_lsma(data=closes, period=LSMA_PERIOD)
        if lsma[-1] > lsma[-2] > lsma[-3]:
            if lsma[-1] > closes[-1]:
                glbExplanation = f"BB%(Volume) > 1 ClosePrice:{closes[-1]} pBOLL cur:{round(curPBOLL, 3)} " \
                                 f"LSMA:{round(lsma[-3], 4)}, {round(lsma[-2], 4)}, {round(lsma[-1], 4)}"
                return True

    return False


def control_strategy_5(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None):
    hl2 = (highs + lows) / 2
    pBollHl2 = ind.get_percent_bollinger(prices=hl2, period=BOLLINGER_PERIOD,
                                         standard_deviation=STANDARD_DEVIATION)
    if pBollHl2 is None:
        return False

    pBollVolume = ind.get_percent_bollinger(prices=volumes, period=BOLLINGER_PERIOD,
                                            standard_deviation=STANDARD_DEVIATION)
    if pBollVolume is None:
        return False

    curCandleColor = ind.get_candle_color(open=opens[-1], close=closes[-1])
    if curCandleColor == cons.CANDLE_GREEN:
        if (pBollHl2[-1] > 1.0) and (pBollVolume[-1] > 1.0):
            lsma = ind.get_lsma(data=hl2, period=LSMA_PERIOD)
            log(f"    ***** {symbol} stj:5 pBollHl2:{pBollHl2[-1]} pBollVolume {pBollVolume[-1]} ClosePrice:{closes[-1]} LSMA: {lsma[-1]} *****")
            # if lsma[-1] > closes[-1]:
            global glbExplanation
            glbExplanation = f"BB%(HL2) > 1):{round(pBollHl2[-1], 4)} BB%(VOL) > 1):{round(pBollVolume[-1], 4)} " \
                             f"CurPrice:{closes[-1]} LSMA:{round(lsma[-1],4)}"
            return True
    return False

def control_strategy_4(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None):

    prev1CandleColor = ind.get_candle_color(open=opens[-2], close=closes[-2])
    if (prev1CandleColor == cons.CANDLE_RED):
        return False

    lsma = ind.get_lsma(data=closes, period=LSMA_PERIOD)
    sma = ind.sma(data=closes, period=SMA_PERIOD)
    percentBoll = ind.get_percent_bollinger(prices=closes,
                                            period=BOLLINGER_PERIOD,
                                            standard_deviation=STANDARD_DEVIATION)
    if percentBoll is None:
        return False

    px = 0
    pbFlg = False
    for ix in range(-2, -6, -1):
        if (percentBoll[ix] < 0.0) and (percentBoll[ix] is not None):
            pbFlg = True
            px = ix
            break

    if pbFlg is False:
        return False

    lFlg = True
    for jx in range(px + 1, -1, 1):
        prevLSMA = lsma[jx-1]
        curLSMA = lsma[jx]
        if prevLSMA < curLSMA:
            lFlg = False
            break

    if lsma[-2] > lsma[-1]:
        return False

    if closes[-1] > lsma[-1]:
        return False

    global glbExplanation
    if lFlg is True:
        glbExplanation = f"BB%(CLOSE) < 0 PBOLL({px}): {round(percentBoll[px], 4)} CurPrice: {closes[-1]}"
        return True

    return False


def control_strategy_3(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None):
    percentBoll = ind.get_percent_bollinger(prices=volumes,
                                            period=BOLLINGER_PERIOD,
                                            standard_deviation=STANDARD_DEVIATION)
    if percentBoll is None:
        return False

    curCandleColor = ind.get_candle_color(open=opens[-1], close=closes[-1])
    prevCandleColor = ind.get_candle_color(open=opens[-2], close=closes[-2])

    curPBOLL = percentBoll[-1]
    prevPBOLL = percentBoll[-2]
    if (prevPBOLL > 1.0) and (prevCandleColor == cons.CANDLE_RED):
        if curCandleColor == cons.CANDLE_GREEN:
            lsma = ind.get_lsma(data=closes, period=LSMA_PERIOD)
            if lsma[-1] > closes[-1]:
                global glbExplanation
                glbExplanation = f"BB%(Volume1) > 1 ClosePrice:{closes[-1]} pBOLL cur:{round(curPBOLL, 3)} " \
                                 f"LSMA:{round(lsma[-3],4)}, {round(lsma[-2],4)}, {round(lsma[-1],4)}"
                return True
    return False


def control_strategy_2(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None):
    hl2 = (highs + lows) / 2
    pBollHl2 = ind.get_percent_bollinger(prices=hl2, period=BOLLINGER_PERIOD,
                                         standard_deviation=STANDARD_DEVIATION)
    if pBollHl2 is None:
        return False

    pBollVolume = ind.get_percent_bollinger(prices=volumes, period=BOLLINGER_PERIOD,
                                            standard_deviation=STANDARD_DEVIATION)
    if pBollVolume is None:
        return False

    curPBolVolume = pBollVolume[-1]
    prevPBollVolume = pBollVolume[-2]
    curCandleColor = ind.get_candle_color(open=opens[-1], close=closes[-1])
    prevCandleColor = ind.get_candle_color(open=opens[-2], close=closes[-2])

    if (curPBolVolume > 1.0) and (prevPBollVolume > 1.0):
        if (curCandleColor == cons.CANDLE_GREEN) and (prevCandleColor == cons.CANDLE_GREEN):
            lsma = ind.get_lsma(data=hl2, period=LSMA_PERIOD)
            log(f"    ***** {symbol} stj:2 BB(VOL):{prevPBollVolume}, {curPBolVolume} ClosePrice:{closes[-1]} LSMA: {lsma[-1]} *****")
            # if lsma[-1] > closes[-1]:
            global glbExplanation
            glbExplanation = f"BB(VOL):{prevPBollVolume}, {curPBolVolume} ClosePrice:{closes[-1]} LSMA: {lsma[-1]}"
            return True

    return False

def control_strategy_1(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None):
    """
    KURAL
    - LSMA dipten dönüş
    - MA inişte
    - MA, LSMA nın üstünde
    - Mevcut kapanış, LSMA nın altında
    """
    # ohlc = (opens + highs + lows + closes) / 4
    # hl2 = (highs + lows) / 2
    lsma = ind.get_lsma(data=closes, period=LSMA_PERIOD)
    sma = ind.sma(data=closes, period=SMA_PERIOD)

    currLSMA = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    prev1LSMA = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    prev2LSMA = func.round_tick_size(price=lsma[-3], tick_size=tickSize)
    currMA = func.round_tick_size(price=sma[-1], tick_size=tickSize)
    prev1MA = func.round_tick_size(price=sma[-2], tick_size=tickSize)

    global glbExplanation
    if (prev2LSMA > prev1LSMA) and (currLSMA > prev1LSMA):
        if currMA < prev1MA:
            if currMA > currLSMA:
                currClose = closes[-1]
                if currClose < currLSMA:
                    glbExplanation = f"LSMA DEEP TURN CurPrice: {currClose} LSMA: {prev2LSMA}, {prev1LSMA}, {currLSMA} MA:{currMA}"
                    return True

    return False


def control_buy_signal(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None,
                       btcTicker=None):

    # signal = control_strategy_2(symbol=symbol, opens=opens, highs=highs, lows=lows, closes=closes,
    #                             volumes=volumes, tickSize=tickSize)
    # if signal is True:
    #     return True, 2
    #
    # signal = control_strategy_4(symbol=symbol, opens=opens, highs=highs, lows=lows, closes=closes,
    #                             volumes=volumes, tickSize=tickSize)
    # if signal is True:
    #     return True, 4
    #
    # signal = control_strategy_5(symbol=symbol, opens=opens, highs=highs, lows=lows, closes=closes,
    #                             volumes=volumes, tickSize=tickSize)
    # if signal is True:
    #     return True, 5

    signal = control_strategy_7(symbol=symbol, opens=opens, highs=highs, lows=lows, closes=closes,
                                volumes=volumes, tickSize=tickSize, btcTicker=btcTicker)
    if signal is True:
        return True, 7

    return False, 0

def readSummary(dbCursor=None):
    trade = tradeClass()
    res = trade.readProfitSummary(dbCursor=dbCursor)
    return res


def notify(connSession=None):
    db = func.connectDB()
    dbCursor = db.cursor()

    def job():
        row = readSummary(dbCursor=dbCursor)
        record = json.loads(row[0])
        kar = record["kar"]
        if kar is None:
            kar = 0.0
        zarar = record["zarar"]
        if zarar is None:
            zarar = 0.0
        fark = record["fark"]
        message = f"{TEST_NAME}: {CANDLE_INTERVAL}\n {kar} - {zarar} = *{fark}*"
        r = tlg.sendNotification(connSession=connSession, notification=message)

    schedule.every().hour.at(":00").do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)


def buy(connSession=None):
    log(f"BUY Thread Start")

    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_candle = binanceConfig["url_historical"] + binanceConfig["url_candle_historical"]
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    symbol = symbolClass()

    while True:
        # start_time = time.time()

        # symbolRows = symbol.readAll(dbCursor=dbCursor, exchangeId=1, quoteAsset='USDT')
        symbolRows = symbol.readAllByGroup(dbCursor=dbCursor, exchangeId=1, quoteAsset='USDT', marketGroup=1)
        if symbolRows is None:
            continue

        for symbolRow in symbolRows:
            item = json.loads(symbolRow[0])
            coinSymbol = item['symbol']
            tickSize = float(item['tick_size'])
            stepSize = float(item['step_size'])

            """ Coine ait işlemde olan bir kayıt var ise tekrar alış yapılmaz """
            isPosition, positionRow = position_control(dbCursor=dbCursor, symbol=coinSymbol, status=STATUS_BUY)
            if isPosition is True:
                continue

            """ Coin mum verileri okunur """
            candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle, symbol=coinSymbol,
                                         interval=CANDLE_INTERVAL,
                                         limit=CANDLE_COUNT)
            if (candleBars is None) or (len(candleBars) < CANDLE_COUNT):
                continue

            df = convert_dataframe(bars=candleBars)
            date_kline = df['date']
            open_prices = df['open']
            high_prices = df['high']
            low_prices = df['low']
            closing_prices = df['close']
            volume_values = df['volume']

            opens = open_prices.to_numpy(dtype=float)
            highs = high_prices.to_numpy(dtype=float)
            lows = low_prices.to_numpy(dtype=float)
            closes = closing_prices.to_numpy(dtype=float)
            volumes = volume_values.to_numpy(dtype=float)
            dates = date_kline.to_numpy(dtype=datetime)

            if coinSymbol == "BTCUSDT":
                btcTicker = TickerData(opens=opens, highs=highs, lows=lows, closes=closes, dates=dates)


            buySignal, strategy = control_buy_signal(symbol=coinSymbol, opens=opens, highs=highs, lows=lows,
                                                     closes=closes, volumes=volumes, tickSize=tickSize,
                                                     btcTicker=btcTicker)
            if buySignal is False:
                continue

            tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                               url=url_book_ticker,
                                                                               symbol=coinSymbol)
            if tickerStatus is False:
                continue

            """ Tahtada işlem yapmak için yeterli sayıda adet yoksa """
            if (askPrice * askQty) < (MAX_AMOUNT_LIMIT * LIMIT_FACTOR):
                continue

            global glbExplanation
            periodTimestamps = dates[-1]
            periodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            """ Stop fiyatı belirlenir """
            stopPrice, stopHeight = get_stop_price(stopType=cons.DEFAULT_STOP_TYPE, opens=opens,
                                                   highs=highs, lows=lows, closes=closes, tickSize=tickSize)

            glbExplanation = f"Buy Price:{askPrice} {glbExplanation}"
            insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                               price=askPrice, stopPrice=stopPrice, stopType=cons.DEFAULT_STOP_TYPE,
                               stopHeight=stopHeight, sellTarget=None, period=CANDLE_INTERVAL,
                               periodTime=periodTime, currentPeriodTime=periodTime, signalName=None,
                               explanation=glbExplanation,
                               mfi=None, fastk=None, slowd=None, pboll=None, strategy=strategy)
            log(f"{SIDE_BUY} {coinSymbol} stop:{stopPrice} {glbExplanation} strategy:{strategy}")

        # end for symbolRow in symbolRows:
        # log(f"  ")
        # log(f"---- Toplam süre: {time.time() - start_time} saniye {(time.time() - start_time)/60} dakika -----")
        # log(f"  ")

    # end while True:


def sell(connSession=None):
    log(f"SELL Thread Start")

    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]
    url_candle = binanceConfig["url_historical"] + binanceConfig["url_candle_historical"]

    while True:
        """ Alınmış durumdaki (trade.status = 0) kayıtlar okunur """
        trade = tradeClass()
        positionRecords = trade.readTradeAll(dbCursor=dbCursor, status=STATUS_BUY)
        if positionRecords is None:
            continue

        for item in positionRecords:
            position = json.loads(item[0])
            coinSymbol = position['symbol']
            currentPeriodTime = position['current_period_time']
            stopType = position['stop_type']
            stopPrice = position['stop_price']
            expl = position['explanation']
            strategy = position['strategy']

            symbol = symbolClass()
            symbolRow = symbol.readOne(dbCursor=dbCursor, exchangeId=1, symbol=coinSymbol)
            if (symbolRow is None) or (len(symbolRow) <= 0):
                log(f"{coinSymbol} okunamadı")
                continue

            sym = json.loads(symbolRow[0])
            coinSymbol = sym['symbol']
            tickSize = float(sym['tick_size'])
            stepSize = float(sym['step_size'])
            minNotional = float(sym['min_notional'])
            minLot = float(sym['min_lot'])

            candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle,
                                         symbol=coinSymbol,
                                         interval=CANDLE_INTERVAL,
                                         limit=2)
            if (candleBars is None) or (len(candleBars) < 2):
                continue

            df = convert_dataframe(bars=candleBars)
            date_kline = df['date']
            low_prices = df['low']
            close_prices = df['close']

            dates = date_kline.to_numpy(dtype=datetime)
            lows = low_prices.to_numpy(dtype=float)
            closes = close_prices.to_numpy(dtype=float)

            """ STOP kontrolleri en son işlem görmüş (close) fiyat ile yapılır """
            isStop = stop_control(currentPrice=closes[-1], stopPrice=stopPrice)
            isProfit = profit_control(currentPrice=closes[-1], buyingPrice=position['buy_price'], strategy=strategy)

            """ Stop olmuş ise veya kar alma noktasına gelmiş ise satış yapılır. """
            if (isStop is True) or (isProfit is True):
                """ Coin in anlık tahta fiyatı okunur. """
                tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                                   url=url_book_ticker,
                                                                                   symbol=coinSymbol)
                if tickerStatus is False:
                    continue

                # TODO: Burada SATIM işlemi yapılacak
                # Satış yapılmış gibi satış kaydı update edilir.
                update_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_SELL,
                                   price=bidPrice, explanation=expl, oldStatus=position['status'],
                                   newStatus=STATUS_STOP)

                if bidPrice > position['buy_price']:
                    kar = ((bidPrice - position['buy_price']) * 100) / (position['buy_price'])
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} strategy:{strategy} KAR: %{round(kar, 2)} >>>")
                else:
                    zarar = ((position['buy_price'] - bidPrice) * 100) / bidPrice
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} strategy:{strategy} ZARAR: %{round(zarar, 2)} <<<")

                continue
            # end if isStop is True:

            """ Yeni stop fiyatı belirlenir """
            if stopType == cons.STOP_TYPE_PREVIOUS_LOW:
                periodTimestamps = dates[-1]
                candlePeriodTime = datetime.fromtimestamp(periodTimestamps / 1000)

                """ Period değişmemiş ise stopPrice değiştirilmez """
                if str(currentPeriodTime) == str(candlePeriodTime):
                    continue

                """ Period değişmiş ise stopPrice önceki mumun low değeri yapılır. """
                newStopPrice = lows[-2]
                """ Yeni stop fiyatı, eskisinden küçük ise stop fiyat değiştirilmez """
                if newStopPrice <= stopPrice:
                    continue

                stopChangeCount = position['stop_change_count']
                stopChangeCount += 1

                update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                                     currentPeriodTime=candlePeriodTime,
                                                     stopChangeCount=stopChangeCount)
                log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {position['buy_price']} New Stop: {newStopPrice} fark: {round((newStopPrice - position['buy_price']), 8)} %:{round((newStopPrice - position['buy_price']) / position['buy_price'] * 100, 3)}")
            # end if stopType == cons.STOP_TYPE_PREVIOUS_LOW

            if stopType == cons.STOP_TYPE_TRAILING:
                """ Coin in anlık tahta fiyatı okunur. """
                # tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                #                                                                    url=url_book_ticker,
                #                                                                    symbol=coinSymbol)
                # if tickerStatus is False:
                #     continue

                """ 
                Trailing Stop (iz süren stop) kontrolü
                Not: Mevcut mumum kapanış fiyatı ile mi, yoksa tahtadaki o andaki alış (bid) fiyatı ile mi
                     kontrol edilecek. 
                     Şu anda mevcut mumun kapanış (en son işlem gören) fiyatı ile yapılıyor
                """
                isTrailingStopChange, newStopPrice = trailing_stop_control(stopHeight=position['stop_height'],
                                                                           currentPrice=closes[-1], # bidPrice,
                                                                           buyPrice=position['buy_price'],
                                                                           stopPrice=position['stop_price'])
                if isTrailingStopChange is True:
                    stopChangeCount = position['stop_change_count']
                    stopChangeCount += 1

                    update_stop_price(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                      stopChangeCount=stopChangeCount)
                    log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {position['buy_price']} New Stop: {newStopPrice} fark: {round((newStopPrice - position['buy_price']), 8)} %:{round((newStopPrice - position['buy_price'])/position['buy_price']*100, 3)}")
                    position['stopPrice'] = newStopPrice
            # end stopType == cons.STOP_TYPE_TRAILING


        # end for item in positionRecords:
    # end while True:

""" Karma strateji """
def main():
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"========== TEST START TIME: {t} ============="
    log(msg=msg)
    msg = f"========== {Path(__file__).stem} {TEST_NAME} ============="
    log(msg=msg)
    msg = f"========== CANDLE INTERVAL: {CANDLE_INTERVAL} ============="
    log(msg=msg)

    connSession = requests.session()

    thread_notify = Thread(name='notify', target=notify, args=(connSession, ), daemon=True)
    thread_sell = Thread(name='sell', target=sell, args=(connSession, ), daemon=True)
    thread_buy = Thread(name='buy', target=buy, args=(connSession,))

    thread_notify.start()
    thread_sell.start()
    thread_buy.start()

    thread_notify.join()
    thread_sell.join()
    thread_buy.join()


main()
