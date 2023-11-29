import json
import requests
import pandas as pd
import numpy
from datetime import datetime
import time
import schedule
from threading import Thread

import constant as cons
import botfunction as func
import indicator as ind
from botclass import BinanceCandlePrice as candleClass
from botclass import BinanceSymbol as symbolClass
from botclass import BinanceBookTicker as tickerClass
from botclass import Trade as tradeClass
from simulationclass import SimCandleClass as simCandleClass

import logging
import winsound

COIN_SYMBOL = 'FTMUSDT'
PERIOD = '2h'

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP = 2

CANDLE_COUNT = 14           # Okunacak mum verisi adeti
SMA_PERIOD = 12
LSMA_PERIOD = 12
USE_PROFIT_RANGE = True
PROFIT_PERCENTAGE = 5      # Kazanç yüzdesi

IS_LOG = True
IS_PRINT = True
IS_ALARM = False

glbExplanation = ""
logging.basicConfig(filename="debug_test07", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )

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
    trade.buy_date = periodTime #datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
                       oldStatus=None, newStatus=None, sellDate=None):
    trade = tradeClass()
    trade.symbol = symbol
    if buySell == SIDE_BUY:
        trade.buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        trade.buy_price = price
        trade.buy_signal_name = signalName
    if buySell == SIDE_SELL:
        trade.sell_date = sellDate #datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        trade.sell_price = price
        trade.sell_signal_name = signalName

    trade.updateTrade(dbCursor=dbCursor, symbol=symbol, oldStatus=oldStatus, newStatus=newStatus)


def position_control(dbCursor=None, symbol=None, status=None):
    trade = tradeClass()
    row = trade.readTrade(dbCursor=dbCursor, symbol=symbol, status=status)
    if (row is None) or (len(row) <= 0):
        return False, row
    return True, row

def control_buy_signal(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None):
    lsma = ind.get_lsma(data=closes, period=LSMA_PERIOD)
    sma = ind.sma(data=closes, period=SMA_PERIOD)

    highCurr = func.round_tick_size(price=highs[-1], tick_size=tickSize)
    highPrev1 = func.round_tick_size(price=highs[-2], tick_size=tickSize)
    highPrev2 = func.round_tick_size(price=highs[-3], tick_size=tickSize)

    lowCurr = func.round_tick_size(price=lows[-1], tick_size=tickSize)
    lowPrev1 = func.round_tick_size(price=lows[-2], tick_size=tickSize)
    lowPrev2 = func.round_tick_size(price=lows[-3], tick_size=tickSize)

    lsmaCurr = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    lsmaPrev1 = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    lsmaPrev2 = func.round_tick_size(price=lsma[-3], tick_size=tickSize)

    maCurr = func.round_tick_size(price=sma[-1], tick_size=tickSize)
    maPrev1 = func.round_tick_size(price=sma[-2], tick_size=tickSize)
    maPrev2 = func.round_tick_size(price=sma[-3], tick_size=tickSize)

    closeCurr = func.round_tick_size(price=closes[-1], tick_size=tickSize)

    if (lsmaPrev1 < lsmaPrev2) and (lsmaPrev1 < lsmaCurr):
        if (maPrev2 > lsmaPrev2) and (maPrev1 > lsmaPrev1) and (maCurr > lsmaCurr):
            if (lowPrev2 <= lsmaPrev2 <= highPrev2 ) and (lowPrev1 <= lsmaPrev1 <= highPrev1) and (lowCurr <= lsmaCurr <= highCurr):
                # if (closeCurr < maCurr):
                log(f"   === LSMA: {lsmaPrev2} {lsmaPrev1} {lsmaCurr} closeCurr: {closeCurr}")
                return True, maCurr

    return False, None

def stop_control(currentPrice=None, stopPrice=None, closes=None, tickSize=None):
    if currentPrice < stopPrice:
        log(f"   STOP StopPrice: {stopPrice} CurrentPrice: {currentPrice}")
        return True

    lsma = ind.get_lsma(data=closes, period=LSMA_PERIOD)
    sma = ind.sma(data=closes, period=SMA_PERIOD)

    lsmaCurr = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    lsmaPrev1 = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    lsmaPrev2 = func.round_tick_size(price=lsma[-3], tick_size=tickSize)
    maCurr = func.round_tick_size(price=sma[-1], tick_size=tickSize)
    maPrev = func.round_tick_size(price=sma[-2], tick_size=tickSize)

    if (lsmaPrev1 > maPrev) and (lsmaCurr < maCurr):
        log(f"=== Aşağı Kesti: LSMA: {lsmaCurr} MA: {maCurr}")
        return True

    if (lsmaPrev1 > lsmaPrev2) and (lsmaPrev1 > lsmaCurr):
        log(f"   === LSMA Geri Dönüş: {lsmaPrev2} {lsmaPrev1} {lsmaCurr}")
        return True

    return False

def profit_control(currentPrice=None, buyingPrice=None):
    """ belli bir kara ulaşınca satış yapma kontrolu kullanılmayacaksa return True """
    if USE_PROFIT_RANGE is False:
        return True

    if currentPrice > buyingPrice:
        profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
        if profit >= PROFIT_PERCENTAGE:
            log(f"   PROFIT STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
            return True
    return False

def buy(dbCursor=None, symbol=None, dates=None, opens=None,
        highs=None, lows=None, closes=None, volumes=None, tickSize=None):

    # d = dates[-1]
    # if str(d) == '2021-04-16 03:00:00':
    #     print(d)

    buySignal, maCurr = control_buy_signal(symbol=symbol, opens=opens, highs=highs, lows=lows, closes=closes,
                                           volumes=volumes, tickSize=tickSize)
    if buySignal is False:
        return False

    """ stop fiyatı belirlenir """
    atr = ind.get_atr(highPrices=highs,
                      lowPrices=lows,
                      closePrices=closes,
                      period=LSMA_PERIOD)
    atrIndicator = func.round_tick_size(price=atr[-2], tick_size=tickSize)
    stopPrice = lows[-2] # - atrIndicator

    strategy = 7

    buyPrice = closes[-1]
    if buyPrice > maCurr:
        buyPrice = maCurr
    insert_trade_table(dbCursor=dbCursor, symbol=COIN_SYMBOL, buySell=SIDE_BUY,
                       price=buyPrice, stopPrice=stopPrice, stopType=cons.DEFAULT_STOP_TYPE,
                       stopHeight=None, sellTarget=None, period=PERIOD,
                       periodTime=dates[-1], currentPeriodTime=dates[-1], signalName=None,
                       explanation=None,
                       mfi=None, fastk=None, slowd=None, pboll=None, strategy=strategy)


def sell(dbCursor=None, symbol=None, opens=None, highs=None, lows=None, closes=None, volumes=None,
         buyingPrice=None, stopPrice=None, tickSize=None, oldStatus= None, sellDate=None):
    isStop = stop_control(currentPrice=lows[-1], stopPrice=stopPrice, closes=closes, tickSize=tickSize)
    isProfit = profit_control(currentPrice=highs[-1], buyingPrice=buyingPrice)
    if (isStop is True) or (isProfit is True):
        sellPrice = closes[-1]
        if isStop is True:
            if stopPrice > sellPrice:
                sellPrice = stopPrice
        update_trade_table(dbCursor=dbCursor, symbol=symbol, buySell=SIDE_SELL,
                           price=sellPrice, explanation=None, oldStatus=oldStatus,
                           newStatus=STATUS_STOP, sellDate=sellDate)

def main():
    db = func.connectDB()
    dbCursor = db.cursor()

    symbol = symbolClass()
    symbolRow = symbol.readOne(dbCursor=dbCursor, exchangeId=1, symbol=COIN_SYMBOL)
    # if (symbolRow is None) or (len(symbolRow) <= 0):
    #     exit(1)

    sym = json.loads(symbolRow[0])
    coinSymbol = sym['symbol']
    tickSize = float(sym['tick_size'])
    stepSize = float(sym['step_size'])
    minNotional = float(sym['min_notional'])
    minLot = float(sym['min_lot'])

    """ Coinin tarihi bütün verileri okunur """
    candle = simCandleClass()
    candles = candle.readAll(dbCursor=dbCursor, symbol=coinSymbol, period=PERIOD)
    lenCandles = len(candles)

    """ 
    Her iterasyonda CANDLE_COUNT kadar period verisi okunarak 
    baştan sona doğru birer period ilerleyerek devam edilir
    """
    startIndex = CANDLE_COUNT - 1
    for ix in range(startIndex, lenCandles):
        first = ix + 1 - CANDLE_COUNT
        last = ix + 1
        dates = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        for ix in range(first, last):
            item = json.loads(candles[ix][0])
            dates.append(item['date'])
            opens.append(item['open'])
            highs.append(item['high'])
            lows.append(item['low'])
            closes.append(item['close'])
            volumes.append(item['volume'])

        dates = numpy.array(dates, dtype=datetime)
        opens = numpy.array(opens, dtype=float)
        highs = numpy.array(highs, dtype=float)
        lows = numpy.array(lows, dtype=float)
        closes = numpy.array(closes, dtype=float)
        volumes = numpy.array(volumes, dtype=float)

        # buy(dbCursor=dbCursor, symbol=COIN_SYMBOL, dates=dates,
        #     opens=opens, highs=highs, lows=lows, closes=closes, volumes=volumes, tickSize=tickSize)

        isPosition, positionRow = position_control(dbCursor=dbCursor, symbol=COIN_SYMBOL, status=STATUS_BUY)
        if isPosition is True:
            item = json.loads(positionRow[0])
            stopPrice = item['stop_price']
            buyingPrice = item['buy_price']
            tickSize = float(sym['tick_size'])
            status = item['status']

            sell(dbCursor=dbCursor, symbol=COIN_SYMBOL, opens=opens, highs=highs, lows=lows, closes=closes,
                 volumes=volumes, buyingPrice=buyingPrice, stopPrice=stopPrice, tickSize=tickSize,
                 oldStatus=status, sellDate=dates[-1])
        else:
            buy(dbCursor=dbCursor, symbol=COIN_SYMBOL, dates=dates,
                opens=opens, highs=highs, lows=lows, closes=closes, volumes=volumes, tickSize=tickSize)


main()