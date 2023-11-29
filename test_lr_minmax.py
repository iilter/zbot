import json

import numpy as np
import requests
import pandas as pd
import scipy.stats as stats
from datetime import datetime
import time
import schedule
from threading import Thread
from pathlib import Path
import talib

import constant as cons
import botfunction as func
import indicator as ind
from botclass import BinanceCandlePrice as candleClass
from botclass import BinanceSymbol as symbolClass
from botclass import BinanceBookTicker as tickerClass
from botclass import Trade as tradeClass
from botclass import TickerData
from botclass import StrategyResponse
from botclass import BinanceOrderBook as orderBookClass

import notify as tlg
import logging
import winsound

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP = 2

BTC_CANDLE_INTERVAL = "1h"
BTC_CANDLE_COUNT = 168
BTC_FAST_PERIOD = 25
BTC_SLOW_HMA = 96
BTC_SIGNAL = False
BTC_INNER_SIGNAL = False
BTC_RED_ALERT = False
BTC_GREEN_ALERT = False
BTC_SLOPE = float(0.0)
BTC_HEIKIN_COLOR = None
BTC_GREEN_THRESHOLD = float(1.5)  # yüzde
BTC_RED_THRESHOLD = float(1.5)   # yüzde

CANDLE_INTERVAL = "1h"             # Okunacak mum verisi periyodu
CANDLE_COUNT = 220  # 33            # Okunacak mum verisi adeti
MAX_TRADE_LIMIT = float(100)
COMMISSION_RATE = float(0.00075)    # %0.075
LIMIT_FACTOR = float(5)
# RSI_FAST = 24
# RSI_SLOW = 96
# BB_PERIOD = 25
# BB_STD_DEV = 2.5
SRSI_PERIOD = 5
LR_PERIOD = 50
STDDEV_UP = float(1.8)
STDDEV_DOWN = float(1.8)
ATR_PERIOD = 5
ATR_MULTIPLIER = float(1.5)
ATR_STOP_FACTOR = float(0.5)
TARGET_TO_CENTER_LINE = 1
TARGET_TO_UP_LINE = 2
# ZSCORE_PERIOD = 18
# ZSCORE_DOWN_RANGE = float(-2.0)
# ZSCORE_LIMIT_DOWN = float(-2.3)
# ZSCORE_LIMIT_UP = float(2.3)
# EMA_PERIOD = CANDLE_COUNT - 1
# HMA_FAST_PERIOD = 10  # 96
# HMA_SLOW_PERIOD = 96
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
ADX_RANGE_LEVEL = float(20.0)
DMI_PLUS_TREND_LEVEL = float(35.0)

RISK_REWARD_RATIO = float(1.2)
RISK_REWARD_RATIO_48 = float(1.5)
RISK_REWARD_RATIO_54 = float(1.5)
MAX_ACCEPTABLE_RISK = float(3.0)  # %3
PROFIT_TARGET = float(1.0)  # 2.0
PROFIT_TARGET_47 = float(1.15)  # 2.0
PROFIT_TARGET_50 = float(0.45)
PROFIT_TARGET_55 = float(2.15)
PROFIT_TARGET_56 = float(2.15)
PROFIT_STOP_TRAIL = float(0.7)  # 0.8

IS_LOG = True
IS_PRINT = True
IS_ADD_TIME = True
IS_ALARM = False

TEST_NAME = "SRSI"
glbExplanation = ""
logging.basicConfig(filename="debug_test_srsi", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )

#region Yardımcı Fonksiyonlar
def log(msg=None):
    if IS_ADD_TIME:
        msg = datetime.now().strftime("%Y-%m-%d %H:%M:%S ") + msg
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
#endregion

#region Helper Functions
def readSummary(dbCursor=None):
    trade = tradeClass()
    res = trade.readProfitSummary(dbCursor=dbCursor)
    return res


def get_candle_data(dbCursor=None, session=None, url=None, symbol=None, interval=None, limit=None):
    candle = candleClass()
    candle.dbCursor = dbCursor
    bars = candle.getDataWithSession(session=session, url=url, symbol=symbol, candleInterval=interval, limit=limit)
    return bars


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
#endregion


#region Stop Functions
def get_stop_price(stopType=None, coinTicker=None, tickSize=None):
    stopPrice = None
    stopHeight = None
    if stopType == cons.STOP_TYPE_LR:
        stopPrice = coinTicker.lows[-2]
        if stopPrice > coinTicker.lows[-1]:
            stopPrice = coinTicker.lows[-1]

    if stopType == cons.STOP_TYPE_PREVIOUS_LOW:
        stopPrice = coinTicker.lows[-2]
        if stopPrice > coinTicker.lows[-1]:
            stopPrice = coinTicker.lows[-1]

    if stopType == cons.STOP_TYPE_PROFIT:
        stopPrice = coinTicker.lows[-2]
        if stopPrice > coinTicker.lows[-1]:
            stopPrice = coinTicker.lows[-1]

    if stopType == cons.STOP_TYPE_VOLUME:
        stopPrice = coinTicker.lows[-1]

    if stopType == cons.STOP_TYPE_TRAILING:
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows,
                          closePrices=coinTicker.closes, period=ATR_PERIOD)
        atrIndicator = func.round_tick_size(price=atr[-2], tick_size=tickSize)
        stopPrice = coinTicker.lows[-2]
        if ATR_STOP_FACTOR > 0:
            stopPrice = stopPrice - (atrIndicator * ATR_STOP_FACTOR)
        else:
            stopPrice = stopPrice - atrIndicator

        """ 
        Stop fiyatının coin in minumum artım miktarına (tick size) göre ayarlanır.
        Minimum artım miktarından fazla olan digitler silinir.
        Örnek: Stop: 0.2346328 ise ve tick_size: 0.0001 ise stop: 0.2346 yapılır.
        """
        stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
        stopHeight = coinTicker.lows[-2] - stopPrice
        stopHeight = func.round_tick_size(price=stopHeight, tick_size=tickSize)
        log(f" -> Stop Price: {stopPrice} Stop Height: {stopHeight} Under Low: %{round((stopHeight/stopPrice)*100, 3)}")
    return stopPrice, stopHeight


def get_stop_price_by_atr(coinTicker=None, period=6, multiplier=3.0):
    atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows,
                      closePrices=coinTicker.closes,
                      period=ATR_PERIOD)
    stopPrice = coinTicker.closes[-1] - (atr[-1] * multiplier)
    log(f" -> ATR: {atr[-1]} multiplier: {multiplier} stop: {stopPrice} period:{period}")

    return stopPrice


def get_stop_price_under_target(currentPrice=None, buyingPrice=None, profitTarget=None,
                                tickSize=None, strategy=None) -> (float):
    log(f"          currentPrice:{currentPrice} buyingPrice:{buyingPrice} profitTarget:{profitTarget} tickSize:{tickSize}")
    stopProfitLevel = profitTarget - PROFIT_TARGET
    stopProfitLevel = stopProfitLevel - PROFIT_STOP_TRAIL

    log(f"          profitTarget: {profitTarget}  stopProfitLevel: {stopProfitLevel} ")
    newStop = buyingPrice + (stopProfitLevel * buyingPrice)/100
    newStop = func.round_tick_size(price=newStop, tick_size=tickSize)
    log(f"          newStop: {newStop}")
    return newStop


def get_stop_price_by_max_price(maxPrice=None, buyPrice=None, stopPrice=None, symbol=None, tickSize=None) -> (bool, float):
    priceRatioArray = [0.39, 0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49, 0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 0.70, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.80, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00, 1.10, 1.20, 1.30, 1.40, 1.50, 1.60, 1.70, 1.80, 1.90, 2.00, 2.10, 2.20, 2.30, 2.40, 2.50, 2.60, 2.70, 2.80, 2.90, 3.00, 3.10, 3.20, 3.30, 3.40, 3.50, 3.60, 3.70, 3.80, 3.90, 4.00, 4.10, 4.20, 4.30, 4.40, 4.50, 4.60, 4.70, 4.80, 4.90, 5.00, 5.10, 5.20, 5.30, 5.40, 5.50, 5.60, 5.70, 5.80, 5.90, 6.00, 6.10, 6.20, 6.30, 6.40, 6.50, 6.60, 6.70, 6.80, 6.90, 7.00, 7.10, 7.20, 7.30, 7.40, 7.50, 7.60, 7.70, 7.80, 7.90, 8.00, 8.10, 8.20, 8.30, 8.40, 8.50, 8.60, 8.70, 8.80, 8.90, 9.00]
    stopRatioArray =  [0.15, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.25, 0.26, 0.26, 0.27, 0.27, 0.28, 0.28, 0.29, 0.29, 0.30, 0.30, 0.31, 0.31, 0.32, 0.32, 0.33, 0.33, 0.34, 0.34, 0.35, 0.35, 0.36, 0.36, 0.37, 0.37, 0.38, 0.38, 0.39, 0.39, 0.40, 0.40, 0.41, 0.41, 0.42, 0.42, 0.43, 0.43, 0.44, 0.44, 0.45, 0.45, 0.46, 0.46, 0.47, 0.47, 0.48, 0.48, 0.49, 0.49, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00, 1.10, 1.20, 1.30, 1.40, 1.50, 1.60, 1.70, 1.80, 1.90, 2.00, 2.10, 2.20, 2.30, 2.40, 2.50, 2.50, 2.60, 2.60, 2.70, 2.70, 2.80, 2.80, 2.90, 2.90, 3.00, 3.10, 3.20, 3.30, 3.40, 3.50, 3.60, 3.70, 3.80, 3.90, 4.00, 4.10, 4.10, 4.20, 4.20, 4.30, 4.30, 4.40, 4.40, 4.50, 4.50, 4.60, 4.60, 4.70, 4.70, 4.80, 4.80, 4.90, 4.90, 5.00, 5.00, 5.10, 5.10, 5.20, 5.20, 5.30, 5.30, 5.40, 5.40, 5.50, 5.50, 5.60, 5.60, 5.70, 5.70, 5.80, 5.80, 5.90, 5.90, 6.00, 6.00]

    # if stopPrice == 0.0:
    #     return False, 0.0

    if maxPrice < buyPrice:
        return False, 0.0

    currentStopRatio = func.calculate_ratio(stopPrice, buyPrice)

    maxPriceRatio = func.calculate_ratio(maxPrice, buyPrice)
    maxPriceRatio = round(maxPriceRatio, 3)
    if maxPriceRatio < 0.4:
        return False, 0.0

    stopRatio = 0.0
    for ix in range(0, len(priceRatioArray)):
        if priceRatioArray[ix] > maxPriceRatio:
            stopRatio = stopRatioArray[ix-1]
            break

    if maxPriceRatio >= 9.00:
        stopRatio = maxPriceRatio - 3

    newStopPrice = buyPrice * (1 + (stopRatio / 100))

    ts = tickSize
    coef = 0
    while ts < 1:
        coef = coef + 1
        ts = ts * 10

    newStopPrice = round(newStopPrice, coef)
    newStopPrice = func.round_tick_size(price=newStopPrice, tick_size=tickSize)

    if stopPrice == newStopPrice:
        return False, 0.0

    log(f"  * NEW STOP PRICE * {symbol} Old: {stopPrice} New: {newStopPrice} Buyprice: {buyPrice} stopRatio: {stopRatio} maxPriceRatio:{maxPriceRatio}")

    return True, newStopPrice


def stop_control(currentPrice=None, stopPrice=None) -> (bool):
    if currentPrice < stopPrice:
        log(f" -> STOP StopPrice: {stopPrice} CurrentPrice: {currentPrice}")
        return True
    return False

#endregion


#region Profit Functions
def get_profit_target(price=None, stopPrice=None, tickSize=None, strategy=None):
    targetRatio = float(0.0)
    targetPrice = float(0.0)

    if strategy == 47:
        # targetPrice = (price * (100 + PROFIT_TARGET_47)) / 100
        # targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)
        targetRatio = PROFIT_TARGET_47
    elif strategy == 48:
        if stopPrice > float(0.0):
            riskRatio = func.calculate_ratio(price, stopPrice)
            targetRatio = riskRatio * RISK_REWARD_RATIO_48
    elif strategy == 50:
        targetRatio = PROFIT_TARGET_50
    elif strategy == 54:
        if stopPrice > float(0.0):
            riskRatio = func.calculate_ratio(price, stopPrice)
            targetRatio = riskRatio * RISK_REWARD_RATIO_54
    elif strategy == 55:
        targetRatio = PROFIT_TARGET_55
    elif strategy == 56:
        targetRatio = PROFIT_TARGET_56
    else:
        if stopPrice > float(0.0):
            riskRatio = func.calculate_ratio(price, stopPrice)
            targetRatio = riskRatio * RISK_REWARD_RATIO

    return targetPrice, targetRatio


def profit_control(currentPrice=None, buyingPrice=None, targetProfit= None, targetPrice=None, strategy=None) -> (bool):
    if currentPrice > buyingPrice:
        profit = func.calculate_ratio(currentPrice, buyingPrice)

        if BTC_RED_ALERT == True:
            log(f" !!! BTC_RED_ALERT True: {BTC_RED_ALERT}")
            log(f"        PROFIT BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
            log(f"        Sell PROFIT > 0 - targetProfit: %{targetProfit} profit: %{profit}")
            return True

        if (profit >= targetProfit):
            log(f"   BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
            log(f"   targetProfit: %{targetProfit} profit: %{profit}")
            return True
    return False


def profit_target_control(currentPrice=None, buyingPrice=None, profitTarget=None, strategy=None) -> (bool, float):
    profitTargetLevel = PROFIT_TARGET
    newProfitTarget = profitTarget
    profit = func.calculate_ratio(currentPrice, buyingPrice)
    if profit > profitTarget:
        newProfitTarget += profitTargetLevel
        while profit >= newProfitTarget:
            newProfitTarget += profitTargetLevel
        return True, newProfitTarget
    return False, newProfitTarget
#endregion


#region DB Functions
def insert_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, buyLot=None, buyAmount=None,
                       buyCommission=None, stopPrice=None, stopType=None, stopHeight=None,
                       sellTarget=None, period=None, periodTime=None, currentPeriodTime=None,
                       signalName=None, explanation=None, profitTarget=None, strategy=None, targetLine=None,
                       targetPrice=None, slope=None, maxPrice=None, minPrice=None):
    trade = tradeClass()
    trade.symbol = symbol
    trade.period = period
    trade.period_time = periodTime
    trade.current_period_time = currentPeriodTime
    trade.explanation = explanation
    trade.buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    trade.buy_price = price
    trade.buy_lot = buyLot
    trade.buy_amount = buyAmount
    trade.buy_commission = buyCommission
    trade.stop_price = stopPrice
    trade.stop_type = stopType
    trade.stop_height = stopHeight
    trade.stop_change_count = 0
    trade.buy_signal_name = signalName
    trade.sell_target = sellTarget
    trade.status = STATUS_BUY
    trade.profit_target = profitTarget
    trade.strategy = strategy
    trade.btc_signal = int(BTC_SIGNAL)
    trade.btc_inner_signal = int(BTC_INNER_SIGNAL)
    trade.btc_red_alert = int(BTC_RED_ALERT)
    trade.btc_green_alert = int(BTC_GREEN_ALERT)
    trade.target_line = targetLine
    trade.target_price = targetPrice
    trade.slope = slope
    trade.max_price = maxPrice
    trade.min_price = minPrice

    trade.addTrade(dbCursor=dbCursor)


def update_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, sellLot=None, sellAmount=None,
                       sellCommission=None, signalName=None, oldStatus=None, newStatus=None):
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
        trade.sell_lot = sellLot
        trade.sell_amount = sellAmount
        trade.sell_commission = sellCommission

    trade.updateTrade(dbCursor=dbCursor, symbol=symbol, oldStatus=oldStatus, newStatus=newStatus)



def update_profit_target(dbCursor=None, symbol=None, profitTarget=None, maxPrice=None, minPrice=None):
    trade = tradeClass()
    trade.updateProfitTarget(dbCursor=dbCursor, symbol=symbol, profitTarget=profitTarget,
                             maxPrice=maxPrice, minPrice=minPrice, status=STATUS_BUY)


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

#endregion


#region BTC controls
def control_btc(symbol=None, coinTicker=None, tickSize=None):
    global BTC_GREEN_ALERT
    global BTC_RED_ALERT
    global BTC_SLOPE

    curColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    prev1Color = ind.get_candle_color(open=coinTicker.opens[-2], close=coinTicker.closes[-2])
    prev2Color = ind.get_candle_color(open=coinTicker.opens[-3], close=coinTicker.closes[-3])

    curClose = coinTicker.closes[-1]
    curLow = coinTicker.lows[-1]
    curHigh = coinTicker.highs[-1]

    fastHMA = ind.get_hma(data=coinTicker.closes, period=BTC_FAST_PERIOD)
    slowHMA = ind.get_hma(data=coinTicker.closes, period=BTC_SLOW_HMA)

    haOpen, haHigh, haLow, haClose = ind.get_heikin_ashi(open=coinTicker.opens, high=coinTicker.highs,
                                                         low=coinTicker.lows, close=coinTicker.closes)

    BTC_HEIKIN_COLOR = ind.get_candle_color(open=haOpen[-1], close=haClose[-1])

    # chLong, chShort = ind.get_chandelier(open=coinTicker.opens, high=coinTicker.highs, low=coinTicker.lows,
    #                                      close=coinTicker.closes, period=2, multiplier=1.8)

    slope = ind.get_slope_talib(data=coinTicker.closes, period=90)
    BTC_SLOPE = slope[-1]

    if fastHMA[-1] > fastHMA[-2]:
        BTC_SIGNAL = True
        BTC_INNER_SIGNAL = True
    else:
        BTC_SIGNAL = False
        BTC_INNER_SIGNAL = False

    """
    Mumun boyu bir yöne eşik değerini geçmiş ise ALARM (RED veya GREEN) aktif olur.
    Mumun renge terse dönünceye kadar ALARM aktif olarak kalır.
    """
    threshold = func.calculate_ratio(curHigh, curLow)
    if curColor == cons.CANDLE_GREEN:
        if threshold >= BTC_GREEN_THRESHOLD:
            BTC_GREEN_ALERT = True
            BTC_RED_ALERT = False
    if curColor == cons.CANDLE_RED:
        if threshold >= BTC_RED_THRESHOLD:
            BTC_GREEN_ALERT = False
            BTC_RED_ALERT = True

    # Mumun rengi terse dönünceye kadar ALARM aktif olarak kalır.
    if BTC_GREEN_ALERT is True:
        if curColor == cons.CANDLE_GREEN:
            BTC_GREEN_ALERT = True
        else:
            BTC_GREEN_ALERT = False

    if BTC_RED_ALERT is True:
        if curColor == cons.CANDLE_RED:
            BTC_RED_ALERT = True
        else:
            BTC_RED_ALERT = False
#endregion


#region Strategies
def control_strategy_46(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, float, int, float):
    slope = ind.get_slope_talib(data=coinTicker.closes, period=LR_PERIOD)
    up, center, down, sl = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                     standard_deviation_up_factor=STDDEV_UP,
                                                     standard_deviation_down_factor=STDDEV_DOWN)
    minPrice, maxPrice = ind.get_min_max(minData=coinTicker.lows, maxData=coinTicker.highs, period=90)

    prvCandleColor = ind.get_candle_color(open=coinTicker.opens[-2], close=coinTicker.closes[-2])
    curCandleColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])

    prvPrice = coinTicker.closes[-2]
    curPrice = coinTicker.closes[-1]

    # Eğim NEGATİF ise
    # if slope[-1] < 0:
    #     if (curCandleColor == cons.CANDLE_GREEN):
    #         if (prvPrice < down[-2]) and (curPrice > down[-1]):
    #             targetPrice = center[-1]
    #             stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    #             stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
    #             targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)
    #             targetLine = TARGET_TO_CENTER_LINE
    #
    #             log(f" {symbol} Eğim NEGATIF {slope[-1]} *S46*")
    #             log(f"          Price: {curPrice} Stop: {stopPrice} Target: {targetPrice}")
    #             log(f"          TargetLine: {targetLine} TARGET_LINE_CENTER")
    #
    #             return True, stopPrice, targetPrice, targetLine, slope[-1]
    #     return False, float(0.0), float(0.0), 0, float(0.0)

    # Eğim POZİTİF ise
    if slope[-1] >= 0:
        if curCandleColor == cons.CANDLE_GREEN:
            if (prvPrice < down[-2]) and (curPrice >= down[-1]):
                targetPrice = center[-1]
                stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
                stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
                targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)
                targetLine = TARGET_TO_CENTER_LINE

                log(f" {symbol} Eğim POZITIF {slope[-1]} DOWN to CENTER *S46*")
                log(f"          Price: {curPrice} Stop: {stopPrice} Target: {targetPrice}")
                log(f"          TargetLine: {targetLine} TARGET_LINE_CENTER")

                return True, stopPrice, targetPrice, targetLine, slope[-1]

            if (prvPrice < center[-2]) and (curPrice >= center[-1]):
                # CENTER TO UP BTC eğimi pozitif iken çalışacak, negatif iken kar vermiyor.
                if BTC_SLOPE >= float(0.0):
                    targetPrice = up[-1]
                    targetLine = TARGET_TO_UP_LINE
                    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
                    if stopPrice < down[-1]:
                        stopPrice = down[-1]

                    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
                    targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)

                    log(f" {symbol} Eğim POZITIF {slope[-1]} CENTER to UP *S46*")
                    log(f"          Price: {curPrice} Stop: {stopPrice} Target: {targetPrice}")
                    log(f"          TargetLine: {targetLine}")

                    return True, stopPrice, targetPrice, targetLine, slope[-1]

        return False, float(0.0), float(0.0), 0, float(0.0)

    return False, float(0.0), float(0.0), 0, float(0.0)


def control_strategy_47(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, float, int, float):
    """
    SUPERTREND, üstel hareketli ortalama (EMA) stratejisi
    Ayarlar;
      - Varsayılan (10, 3) ayarlı SUPERTREND
      - 3 ve 20 periyot ayarlı iki EMA
    Strateji;
      LONG işlem için
      Fiyat YEŞİL süpertrend çizgisinin üzerinde olacak
      EMA(3) EMA(20) yi yukarı kesecek
    """
    trend, direction, long, short = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                       close=coinTicker.closes, period=10, multiplier=3)

    if (direction[-1] == 1):
        if coinTicker.closes[-1] > long[-1]:
            emaFast = ind.get_ema(data=coinTicker.closes, period=3)
            emaSlow = ind.get_ema(data=coinTicker.closes, period=20)

            # isCross = func.cross_over(emaFast[:-1], emaSlow[:-1])
            isCross = func.cross_over(emaFast, emaSlow)

            # if (isCross == True) and (emaFast[-1] > emaSlow[-1]):
            if isCross == True:
                stopPrice = long[-1]
                stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
                targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                             strategy=47)
                targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)

                log(f" {symbol} SuperTrend, iki EMA cross stratejisi *S47*")
                log(f"          Price: {coinTicker.closes[-1]} Stop: {stopPrice} Target: {targetPrice}")
                log(f"          prevSlow > prevFast: {emaSlow[-2]} > {emaFast[-2]}")
                log(f"          curSlow < curFast: {emaSlow[-1]} < {emaFast[-1]}")
                return True, stopPrice, targetPrice, targetRatio, 0

    return False, float(0.0), float(0.0), float(0.0), 0


def control_strategy_48(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, float, int, float):
    fastMacd, slowMacd, histMacd = ind.get_macd(data=coinTicker.closes, fastPeriod=MACD_FAST,
                                                slowPeriod=MACD_SLOW, signalPeriod=MACD_SIGNAL)
    isCrossOverMACD = func.cross_over(fastMacd[:-1], slowMacd[:-1])

    if (isCrossOverMACD == True) and (fastMacd[-1] > slowMacd[-1]):
        trend, direction, long, short = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                           close=coinTicker.closes, period=10, multiplier=3)

        if (direction[-1] == 1) and (direction[-2] == 1):
            stopPrice = long[-1]
            stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
            # targetPrice = (coinTicker.closes[-1] * (100 + PROFIT_TARGET)) / 100
            targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                         strategy=48)
            targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)

            log(f" {symbol} SuperTrend, MACD cross stratejisi *S48*")
            log(f"          Price: {coinTicker.closes[-1]} Stop: {stopPrice} Target: {targetPrice}")
            log(f"          prevSlowMACD > prevFastMACD: {slowMacd[-2]} > {fastMacd[-2]}")
            log(f"          curSlowMACD < curFastMACD: {slowMacd[-1]} < {fastMacd[-1]}")
            return True, stopPrice, targetPrice, targetRatio, 0

    return False, float(0.0), float(0.0), float(0.0), 0


def control_strategy_49(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, float, int, float):
    slope = ind.get_slope_talib(data=coinTicker.closes, period=20)
    if (slope[-1] > slope[-5]):
        trend, direction, long, short = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                           close=coinTicker.closes, period=10, multiplier=3)
        if direction[-1] == 1:
            stopPrice = long[-1]
            stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
            targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                         strategy=49)
            targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)

            log(f" {symbol} SuperTrend, LR slope *S49*")
            log(f"          Price: {coinTicker.closes[-1]} Stop: {stopPrice} Target: {targetPrice}")
            log(f"          curSlope > prev5Slope {slope[-1]} > {slope[-5]}")
            return True, stopPrice, targetPrice, targetRatio, 0

    return False, float(0.0), float(0.0), float(0.0), 0


def control_strategy_50(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, float, int, float):
    lr = ind.get_linear_regression_talib(prices=coinTicker.closes, period=20)
    isLR = lr[-1] > lr[-20]
    if isLR == False:
        return False, float(0.0), float(0.0), float(0.0), 0

    emaFast = ind.get_ema(data=coinTicker.closes, period=4)
    for ix in range(0, len(emaFast)):
        emaFast[ix] = func.round_tick_size(price=emaFast[ix], tick_size=tickSize)

    emaSlow = ind.get_ema(data=coinTicker.closes, period=20)
    for ix in range(0, len(emaSlow)):
        emaSlow[ix] = func.round_tick_size(price=emaSlow[ix], tick_size=tickSize)

    isEmaCross = func.cross_over(emaFast, emaSlow)
    if isEmaCross == False:
        return False, float(0.0), float(0.0), float(0.0), 0

    adx, plus, minus = ind.get_adx_dmi(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                       period=14)
    isAdxUp = (adx[-1] < ADX_RANGE_LEVEL) and (adx[-1] > adx[-2])
    isPlusOverMinus = (plus[-1] > minus[-1]) and (plus[-1] > DMI_PLUS_TREND_LEVEL) and (plus[-1] > plus[-2])

    isAdx = (isAdxUp == True) and (isPlusOverMinus == True)
    if isAdx == False:
        return False, float(0.0), float(0.0), float(0.0), 0

    st1, stDir1, stLong1, stShort1 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=10, multiplier=1)
    st2, stDir2, stLong2, stShort2 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=11, multiplier=2)
    st3, stDir3, stLong3, stShort3 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=12, multiplier=3)
    dir = 0
    if stDir1[-1] == 1:
        dir = dir + 1
    if stDir2[-1] == 1:
        dir = dir + 1
    if stDir3[-1] == 1:
        dir = dir + 1

    if dir >= 2:
        if stDir2[-1] == 1:
            stopPrice = stLong2[-1]
        elif (stDir3[-1] == 1):
            stopPrice = stLong3[-1]
        else:
            stopPrice = stLong1[-1]

        stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
        targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                     strategy=50)
        targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)

        log(f" {symbol} SuperTrend 3 lü, ema3 X ema20, adx < 20, Plus > 20 stratejisi *S50*")
        log(f"          Price: {coinTicker.closes[-1]} Stop: {stopPrice} Target: {targetPrice}")
        log(f"          Green ST: {dir}")
        log(f"          dmi: {plus[-1]} > {minus[-1]} adx: {adx[-1]}")
        return True, stopPrice, targetPrice, targetRatio, 0

    return False, float(0.0), float(0.0), float(0.0), 0


def control_strategy_51(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, float, int, float):
    lr = ind.get_linear_regression_talib(prices=coinTicker.closes, period=20)
    isLR = lr[-1] > lr[-20]
    if isLR == False:
        return False, float(0.0), float(0.0), float(0.0), 0

    adx, plus, minus = ind.get_adx_dmi(high=coinTicker.highs, low=coinTicker.lows,
                                       close=coinTicker.closes, period=14)
    isADXLevelUp = (adx[-1] > ADX_RANGE_LEVEL) and (adx[-2] < ADX_RANGE_LEVEL)
    isPlusLevel = (plus[-1] > minus[-1]) and (plus[-1] > DMI_PLUS_TREND_LEVEL) and (plus[-1] > plus[-2])
    isADX = (isADXLevelUp == True) and (isPlusLevel == True)

    if isADX == False:
        return False, float(0.0), float(0.0), float(0.0), 0

    ema = ind.get_ema(data=coinTicker.closes, period=200)
    isEma = (coinTicker.closes[-1] > ema[-1]) and (ema[-1] > ema[-2])

    if isEma == False:
        return False, float(0.0), float(0.0), float(0.0), 0


    st1, stDir1, stLong1, stShort1 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=10, multiplier=1)
    st2, stDir2, stLong2, stShort2 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=11, multiplier=2)
    st3, stDir3, stLong3, stShort3 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=12, multiplier=3)
    dir = 0
    if stDir1[-1] == 1:
        dir = dir + 1
    if stDir2[-1] == 1:
        dir = dir + 1
    if stDir3[-1] == 1:
        dir = dir + 1

    if dir >= 2:
        if stDir2[-1] == 1:
            stopPrice = stLong2[-1]
        elif (stDir3[-1] == 1):
            stopPrice = stLong3[-1]
        else:
            stopPrice = stLong1[-1]

        stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
        targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                     strategy=51)
        targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)

        log(f" {symbol} ADX > {ADX_RANGE_LEVEL}, PLUS > {DMI_PLUS_TREND_LEVEL}, ema > 200 *S51*")
        log(f"          Price: {coinTicker.closes[-1]} Stop: {stopPrice} Target: {targetPrice}")
        log(f"          price > ema: {coinTicker.closes[-1]} > {ema[-1]}  Green ST: {dir}")
        log(f"          dmi: {plus[-1]} > {minus[-1]} adx: {adx[-1]}")
        return True, stopPrice, targetPrice, targetRatio, 0

    return False, float(0.0), float(0.0), float(0.0), 0


def control_strategy_52(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, float, int, float):
    volAvg = ind.sma(data=coinTicker.volumes, period=20)
    isVolUp = coinTicker.volumes[-1] > (volAvg[-2] * 7)
    if isVolUp == False:
        return False, float(0.0), float(0.0), float(0.0), 0

    adx, plus, minus = ind.get_adx_dmi(high=coinTicker.highs, low=coinTicker.lows,
                                       close=coinTicker.closes, period=14)
    isADXUp = adx[-1] > adx[-2]
    isPlusLevel = (plus[-1] > minus[-1]) and (plus[-1] > plus[-2])
    isADX = (isADXUp == True) and (isPlusLevel == True)

    if isADX == False:
        return False, float(0.0), float(0.0), float(0.0), 0

    st2, stDir2, stLong2, stShort2 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=11, multiplier=2)
    stopPrice = stLong2[-1]
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
    targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                 strategy=51)
    targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)

    log(f" {symbol} Volume: {coinTicker.volumes[-1]}, Average Volume: {volAvg[-2]} *S52*")
    log(f"          ADX: {adx[-1]}, PLUS: {plus[-1]}, MINUS: {minus[-1]}")
    log(f"          Price: {coinTicker.closes[-1]} Stop: {stopPrice} Target: {targetPrice}")
    return True, stopPrice, targetPrice, targetRatio, 0


def control_strategy_53(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, float, int, float):
    lastIndex = -7
    # st1, stDir1, stLong1, stShort1 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
    #                                                     close=coinTicker.closes, period=10, multiplier=1)
    # for ix in range(-1, lastIndex, -1):
    #     if stDir1[ix] != 1:
    #         return False, float(0.0), float(0.0), float(0.0), 0

    st2, stDir2, stLong2, stShort2 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=11, multiplier=2)
    for ix in range(-1, lastIndex, -1):
        if stDir2[ix] != 1:
            return False, float(0.0), float(0.0), float(0.0), 0
        if coinTicker.lows[ix] < stLong2[ix]:
            return False, float(0.0), float(0.0), float(0.0), 0

    st3, stDir3, stLong3, stShort3 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=12, multiplier=3)
    for ix in range(-1, lastIndex, -1):
        if stDir3[ix] != 1:
            return False, float(0.0), float(0.0), float(0.0), 0
        if coinTicker.lows[ix] < stLong3[ix]:
            return False, float(0.0), float(0.0), float(0.0), 0

    stopPrice = stLong2[-1]
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
    targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                 strategy=51)
    targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)

    log(f" {symbol} iki ADX 10 periyot pozitif *S53*")
    log(f"          Price: {coinTicker.closes[-1]} Stop: {stopPrice} Target: {targetPrice}")

    return True, stopPrice, targetPrice, targetRatio, 0


def control_strategy_54(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, float, int, float):
    hmaFast = ind.get_hma(data=coinTicker.closes, period=20)
    hmaSlow = ind.get_hma(data=coinTicker.closes, period=50)
    hmaTrend = ind.get_hma(data=coinTicker.closes, period=200)

    isHmaCrossOver = func.cross_over(source1=hmaFast, source2=hmaSlow)
    if isHmaCrossOver == False:
        return False, float(0.0), float(0.0), float(0.0), 0

    candleColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    isPriceOverTrend = (coinTicker.closes[-1] > hmaTrend[-1]) and (candleColor == cons.CANDLE_GREEN)
    if isPriceOverTrend == False:
        return False, float(0.0), float(0.0), float(0.0), 0

    shaOpen, shaHigh, shaLow, shaClose = ind.get_smoothed_heikin_ashi(open=coinTicker.opens,
                                                                      high=coinTicker.highs,
                                                                      low=coinTicker.lows,
                                                                      close=coinTicker.closes,
                                                                      period=8,
                                                                      averageType=cons.AVERAGE_HMA)
    shaColor = ind.get_candle_color(open=shaOpen[-1], close=shaClose[-1])
    if shaColor == cons.CANDLE_RED:
        return False, float(0.0), float(0.0), float(0.0), 0

    haOpen, haHigh, haLow, haClose = ind.get_heikin_ashi(open=coinTicker.opens,
                                                         high=coinTicker.highs,
                                                         low=coinTicker.lows,
                                                         close=coinTicker.closes)
    haColor = ind.get_candle_color(open=haOpen[-1], close=haClose[-1])
    if haColor == cons.CANDLE_RED:
        return False, float(0.0), float(0.0), float(0.0), 0

    st, stDir, stLong, stShort = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                    close=coinTicker.closes, period=11, multiplier=2)
    if stDir[-1] != cons.SUPERTREND_TYPE_LONG:
        return False, float(0.0), float(0.0), float(0.0), 0

    stopPrice = stLong[-1]
    # stopPrice = coinTicker.lows[-2]
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
    targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                 strategy=54)
    targetPrice = func.round_tick_size(price=targetPrice, tick_size=tickSize)

    log(f" {symbol} Smoothed HA *S54*")
    log(f"          Price: {coinTicker.closes[-1]} Stop: {stopPrice} TargetPrice: {targetPrice} TargetRatio: {targetRatio}")
    log(f"          hmaFast: {hmaFast[-1]} hmaSlow: {hmaSlow[-1]} hmaTrend: {hmaTrend[-1]}")

    return True, stopPrice, targetPrice, targetRatio, 0


def control_strategy_55(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=55)

    srsi = ind.get_srsi(data=coinTicker.closes, period=SRSI_PERIOD)

    isSRSI = srsi[-2] == 0.0 and srsi[-1] > 0.0
    if isSRSI == False:
        return response

    hmaFast = ind.get_hma(data=coinTicker.closes, period=50)
    hmaTrend = ind.get_hma(data=coinTicker.closes, period=200)

    isHmaFastUp = hmaFast[-1] > hmaFast[-2]
    isHmaTrendUp = hmaTrend[-1] > hmaTrend[-2]

    if (isHmaTrendUp == False) and (isHmaFastUp == False):
        return response

    targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=0.0,
                                                 strategy=55)
    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    log(f" {symbol} Smoothed RSI *S{response.strategy}*")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          curSRSI {srsi[-1]} prevSRSI: {srsi[-2]}")
    log(f"          HMA200Up: {isHmaTrendUp} HMA50Up: {isHmaFastUp}")
    log(f"          curHMA200 {hmaTrend[-1]} prevHMA200: {hmaTrend[-2]}")
    log(f"          curHMA50 {hmaFast[-1]} prevHMA50: {hmaFast[-2]}")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = targetPrice
    response.targetRatio = targetRatio
    return response


def control_strategy_56(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=56)

    # srsi = ind.get_srsi(data=coinTicker.closes, period=SRSI_PERIOD)
    #
    # isSRSI = srsi[-1] <= 0.2
    # if isSRSI == False:
    #     return response

    hmaTrend = ind.get_hma(data=coinTicker.closes, period=200)
    hma50 = ind.get_hma(data=coinTicker.closes, period=50)

    isHMA50Up = hma50[-1] > hma50[-2]
    if isHMA50Up == False:
        return response

    isHMA50Over200 = hma50[-1] > hmaTrend[-1]

    # st, stDir, stLong, stShort = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
    #                                                 close=coinTicker.closes, period=11, multiplier=2)
    # isSuperTrendChangeLong = (stDir[-2] == cons.SUPERTREND_TYPE_SHORT) and (stDir[-1] == cons.SUPERTREND_TYPE_LONG)
    # if isSuperTrendChangeLong == False:
    #     return response

    st1, stDir1, stLong1, stShort1 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=10, multiplier=1)
    st2, stDir2, stLong2, stShort2 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=11, multiplier=2)
    st3, stDir3, stLong3, stShort3 = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
                                                        close=coinTicker.closes, period=12, multiplier=3)
    prevDir = 0
    if stDir1[-2] == 1:
        prevDir = prevDir + 1
    if stDir2[-2] == 1:
        prevDir = prevDir + 1
    if stDir3[-2] == 1:
        prevDir = prevDir + 1

    curDir = 0
    if stDir1[-1] == 1:
        curDir = curDir + 1
    if stDir2[-1] == 1:
        curDir = curDir + 1
    if stDir3[-1] == 1:
        curDir = curDir + 1

    isSuperTrendChangeLong2 = prevDir < 2 and curDir >= 2
    isSuperTrendChangeLong3 = prevDir < 3 and curDir == 3
    if isHMA50Over200 == True:
        if (isSuperTrendChangeLong2 == False) and (isSuperTrendChangeLong3 == False):
            return response
    else:
        if isSuperTrendChangeLong3 == False:
            return response

    # if stDir2[-1] == 1:
    #     stopPrice = stLong2[-1]
    # elif (stDir3[-1] == 1):
    #     stopPrice = stLong3[-1]
    # else:
    #     stopPrice = stLong1[-1]

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = 0.0
    response.targetRatio = 0.0

    log(f" {symbol} Smoothed RSI *S{response.strategy}*")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          isHMA50Up: {isHMA50Up}")
    log(f"          prevDir: {prevDir} curDir: {curDir}")
    log(f"          STChange2: {isSuperTrendChangeLong2} STChange3: {isSuperTrendChangeLong3}")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")

    return response


def control_strategy_57(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    # nwe = ind.get_nadaraya_watson_envelope(data=coinTicker.closes, bandWidth=8, multiplier=3)

    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=57)

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=1)
    isBuySignal = buySignal[-1]
    if isBuySignal == False:
        return response

    sqzMom = ind.get_squeeze_momentum(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                      BBPeriod=20, BBMultFactor=2, KCPeriod=20, KCMultFactor=1.5,
                                      stdDev=0, useTrueRange=True)

    isSqzMom = (sqzMom[-1] < 0) and (sqzMom[-1] > sqzMom[-2])
    if isSqzMom == False:
        return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = 0.0
    response.targetRatio = 0.0

    log(f" {symbol} Squeeze Momentum *S{response.strategy}*")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          isSqzMom: {isSqzMom}")
    log(f"          prevSqz: {sqzMom[-2]} curSqz: {sqzMom[-1]}")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")

    return response


def control_strategy_58(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=58)

    nwe = ind.get_nadaraya_watson_envelope(data=coinTicker.closes, bandWidth=7, multiplier=3)
    nweLowerBand = nwe["Lower_Band"].to_numpy()
    isPriceUnderLower = coinTicker.lows[-1] < nweLowerBand[-1]
    if isPriceUnderLower == False:
        return response

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=1)
    isBuySignal = buySignal[-1]
    if isBuySignal == False:
        return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = 0.0
    response.targetRatio = 0.0

    log(f" {symbol} Squeeze Momentum *S{response.strategy}*")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          isPriceUnderLower: {isPriceUnderLower}")
    log(f"          nweLowerBand: {nweLowerBand[-1]}")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")

    return response


def control_strategy_59(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=59)

    nwe = ind.get_nadaraya_watson_envelope(data=coinTicker.closes, bandWidth=7, multiplier=3)
    nweLowerBand = nwe["Lower_Band"].to_numpy()
    nweUpperBand = nwe["Upper_Band"].to_numpy()
    nweMiddleBand = (nweUpperBand + nweLowerBand) / 2

    isPriceUnderMiddle = ((coinTicker.closes[-1]) < nweMiddleBand[-1]) and (nweLowerBand[-1] > nweLowerBand[-2])
    if isPriceUnderMiddle == False:
        return response

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=1)
    isBuySignal = buySignal[-1]
    if isBuySignal == False:
        return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = 0.0
    response.targetRatio = 0.0

    log(f" {symbol} Squeeze Momentum *S{response.strategy}*")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          isPriceUnderLower: {isPriceUnderMiddle}")
    log(f"          nweLowerBand: {nweMiddleBand[-1]}")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")

    return response
#endregion

def control_buy_signal(dbCursor=None, session=None, url=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    # signal, stop, targetPrice, targetLine, slope = control_strategy_46(session=session, symbol=symbol,
    #                                                                    coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     # alarm()
    #     return True, stop, targetPrice, targetLine, slope, 46

    # signal, stop, targetPrice, targetRatio, slope = control_strategy_49(session=session, symbol=symbol,
    #                                                                     coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stop, targetPrice, targetRatio, slope, 49
    #
    # signal, stop, targetPrice, targetRatio, slope = control_strategy_47(session=session, symbol=symbol,
    #                                                                     coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stop, targetPrice, targetRatio, slope, 47
    #
    # signal, stop, targetPrice, targetRatio, slope = control_strategy_48(session=session, symbol=symbol,
    #                                                                     coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stop, targetPrice, targetRatio, slope, 48

    # signal, stop, targetPrice, targetRatio, slope = control_strategy_50(session=session, symbol=symbol,
    #                                                                     coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stop, targetPrice, targetRatio, slope, 50
    #
    # signal, stop, targetPrice, targetRatio, slope = control_strategy_51(session=session, symbol=symbol,
    #                                                                     coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stop, targetPrice, targetRatio, slope, 51
    #
    # signal, stop, targetPrice, targetRatio, slope = control_strategy_52(session=session, symbol=symbol,
    #                                                                     coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stop, targetPrice, targetRatio, slope, 52
    #
    # signal, stop, targetPrice, targetRatio, slope = control_strategy_53(session=session, symbol=symbol,
    #                                                                     coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stop, targetPrice, targetRatio, slope, 53

    # signal, stop, targetPrice, targetRatio, slope = control_strategy_54(session=session, symbol=symbol,
    #                                                                     coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stop, targetPrice, targetRatio, slope, 54

    # response = control_strategy_55(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response
    #
    # response = control_strategy_56(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response
    response = control_strategy_58(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if response.signal == True:
        return response

    response = control_strategy_59(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if response.signal == True:
        return response

    response = control_strategy_57(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if response.signal == True:
        return response


    return response

#region Threads
def buy(connSession=None):
    log(f"BUY Thread Start")
    # global BTC_SLOPE

    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_candle = binanceConfig["url_historical"] + binanceConfig["url_candle_historical"]
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]
    url_order_book = binanceConfig["url_base"] + binanceConfig["url_order_book"]

    symbol = symbolClass()

    while True:
        start_time = time.time()
        symbolRows = symbol.readAll(dbCursor=dbCursor, exchangeId=1, quoteAsset='USDT')
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
            if isPosition == True:
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

            coinTicker = TickerData(opens=opens, highs=highs, lows=lows, closes=closes, volumes=volumes, dates=dates)

            global glbExplanation
            glbExplanation = ""

            buyResponse = control_buy_signal(dbCursor=dbCursor, session=connSession, symbol=coinSymbol,
                                             url=url_candle, coinTicker=coinTicker, tickSize=tickSize)
            if buyResponse.signal == False:
                continue

            if buyResponse.targetRatio > 0:
                if buyResponse.targetRatio < (COMMISSION_RATE * 2 * 100) * 2:
                    continue

            tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                               url=url_book_ticker,
                                                                               symbol=coinSymbol)
            if tickerStatus == False:
                continue

            """ Tahtada işlem yapmak için yeterli sayıda adet yoksa """
            if (askPrice * askQty) < (MAX_TRADE_LIMIT * LIMIT_FACTOR):
                continue

            periodTimestamps = dates[-1]
            periodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            buyLot = MAX_TRADE_LIMIT / askPrice
            buyLot = func.round_step_size(quantity=buyLot, step_size=stepSize)
            buyAmount = buyLot * askPrice
            buyCommission = buyAmount * COMMISSION_RATE

            targetProfit = float(0.0)
            riskRatio = float(0.0)
            if buyResponse.stopPrice > float(0.0):
                if askPrice == buyResponse.stopPrice:
                    continue
                riskRatio = func.calculate_ratio(askPrice, buyResponse.stopPrice)
                # MAX_ACCEPTABLE_RISK dan (örneğin %2) fazla risk alınmaz
                if riskRatio > MAX_ACCEPTABLE_RISK:
                    continue
                log(f"   stopPrice :{buyResponse.stopPrice} askPrice: {askPrice} Ratio: %{riskRatio}")
                # Alınan risk karşılığında beklenen kazanç oranı (örneğin 2 katı)
                # targetProfit = riskRatio * RISK_REWARD_RATIO
                # targetProfit = func.calculate_ratio(targetPrice, askPrice)

            # if targetProfit < (COMMISSION_RATE * 2 * 100) * 2:
            #     continue

            insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                               price=askPrice, buyLot=buyLot, buyAmount=buyAmount, buyCommission=buyCommission,
                               stopPrice=buyResponse.stopPrice, stopType=0,
                               stopHeight=0, sellTarget=None, period=CANDLE_INTERVAL,
                               periodTime=periodTime, currentPeriodTime=periodTime, signalName=None,
                               explanation=glbExplanation, profitTarget=buyResponse.targetRatio,
                               strategy=buyResponse.strategy, targetLine=0, targetPrice=buyResponse.targetPrice,
                               slope=0, maxPrice=askPrice, minPrice=askPrice)
            log(f" {SIDE_BUY} {coinSymbol} Price:{askPrice} Stop:{buyResponse.stopPrice} Target:%{buyResponse.targetRatio} {glbExplanation} Strategy:{buyResponse.strategy}")
            log(f" ********** ")
            alarm()

            telegramMessage = f"{coinSymbol} {SIDE_BUY} S{buyResponse.strategy} {CANDLE_INTERVAL}\n Price:{askPrice}"
            tlg.sendNotification(connSession=connSession, notification=telegramMessage)

            glbExplanation = ""

        # end for symbolRow in symbolRows:

        # log(f"  ")
        # log(f"---- Toplam süre: {time.time() - start_time} saniye {(time.time() - start_time)/60} dakika -----")

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
        if (positionRecords is None) or (len(positionRecords) == 0):
            continue

        for item in positionRecords:
            position = json.loads(item[0])
            coinSymbol = position['symbol']
            currentPeriodTime = position['current_period_time']
            buyPeriodTime = position['period_time']
            period = position['period']
            buyPrice = position['buy_price']
            buyAmount = position['buy_amount']
            buyCommission = position['buy_commission']
            buyLot = position['buy_lot']
            stopType = position['stop_type']
            stopPrice = position['stop_price']
            currentStatus = position['status']
            expl = position['explanation']
            profitTarget = position['profit_target']
            strategy = position['strategy']
            stopChangeCount = position['stop_change_count']
            targetLine = position['target_line']
            targetPrice = position['target_price']
            slope = position['slope']
            maxPrice = position['max_price']
            minPrice = position['min_price']

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

            if strategy == 46:
                candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle,
                                             symbol=coinSymbol, interval=CANDLE_INTERVAL, limit=CANDLE_COUNT)
                if (candleBars is None) or (len(candleBars) < CANDLE_COUNT):
                    continue
            else:
                candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle,
                                             symbol=coinSymbol, interval=CANDLE_INTERVAL, limit=15)
                if (candleBars is None) or (len(candleBars) < 2):
                    continue

            df = convert_dataframe(bars=candleBars)
            date_kline = df['date']
            open_prices = df['open']
            high_prices = df['high']
            low_prices = df['low']
            close_prices = df['close']
            volume_values = df['volume']

            dates = date_kline.to_numpy(dtype=datetime)
            opens = open_prices.to_numpy(dtype=float)
            highs = high_prices.to_numpy(dtype=float)
            lows = low_prices.to_numpy(dtype=float)
            closes = close_prices.to_numpy(dtype=float)
            volumes = volume_values.to_numpy(dtype=float)

            periodTimestamps = dates[-1]
            candlePeriodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            coinTicker = TickerData(opens=opens, highs=highs, lows=lows, closes=closes, volumes=volumes, dates=dates)

            """ STOP kontrolleri en son işlem görmüş (close) fiyat ile yapılır """

            isStop = False
            isProfit = False
            # isProfit = profit_control(currentPrice=closes[-1], buyingPrice=buyPrice,
            #                           targetProfit=profitTarget, targetPrice=targetPrice, strategy=strategy)
            isStop = stop_control(currentPrice=closes[-1], stopPrice=stopPrice)
            # if (isStop == True) or (isProfit == True):
            if (isStop == True):
                """ Coin in anlık tahta fiyatı okunur. """
                tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                                   url=url_book_ticker,
                                                                                   symbol=coinSymbol)
                if tickerStatus == False:
                    continue

                # TODO: Burada SATIM işlemi yapılacak
                sellAmount = buyLot * bidPrice
                # sellAmount = func.round_tick_size(price=sellAmount, tick_size=tickSize)
                sellCommission = sellAmount * COMMISSION_RATE
                sellLot = sellAmount / bidPrice
                # Satış yapılmış gibi satış kaydı update edilir.
                update_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_SELL, price=bidPrice,
                                   sellLot=sellLot, sellAmount=sellAmount, sellCommission=sellCommission,
                                   oldStatus=currentStatus, newStatus=STATUS_STOP)

                sellTotal = sellAmount - sellCommission
                buyTotal = buyAmount + buyCommission
                if sellTotal > buyTotal:
                    kar = sellTotal - buyTotal
                    yuzde = (kar * 100) / buyTotal
                    log(f" {SIDE_SELL} {coinSymbol} Buy: {buyTotal} Sell: {sellTotal} price:{bidPrice} strategy:{strategy} KAR: {kar} (%{round(yuzde, 2)}) >>>")

                    telegramMessage = f"{coinSymbol} {SIDE_SELL} S{strategy} {period}\nBuy:{buyPrice} Sell:{bidPrice}\nKAR:{round(kar,3)} (%{round(yuzde, 2)})"
                    tlg.sendNotification(connSession=connSession, notification=telegramMessage)

                else:
                    zarar = buyTotal - sellTotal
                    yuzde = (zarar * 100) / sellTotal
                    log(f" {SIDE_SELL} {coinSymbol} Buy: {buyTotal} Sell: {sellTotal} price:{bidPrice} strategy:{strategy} ZARAR: {zarar} (%{round(yuzde, 2)}) <<<")

                    telegramMessage = f"{coinSymbol} {SIDE_SELL} S{strategy} {period}\nBuy:{buyPrice} Sell:{bidPrice}\nZARAR:{round(zarar,3)} (%{round(yuzde, 2)})"
                    tlg.sendNotification(connSession=connSession, notification=telegramMessage)
                alarm()
                log(f" ************* ")
                continue
            # end if (isStop is True) or (isProfit is True):

            if minPrice is None:
                minPrice = buyPrice

            if coinTicker.closes[-1] < minPrice:
                minPrice = coinTicker.closes[-1]
                update_profit_target(dbCursor=dbCursor, symbol=coinSymbol, profitTarget=profitTarget, maxPrice=maxPrice, minPrice=minPrice)
                log(f"  * UPDATE MIN PRICE * {coinSymbol} Buy:{buyPrice} Min Price:{minPrice}")

            if maxPrice is None:
                maxPrice = buyPrice

            if coinTicker.closes[-1] > maxPrice :
                maxPrice = coinTicker.closes[-1]
                maxPriceRatio = round((maxPrice - buyPrice) / buyPrice * 100, 3)
                update_profit_target(dbCursor=dbCursor, symbol=coinSymbol, profitTarget=profitTarget, maxPrice=maxPrice, minPrice=minPrice)
                log(f"  * UPDATE MAX PRICE * {coinSymbol} Buy:{buyPrice} Max Price:{maxPrice} Max Profit %:{maxPriceRatio}")

                changeSignal, newStopPrice = get_stop_price_by_max_price(maxPrice=maxPrice,
                                                                         buyPrice=buyPrice,
                                                                         stopPrice=stopPrice,
                                                                         symbol=coinSymbol,
                                                                         tickSize=tickSize)
                if changeSignal == True:
                    stopChangeCount += 1
                    log(f"  * UPDATE STOP PRICE * {coinSymbol} Old: {stopPrice} New: {newStopPrice} Ratio: {round(func.calculate_ratio(newStopPrice, buyPrice),3)} Buyprice: {buyPrice}")

                    update_stop_price(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                      stopChangeCount=stopChangeCount)

            # trend, direction, long, short = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
            #                                                    close=coinTicker.closes, period=11, multiplier=2)
            # curLong = func.round_tick_size(long[-1], tick_size=tickSize)
            # newStopPrice = stopPrice
            # if curLong > stopPrice:
            #     newStopPrice = curLong
            #     stopChangeCount += 1
            #     log(f"  *** UPDATE STOP PRICE ST *** {coinSymbol} Buy:{buyPrice} Old:{stopPrice} New:{newStopPrice} %:{round((newStopPrice - buyPrice) / buyPrice * 100, 3)}")
            #
            #     update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol,
            #                                          stopPrice=newStopPrice,
            #                                          currentPeriodTime=candlePeriodTime,
            #                                          stopChangeCount=stopChangeCount)

            # if str(currentPeriodTime) != str(candlePeriodTime):
            #     candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle,
            #                                  symbol=coinSymbol, interval=CANDLE_INTERVAL, limit=CANDLE_COUNT)
            #     if (candleBars is None) or (len(candleBars) < CANDLE_COUNT):
            #         continue
            #
            #     df = convert_dataframe(bars=candleBars)
            #     date_kline = df['date']
            #     open_prices = df['open']
            #     high_prices = df['high']
            #     low_prices = df['low']
            #     close_prices = df['close']
            #     volume_values = df['volume']
            #
            #     dates = date_kline.to_numpy(dtype=datetime)
            #     opens = open_prices.to_numpy(dtype=float)
            #     highs = high_prices.to_numpy(dtype=float)
            #     lows = low_prices.to_numpy(dtype=float)
            #     closes = close_prices.to_numpy(dtype=float)
            #     volumes = volume_values.to_numpy(dtype=float)
            #
            #     coinTicker = TickerData(opens=opens, highs=highs, lows=lows, closes=closes, volumes=volumes, dates=dates)
            #     trend, direction, long, short = ind.get_supertrend(high=coinTicker.highs, low=coinTicker.lows,
            #                                                        close=coinTicker.closes, period=11, multiplier=2)
            #     curLong = func.round_tick_size(long[-1], tick_size=tickSize)
            #     newStopPrice = stopPrice
            #     if curLong > stopPrice:
            #         newStopPrice = curLong
            #         stopChangeCount += 1
            #         log(f"  *** UPDATE STOP PRICE *** {coinSymbol} Buy:{buyPrice} Old:{stopPrice} New:{newStopPrice} %:{round((newStopPrice - buyPrice) / buyPrice * 100, 3)}")
            #
            #     update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol,
            #                                          stopPrice=newStopPrice,
            #                                          currentPeriodTime=candlePeriodTime,
            #                                          stopChangeCount=stopChangeCount)

                # if (profitTarget == 0.0) or (coinTicker.closes[-1] > profitTarget):
                #     profitTarget = coinTicker.closes[-1]
                #     update_profit_target(dbCursor=dbCursor, symbol=coinSymbol, profitTarget=profitTarget)
                #     log(f"  *** UPDATE MAX PROFIT *** {coinSymbol} Buy:{buyPrice} Max Profit:{profitTarget} %:{round((profitTarget - buyPrice) / buyPrice * 100, 3)}")
            # end if str(currentPeriodTime) != str(candlePeriodTime)

        # end for item in positionRecords:
    # end while True:


def btc(connSession=None):
    log(f"BTC Thread Start")

    # global BTC_SIGNAL
    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_candle = binanceConfig["url_historical"] + binanceConfig["url_candle_historical"]
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    coinSymbol = "BTCUSDT"
    tickSize = 4
    symbol = symbolClass()
    symbolRow = symbol.readOne(dbCursor=dbCursor, exchangeId=1, symbol=coinSymbol)
    if symbolRow is not None:
        sym = json.loads(symbolRow[0])
        coinSymbol = sym['symbol']
        tickSize = float(sym['tick_size'])
        stepSize = float(sym['step_size'])
        minNotional = float(sym['min_notional'])
        minLot = float(sym['min_lot'])

    while True:
        """ Coin mum verileri okunur """
        candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle, symbol=coinSymbol,
                                     interval=BTC_CANDLE_INTERVAL,
                                     limit=BTC_CANDLE_COUNT)
        if (candleBars is None) or (len(candleBars) < BTC_CANDLE_COUNT):
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

        coinTicker = TickerData(opens=opens, highs=highs, lows=lows, closes=closes, volumes=volumes, dates=dates)

        control_btc(symbol=coinSymbol, coinTicker=coinTicker, tickSize=tickSize)

        time.sleep(3)
    # end while True:


def notify(connSession=None):
    log(f"NOTIFY Thread Start")

    db = func.connectDB()
    dbCursor = db.cursor()

    def job():
        message = f"Periyot: {CANDLE_INTERVAL}\n"
        topKar = float(0.0)
        topZarar = float(0.0)
        topFark = float(.0)
        rows = readSummary(dbCursor=dbCursor)
        if rows is not None:
            for item in rows:
                record = json.loads(item[0])
                strategy = record["strategy"]
                kar = round(record["kar"], 2)
                zarar = round(record["zarar"], 2)
                fark = round(record["fark"], 2)
                topKar += kar
                topZarar += zarar
                topFark += fark
                message = message + f" (S{strategy}): {kar} - {zarar} = *{fark}*\n"
        message = message + f" Top: {round(topKar, 2)} - {round(topZarar, 2)} = *{round(topFark, 2)}*\n"
        r = tlg.sendNotification(connSession=connSession, notification=message)

    schedule.every().hour.at(":00").do(job)
    # schedule.every(1).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
#endregion

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
    thread_btc = Thread(name='btc', target=btc, args=(connSession, ), daemon=True)
    thread_buy = Thread(name='buy', target=buy, args=(connSession,))

    thread_notify.start()
    thread_sell.start()
    thread_btc.start()
    thread_buy.start()

    thread_notify.join()
    thread_sell.join()
    thread_btc.join()
    thread_buy.join()


main()