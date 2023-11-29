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
from botclass import BinanceOrderBook as orderBookClass

import notify as tlg
import logging
import winsound

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP = 2

BTC_CANDLE_INTERVAL = "5m"
BTC_CANDLE_COUNT = 168
BTC_FAST_PERIOD = 25
BTC_SLOW_HMA = 96
BTC_SIGNAL = False
BTC_INNER_SIGNAL = False
BTC_RED_ALERT = False
BTC_GREEN_ALERT = False
BTC_SLOPE = False
BTC_HEIKIN_COLOR = None
BTC_STD_DEV_UP = float(2.0)
BTC_STD_DEV_DOWN = float(2.0)
BTC_GREEN_THRESHOLD = float(1.5)  # yüzde
BTC_RED_THRESHOLD = float(1.5)   # yüzde

CANDLE_INTERVAL = "5m"      # Okunacak mum verisi periyodu
CANDLE_COUNT = 168  # 33            # Okunacak mum verisi adeti
MAX_TRADE_LIMIT = float(100)
LIMIT_FACTOR = float(5)
RSI_FAST = 24
RSI_SLOW = 96
BB_PERIOD = 25
BB_STD_DEV = 2.5
LR_PERIOD = 18
LR_LOWER_BAND = float(2.0)
LR_UPPER_BAND = float(2.0)
ATR_PERIOD = 6
ATR_MULTIPLIER = float(2.0)
ATR_STOP_FACTOR = float(0.5)
COMMISSION_RATE = float(0.00075)  # %0.075
ZSCORE_PERIOD = 18
ZSCORE_DOWN_RANGE = float(-2.0)
ZSCORE_LIMIT_DOWN = float(-2.3)
ZSCORE_LIMIT_UP = float(2.3)
EMA_PERIOD = CANDLE_COUNT - 1
HMA_FAST_PERIOD = 10  # 96
HMA_SLOW_PERIOD = 96
MACD_FAST = 10
MACD_SLOW = 26
MACD_SIGNAL = 9

RISK_REWARD_RATIO = float(1.2)
MAX_ACCEPTABLE_RISK = float(4.0)
FIRST_PROFIT_TARGET = float(1.0)  # 2.0
FIRST_PROFIT_STOP_TRAIL = float(0.5)  # %0.5
PROFIT_TARGET = float(1.0)  # 2.0
PROFIT_STOP_TRAIL = float(0.7)  # 0.8
PROFIT_STRATEGY_26 = float(0.5)
PROFIT_STRATEGY_27 = float(0.7)
PROFIT_STRATEGY_34 = float(2.0)
PROFIT_STRATEGY_36 = float(1.5)
PROFIT_STRATEGY_38 = float(1.5)
PROFIT_STRATEGY_39 = float(2.0)
PROFIT_STRATEGY_40 = float(2.0)

IS_LOG = True
IS_PRINT = True
IS_ALARM = False

TEST_NAME = "ZSCORE"
glbExplanation = ""
logging.basicConfig(filename="debug_test_hma2", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )

#region Yardımcı Fonksiyonlar
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
#endregion

#region Diğer Fonksiyonlar
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
        log(f"   Stop Price: {stopPrice} Stop Height: {stopHeight} Under Low: %{round((stopHeight/stopPrice)*100, 3)}")
    return stopPrice, stopHeight


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


def insert_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, buyLot=None, buyAmount=None,
                       buyCommission=None, stopPrice=None, stopType=None, stopHeight=None,
                       sellTarget=None, period=None, periodTime=None, currentPeriodTime=None,
                       signalName=None, explanation=None, profitTarget=None, strategy=None):
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


def update_profit_target(dbCursor=None, symbol=None, profitTarget=None):
    trade = tradeClass()
    trade.updateProfitTarget(dbCursor=dbCursor, symbol=symbol, profitTarget=profitTarget, status=STATUS_BUY)


def stop_control(currentPrice=None, stopPrice=None) -> (bool):
    if currentPrice < stopPrice:
        log(f"   STOP StopPrice: {stopPrice} CurrentPrice: {currentPrice}")
        return True
    return False


def profit_control(coinTicker=None, currentPrice=None, buyingPrice=None, targetProfit= None, strategy=None) -> (bool):
    if currentPrice > buyingPrice:
        profit = func.calculate_ratio(currentPrice, buyingPrice)

        if BTC_RED_ALERT == True:
            log(f"   BTC_RED_ALERT True: {BTC_RED_ALERT}")
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


def control_btc(symbol=None, coinTicker=None, tickSize=None):
    global BTC_SIGNAL
    global BTC_INNER_SIGNAL
    global BTC_GREEN_ALERT
    global BTC_RED_ALERT
    global BTC_SLOPE
    global BTC_HEIKIN_COLOR

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

    BTC_SLOPE = func.slope(slowHMA)
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
def control_strategy_42(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    hma = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    isHmaDeepTurn = func.deep_turn(hma)
    isPreviousCandleCloseOverHma = coinTicker.lows[-2] > hma[-2]
    isCurrentCandleLowOverHma = coinTicker.lows[-1] > hma[-1]

    if (isHmaDeepTurn == True) and (isPreviousCandleCloseOverHma == True) and (isCurrentCandleLowOverHma == True):
        stopPrice = hma[-1]
        return True, stopPrice

    return False, float(0.0)

def control_strategy_43(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    - Heikin mum kırmızıdan yeşile döner
    - RSI hızlı yavaşın üstünde
    - HMA eğim yukarı
    """
    # if BTC_SLOPE == False:
    #     return False, float(0.0)
    # if BTC_HEIKIN_COLOR == cons.CANDLE_RED:
    #     return False, float(0.0)

    # chLong, chShort = ind.get_chandelier(open=coinTicker.opens, high=coinTicker.highs, low=coinTicker.lows,
    #                                      close=coinTicker.closes, period=2, multiplier=1.8)

    haOpen, haHigh, haLow, haClose = ind.get_heikin_ashi(open=coinTicker.opens, high=coinTicker.highs,
                                                         low=coinTicker.lows, close=coinTicker.closes)

    volAvg = ind.get_volume_average(volumes=coinTicker.volumes, period=24)

    hma = ind.get_hma(data=haClose, period=HMA_SLOW_PERIOD)
    for ix in range(0, len(hma)):
        hma[ix] = func.round_tick_size(price=hma[ix], tick_size=tickSize)

    rsiFast = ind.get_rsi(prices=haClose, timePeriod=RSI_FAST)
    rsiSlow = ind.get_rsi(prices=haClose, timePeriod=RSI_SLOW)

    isRSIAbove = func.above(rsiFast, rsiSlow, 2)
    curHeikinCandleColor = ind.get_candle_color(open=haOpen[-1], close=haClose[-1])
    prev1HeikinCandleColor = ind.get_candle_color(open=haOpen[-2], close=haClose[-2])
    prev2HeikinCandleColor = ind.get_candle_color(open=haOpen[-3], close=haClose[-3])
    isHeikinCandleRedToGreen = (prev2HeikinCandleColor == cons.CANDLE_RED) and \
                               (prev1HeikinCandleColor == cons.CANDLE_GREEN) and \
                               (curHeikinCandleColor == cons.CANDLE_GREEN)

    curCandleColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])

    isTrade = False
    if (isHeikinCandleRedToGreen == True):
        if (isRSIAbove == True) and (func.slope(hma) == True):
            isTrade = True

    if isTrade == True:
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=ATR_PERIOD)
        stopPrice = coinTicker.closes[-2] - (atr[-2] * ATR_MULTIPLIER)
        stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

        log(f" {symbol} isHeikinCandleRedToGreen : {isHeikinCandleRedToGreen} *S43*")
        log(f"          isRSIAbove : {isRSIAbove}  rsiFast: {rsiFast[-1]} rsiSlow: {rsiSlow[-1]}")
        log(f"          Slope curHMA > prevHma {hma[-1]} > {hma[-2]} ")
        log(f"          Price: {coinTicker.closes[-1]}  Stop Price: {stopPrice}")

        return True, stopPrice

    return False, float(0.0)

def control_strategy_44(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    - RSI hızlı yavaşı yukarı keser
    - Heikin mumun rengi yeşil
    - HMA eğim yukarı
    """
    # if BTC_SLOPE == False:
    #     return False, float(0.0)
    # if BTC_HEIKIN_COLOR == cons.CANDLE_RED:
    #     return False, float(0.0)

    haOpen, haHigh, haLow, haClose = ind.get_heikin_ashi(open=coinTicker.opens, high=coinTicker.highs,
                                                         low=coinTicker.lows, close=coinTicker.closes)

    hma = ind.get_hma(data=haClose, period=HMA_SLOW_PERIOD)
    for ix in range(0, len(hma)):
        hma[ix] = func.round_tick_size(price=hma[ix], tick_size=tickSize)

    rsiFast = ind.get_rsi(prices=haClose, timePeriod=RSI_FAST)
    rsiSlow = ind.get_rsi(prices=haClose, timePeriod=RSI_SLOW)

    volAvg = ind.get_volume_average(volumes=coinTicker.volumes, period=24)

    isRSICrossOver = func.cross_over(rsiFast, rsiSlow)
    curHeikinCandleColor = ind.get_candle_color(open=haOpen[-1], close=haClose[-1])
    prevHeikinCandleColor = ind.get_candle_color(open=haOpen[-2], close=haClose[-2])
    isCurHeikinCandleGreen = curHeikinCandleColor == cons.CANDLE_GREEN

    curCandleColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    isCurCandleGreen = curCandleColor == cons.CANDLE_GREEN
    isVolAvg = (func.slope(hma) == True) or (func.slope(hma) == False) and (coinTicker.volumes[-1] > volAvg[-1]) and (isCurCandleGreen == True)

    isTrade = False
    if isRSICrossOver == True:
        if (isCurHeikinCandleGreen == True) and (func.slope(hma) == True):
            isTrade = True

    if isTrade == True:
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=ATR_PERIOD)
        stopPrice = coinTicker.closes[-2] - (atr[-2] * ATR_MULTIPLIER)
        stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

        log(f" {symbol} isRSICrossOver : {isRSICrossOver} Fast > Slow {rsiFast[-1]} > {rsiSlow[-1]}) *S44*")
        log(f"          Slope curHMA > prevHma {hma[-1]} > {hma[-2]} ")
        log(f"          CurHeikinCandleGREEN : {isCurHeikinCandleGreen} ")
        log(f"          Price: {coinTicker.closes[-1]}  Stop Price: {stopPrice}")

        return True, stopPrice

    return False, float(0.0)

def control_strategy_45(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    - HMA dipten yukarı döner
    - RSI hızlı yavaşın üzerinde
    - Heikin mumun rengi yeşil
    """
    # if BTC_SLOPE == False:
    #     return False, float(0.0)
    # if BTC_HEIKIN_COLOR == cons.CANDLE_RED:
    #     return False, float(0.0)

    haOpen, haHigh, haLow, haClose = ind.get_heikin_ashi(open=coinTicker.opens, high=coinTicker.highs,
                                                         low=coinTicker.lows, close=coinTicker.closes)

    hma = ind.get_hma(data=haClose, period=HMA_SLOW_PERIOD)
    for ix in range(0, len(hma)):
        hma[ix] = func.round_tick_size(price=hma[ix], tick_size=tickSize)

    rsiFast = ind.get_rsi(prices=haClose, timePeriod=RSI_FAST)
    rsiSlow = ind.get_rsi(prices=haClose, timePeriod=RSI_SLOW)

    volAvg = ind.get_volume_average(volumes=coinTicker.volumes, period=24)

    isHMADeepTurn = func.deep_turn(hma)
    isRSIAbove = func.above(rsiFast, rsiSlow, 2)
    curHeikinCandleColor = ind.get_candle_color(open=haOpen[-1], close=haClose[-1])
    isCurHeikinCandleGreen = curHeikinCandleColor == cons.CANDLE_GREEN

    # curCandleColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    # isCurCandleGreen = curCandleColor == cons.CANDLE_GREEN
    # isVolAvg = (func.slope(hma) == True) or (func.slope(hma) == False) and (
    #             coinTicker.volumes[-1] > volAvg[-1]) and (isCurCandleGreen == True)

    isTrade = False
    if isHMADeepTurn == True:
        if (isCurHeikinCandleGreen == True) and (isRSIAbove == True):
            isTrade = True

    if isTrade == True:
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=ATR_PERIOD)
        stopPrice = coinTicker.closes[-2] - (atr[-2] * ATR_MULTIPLIER)
        stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

        log(f" {symbol} isHMADeepTurn : {isHMADeepTurn} {hma[-3]} > {hma[-2]} < {hma[-1]}) *S45*")
        log(f"          isRSIAbove : {isRSIAbove}  rsiFast: {rsiFast[-1]} rsiSlow: {rsiSlow[-1]}")
        log(f"          CurHeikinCandleGREEN : {isCurHeikinCandleGreen} ")
        log(f"          Price: {coinTicker.closes[-1]}  Stop Price: {stopPrice}")

        return True, stopPrice

    return False, float(0.0)

def control_strategy_46(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    slope = ind.get_slope_talib(data=coinTicker.closes, period=90)
    up, center, down, sl = ind.get_linear_regression(prices=coinTicker.closes, period=90,
                                                     standard_deviation_up_factor=2,
                                                     standard_deviation_down_factor=2)


    return False, float(0.0)

#endregion

def control_buy_signal(dbCursor=None, session=None, url=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, int):
    signal, stopLine = control_strategy_43(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, stopLine, 43

    signal, stopLine = control_strategy_44(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, stopLine, 44

    signal, stopLine = control_strategy_45(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, stopLine, 45

    signal, stopLine = control_strategy_46(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, stopLine, 46

    return False, 0.0, 0

#region Threads
def buy(connSession=None):
    log(f"BUY Thread Start")
    global BTC_SLOPE

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
            # if BTC_SLOPE == False:
            #     continue
            # if BTC_HEIKIN_COLOR == cons.CANDLE_RED:
            #     return False, float(0.0)

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

            buySignal, stopLine, strategy = control_buy_signal(dbCursor=dbCursor, session=connSession,
                                                               symbol=coinSymbol, url=url_candle,
                                                               coinTicker=coinTicker, tickSize=tickSize)
            if buySignal == False:
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

            """ Stop fiyatı belirlenir """
            # stopType = cons.STOP_TYPE_PREVIOUS_LOW
            # stopPrice, stopHeight = get_stop_price(stopType=stopType,
            #                                        coinTicker=coinTicker, tickSize=tickSize)

            # stopPrice < %PROFIT_STOP_TRAIL den küçük ise %PROFIT_STOP_TRAIL olarak ayarlanır
            # if askPrice != stopPrice:
            #     stopPercentage = func.calculate_ratio(askPrice, stopPrice)
            # else:
            # stopPercentage = FIRST_PROFIT_STOP_TRAIL
            # newSP = askPrice - (askPrice * stopPercentage) / 100
            # log(f"   stopPrice %{FIRST_PROFIT_STOP_TRAIL} e ayarlandı: eski: {stopPrice} new: {newSP}")
            # stopPrice = func.round_tick_size(price=newSP, tick_size=tickSize)

            # if stopPrice > stopLine:
            #     stopPrice = stopLine
            stopPrice = stopLine
            stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

            if stopPrice > float(0.0):
                log(f"   stopPrice :{stopPrice} Ratio: {func.calculate_ratio(askPrice, stopPrice)}")

            buyLot = MAX_TRADE_LIMIT / askPrice
            buyLot = func.round_step_size(quantity=buyLot, step_size=stepSize)
            buyAmount = buyLot * askPrice
            buyCommission = buyAmount * COMMISSION_RATE

            targetProfit = float(0.0)
            riskRatio = float(0.0)
            if stopPrice > float(0.0):
                if askPrice == stopPrice:
                    continue
                riskRatio = func.calculate_ratio(askPrice, stopPrice)
                # MAX_ACCEPTABLE_RISK dan (örneğin %4) fazla risk alınmaz
                if riskRatio > MAX_ACCEPTABLE_RISK:
                    continue
                # Alınan risk karşılığında beklenen kazanç oranı (örneğin 2 katı)
                targetProfit = riskRatio * RISK_REWARD_RATIO

            if targetProfit < (COMMISSION_RATE * 2 * 100):
                continue

            insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                               price=askPrice, buyLot=buyLot, buyAmount=buyAmount, buyCommission=buyCommission,
                               stopPrice=stopPrice, stopType=0,
                               stopHeight=0, sellTarget=None, period=CANDLE_INTERVAL,
                               periodTime=periodTime, currentPeriodTime=periodTime, signalName=None,
                               explanation=glbExplanation, profitTarget=targetProfit, strategy=strategy)
            log(f"{SIDE_BUY} {coinSymbol} buyPrice:{askPrice} stop:{stopPrice} {glbExplanation} strategy:{strategy}")
            log(f"  ********** ")

            telegramMessage = f"{coinSymbol} S{strategy} Price:{askPrice}"
            tlg.sendNotification(connSession=connSession, notification=telegramMessage)

            glbExplanation = ""

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
            buyPeriodTime = position['period_time']
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

            # if strategy == 34:
            #     candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle, symbol=coinSymbol,
            #                                  interval=CANDLE_INTERVAL,
            #                                  limit=CANDLE_COUNT)
            #     if (candleBars is None) or (len(candleBars) < CANDLE_COUNT):
            #         continue
            # else:
            candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle,
                                         symbol=coinSymbol, interval=CANDLE_INTERVAL, limit=2)
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
            # isStrategy34 = False
            # if strategy == 34:
            #     hma = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
            #     hmaRSI = ind.get_rsi(prices=hma, timePeriod=HMA_FAST_PERIOD)
            #     if (hmaRSI[-1] > float(75.0)):
            #         isStrategy34 = True

            isStop = False
            isProfit = profit_control(coinTicker=coinTicker, currentPrice=closes[-1],
                                      buyingPrice=buyPrice, targetProfit=profitTarget, strategy=strategy)
            isStop = stop_control(currentPrice=closes[-1], stopPrice=stopPrice)
            if (isStop == True) or (isProfit == True):
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
                else:
                    zarar = buyTotal - sellTotal
                    yuzde = (zarar * 100) / sellTotal
                    log(f" {SIDE_SELL} {coinSymbol} Buy: {buyTotal} Sell: {sellTotal} price:{bidPrice} strategy:{strategy} ZARAR: {zarar} (%{round(yuzde, 2)}) <<<")
                log(f"************* ")
                continue
            # end if (isStop is True) or (isProfit is True):

            # if str(currentPeriodTime) != str(candlePeriodTime):
            #     candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle,
            #                                  symbol=coinSymbol, interval=CANDLE_INTERVAL, limit=CANDLE_COUNT)
            #     if (candleBars is None) or (len(candleBars) < 2):
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
            #     slowHMA = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
            #     curHMA = func.round_tick_size(slowHMA[-1], tick_size=tickSize)
            #     newStopPrice = stopPrice
            #     if curHMA > stopPrice:
            #         newStopPrice = curHMA
            #         stopChangeCount += 1
            #         log(f"  *** UPDATE STOP PRICE *** {coinSymbol} Buy:{buyPrice} Old:{stopPrice} New:{newStopPrice} %:{round((newStopPrice - buyPrice) / buyPrice * 100, 3)}")
            #
            #     update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol,
            #                                          stopPrice=newStopPrice,
            #                                          currentPeriodTime=candlePeriodTime,
            #                                          stopChangeCount=stopChangeCount)
            # end if str(currentPeriodTime) != str(candlePeriodTime)

        # end for item in positionRecords:
    # end while True:


def btc(connSession=None):
    log(f"BTC Thread Start")

    global BTC_SIGNAL
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