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

BTC_CANDLE_INTERVAL = "15m"
BTC_CANDLE_COUNT = 40
BTC_FAST_PERIOD = 25
BTC_SIGNAL = False
BTC_INNER_SIGNAL = False
BTC_RED_ALERT = False
BTC_GREEN_ALERT = False
BTC_SLOPE = float(0.0)
BTC_STD_DEV_UP = float(2.0)
BTC_STD_DEV_DOWN = float(2.0)
BTC_GREEN_THRESHOLD = float(1.5)  # yüzde
BTC_RED_THRESHOLD = float(1.5)   # yüzde

CANDLE_INTERVAL = "2h"      # Okunacak mum verisi periyodu
CANDLE_COUNT = 168  # 33            # Okunacak mum verisi adeti
MAX_TRADE_LIMIT = float(100)
LIMIT_FACTOR = float(5)
BB_PERIOD = 25
BB_STD_DEV = 2.5
LR_PERIOD = 18
LR_LOWER_BAND = float(2.0)
LR_UPPER_BAND = float(2.0)
ATR_PERIOD = 6
ATR_STOP_FACTOR = float(0.5)
COMMISSION_RATE = float(0.00075)  # %0.075
ZSCORE_PERIOD = 18
ZSCORE_DOWN_RANGE = float(-2.0)
ZSCORE_LIMIT_DOWN = float(-2.3)
ZSCORE_LIMIT_UP = float(2.3)
EMA_PERIOD = CANDLE_COUNT - 1
HMA_FAST_PERIOD = 10  # 96
HMA_SLOW_PERIOD = 154  # 154
MACD_FAST = 10
MACD_SLOW = 26
MACD_SIGNAL = 9

RISK_REWARD_RATIO = float(1.5)
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
logging.basicConfig(filename="debug_test_hma", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )

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
    # BTC_RED_ALERT = True ise alınmış olan coinler o anki fiyattan satılır.
    # if BTC_RED_ALERT == True:
    #     log(f"   ****** BTC_RED_ALERT: {BTC_RED_ALERT} ******")
    #     return True

    # BTC_INNER_SIGNAL = False ise coin alış fiyatına göre belli bir (örn: %0.6)
    # fiyatın üzerine çıkmış ise satılır.
    # if BTC_INNER_SIGNAL is True:
    #     return False

    # curColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    # if curColor == cons.CANDLE_GREEN:
    #     return False

    # if strategy == 34 or strategy == 35 or strategy == 36 or strategy == 37 or strategy == 38 or strategy == 39:
    if currentPrice > buyingPrice:
        # buyStopRatio = func.calculate_ratio(buyingPrice, stopPrice)
        profit = func.calculate_ratio(currentPrice, buyingPrice)

        if BTC_RED_ALERT == True:
            log(f"   BTC_RED_ALERT True: {BTC_RED_ALERT}")
            log(f"        PROFIT BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
            log(f"        Sell PROFIT > 0 - targetProfit: %{targetProfit} profit: %{profit}")
            return True

        # if (BTC_SIGNAL == False) and (profit > float(0.3)):
        #     log(f"   BTC_SIGNAL False: {BTC_SIGNAL}")
        #     log(f"        PROFIT BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
        #     log(f"        Sell PROFIT > 0 - targetProfit: %{targetProfit} profit: %{profit}")
        #     return True

        if strategy == 34:
            if profit >= PROFIT_STRATEGY_34:  # target:
                log(f"   %{PROFIT_STRATEGY_34} PROFIT BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                log(f"   profit: %{profit}")
                return True

        if strategy == 36:
            if profit >= PROFIT_STRATEGY_36:  # target:
                log(f"   %{PROFIT_STRATEGY_36} PROFIT BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                log(f"   profit: %{profit}")
                return True

        if strategy == 38:
            if profit >= PROFIT_STRATEGY_38:  # target:
                log(f"   %{PROFIT_STRATEGY_38} PROFIT BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                log(f"   profit: %{profit}")
                return True

        if strategy == 39:
            if profit >= PROFIT_STRATEGY_39:  # target:
                log(f"   %{PROFIT_STRATEGY_39} PROFIT BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                log(f"   profit: %{profit}")
                return True

        if (profit >= targetProfit):
            log(f"   RISK_REWARD_RATIO: %{RISK_REWARD_RATIO} PROFIT BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
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

    curColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    prev1Color = ind.get_candle_color(open=coinTicker.opens[-2], close=coinTicker.closes[-2])
    prev2Color = ind.get_candle_color(open=coinTicker.opens[-3], close=coinTicker.closes[-3])

    curClose = coinTicker.closes[-1]
    curLow = coinTicker.lows[-1]
    curHigh = coinTicker.highs[-1]

    fastHMA = ind.get_hma(data=coinTicker.closes, period=BTC_FAST_PERIOD)
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
def control_strategy_29(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    global glbExplanation
    glbExplanation = ""

    slowHMA = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    fastHMA = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)

    isCrossOver = func.cross_over(fastHMA, slowHMA)
    if isCrossOver == False:
        return False, float(0.0)

    if coinTicker.closes[-1] < slowHMA[-1]:
        return False, float(0.0)

    if (fastHMA[-1] > fastHMA[-2]) and (slowHMA[-1] < slowHMA[-2]):
        # message = ""
        # message = message + f"{symbol} HMA Fast crossover slow\n Price:{coinTicker.closes[-1]}"
        # tlg.sendNotification(connSession=session, notification=message)
        log(f" {symbol} HMA Fast crosssover slow Price:{coinTicker.closes[-1]} *S29*")
        stopHMA = func.round_tick_size(slowHMA[-1], tick_size=tickSize)
        return True, stopHMA

    return False, float(0.0)


def control_strategy_30(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    global glbExplanation
    glbExplanation = ""

    slowHMA = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    fastHMA = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)

    isDeepTurn = func.deep_turn(fastHMA)
    if isDeepTurn == False:
        return False, float(0.0)

    rsi = ind.get_rsi(prices=fastHMA, timePeriod=HMA_FAST_PERIOD)
    rsiSMA = ind.sma(rsi, period=HMA_FAST_PERIOD)

    if rsi[-1] > float(70.0):
        return False, float(0.0)

    if rsi[-1] < rsiSMA[-1]:
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes, period=HMA_FAST_PERIOD)
        stopPrice = coinTicker.lows[-1] - (atr[-1] * 0.5)
        # if coinTicker.lows[-1] < coinTicker.lows[-2]:
        #     stopPrice = coinTicker.lows[-1] - atr[-1]
        # else:
        #     stopPrice = coinTicker.lows[-2] - atr[-2]

        # message = ""
        # message = message + f"{symbol} HMA Fast deep turn \nRSI < RSISMA\nPrice:{coinTicker.closes[-1]}"
        # tlg.sendNotification(connSession=session, notification=message)
        log(f" {symbol} HMA Fast deep turn: Price: {coinTicker.closes[-1]} *S30*")
        log(f"          RSI < RSISMA :{rsi[-1]} < {rsiSMA[-1]} ")
        return True, stopPrice

    return False, float(0.0)


def control_strategy_31(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    global glbExplanation
    glbExplanation = ""

    slowHMA = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    fastHMA = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
    rsi = ind.get_rsi(prices=fastHMA, timePeriod=HMA_FAST_PERIOD)
    rsiSMA = ind.sma(data=rsi, period=HMA_FAST_PERIOD)

    isCrossOver = func.cross_over(rsi, rsiSMA)
    if isCrossOver == False:
        return False, float(0.0)

    isDeepTurn = False
    curIndex = 0
    while isDeepTurn == False:
        curIndex -= 1
        prevIndex = curIndex - 1
        cur = fastHMA[curIndex]
        prev = fastHMA[prevIndex]
        if prev > cur:
            isDeepTurn = True

    if isDeepTurn == True:
        if coinTicker.lows[curIndex] < coinTicker.lows[curIndex-1]:
            stopPrice = coinTicker.lows[curIndex]
        else:
            stopPrice = coinTicker.lows[curIndex-1]
        message = ""
        # message = message + f"{symbol} HMA rsi X rsiSMA \n Price:{coinTicker.closes[-1]}"
        # tlg.sendNotification(connSession=session, notification=message)
        log(f" {symbol} HMA rsi X rsiSMA *S31*")
        log(f"          Price: {coinTicker.closes[-1]} Stop Price: {stopPrice}")
        return True, stopPrice

    return False, float(0.0)


def control_strategy_32(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    Hızlı ve yavaş HMA (hull moving average) değerlerinin RSI ları hesaplanır.
    Yavaş RSI nın SMA ile ortalaması hesaplanır.
    Strateji:
    - SMA 70 in altında olacak.
    - Hızlı RSI 70 in altında olacak.
    - SMA ile hesaplanan ortalamanın eğimi POZITIF olacak
    - hızlı RSI dipten dönüş yapmış olacak
    """
    global glbExplanation
    glbExplanation = ""

    slowHMA = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    fastHMA = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)

    fastRSI = ind.get_rsi(prices=fastHMA, timePeriod=HMA_FAST_PERIOD)
    slowRSI = ind.get_rsi(prices=slowHMA, timePeriod=HMA_SLOW_PERIOD)
    slowRsiSMA = ind.sma(data=slowRSI, period=HMA_SLOW_PERIOD)

    isFastDeepTurn = func.deep_turn(fastRSI)
    isSMASlope = func.slope(slowRsiSMA)

    if slowRsiSMA[-1] > float(50.0):
        return False, float(0.0)

    if fastRSI[-1] > float(50.0):
        return False, float(0.0)

    if isSMASlope == True:
        if isFastDeepTurn == True:
            atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                              period=HMA_FAST_PERIOD)
            lp = coinTicker.lows[-1] if coinTicker.lows[-1] < coinTicker.lows[-2] else coinTicker.lows[-2]
            stopPrice = lp - (atr[-2] * 0.5)

            # message = f"{symbol} S32 \n Price:{coinTicker.closes[-1]}"
            # tlg.sendNotification(connSession=session, notification=message)
            log(f" {symbol} slowSMA Slope POZITIF, fastRSI Deep Turn *S32*")
            log(f"          slowSMA < 70, fastRSI < 70 *S32*")
            log(f"          Price: {coinTicker.closes[-1]} Stop Price: {stopPrice}")
            return True, stopPrice

    return False, float(0.0)


def control_strategy_33(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    Hızlı ve yavaş HMA (hull moving average) değerlerinin RSI ları hesaplanır.
    Yavaş RSI nın SMA ile ortalaması hesaplanır.
    Strateji:
    - SMA 30 in altında olacak.
    - Hızlı RSI 30 in altında olacak.
    - SMA ile hesaplanan ortalamanın eğimi NEGATİF olacak
    - hızlı RSI dipten dönüş yapmış olacak
    """
    global glbExplanation
    glbExplanation = ""

    slowHMA = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    fastHMA = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)

    fastRSI = ind.get_rsi(prices=fastHMA, timePeriod=HMA_FAST_PERIOD)
    slowRSI = ind.get_rsi(prices=slowHMA, timePeriod=HMA_SLOW_PERIOD)
    slowRsiSMA = ind.sma(data=slowRSI, period=HMA_SLOW_PERIOD)

    isFastDeepTurn = func.deep_turn(fastRSI)
    isSMASlope = func.slope(slowRsiSMA)

    if slowRsiSMA[-1] > float(30.0):
        return False, float(0.0)

    if fastRSI[-1] > float(30.0):
        return False, float(0.0)

    # if isSMASlope == False:
    if isFastDeepTurn == True:
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=HMA_FAST_PERIOD)
        lp =  coinTicker.lows[-1] if coinTicker.lows[-1] < coinTicker.lows[-2] else coinTicker.lows[-2]
        stopPrice = lp - (atr[-2] * 0.5)

        # message = f"{symbol} S33 \n Price:{coinTicker.closes[-1]}"
        # tlg.sendNotification(connSession=session, notification=message)
        log(f" {symbol} slowSMA Slope NEGATIF, fastRSI Deep Turn *S33*")
        log(f"          slowSMA < 30, fastRSI < 30 *S33*")
        log(f"          Price: {coinTicker.closes[-1]} Stop Price: {stopPrice}")
        return True, stopPrice

    return False, float(0.0)


def control_strategy_34(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    Strateji:
    - hmaRSI(fast) dipten dönüş.
    - hmaRSI[-2] <= 10
    """
    global glbExplanation
    glbExplanation = ""

    hma = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
    hmaRSI = ind.get_rsi(prices=hma, timePeriod=HMA_FAST_PERIOD)

    isDeepTurn = func.deep_turn(hmaRSI) and (hmaRSI[-2] <= 15.0)

    if (isDeepTurn == True):
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=HMA_FAST_PERIOD)
        stopPrice = coinTicker.lows[-2] - (atr[-2] * 0.5)

        log(f" {symbol} HMARSI Deep Turn *S34*")
        log(f"          hmaRSIPrev <= 15: {hmaRSI[-2]}")
        log(f"          Price: {coinTicker.closes[-1]} Stop Price: {stopPrice}")
        return True, stopPrice

    return False, float(0.0)


def control_strategy_35(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    Strateji:
    - MACD hızlı yavaşı YUKARI keser
    - hmaRSI DİPTEM dönüş.
    - SAR YEŞİL (low un altında)
    """
    global glbExplanation
    glbExplanation = ""

    fastMacd, slowMacd, histMacd = ind.get_macd(data=coinTicker.closes, fastPeriod=MACD_FAST,
                                                slowPeriod=MACD_SLOW, signalPeriod=MACD_SIGNAL)
    isCrossOverMACD = func.cross_over(fastMacd, slowMacd)
    isHistMACDUp = (histMacd[-1] > 0) and (histMacd[-1] > histMacd[-2])

    hma = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
    hmaRSI = ind.get_rsi(prices=hma, timePeriod=HMA_FAST_PERIOD)
    isDeepTurnRSI = func.deep_turn(hmaRSI)

    sar = ind.get_sar(high=coinTicker.highs, low=coinTicker.lows, acceleration=0.02, maximum=0.2)
    isSarTurnUp = (sar[-1] <= coinTicker.lows[-1]) and (sar[-2] > coinTicker.lows[-2])

    isPrice = coinTicker.closes[-1] <= coinTicker.highs[-2]

    if (isCrossOverMACD == True) and (isDeepTurnRSI == True) and  (isSarTurnUp == True) and (isHistMACDUp == True) and (isPrice == True):
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=HMA_FAST_PERIOD)
        stopPrice = sar[-1]

        log(f" {symbol} CrossOverMACD( fast: {fastMacd[-1]}, slow: {slowMacd[-1]}) *S35*")
        log(f"          DeepTurnRSI {hmaRSI[-1]} ")
        log(f"          SARTurnUp {sar[-1]} low:{coinTicker.lows[-1]}")
        log(f"          histMACD > 0 {histMacd[-1]}")
        log(f"          Stop Price: (sar) {stopPrice}")
        log(f"          Price: {coinTicker.closes[-1]}")
        return True, stopPrice

    return False, float(0.0)


def control_strategy_36(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    Strateji:
    - hmaRSI dipten dönüş.
    - SAR yukarı trend.
    - SAR yukarı trend baslangıç noktasına uzaklık 4 periyot noktasından küçük.
    """
    global glbExplanation
    glbExplanation = ""

    sar = ind.get_sar(high=coinTicker.highs, low=coinTicker.lows, acceleration=0.02, maximum=0.2)
    sarDiff = sar - coinTicker.lows
    hma = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
    hmaRSI = ind.get_rsi(prices=hma, timePeriod=HMA_FAST_PERIOD)

    fastMacd, slowMacd, histMacd = ind.get_macd(data=coinTicker.closes, fastPeriod=MACD_FAST,
                                                slowPeriod=MACD_SLOW, signalPeriod=MACD_SIGNAL)
    isHistMACDUp = (histMacd[-1] > 0) and (histMacd[-1] > histMacd[-2])

    isDeepTurn = func.deep_turn(hmaRSI)
    isSarUp = sar[-1] <= coinTicker.lows[-1]
    # sarUpCount = 0
    # if isSarUp == True:
    #     sarUpCount = sarUpTrendCount(data=sarDiff)

    if (isDeepTurn == True) and (isSarUp == True) and (isHistMACDUp == True) and (hmaRSI[-1] < 75.0):
        # stopPrice = sar[-1]
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=HMA_FAST_PERIOD)
        log(f" {symbol} HMARSI Deep Turn {hmaRSI[-1]} *S36*")
        log(f"          SAR Up {sar[-1]} low: {coinTicker.lows[-1]}")
        log(f"          Price: {coinTicker.closes[-1]}")
        hmaSlow = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
        isHMASlowUp = func.slope(hmaSlow)
        if (isHMASlowUp == True):
            stopPrice = sar[-1]
            log(f"          Stop Price: (sar) {stopPrice}")
        else:
            stopPrice = coinTicker.lows[-2] - (atr[-2] * 0.5)
            log(f"          Stop Price: (atr) {stopPrice}")

        return True, stopPrice

    return False, float(0.0)


def control_strategy_37(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    Strateji:
    - hmaRSI(fast) dipten dönüş.
    - hmaRSI < 75
    - SAR yukarı trend.
    - Slow HMA Trend yukarı
    - Fiyat Slow HMA nın üstünde
    """
    global glbExplanation
    glbExplanation = ""

    sar = ind.get_sar(high=coinTicker.highs, low=coinTicker.lows, acceleration=0.02, maximum=0.2)
    sarDiff = sar - coinTicker.lows
    hma = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
    hmaRSI = ind.get_rsi(prices=hma, timePeriod=HMA_FAST_PERIOD)

    hmaSlow = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    isSlowUp = func.slope(hmaSlow)

    isDeepTurn = func.deep_turn(hmaRSI)
    isSarUp = sar[-1] <= coinTicker.lows[-1]

    isPrice = coinTicker.closes[-1] <= coinTicker.highs[-2]

    if (isDeepTurn == True) and (isSarUp == True) and (isSlowUp == True) and (coinTicker.closes[-1] > hmaSlow[-1]) \
            and (hmaRSI[-1] < 75.0) and (isPrice == True):
        # stopPrice = sar[-1]
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=HMA_FAST_PERIOD)
        stopPrice = coinTicker.lows[-2] - (atr[-2] * 0.5)

        log(f" {symbol} HMARSI Deep Turn *S37*")
        log(f"          SAR Up {sar[-1]} ")
        log(f"          HMA Slow Slope Up {hmaSlow[-1]} ")
        log(f"          Price > hmaSlow: {coinTicker.closes[-1]} > {hmaSlow[-1]}")
        log(f"          hmaRSI < 75: {hmaRSI[-1]}")
        log(f"          Stop Price: (atr*0.5) {stopPrice}")
        log(f"          Price: {coinTicker.closes[-1]} ")
        return True, stopPrice

    return False, float(0.0)


def control_strategy_38(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    """
    Strateji:
    - SAR kırmızıdan yeşile dönüş.
    - HMA(154) yukarı trend
    - SAR yukarı trend.
    - MACD Hist > 0 (YEŞİL)
    - Fiyat > HMA(154)
    """
    global glbExplanation
    glbExplanation = ""

    sar = ind.get_sar(high=coinTicker.highs, low=coinTicker.lows, acceleration=0.02, maximum=0.2)
    sarDiff = sar - coinTicker.lows
    isSarTurnUp = (sar[-1] <= coinTicker.lows[-1]) and (sar[-2] > coinTicker.lows[-2])

    hma = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    isHmaSlopeUp = func.slope(hma)

    fastMacd, slowMacd, histMacd = ind.get_macd(data=coinTicker.closes, fastPeriod=MACD_FAST,
                                                slowPeriod=MACD_SLOW, signalPeriod=MACD_SIGNAL)
    isHistMACDUp = (histMacd[-1] > 0) and (histMacd[-1] > histMacd[-2])

    isPrice = coinTicker.closes[-1] <= coinTicker.highs[-2]

    # if (isSarTurnUp == True) and (isHmaSlopeUp == True) and  (coinTicker.closes[-1] > hma[-1]) and (histMacd[-1] > 0):
    if (isSarTurnUp == True) and (isHmaSlopeUp == True) and (isHistMACDUp == True) and (isPrice == True) :
        # stopPrice = sar[-1]
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=HMA_FAST_PERIOD)
        stopPrice = coinTicker.lows[-2] - (atr[-2] * 0.5)

        log(f"  {symbol} SAR Turn Up {sar[-1]} <= {coinTicker.lows[-1]} AND  {sar[-2]} > {coinTicker.lows[-2]} *S38*")
        log(f"          HMA(154) Slope Up, {hma[-1]} > {hma[-2]}")
        log(f"          StopPrice atr*0.5 {stopPrice}")
        return True, stopPrice

    return False, float(0.0)


def control_strategy_39(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    hma = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)

    curCandleColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    prevCandleColor = ind.get_candle_color(open=coinTicker.opens[-2], close=coinTicker.closes[-2])
    isPrevOnLine = (prevCandleColor == cons.CANDLE_GREEN) and (coinTicker.lows[-2] <= hma[-2] <= coinTicker.highs[-2])

    isRatio = False
    if coinTicker.closes[-1] > hma[-1]:
        ratio = func.calculate_ratio(coinTicker.closes[-1], hma[-1])
        isRatio = ratio <= 1.5

    isPrice = coinTicker.closes[-1] <= coinTicker.highs[-2]

    if (isPrevOnLine == True) and (curCandleColor == cons.CANDLE_GREEN) and (coinTicker.lows[-1] > hma[-1]) \
            and (isPrice == True) and (isRatio == True):
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=HMA_FAST_PERIOD)
        stopPrice = coinTicker.lows[-2] - (atr[-2] * 0.5)

        log(f"  {symbol} isPrevOnLine HMA  {coinTicker.lows[-2]} <= {hma[-2]} <= {coinTicker.highs[-2]} *S39*")
        # log(f"          HMA(154) Slope Up, {hma[-1]} > {hma[-2]}")
        log(f"          StopPrice atr*0.5 {stopPrice}")

        return True, stopPrice

    return False, float(0.0)


def control_strategy_40(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    hmaSlow = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    isHmaSlopeUp = func.slope(hmaSlow)

    hmaFast = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
    hmaFastRSI = ind.get_rsi(prices=hmaFast, timePeriod=HMA_FAST_PERIOD)
    isHmaDeepTurn = func.deep_turn(hmaFastRSI) and (hmaFastRSI[-1] < 50.0)

    isPrice = coinTicker.closes[-1] <= coinTicker.highs[-2]

    if (isHmaSlopeUp == True) and (isHmaDeepTurn == True) and (isPrice == True):
        atr = ind.get_atr(highPrices=coinTicker.highs, lowPrices=coinTicker.lows, closePrices=coinTicker.closes,
                          period=HMA_FAST_PERIOD)
        stopPrice = coinTicker.lows[-2] - (atr[-2] * 0.5)
        if coinTicker.closes[-1] > hmaSlow[-1]:
            stopPrice = hmaSlow[-1]
            log(f"          Stop Price: hmaSlow: {stopPrice}")

        log(f" {symbol} HMA Slow Slope Up {hmaSlow[-1]} *S40*")
        log(f"          HMA Fast RSI Deep Turn And < 50 {hmaFastRSI[-1]} ")
        log(f"          Price {coinTicker.closes[-1]} ")
        log(f"          Stop Price: {stopPrice}")
        return True, stopPrice

    return False, float(0.0)

def control_strategy_41(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float):
    hma = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    isHmaDeepTurn = func.deep_turn(hma)
    isPreviousCandleCloseOverHma = coinTicker.lows[-2] > hma[-2]
    isCurrentCandleLowOverHma = coinTicker.lows[-1] > hma[-1]

    if (isHmaDeepTurn == True) and (isPreviousCandleCloseOverHma == True) and (isCurrentCandleLowOverHma == True):
        stopPrice = hma[-1]
        return True, stopPrice

    return False, float(0.0)

#endregion

def control_buy_signal(dbCursor=None, session=None, url=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, int):
    # if BTC_RED_ALERT == True:
    #     return False, 0.0, 0
    #
    # if BTC_SIGNAL == False and BTC_GREEN_ALERT == False:
    #     return False, 0.0, 0

    # signal, stopLine = control_strategy_29(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 29
    #
    # signal, stopLine = control_strategy_30(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 30
    #
    # signal, stopLine = control_strategy_31(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 31
    #
    # signal, stopLine = control_strategy_32(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 32
    #
    # signal, stopLine = control_strategy_33(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 33
    #
    # signal, stopLine = control_strategy_34(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 34

    # signal, stopLine = control_strategy_35(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 35

    # signal, stopLine = control_strategy_36(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 36

    # signal, stopLine = control_strategy_37(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 37
    #
    # signal, stopLine = control_strategy_38(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 38

    # signal, stopLine = control_strategy_39(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal == True:
    #     return True, stopLine, 39

    signal, stopLine = control_strategy_41(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, stopLine, 41

    return False, 0.0, 0

#region Threads
def buy(connSession=None):
    log(f"BUY Thread Start")

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

            riskRewardRatio = RISK_REWARD_RATIO
            if strategy == 34:
                riskRewardRatio = PROFIT_STRATEGY_34

            targetProfit = float(0.0)
            if stopPrice > float(0.0):
                targetProfit = func.calculate_ratio(askPrice, stopPrice) * riskRewardRatio

            insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                               price=askPrice, buyLot=buyLot, buyAmount=buyAmount, buyCommission=buyCommission,
                               stopPrice=stopPrice, stopType=0,
                               stopHeight=0, sellTarget=None, period=CANDLE_INTERVAL,
                               periodTime=periodTime, currentPeriodTime=periodTime, signalName=None,
                               explanation=glbExplanation, profitTarget=targetProfit, strategy=strategy)
            log(f"{SIDE_BUY} {coinSymbol} stop:{stopPrice} buyPrice:{askPrice} {glbExplanation} strategy:{strategy}")
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
            if (isStop == True) or (isProfit == True): # or (isStrategy34 == True):
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