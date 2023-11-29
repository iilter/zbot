import json

import numpy as np
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
from botclass import BinanceOrderBook as orderBookClass

import notify as tlg
import logging
import winsound

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP = 2

BTC_CANDLE_INTERVAL = "30m"
BTC_CANDLE_COUNT = 11
BTC_SLOPE_PERIOD = 10
BTC_LSMA_PERIOD = 6
BTC_STD_DEV_UP = float(2.5)
BTC_STD_DEV_DOWN = float(2.5)
BTC_SIGNAL = False
BTC_INNER_SIGNAL = False
BTC_RED_ALERT = False
BTC_GREEN_ALERT = False
BTC_GREEN_THRESHOLD = float(1.5)
BTC_RED_THRESHOLD = float(1.5)
BTC_SLOPE = float(0.0)

CANDLE_INTERVAL = "30m"      # Okunacak mum verisi periyodu
CANDLE_COUNT = 33           # Okunacak mum verisi adeti
LR_PERIOD = 18
STOCH_PERIOD = 18
RSI_PERIOD = 18
ROC_PERIOD = 18
MACD_FAST = 7
MACD_SLOW = 18
MACD_SIGNAL = 9
VWMA_PERIOD = 18
LSMA_PERIOD = 6
LR_STD_UP_FACTOR = float(2.3)
LR_STD_DOWN_FACTOR = float(2.8)
LR_UP_HEIGHT = float(2.3)
LR_DOWN_HEIGHT = float(3.0)
AVERAGE_CANDLE_COUNT = 18   # Ortalaması hesaplanacak mum adeti
ATR_PERIOD = 6
ATR_STOP_FACTOR = float(0.5)
MAX_AMOUNT_LIMIT = float(100)
LIMIT_FACTOR = float(5)
COMMISSION_RATE = float(0.00075)  # %0.075
VOLUME_FACTOR = float(2.0)

FIRST_PROFIT_TARGET = float(1.5) # 2.0
PROFIT_TARGET = float(1.5) # 2.0
PROFIT_STOP_TRAIL = float(0.7) # 0.8
PROFIT_STRATEGY_17 = float(2.0)

IS_LOG = True
IS_PRINT = True
IS_ALARM = False

TEST_NAME = "STRATEJI GRID"
glbExplanation = ""
logging.basicConfig(filename="debug_test_grid", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )


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


def readSummary(dbCursor=None):
    trade = tradeClass()
    res = trade.readProfitSummary(dbCursor=dbCursor)
    return res


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


def position_control(dbCursor=None, symbol=None, status=None):
    trade = tradeClass()
    row = trade.readTrade(dbCursor=dbCursor, symbol=symbol, status=status)
    if (row is None) or (len(row) <= 0):
        return False, row
    return True, row


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


def lr_stop_control(coinTicker=None, stopPrice=None, buyPrice=None, tickSize=None) -> (bool):
    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=2.0,
                                                        standard_deviation_down_factor=2.0)
    if slope > 0.0:
        return False

    closePrice = coinTicker.closes[-1]
    upPrice = up[-1]
    if closePrice >= upPrice:
        log(f"  Price >= LR Up: {coinTicker.closes[-1]} > {up[-1]}")
        return True

    return False


def trailing_stop_control(stopHeight=None, currentPrice=None, stopPrice=None, tickSize=None) -> (bool, float):
    stopChange = False
    newStopPrice = None

    if currentPrice > stopPrice:
        difference = currentPrice - stopPrice
        difference = func.round_tick_size(price=difference, tick_size=tickSize)
        if difference > stopHeight:
            newStopPrice = currentPrice - stopHeight
            newStopPrice = func.round_tick_size(price=newStopPrice, tick_size=tickSize)
            stopChange = True

    return stopChange, newStopPrice


def profit_control(coinTicker=None, currentPrice=None, buyingPrice=None, strategy=None) -> (bool):
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

    if strategy == 17:
        if currentPrice > buyingPrice:
            profit = ((currentPrice - buyingPrice) * 100) / buyingPrice

            # up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
            #                                                     standard_deviation_up_factor=1.8,
            #                                                     standard_deviation_down_factor=2.3)
            # if slope < 0:
            #     target = 0.90
            # else:
            #     target = PROFIT_STRATEGY_17

            if profit >= PROFIT_STRATEGY_17: # target:
                log(f"   BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL}")
                log(f"   PROFIT STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                return True
    return False


def profit_target_control(currentPrice=None, buyingPrice=None, profitTarget=None, strategy=None) -> (bool, float):
    profitTargetLevel = PROFIT_TARGET
    newProfitTarget = profitTarget
    profit = (currentPrice - buyingPrice)*100/buyingPrice
    if profit > profitTarget:
        newProfitTarget += profitTargetLevel
        while profit >= newProfitTarget:
            newProfitTarget += profitTargetLevel
        return True, newProfitTarget
    return False, newProfitTarget


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

    lsma = ind.get_lsma(data=coinTicker.closes, period=BTC_LSMA_PERIOD)
    curLSMA = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    prev1LSMA = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    prev2LSMA = func.round_tick_size(price=lsma[-3], tick_size=tickSize)

    ups, center, downs, slope = ind.get_linear_regression(prices=coinTicker.closes, period=BTC_SLOPE_PERIOD,
                                                        standard_deviation_up_factor=BTC_STD_DEV_UP,
                                                        standard_deviation_down_factor=BTC_STD_DEV_DOWN)
    BTC_SLOPE = slope
    if slope < 0:
        BTC_SIGNAL = False
    else:
        BTC_SIGNAL = True


    u, c, d, innerSlope = ind.get_linear_regression(prices=coinTicker.closes, period=3,
                                                    standard_deviation_up_factor=BTC_STD_DEV_UP,
                                                    standard_deviation_down_factor=BTC_STD_DEV_DOWN)
    if innerSlope < 0:
        BTC_INNER_SIGNAL = False
    else:
        BTC_INNER_SIGNAL = True

    """
    Mumun boyu bir yöne eşik değerini geçmiş ise ALARM (RED veya GREEN) aktif olur.
    Mumun renge terse dönünceye kadar ALARM aktif olarak kalır.
    """
    threshold = ((curHigh - curLow) * 100) / curLow
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


def control_strategy_1(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT is True:
        return False

    if BTC_SIGNAL is False and BTC_INNER_SIGNAL is False:
        return False

    curClose = coinTicker.closes[-1]
    prevHigh = coinTicker.highs[-2]

    curColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    if curColor == cons.CANDLE_RED:
        return False

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=10)
    curRSI = rsi[-1]
    prev1RSI = rsi[-2]
    prev2RSI = rsi[-3]

    if (curRSI < 20) and (prev1RSI < 20) and (prev2RSI < 20):
        if (prev2RSI > prev1RSI < curRSI):
            if (curClose <= prevHigh):
                log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S1")
                log(f"          curRSI, prev1RSI, prev2RSI < 20: {curRSI}, {prev1RSI}, {prev2RSI}")
                log(f"          prev2RSI > prev1RSI < curRSI: {prev2RSI} > {prev1RSI} < {curRSI} ")
                log(f" +++      curClose <= prevHigh: {curClose} < {prevHigh}")
                glbExplanation = f" curRSI, prev1RSI, prev2RSI < 20: {curRSI}, {prev1RSI}, {prev2RSI} prev2RSI > prev1RSI < curRSI: {prev2RSI} > {prev1RSI} < {curRSI}"
                return True

    return False


def control_strategy_2(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT is True:
        return False

    if BTC_SIGNAL is False and BTC_INNER_SIGNAL is False:
        return False

    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=2.0,
                                                        standard_deviation_down_factor=2.2)
    curClose = coinTicker.closes[-1]
    curOpen = coinTicker.opens[-1]
    prev1Close = coinTicker.closes[-2]
    prev1Open = coinTicker.opens[-2]
    prev1High = coinTicker.highs[-1]

    curCandleColor = ind.get_candle_color(open=curOpen, close=curClose)
    prev1CandleColor = ind.get_candle_color(open=prev1Open, close=prev1Close)

    if slope < 0:
        if (BTC_SIGNAL is True) and (BTC_INNER_SIGNAL is True):
            rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=10)
            curRSI = rsi[-1]
            prevRSI = rsi[-2]

            rsiEMA = ind.get_ema(data=rsi, period=10)
            curRSIEMA = rsiEMA[-1]
            if (prevRSI < 15)  and (prev1CandleColor == cons.CANDLE_RED) and (curCandleColor == cons.CANDLE_GREEN):
                if (curRSI > prevRSI) and (curRSI > curRSIEMA):
                    if curClose < prev1High:
                        log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S2")
                        log(f"          prevRSI < 15: {prevRSI} slope:{slope} ")
                        log(f"          curRSI > curRSIEMA: {curRSI} > {curRSIEMA} ")
                        log(f"          curRSI > prevRSI: {curRSI} > {prevRSI} ")
                        log(f" +++      curClose < prev1High: {curClose} < {prev1High}")
                        glbExplanation = f" prevRSI:{prevRSI} curRSI > curRSIEMA: {curRSI} > {curRSIEMA} slope:{slope}"
                        return True
    return False


def control_strategy_3(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    curOpen = coinTicker.opens[-1]
    curClose = coinTicker.closes[-1]

    curCandleColor = ind.get_candle_color(open=curOpen, close=curClose)
    if curCandleColor == cons.CANDLE_RED:
        return False

    start = CANDLE_COUNT - AVERAGE_CANDLE_COUNT - 1
    end = CANDLE_COUNT - 1
    avgVolume = coinTicker.volumes[start:end].mean()
    curVolume = coinTicker.volumes[-1]

    if curVolume < avgVolume * VOLUME_FACTOR:
        return False

    fastST, slowST = ind.get_stochastic(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                        timePeriod=LR_PERIOD, slowKPeriod=3, slowDPeriod=3)
    if fastST[-1] <= slowST[-1]:
        return False

    if (fastST[-1] < 25) and (slowST[-1] < 25):
        log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S3")
        log(f"          curVolume > avgVolume*VOLUME_FACTOR: {curVolume} > {avgVolume * VOLUME_FACTOR}")
        log(f"          fastST > slowST: {fastST[-1]} > {slowST[-1]}")
        glbExplanation = f" {symbol} curVolume > avgVolume*VOLUME_FACTOR: {curVolume} > {avgVolume*VOLUME_FACTOR} fastST > slowST: {fastST[-1]} > {slowST[-1]} "
        return True
    return False


def control_strategy_4(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT is True:
        return False

    if BTC_SIGNAL is False and BTC_INNER_SIGNAL is False:
        return False

    curClose = coinTicker.closes[-1]
    prev1High = coinTicker.highs[-2]

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=10)
    curRSI = rsi[-1]
    prev1RSI = rsi[-2]
    prev2RSI = rsi[-3]

    rsiEMA = ind.get_ema(data=rsi, period=10)
    curRSIEMA = rsiEMA[-1]

    lsma = ind.get_lsma(data=coinTicker.closes, period=LSMA_PERIOD)
    curLSMA = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    prev1LSMA = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    prev2LSMA = func.round_tick_size(price=lsma[-3], tick_size=tickSize)
    prev3LSMA = func.round_tick_size(price=lsma[-4], tick_size=tickSize)

    if (BTC_SIGNAL is True) and (BTC_INNER_SIGNAL is True):
        if (curLSMA > prev1LSMA < prev2LSMA): # and (slope > 0):
            if (curRSI > curRSIEMA) and (curRSI > prev1RSI) :
                if (prev1RSI < 20) or (prev2RSI < 20):
                    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S4")
                    log(f"          LSMA: {curLSMA} > {prev1LSMA} < {prev2LSMA}")
                    log(f"          curRSI > curRSIEMA : {curRSI} > {curRSIEMA} curRSI > prev1RSI:{curRSI} > {prev1RSI}")
                    log(f"          prev1RSI OR prev2RSI < 20: {prev1RSI} {prev2RSI}")
                    if curClose <= prev1High:
                        log(f" +++      curClose <= prev1High : {curClose} <= {prev1High} ")
                        glbExplanation = f" LSMA: {curLSMA} > {prev1LSMA} < {prev2LSMA} curRSI > prev1RSI: {curRSI} > {prev2RSI} "
                        return True
    return False


def control_strategy_5(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT is True:
        return False

    if BTC_SIGNAL is False and BTC_INNER_SIGNAL is False:
        return False

    curClose = coinTicker.closes[-1]
    prev1High = coinTicker.highs[-2]

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=10)
    curRSI = rsi[-1]
    prev1RSI = rsi[-2]
    prev2RSI = rsi[-3]

    rsiEMA = ind.get_ema(data=rsi, period=10)
    curRSIEMA = rsiEMA[-1]

    lsma = ind.get_lsma(data=coinTicker.closes, period=LSMA_PERIOD)
    curLSMA = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    prev1LSMA = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    prev2LSMA = func.round_tick_size(price=lsma[-3], tick_size=tickSize)
    prev3LSMA = func.round_tick_size(price=lsma[-4], tick_size=tickSize)

    if (BTC_SIGNAL is True) and (BTC_INNER_SIGNAL is False):
        if curLSMA > prev1LSMA < prev2LSMA:
            if (curRSI > curRSIEMA) and (curRSI > prev1RSI):
                if (prev1RSI < 20) or (prev2RSI < 20):
                    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S5")
                    log(f"          LSMA: {curLSMA} > {prev1LSMA} < {prev2LSMA}")
                    log(f"          curRSI > curRSIEMA : {curRSI} > {curRSIEMA} curRSI > prev1RSI:{curRSI} > {prev1RSI}")
                    log(f"          prev1RSI OR prev2RSI < 20: {prev1RSI} {prev2RSI}")
                    if curClose <= prev1High:
                        log(f" +++      curClose <= prev1High : {curClose} <= {prev1High} ")
                        glbExplanation = f" LSMA: {curLSMA} > {prev1LSMA} < {prev2LSMA} prev1RSI or prev2RSI < 20: {prev1RSI} {prev2RSI} "
                        return True
    return False


def control_strategy_6(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    curOpen = coinTicker.opens[-1]
    curClose = coinTicker.closes[-1]

    curCandleColor = ind.get_candle_color(open=curOpen, close=curClose)
    if curCandleColor == cons.CANDLE_RED:
        return False

    start = CANDLE_COUNT - AVERAGE_CANDLE_COUNT - 1
    end = CANDLE_COUNT - 1
    avgVolume = coinTicker.volumes[start:end].mean()
    avgVolume = round(avgVolume, 1)
    curVolume = coinTicker.volumes[-1]

    volRatio = round(curVolume/avgVolume, 1)
    if volRatio < 3.0:
        return False

    prevHigh = coinTicker.highs[-2]
    if curClose > prevHigh:
        return False

    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=LR_STD_UP_FACTOR,
                                                        standard_deviation_down_factor=LR_STD_DOWN_FACTOR)
    curLRUp = func.round_tick_size(price=up[-1], tick_size=tickSize)
    if curClose > curLRUp:
        return False

    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S6")
    log(f"          curVolume: {curVolume} avgVolume: {avgVolume} volRatio: {volRatio}")
    log(f"          curClose: {curClose} prevHigh: {prevHigh} curLRUp: {curLRUp}")
    glbExplanation = f" curVolume: {curVolume} avgVolume: {avgVolume} volRatio: {volRatio} prevHigh {prevHigh} curLRUp: {curLRUp} "
    return True


def control_strategy_7(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT is True:
        return False

    if BTC_SIGNAL is False and BTC_INNER_SIGNAL is False:
        return False

    curColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    prevColor = ind.get_candle_color(open=coinTicker.opens[-2], close=coinTicker.closes[-2])

    curClose = coinTicker.closes[-1]
    prevHigh = coinTicker.highs[-2]
    prevOpen = coinTicker.opens[-2]

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=10)
    curRSI = rsi[-1]
    prev1RSI = rsi[-2]
    prev2RSI = rsi[-3]
    prev3RSI = rsi[-4]

    rsiMA = ind.get_ema(data=rsi, period=10)
    curRSIMA = rsiMA[-1]
    prev1RSIMA = rsiMA[-2]
    prev2RSIMA = rsiMA[-3]

    if prevColor == cons.CANDLE_GREEN:
        priceLevel = prevHigh
    else:
        priceLevel = (prevHigh + prevOpen) / 2

    if curColor == cons.CANDLE_GREEN:
        if curRSI < 20:
            if prev1RSI < prev1RSIMA and curRSI > curRSIMA:
                if curRSI > prev1RSI and curRSIMA > prev1RSIMA:
                    if curClose <= priceLevel:
                        log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S7")
                        log(f"          curRSI < 40:{curRSI} ")
                        log(f"          prev1RSI < prev1RSIMA and curRSI > curRSIMA : {prev1RSI} < {prev1RSIMA} AND {curRSI} > {curRSIMA} ")
                        log(f"          curRSI > prev1RSI and curRSIMA > prev1RSIMA:  {curRSI} > {prev1RSI} AND {curRSIMA} > {prev1RSIMA}")
                        log(f" +++      curClose <= priceLevel: {curClose} <= {priceLevel} ")
                        glbExplanation = f" {symbol} prev1RSI < prev1RSIMA and curRSI > curRSIMA : {prev1RSI} < {prev1RSIMA} AND {curRSI} > {curRSIMA} "
                        return True
    return False


def control_strategy_8(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT is True:
        return False

    if BTC_SIGNAL is False and BTC_INNER_SIGNAL is False:
        return False

    curColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    prevColor = ind.get_candle_color(open=coinTicker.opens[-2], close=coinTicker.closes[-2])

    curClose = coinTicker.closes[-1]
    prevHigh = coinTicker.highs[-2]
    prevOpen = coinTicker.opens[-2]

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=10)
    curRSI = rsi[-1]
    prev1RSI = rsi[-2]
    prev2RSI = rsi[-3]
    prev3RSI = rsi[-4]

    rsiMA = ind.get_ema(data=rsi, period=10)
    curRSIMA = rsiMA[-1]
    prev1RSIMA = rsiMA[-2]
    prev2RSIMA = rsiMA[-3]

    if prevColor == cons.CANDLE_GREEN:
        priceLevel = prevHigh
    else:
        priceLevel = (prevHigh + prevOpen) / 2

    if curColor == cons.CANDLE_GREEN:
        if curRSI < 30:
            if prev1RSI < prev1RSIMA and curRSI > curRSIMA:
                if curRSI > prev1RSI and curRSIMA > prev1RSIMA:
                    if curClose <= priceLevel:
                        log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S8")
                        log(f"          curRSI < 30:{curRSI} ")
                        log(f"          prev1RSI < prev1RSIMA and curRSI > curRSIMA : {prev1RSI} < {prev1RSIMA} AND {curRSI} > {curRSIMA} ")
                        log(f"          curRSI > prev1RSI and curRSIMA > prev1RSIMA:  {curRSI} > {prev1RSI} AND {curRSIMA} > {prev1RSIMA}")
                        log(f" +++      curClose <= priceLevel: {curClose} <= {priceLevel} ")
                        glbExplanation = f" {symbol} prev1RSI < prev1RSIMA and curRSI > curRSIMA : {prev1RSI} < {prev1RSIMA} AND {curRSI} > {curRSIMA} "
                        return True
    return False


def control_strategy_9(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    """
    STOCH hızlı yavaşı yukarı keser
          hızlı eğim yukarı
          yavaş eğim yukarı
          hızlı veya yavaş eşik değerinin altında
    RSI   hızlı yavaşı yukarı keser
          hızlı eğim yukarı
          yavaş eğim yukarı
          hızlı veya yavaş eşik değerinin altında
    VWMA  Fiyat(low) VWMA nın eşik değeri kadar altında
    """
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT == True:
        return False

    if (BTC_SIGNAL == False) and (BTC_INNER_SIGNAL == False):
        return False

    curColor = ind.get_candle_color(open=float(coinTicker.opens[-1]), close=float(coinTicker.closes[-1]))
    if (curColor == cons.CANDLE_RED):
        return False

    # LR Kontrolleri
    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=LR_STD_UP_FACTOR,
                                                        standard_deviation_down_factor=LR_STD_DOWN_FACTOR)
    # LR alt ve üst bandı arasındaki yüksekliğin belli bir oranın üstünde olması aranır.
    hFactor = LR_UP_HEIGHT
    if slope < 0:
        hFactor = LR_DOWN_HEIGHT
    heightLR = func.calculate_ratio(float(up[-1]), float(down[-1]))
    heightLRCond = heightLR > hFactor
    if heightLRCond == False:
        return False

    # STOCHASTIC Kontrolleri
    fastST, slowST = ind.get_stochastic(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                        timePeriod=STOCH_PERIOD, slowKPeriod=3, slowDPeriod=3)
    stochThreshold = 10.0
    stochBelowThreshold = (fastST[-1] < stochThreshold) or (slowST[-1] < stochThreshold)
    if (stochBelowThreshold == False):
        return False

    stochCrossOver = func.cross_over(fastST, slowST )
    if (stochCrossOver == False):
        return False

    stochFastSlopeUp = fastST[-1] > fastST[-2]
    if (stochFastSlopeUp == False):
        return False

    stochSlowSlopeUp = slowST[-1] > slowST[-2]
    if (stochSlowSlopeUp == False):
        return False

    # RSI Kontrolleri
    rsiFast = ind.get_rsi(prices=coinTicker.closes, timePeriod=RSI_PERIOD)
    rsiSlow = ind.get_ema(data=rsiFast, period=RSI_PERIOD)

    rsiThreshold = 50.0
    rsiBelowThreshold = rsiFast[-1] < rsiThreshold
    if (rsiBelowThreshold == False):
        return False

    rsiCrossOver = func.cross_over(rsiFast, rsiSlow)
    if (rsiCrossOver == False):
        return False

    rsiFastSlopeUp = rsiFast[-1] > rsiFast[-2]
    if (rsiFastSlopeUp == False):
        return False

    rsiSlowSlopeUp = rsiSlow[-1] > rsiSlow[-2]
    if (rsiSlowSlopeUp == False):
        return False

    # VWMA Kontrolleri
    vwap = ind.get_vwma(data=coinTicker.closes, volume=coinTicker.volumes, period=VWMA_PERIOD)

    vwapThreshold = float(1.0)
    if slope < 0:
        vwapThreshold = float(3.0)

    vwapHeight = func.calculate_ratio(vwap[-2], coinTicker.lows[-2])
    priceUnderVWAPThreshold = vwapHeight > vwapThreshold
    if (priceUnderVWAPThreshold == False):
        return False

    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S9")
    log(f"   STOCH  stochBelowThreshold: {stochBelowThreshold} stochCrossUp: {stochCrossOver} stochFastSlopeUp: {stochFastSlopeUp} stochSlowSlopeUp: {stochSlowSlopeUp}")
    log(f"   RSI    rsiBelowThreshold: {rsiBelowThreshold} rsiCrossUp: {rsiCrossOver} rsiFastSlopeUp: {rsiFastSlopeUp} rsiSlowSlopeUp: {rsiSlowSlopeUp}")
    log(f"   VWAP   priceUnderVWAPThreshold: {priceUnderVWAPThreshold}")
    glbExplanation = f" {symbol} vwapThreshold: {vwapThreshold} vwHeight: {vwapHeight} stochBelowThreshold: {stochBelowThreshold} rsiBelowThreshold: {rsiBelowThreshold} "
    return True


def control_strategy_10(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    """
    VWMA  Fiyat VWMA nın eşik değeri kadar altında
    STOCH Hızlı veya yavaş eşik değerinin altında
          Hızlı yavaşı yukarı keser
    BB    Bant genişliği eşik değerinin üzerinde
    VWAP  Fiyat(low), VWAP ın eşik değeri kadar altında
          Fiyat(low ve high), VWAP çizgisinin altında
    """
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT == True:
        return False

    if (BTC_SIGNAL == False) and (BTC_INNER_SIGNAL == False):
        return False

    curColor = ind.get_candle_color(open=float(coinTicker.opens[-1]), close=float(coinTicker.closes[-1]))
    if curColor == cons.CANDLE_RED:
        return False

    # LR Kontrolleri
    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=LR_STD_UP_FACTOR,
                                                        standard_deviation_down_factor=LR_STD_DOWN_FACTOR)
    # LR alt ve üst bandı arasındaki yüksekliğin belli bir oranın üstünde olması aranır.
    hFactor = LR_UP_HEIGHT
    if slope < 0:
        hFactor = LR_DOWN_HEIGHT
    heightLR = func.calculate_ratio(float(up[-1]), float(down[-1]))
    heightLRCond = heightLR > hFactor
    if heightLRCond == False:
        return False

    # VWMA Kontrolleri
    vwap = ind.get_vwma(data=coinTicker.closes, volume=coinTicker.volumes, period=VWMA_PERIOD)
    priceUnderVWAP = (vwap[-2] > float(coinTicker.highs[-2])) and (vwap[-1] > float(coinTicker.closes[-1]))
    if (priceUnderVWAP == False):
        return False

    vwapThreshold = float(1.5)
    if slope < 0:
        vwapThreshold = float(3.0)

    vwapHeight = func.calculate_ratio(vwap[-2], coinTicker.lows[-2])
    priceUnderVWAPThreshold = vwapHeight > vwapThreshold
    if (priceUnderVWAPThreshold == False):
        return False

    # STOCHASTIC Kontrolleri
    fastST, slowST = ind.get_stochastic(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                        timePeriod=STOCH_PERIOD, slowKPeriod=3, slowDPeriod=3)
    stochThreshold = 15.0
    stochBelowThreshold = (fastST[-1] < stochThreshold) or (slowST[-1] < stochThreshold)
    if (stochBelowThreshold == False):
        return False

    stochCrossUp = func.cross_over(fastST, slowST)
    if (stochCrossUp == False):
        return False

    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} * S10 *")
    log(f"          heightLRCond: {heightLRCond} priceUnderVWAPThreshold: {priceUnderVWAPThreshold} stochCrossUp: {stochCrossUp}")
    glbExplanation = f" {symbol} heightLRCond: {heightLRCond} priceUnderVWAPThreshold: {priceUnderVWAPThreshold} stochCrossUp: {stochCrossUp}"
    return True


def control_strategy_11(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    # Fiyat, VWMA çizgisini yukarı kesiyor.
    # STOCH hızlı, yavaşı yukarı kesiyor.
    # RSI hzılı, yavaşı yukarı kesiyor.
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT is True:
        return False

    if BTC_SIGNAL is False and BTC_INNER_SIGNAL is False:
        return False

    curColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    if curColor == cons.CANDLE_RED:
        return False

    curClose = coinTicker.closes[-1]
    curLow = coinTicker.lows[-1]
    curHigh = coinTicker.highs[-1]
    prevHigh = coinTicker.highs[-2]
    prevOpen = coinTicker.opens[-2]
    prevLow = coinTicker.lows[-2]


    vwap = ind.get_vwma(data=coinTicker.closes, volume=coinTicker.volumes, period=VWMA_PERIOD)
    curVWAP = func.round_tick_size(price=vwap[-1], tick_size=tickSize)
    prevVWAP = func.round_tick_size(price=vwap[-2], tick_size=tickSize)

    vwSignal = False
    # Fiyat VWAP çizgisini yukarı kesiyor
    if curLow < curVWAP < curHigh:
        if prevHigh < prevVWAP:
            vwSignal = True

    if vwSignal is False:
        return False

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=15)
    curRSIFast = rsi[-1]
    prevRSIFast = rsi[-2]

    rsiMA = ind.get_ema(data=rsi, period=15)
    curRSISlow = rsiMA[-1]
    prevRSISlow = rsiMA[-2]

    rsiSignal = False
    # RSI hızlı, yavaşın üzerinde
    # RSI slow eğim yukarı doğru
    if curRSIFast > curRSISlow:
        if curRSISlow > prevRSISlow:
            rsiSignal = True

    if rsiSignal is False:
        return False

    fast, slow = ind.get_stochastic(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                    timePeriod=15, slowKPeriod=3, slowDPeriod=3)
    curSTOCHFast = fast[-1]
    curSTOCHSlow = slow[-1]

    stochSignal = False
    # STOCHASTIC hızlı, yavaşın üstünde
    if curSTOCHFast > curSTOCHSlow:
        stochSignal = True

    if (rsiSignal is True) and (vwSignal is True) and (stochSignal is True):
        log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S11")
        log(f"   VWAP   curLow < curVWAP < curHigh: {curLow} < {curVWAP} < {curHigh} ")
        log(f"   RSI    curRSIFast > curRSISlow: {curRSIFast} > {curRSISlow} ")
        log(f"          curRSISlow > prevRSISlow: {curRSISlow} > {prevRSISlow} ")
        log(f"   STOCH  curSTOCHFast > curSTOCHSlow: {curSTOCHFast} > {curSTOCHSlow} ")
        log(f"  ********** ")
        glbExplanation = f" {symbol} vwSignal: {vwSignal} rsiSignal: {rsiSignal} stochSignal: {stochSignal}"
        return True

    return False


def control_strategy_12(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT is True:
        return False

    if BTC_SIGNAL is False and BTC_INNER_SIGNAL is False:
        return False


    curColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    if curColor == cons.CANDLE_RED:
        return False


    vwap = ind.get_vwma(data=coinTicker.closes, volume=coinTicker.volumes, period=VWMA_PERIOD)

    slope, average, intercept = ind.get_linear_regression_slope(prices=vwap, period=15)
    # sl, ic = ind.get_slope_scipy(data=vwap, period=30)
    if slope < float(0.0):
        return False

    curVWAP = func.round_tick_size(price=vwap[-1], tick_size=tickSize)
    prevVWAP = func.round_tick_size(price=vwap[-2], tick_size=tickSize)

    sma = ind.sma(data=coinTicker.closes, period=15)
    curSMA = func.round_tick_size(price=sma[-1], tick_size=tickSize)
    prevSMA = func.round_tick_size(price=sma[-2], tick_size=tickSize)

    curClose = coinTicker.closes[-1]
    curLow = coinTicker.lows[-1]
    curHigh = coinTicker.highs[-1]
    prevHigh = coinTicker.highs[-2]
    prevOpen = coinTicker.opens[-2]
    prevLow = coinTicker.lows[-2]

    vwSignal = False
    # Fiyat VWAP çizgisiüzerinde
    if curLow <= curVWAP <= curHigh:
        vwSignal = True

    if vwSignal is False:
        return False

    if curVWAP < curSMA:
        return False

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=15)
    curRSIFast = rsi[-1]
    prevRSIFast = rsi[-2]

    rsiMA = ind.get_ema(data=rsi, period=15)
    curRSISlow = rsiMA[-1]
    prevRSISlow = rsiMA[-2]

    rsiSignal = False
    # RSI hızlı, yavaşın üzerinde
    # RSI slow eğim yukarı doğru
    if curRSIFast > curRSISlow:
        if curRSISlow > prevRSISlow:
            rsiSignal = True

    if rsiSignal is False:
        return False

    fast, slow = ind.get_stochastic(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                    timePeriod=15, slowKPeriod=3, slowDPeriod=3)
    curSTOCHFast = fast[-1]
    prevSTOCHFast = fast[-2]
    curSTOCHSlow = slow[-1]
    prevSTOCHSlow = slow[-2]

    stochSignal = False
    # STOCHASTIC hızlı, yavaşın üstünde
    # STOCHASTIC eğim yukarı doğru
    if curSTOCHFast > curSTOCHSlow:
        if curSTOCHFast > prevSTOCHFast:
            stochSignal = True

    if (rsiSignal is True) and (vwSignal is True) and (stochSignal is True):
        log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} *S12")
        log(f"   VWAP   curLow < curVWAP < curHigh: {curLow} < {curVWAP} < {curHigh} ")
        log(f"          Slope: {slope}")
        log(f"   RSI    curRSIFast > curRSISlow: {curRSIFast} > {curRSISlow} ")
        log(f"          curRSISlow > prevRSISlow: {curRSISlow} > {prevRSISlow} ")
        log(f"   STOCH  curSTOCHFast > curSTOCHSlow: {curSTOCHFast} > {curSTOCHSlow} ")
        log(f"   ------ ")
        glbExplanation = f" {symbol} vwap Slope: {slope}"
        return True

    return False


def control_strategy_13(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT == True:
        return False

    if (BTC_SIGNAL == False) and (BTC_INNER_SIGNAL == False):
        return False

    # ROC Kontrolleri
    roc = ind.get_roc(data=coinTicker.closes, period=ROC_PERIOD)
    rocBelowZero = func.below_level(roc, float(0.0), 3)
    # (roc[-1] < 0.0) and (roc[-2] < 0.0) and (roc[-3] < 0.0)
    if (rocBelowZero == False):
        return False

    rocDeepTurn = func.deep_turn(roc)  # roc[-1] > roc[-2] < roc[-3]
    if rocDeepTurn == False:
        return False

    # MACD Kontrolleri
    fastMacd, slowMacd, histMacd = ind.get_macd(data=coinTicker.closes, fastPeriod=MACD_FAST,
                                              slowPeriod=MACD_SLOW, signalPeriod=MACD_SIGNAL)
    histBelowZero = func.below_level(histMacd, float(0.0), 3)
    # (histMacd[-1] < 0.0) and (histMacd[-2] < 0.0) and (histMacd[-3] < 0.0)
    if (histBelowZero == False):
        return False

    fastMACDBelowZero = func.below_level(fastMacd, float(0.0), 3)
    # (fastMacd[-1] < 0.0) and (fastMacd[-2] < 0.0) and (fastMacd[-3] < 0.0)
    if (fastMACDBelowZero == False):
        return False

    slowMACDBelowZero = func.below_level(slowMacd, float(0.0), 3)
    # (slowMacd[-1] < 0.0) and (slowMacd[-2] < 0.0) and (slowMacd[-3] < 0.0)
    if (slowMACDBelowZero == False):
        return False

    # STOCHASTIC Kontrolleri
    fastST, slowST = ind.get_stochastic(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                        timePeriod=STOCH_PERIOD, slowKPeriod=3, slowDPeriod=3)
    stochThreshold = 20.0
    stochBelowThreshold = (fastST[-1] < stochThreshold) or (slowST[-1] < stochThreshold)
    if (stochBelowThreshold == False):
        return False

    stochDeepTurn = func.deep_turn(fastST)
    # (fastST[-1] > fastST[-2] < fastST[-3])
    if stochDeepTurn == False:
        return False

    stochCrossUp = func.cross_over(fastST, slowST)
    # (slowST[-2] > fastST[-2]) and (fastST[-1] > slowST[-1])
    if stochCrossUp == False:
        return False

    # LR Kontrolleri
    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=LR_STD_UP_FACTOR,
                                                        standard_deviation_down_factor=LR_STD_DOWN_FACTOR)
    # LR alt ve üst bandı arasındaki yüksekliğin belli bir oranın üstünde olması aranır.
    hFactor = LR_UP_HEIGHT
    if slope < 0:
        hFactor = LR_DOWN_HEIGHT
    heightLR = func.calculate_ratio(float(up[-1]), float(down[-1]))
    heightLRCond = heightLR > hFactor
    if heightLRCond == False:
        return False

    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} * S13 *")
    log(f"          rocDeepTurn: {rocDeepTurn} stochDeepTurn: {stochDeepTurn} stochCrossUp: {stochCrossUp}")
    glbExplanation = f" {symbol} rocDeepTurn: {rocDeepTurn} stochDeepTurn: {stochDeepTurn} stochCrossUp: {stochCrossUp}"
    return True


def control_strategy_14(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT == True:
        return False

    if (BTC_SIGNAL == False) and (BTC_INNER_SIGNAL == False):
        return False

    # ROC Kontrolleri
    roc = ind.get_roc(data=coinTicker.closes, period=ROC_PERIOD)
    rocBelowZero = func.below_level(roc, float(0.0), 3)
    if (rocBelowZero == False):
        return False

    rocSlopeUp = roc[-1] > roc[-2] > roc[-3]
    if rocSlopeUp == False:
        return False

    # MACD Kontrolleri
    fastMacd, slowMacd, histMacd = ind.get_macd(data=coinTicker.closes, fastPeriod=MACD_FAST,
                                              slowPeriod=MACD_SLOW, signalPeriod=MACD_SIGNAL)
    histBelowZero = func.below_level(histMacd, float(0.0), 3)
    if (histBelowZero == False):
        return False

    fastMACDBelowZero = func.below_level(fastMacd, float(0.0), 3)
    if (fastMACDBelowZero == False):
        return False

    slowMACDBelowZero = func.below_level(slowMacd, float(0.0), 3)
    if (slowMACDBelowZero == False):
        return False

    # STOCHASTIC Kontrolleri
    fastST, slowST = ind.get_stochastic(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                        timePeriod=STOCH_PERIOD, slowKPeriod=3, slowDPeriod=3)
    stochThreshold = 20.0
    stochBelowThreshold = (fastST[-1] < stochThreshold) or (slowST[-1] < stochThreshold)
    if (stochBelowThreshold == False):
        return False

    stochSlopeUp = fastST[-1] > fastST[-2] > fastST[-3]
    if stochSlopeUp == False:
        return False

    # LR Kontrolleri
    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=LR_STD_UP_FACTOR,
                                                        standard_deviation_down_factor=LR_STD_DOWN_FACTOR)
    # LR alt ve üst bandı arasındaki yüksekliğin belli bir oranın üstünde olması aranır.
    hFactor = LR_UP_HEIGHT
    if slope < 0:
        hFactor = LR_DOWN_HEIGHT
    heightLR = func.calculate_ratio(float(up[-1]), float(down[-1]))
    heightLRCond = heightLR > hFactor
    if heightLRCond == False:
        return False

    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} * S14 *")
    log(f"          stochSlopeUp: {stochSlopeUp} rocSlopeUp: {rocSlopeUp} ")
    glbExplanation = f" {symbol} stochSlopeUp: {stochSlopeUp} rocSlopeUp: {rocSlopeUp}"
    return True


def control_strategy_15(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT == True:
        return False

    if (BTC_SIGNAL == False) and (BTC_INNER_SIGNAL == False):
        return False

    # ROC Kontrolleri
    roc = ind.get_roc(data=coinTicker.closes, period=ROC_PERIOD)
    rocBelowZero = func.below_level(roc, float(0.0), 3)
    if rocBelowZero == False:
        return False

    rocDeepTurn = func.deep_turn(roc)
    if rocDeepTurn == False:
        return False

    # MACD Kontrolleri
    fastMacd, slowMacd, histMacd = ind.get_macd(data=coinTicker.closes, fastPeriod=MACD_FAST,
                                              slowPeriod=MACD_SLOW, signalPeriod=MACD_SIGNAL)
    histBelowZero = func.below_level(histMacd, float(0.0), 3)
    if (histBelowZero == False):
        return False

    fastMACDBelowZero = func.below_level(fastMacd, float(0.0), 3)
    if (fastMACDBelowZero == False):
        return False

    slowMACDBelowZero = func.below_level(slowMacd, float(0.0), 3)
    if (slowMACDBelowZero == False):
        return False

    # STOCHASTIC Kontrolleri
    fastST, slowST = ind.get_stochastic(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                        timePeriod=STOCH_PERIOD, slowKPeriod=3, slowDPeriod=3)
    stochThreshold = 20.0
    stochBelowThreshold = (fastST[-1] < stochThreshold) or (slowST[-1] < stochThreshold)
    if (stochBelowThreshold == False):
        return False

    stochSlopeUp = fastST[-1] > fastST[-2] > fastST[-3]
    if stochSlopeUp == False:
        return False

    stochCrossUp = func.cross_over(fastST, slowST)
    if stochCrossUp == False:
        return False

    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=LR_STD_UP_FACTOR,
                                                        standard_deviation_down_factor=LR_STD_DOWN_FACTOR)
    # LR alt ve üst bandı arasındaki yüksekliğin belli bir oranın üstünde olması aranır.
    hFactor = LR_UP_HEIGHT
    if slope < 0:
        hFactor = LR_DOWN_HEIGHT
    heightLR = func.calculate_ratio(float(up[-1]), float(down[-1]))
    heightLRCond = heightLR > hFactor
    if heightLRCond == False:
        return False

    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} * S15 *")
    log(f"          rocDeepTurn: {rocDeepTurn} stochCrossUp: {stochCrossUp} stochSlopeUp: {stochSlopeUp}")
    glbExplanation = f" {symbol} rocDeepTurn: {rocDeepTurn} stochCrossUp: {stochCrossUp} stochSlopeUp: {stochSlopeUp}"
    return True


def control_strategy_16(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT == True:
        return False

    # if (BTC_SIGNAL == False) and (BTC_INNER_SIGNAL == False):
    #     return False
    # TODO: BTC_INNER_SIGNAL = False iken sonuçları kontrol et
    # if (BTC_SIGNAL == False):
    #     return False

    # LR eğim pozitif ise standart sapma 2.3
    # LR eğim negatif ise standart sapma 2.8
    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=LR_STD_UP_FACTOR,
                                                        standard_deviation_down_factor=LR_STD_DOWN_FACTOR)
    downPrice = down[-1]
    closePrice = coinTicker.closes[-1]
    # Fiyat LR alt bandının altına inmiş ise TRUE
    closeBelowDownLR = downPrice > closePrice
    if closeBelowDownLR == False:
        return False

    # LR alt ve üst bandı arasındaki yüksekliğin belli bir oranın üstünde olması aranır.
    hFactor = LR_UP_HEIGHT
    if slope < 0:
        hFactor = LR_DOWN_HEIGHT
    heightLR = func.calculate_ratio(float(up[-1]), float(down[-1]))
    heightLRCond = heightLR > hFactor
    if heightLRCond == False:
        return False

    fastST, slowST = ind.get_stochastic(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                        timePeriod=LR_PERIOD, slowKPeriod=3, slowDPeriod=3)
    # TODO: stokastik kaldırılmalı mı? Sonuçları kontrol et.
    stochLevel = 50.0
    if slope < 0.0 or BTC_SLOPE < 0.0:
        stochLevel = 20.0
    if (fastST[-1] > stochLevel) or (slowST[-1] > stochLevel):
        return False

    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} * S16 *")
    log(f"          coin slope: {slope} BTC_SLOPE: {BTC_SLOPE}")
    log(f"          LRDown > Close: {downPrice} > {coinTicker.closes[-1]}  ")
    log(f"          stochLevel: {stochLevel} fastST:{fastST[-1]} slowST: {slowST[-1]}")
    log(f"          LR Height: %{heightLR}")
    glbExplanation = f" LRDown > Close: {downPrice} > {closePrice} heightLR%: {heightLR}"
    return True


def control_strategy_17(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    if BTC_RED_ALERT == True:
        return False

    # if (BTC_SIGNAL == False) and (BTC_INNER_SIGNAL == False):
    #     return False
    # TODO: BTC_INNER_SIGNAL = False iken sonuçları kontrol et
    # if (BTC_SIGNAL == False):
    #     return False

    # LR eğim pozitif ise standart sapma 2.3
    # LR eğim negatif ise standart sapma 2.8
    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=2.0,
                                                        standard_deviation_down_factor=2.3)
    if slope < 0:
        return False

    downPrice = down[-1]
    closePrice = coinTicker.closes[-1]
    # Fiyat LR alt bandının altına inmiş ise TRUE
    isCloseBelowDownLR = downPrice > closePrice
    if isCloseBelowDownLR == False:
        return False

    # LR alt ve üst bandı arasındaki yüksekliğin belli bir oranın üstünde olması aranır.
    hFactor = 1.8 # LR_UP_HEIGHT
    # if slope < 0:
    #     hFactor = 2.2 # LR_DOWN_HEIGHT
    heightLR = func.calculate_ratio(float(up[-1]), float(down[-1]))
    heightLRCond = heightLR > hFactor
    if heightLRCond == False:
        return False

    log(f" {symbol} BTC_SIGNAL: {BTC_SIGNAL} BTC_INNER_SIGNAL: {BTC_INNER_SIGNAL} * S17 *")
    log(f"          coin slope: {slope} BTC_SLOPE: {BTC_SLOPE}")
    log(f"          LRDown > Close: {downPrice} > {coinTicker.closes[-1]}  ")
    log(f"          LR Height: %{heightLR}")
    glbExplanation = f" LRDown > Close: {downPrice} > {closePrice} heightLR%: {heightLR}"
    return True


def control_buy_signal(session=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, int):
    # signal = control_strategy_1(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 1
    #
    # signal = control_strategy_2(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 2
    #
    # signal = control_strategy_3(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 3

    # signal = control_strategy_4(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 4
    #
    # signal = control_strategy_5(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 5
    # signal = control_strategy_6(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 6

    # signal = control_strategy_7(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 7
    #
    # signal = control_strategy_8(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 8
    signal = control_strategy_9(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, 9

    signal = control_strategy_10(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, 10

    # signal = control_strategy_11(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 11

    # signal = control_strategy_12(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if signal is True:
    #     return True, 12
    signal = control_strategy_13(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, 13

    signal = control_strategy_15(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, 15

    signal = control_strategy_16(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, 16

    signal = control_strategy_17(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal == True:
        return True, 17

    return False, 0


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

            buySignal, strategy = control_buy_signal(session=connSession, symbol=coinSymbol,
                                                     coinTicker=coinTicker, tickSize=tickSize)
            if buySignal == False:
                continue

            tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                               url=url_book_ticker,
                                                                               symbol=coinSymbol)
            if tickerStatus == False:
                continue

            """ Tahtada işlem yapmak için yeterli sayıda adet yoksa """
            if (askPrice * askQty) < (MAX_AMOUNT_LIMIT * LIMIT_FACTOR):
                continue

            periodTimestamps = dates[-1]
            periodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            """ Stop fiyatı belirlenir """
            stopType = cons.STOP_TYPE_PREVIOUS_LOW
            if strategy == 3:
                stopType = cons.STOP_TYPE_VOLUME
            else:
                stopType = cons.STOP_TYPE_PROFIT

            stopPrice, stopHeight = get_stop_price(stopType=stopType,
                                                   coinTicker=coinTicker, tickSize=tickSize)

            # stopPrice < %PROFIT_STOP_TRAIL den küçük ise %PROFIT_STOP_TRAIL olarak ayarlanır
            if askPrice != stopPrice:
                stopPercentage = ((askPrice - stopPrice) * 100) / stopPrice
            else:
                stopPercentage = PROFIT_STOP_TRAIL
            if stopPercentage <= PROFIT_STOP_TRAIL:
                stopPercentage = PROFIT_STOP_TRAIL
                newSP = askPrice - (askPrice * stopPercentage) / 100
                log(f"   stopPrice %{PROFIT_STOP_TRAIL} e ayarlandı: eski: {stopPrice} new: {newSP}")
                stopPrice = func.round_tick_size(price=newSP, tick_size=tickSize)

            buyLot = MAX_AMOUNT_LIMIT / askPrice
            buyLot = func.round_step_size(quantity=buyLot, step_size=stepSize)
            buyAmount = buyLot * askPrice
            buyCommission = buyAmount * COMMISSION_RATE

            insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                               price=askPrice, buyLot=buyLot, buyAmount=buyAmount, buyCommission=buyCommission,
                               stopPrice=stopPrice, stopType=stopType,
                               stopHeight=stopHeight, sellTarget=None, period=CANDLE_INTERVAL,
                               periodTime=periodTime, currentPeriodTime=periodTime, signalName=None,
                               explanation=glbExplanation, profitTarget=FIRST_PROFIT_TARGET, strategy=strategy)
            log(f"{SIDE_BUY} {coinSymbol} stop:{stopPrice} buyPrice:{askPrice} {glbExplanation} strategy:{strategy}")
            log(f"  ********** ")
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

            candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle,
                                         symbol=coinSymbol, interval=CANDLE_INTERVAL, limit=LR_PERIOD)
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

            isSamePeriod = str(buyPeriodTime) == str(candlePeriodTime)
            if (stopType != cons.STOP_TYPE_VOLUME):
                if (isSamePeriod == True) and (stopPrice < buyPrice):
                    # Coin in alındığı periyot içinde low değeri stopPrice ın altına düşerse
                    # yeni stopPrice low[-1] yapılır
                    if stopPrice > lows[-1]:
                        newStopPrice = lows[-1]
                        stopChangeCount += 1

                        update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                                             currentPeriodTime=candlePeriodTime,
                                                             stopChangeCount=stopChangeCount)
                        log(f"  *** STOP UPDATE in BUY PERIOD *** {coinSymbol} Buy:{buyPrice} Old:{stopPrice} New:{newStopPrice} %:{round((newStopPrice - buyPrice) / buyPrice * 100, 3)}")
                        continue

            coinTicker = TickerData(opens=opens, highs=highs, lows=lows, closes=closes, volumes=volumes, dates=dates)
            """ STOP kontrolleri en son işlem görmüş (close) fiyat ile yapılır """
            # Satın alındığı periyotta değilse
            # Veya satın alındığı periyotta stopPrice > buyPrice ise stop control yapılır.
            isStop = False
            if (isSamePeriod == False) or ((isSamePeriod == True) and (stopPrice > buyPrice)):
                isStop = stop_control(currentPrice=closes[-1], stopPrice=stopPrice)
            isProfit = False
            isProfit = profit_control(coinTicker=coinTicker, currentPrice=closes[-1],
                                      buyingPrice=buyPrice, strategy=strategy)
            isLRStop = False
            if strategy == 16:
                isLRStop = lr_stop_control(coinTicker=coinTicker, stopPrice=stopPrice, buyPrice=buyPrice, tickSize=tickSize)
            if (isStop == True) or (isProfit == True) or (isLRStop == True):
                """ Coin in anlık tahta fiyatı okunur. """
                tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                                   url=url_book_ticker,
                                                                                   symbol=coinSymbol)
                if tickerStatus == False:
                    continue

                # TODO: Burada SATIM işlemi yapılacak
                sellAmount = buyLot * bidPrice
                sellAmount = func.round_tick_size(price=sellAmount, tick_size=tickSize)
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
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {buyTotal} Sell: {sellTotal} price:{bidPrice} strategy:{strategy} KAR: {kar} (%{round(yuzde, 2)}) >>>")
                else:
                    zarar = buyTotal - sellTotal
                    yuzde = (zarar * 100) / sellTotal
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {buyTotal} Sell: {sellTotal} price:{bidPrice} strategy:{strategy} ZARAR: {zarar} (%{round(yuzde, 2)}) <<<")
                log(f"  ********** ")
                continue
            # end if (isStop is True) or (isProfit is True) or (isLRStop == True):

            isProfitTarget = False
            newProfitTarget = None
            if stopType == cons.STOP_TYPE_PROFIT or stopType == cons.STOP_TYPE_VOLUME:
                isProfitTarget, newProfitTarget = profit_target_control(currentPrice=closes[-1],
                                                                        buyingPrice=buyPrice,
                                                                        profitTarget=profitTarget,
                                                                        strategy=strategy)
            if isProfitTarget == True:
                update_profit_target(dbCursor=dbCursor, symbol=coinSymbol, profitTarget=newProfitTarget)
                log(f"  *** PROFIT TARGET UPDATE *** {coinSymbol} old: {profitTarget} New: {newProfitTarget}")
                newStopPrice = get_stop_price_under_target(currentPrice=closes[-1], buyingPrice=buyPrice,
                                                           profitTarget=newProfitTarget,
                                                           tickSize=tickSize, strategy=strategy)
                if newStopPrice <= stopPrice:
                    continue

                stopChangeCount += 1

                update_stop_price(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                  stopChangeCount=stopChangeCount)
                log(f"  *** NEWSTOP UPDATE UNDER PROFIT *** {coinSymbol} Buy:{buyPrice} Old:{stopPrice} New:{newStopPrice} fark:{newStopPrice - buyPrice} %:{round((newStopPrice - buyPrice) / buyPrice * 100, 3)}")
            # end if isProfitTarget is True:

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
