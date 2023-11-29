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

#region BTC Parameters
BTC_CANDLE_INTERVAL = "5m"
BTC_CANDLE_COUNT = 320
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
#endregion

CANDLE_INTERVAL = "5m"           # Okunacak mum verisi periyodu
CANDLE_COUNT = 320               # Okunacak mum verisi adeti
MAX_TRADE_LIMIT = float(100)
COMMISSION_RATE = float(0.00075)    # %0.075
LIMIT_FACTOR = float(5)
TARGET_TYPE = cons.TARGET_TYPE_PROFIT_RATIO

#region INDICATORS Parameters
ATR_PERIOD = 5
ATR_MULTIPLIER = float(3)
ATR_STOP_FACTOR = float(0.5)
HMA_FAST_PERIOD = 46
HMA_SLOW_PERIOD = 200
SRSI_PERIOD = 5
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
#endregion

RISK_REWARD_RATIO = float(1.0)
RISK_REWARD_RATIO_48 = float(1.5)
RISK_REWARD_RATIO_54 = float(1.5)
MAX_ACCEPTABLE_RISK = float(4.0)  # %3
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

TEST_NAME = "UTBot, HMA, SQZMOM"
glbExplanation = ""
logging.basicConfig(filename="debug_test_utbot", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )

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

    # TODO: deneme amaçlı yapıldı. Stop %0.65 den büyük ise %0.65 e çekmek için.
    ratio = func.calculate_ratio(coinTicker.closes[-1], stopPrice)
    if ratio > 0.65:
        ratio = 0.65
        stopPrice = ((100 - ratio) * coinTicker.closes[-1]) / 100
        log(f" -> Stop %0.65 e çekildi")

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
    if maxPrice < buyPrice:
        return False, 0.0

    currentStopRatio = func.calculate_ratio(stopPrice, buyPrice)

    maxPriceRatio = func.calculate_ratio(maxPrice, buyPrice)
    maxPriceRatio = round(maxPriceRatio, 3)
    if maxPriceRatio < 0.65:
        return False, 0.0

    if maxPriceRatio >= 6:
        stopRatio = maxPriceRatio - 2.0
    else:
        stopRatio = (0.7 * maxPriceRatio) - 0.2

    newStopPrice = buyPrice * (1 + (stopRatio / 100))

    ts = tickSize
    coef = 0
    while ts < 1:
        coef = coef + 1
        ts = ts * 10

    newStopPrice = round(newStopPrice, coef)
    newStopPrice = func.round_tick_size(price=newStopPrice, tick_size=tickSize)

    if newStopPrice <= stopPrice:
        return False, 0.0

    log(f"  * NEW STOP PRICE * {symbol} Buyprice: {buyPrice} Old: {stopPrice} New: {newStopPrice} Ratio: {stopRatio} maxPriceRatio:{maxPriceRatio}")

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

    if strategy == 55:
        targetRatio = PROFIT_TARGET_55
    elif strategy == 56:
        targetRatio = PROFIT_TARGET_56
    else:
        if stopPrice > float(0.0):
            riskRatio = func.calculate_ratio(price, stopPrice)
            targetRatio = riskRatio * RISK_REWARD_RATIO
            targetPrice = price * (1 + (targetRatio / 100))

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

        """ 
        TODO: Aşağıdaki kısım deneme amaçlı eklendi.
              %0.65 kara ulaşınca satış yapılacak.
        """
        if (profit >= 0.65):
            log(f"   BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
            log(f"   targetProfit: %{targetProfit} profit: %{profit} <0.65>")
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
def control_strategy_57(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    """
    UT BOT Alım sinyali
    Squeeze Momentum, mevcut bar < 0 VE mevcut bar > önceki bar
    """
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
    """
    UT BOT Alım sinyali
    Smoothed RSI eğim yukarı ve 0.50 den büyük,
    Not: 2h periyotta karlı devam ediyordu
    """
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=58)

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=1)
    isUTBuySignal = buySignal[-1]
    if isUTBuySignal == False:
        return response

    srsi = ind.get_srsi(data=coinTicker.closes, period=SRSI_PERIOD)

    isSRSI = (srsi[-1] > srsi[-2]) and (srsi[-1] > 0.50)
    if isSRSI == False:
        return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = 0.0
    response.targetRatio = 0.0

    log(f" {symbol} SRSI, UTBOT *S{response.strategy}*")
    log(f"          UT BOT Alım, Smoothed RSI eğim yukarı ve 0.50 den büyük")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")
    log(f"          SRSI: {isSRSI} prev:{srsi[-2]} cur:{srsi[-1]}")

    return response


def control_strategy_59(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    """
    UT BOT Alım sinyali
    HMA Slow eğim yukarı,
    HMA Fast eğim yukarı
    Squeeze Momentum mevcut bar > önceki bar
    """
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=59)

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=1)
    isUTBuySignal = buySignal[-1]
    if isUTBuySignal == False:
        return response

    hmaSlow = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    isHMASlowSlopeUp = hmaSlow[-1] >= hmaSlow[-2]
    if isHMASlowSlopeUp == False:
        return response

    hmaFast = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
    isHMAFastSlopeUp = hmaFast[-1] > hmaFast[-2]
    if isHMAFastSlopeUp == False:
        return response

    sqzMom = ind.get_squeeze_momentum(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                      BBPeriod=20, BBMultFactor=2, KCPeriod=20, KCMultFactor=1.5,
                                      stdDev=0, useTrueRange=True)

    isSqzMom = (sqzMom[-1] > sqzMom[-2])
    if isSqzMom == False:
        return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = 0.0
    response.targetRatio = 0.0

    log(f" {symbol} HMA, SQZMOM, UTBOT *S{response.strategy}*")
    log(f"          UT BOT Alım, HMASlow eğim yukarı, HMAFast eğim yukarı, SQZMOM mevcut bar > önceki bar")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")
    log(f"          targetRatio: {response.targetRatio}")
    log(f"          SQZMOM Prev:{sqzMom[-2]} Cur:{sqzMom[-1]}")
    log(f"          HMA Slow Prev:{hmaSlow[-2]} Cur:{hmaSlow[-1]}")

    return response


def control_strategy_60(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    """
    UT BOT Alım sinyali
    TSI > Signal ve Signal < 0
    """
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=60)

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=1)
    isUTBuySignal = buySignal[-1]
    if isUTBuySignal == False:
        return response

    tsi, signal = ind.get_tsi(data=coinTicker.closes, long=25, short=13, signal=13)

    isTSI = (signal[-1] < 0) and (tsi[-1] > signal[-1])
    if isTSI == False:
        return response

    sar = ind.get_sar(high=coinTicker.highs, low=coinTicker.lows, acceleration=0.02, maximum=0.2)
    isSAR = sar[-1] <= coinTicker.lows[-1]
    if isSAR == False:
        return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    targetPrice = 0.0
    targetRatio = 0.0
    if TARGET_TYPE == cons.TARGET_TYPE_PROFIT_RATIO:
        targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                     strategy=response.strategy)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = targetPrice
    response.targetRatio = round(targetRatio, 2)

    log(f" {symbol} TSI, UTBOT *S{response.strategy}*")
    log(f"          UT BOT Alım, TSI > Signal ve signal < 0")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")
    log(f"          targetRatio: {response.targetRatio}")
    log(f"          TSI: {isTSI} tsi:{tsi[-1]} signal:{signal[-1]}")
    log(f"          SAR Up {sar[-1]} <= low: {coinTicker.lows[-1]}")

    return response


def control_strategy_61(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    """
    UT BOT Alım sinyali
    TSI > Signal
    (HMASlowSlopeUp ve price > HMASlow) VEYA (HMAFastSlopeUp ve price > HMAFast)
    """
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=61)

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=1)
    isUTBuySignal = buySignal[-1]
    if isUTBuySignal == False:
        return response

    tsi, signal = ind.get_tsi(data=coinTicker.closes, long=25, short=13, signal=13)

    isTSI = (tsi[-1] > signal[-1])
    if isTSI == False:
        return response

    hmaSlow = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    hmaFast = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
    isHMASlowSlopeUp = (hmaSlow[-1] > hmaSlow[-2]) and (coinTicker.closes[-1] > hmaSlow[-1])
    isHMAFastSlopeUp = (hmaFast[-1] > hmaFast[-2]) and (coinTicker.closes[-1] > hmaFast[-1])
    if (isHMASlowSlopeUp == False) and (isHMAFastSlopeUp == False):
        return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    targetPrice = 0.0
    targetRatio = 0.0
    if TARGET_TYPE == cons.TARGET_TYPE_PROFIT_RATIO:
        targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                     strategy=response.strategy)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = targetPrice
    response.targetRatio = round(targetRatio, 2)

    log(f" {symbol} TSI, UTBOT *S{response.strategy}*")
    log(f"          UT BOT Alım, TSI > Signal, HMASlowSlopeUp")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")
    log(f"          targetRatio: {response.targetRatio}")
    log(f"          TSI: {isTSI} tsi:{tsi[-1]} signal:{signal[-1]}")
    log(f"          isHMASlowSlopeUp {isHMASlowSlopeUp} isHMAFastSlopeUp {isHMAFastSlopeUp}")

    return response


def control_strategy_62(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    """
    UT BOT Alım sinyali
    MACD X Signal
    """
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=62)

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=300)
    isUTBuySignal = buySignal[-1]
    if isUTBuySignal == False:
        return response

    fastMacd, slowMacd, histMacd = ind.get_macd(data=coinTicker.closes, fastPeriod=MACD_FAST,
                                                slowPeriod=MACD_SLOW, signalPeriod=MACD_SIGNAL)
    isMACDCrossUp = (histMacd[-2] < 0) and (histMacd[-1] > 0) and (slowMacd[-1] < 0)
    if isMACDCrossUp == False:
        return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    targetPrice = 0.0
    targetRatio = 0.0
    if TARGET_TYPE == cons.TARGET_TYPE_PROFIT_RATIO:
        targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                     strategy=response.strategy)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = targetPrice
    response.targetRatio = round(targetRatio, 2)

    log(f" {symbol} TSI, UTBOT *S{response.strategy}*")
    log(f"          UT BOT Alım, MACD Cross Up")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")
    log(f"          targetRatio: {response.targetRatio}")
    log(f"          MACD: {isMACDCrossUp} prev:{histMacd[-2]} cur:{histMacd[-1]} signal:{slowMacd[-1]}")

    return response


def control_strategy_63(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    """
    UT BOT Alım sinyali
    STC Slope Up (Yeşil) ve STC <= 20)
    HMA Slope Up
    HMASlow Slope Up
    SMISignal < 0 ve SMI > SMISignal
    """
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=63)

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=3, period=117)
    isUTBuySignal = buySignal[-1]
    if isUTBuySignal == False:
        return response

    stc = ind.get_stc(data=coinTicker.closes, period=80, fastLength=27, slowLength=50)
    isSTC = (stc[-1] > stc[-2]) and (stc[-1] <= 20)
    if isSTC == False:
        return response

    hma = ind.get_hma(data=coinTicker.closes, period=HMA_FAST_PERIOD)
    isHMASlopeUp = hma[-1] > hma[-2]
    if isHMASlopeUp == False:
        return response

    hmaSlow = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    isHMASlowSlopeUp = (hmaSlow[-1] >= hmaSlow[-2]) or (coinTicker.closes[-1] > hmaSlow[-1])
    if isHMASlowSlopeUp == False:
        return response

    smi, smiSignal = ind.get_smi(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                 kLength=32, dLength=4, emaPeriod=2)
    isSMA = (smiSignal[-1] < 0) and (smi[-1] > smiSignal[-1])
    if isSMA == False:
        return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker, period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    targetPrice = 0.0
    targetRatio = 0.0
    if TARGET_TYPE == cons.TARGET_TYPE_PROFIT_RATIO:
        targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                     strategy=response.strategy)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = targetPrice
    response.targetRatio = round(targetRatio, 2)

    log(f" {symbol} UTBOT, STC, HMA *S{response.strategy}*")
    log(f"          UT BOT Alım, STC Slope Up ve <= 20, HMA Slope Up, SMISignal < 0 ve SMI > SMISignal")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")
    log(f"          targetRatio: {response.targetRatio}")
    log(f"          STC: {isSTC} prev:{stc[-2]} cur:{stc[-1]} ")
    log(f"          HMA: {isHMASlopeUp} prev:{hma[-2]} cur:{hma[-1]} ")
    log(f"          HMASlow: {isHMASlowSlopeUp} prev:{hmaSlow[-2]} cur:{hmaSlow[-1]} ")

    return response


def control_strategy_64(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    """
    UT BOT Alım sinyali
    STC Slope Up (Yeşil) ve STC >= 80)
    HMAFast Slope Up,
    HMASlow Slope Up,
    HMAFast Over HMASlow
    """

    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=64)

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=4)
    isUTBuySignal = buySignal[-1]
    if isUTBuySignal == False:
        return response

    stc = ind.get_stc(data=coinTicker.closes, period=80, fastLength=27, slowLength=50)
    isSTC = (stc[-1] > stc[-2]) and (stc[-1] >= 80)
    if isSTC == False:
        return response

    hmaFast = ind.get_hma(data=coinTicker.closes, period=46)
    isHMAFastSlopeUp = hmaFast[-1] > hmaFast[-2]
    if isHMAFastSlopeUp == False:
        return response

    hmaSlow = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    isHMASlowSlopeUp = hmaSlow[-1] > hmaSlow[-2]
    if isHMASlowSlopeUp == False:
        return response

    isHMAFastOverSlow = hmaFast[-1] > hmaSlow[-1]
    if isHMAFastOverSlow == False:
        return response


    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker,
                                      period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    targetPrice = 0.0
    targetRatio = 0.0
    if TARGET_TYPE == cons.TARGET_TYPE_PROFIT_RATIO:
        targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                     strategy=response.strategy)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = targetPrice
    response.targetRatio = round(targetRatio, 2)

    log(f" {symbol} UTBOT, STC Over 80, HMAFast Over HMASlow *S{response.strategy}*")
    log(f"          UT BOT Alım, STC Slope Up ve >= 80, HMAFast Over HMASlow and HMAs Slope Up")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")
    log(f"          targetRatio: {response.targetRatio}")
    log(f"          STC: {isSTC} prev:{stc[-2]} cur:{stc[-1]} ")
    log(f"          HMAFast: {isHMAFastSlopeUp} prev:{hmaFast[-2]} cur:{hmaFast[-1]} ")
    log(f"          HMASlow: {isHMASlowSlopeUp} prev:{hmaSlow[-2]} cur:{hmaSlow[-1]} ")

    return response


def control_strategy_65(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    """
    UT BOT Alım sinyali
    HMAFast Slope Up,
    HMASlow Slope Down,
    HMAFast Under HMASlow
    STC slope up ve STC <= 20
    """
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=65)

    buySignal, sellSignal = ind.get_ut_bot_alerts(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                                  sensivity=2, period=4)
    isUTBuySignal = buySignal[-1]
    if isUTBuySignal == False:
        return response

    hmaSlow = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    isHMASlowSlopeDown = hmaSlow[-1] < hmaSlow[-2]
    if isHMASlowSlopeDown == False:
        return response

    hmaFast = ind.get_hma(data=coinTicker.closes, period=46)
    isHMAFastSlopeUp = hmaFast[-1] > hmaFast[-2]
    if isHMAFastSlopeUp == False:
        return response

    isHMAFastUnderSlow = hmaFast[-1] < hmaSlow[-1]
    if isHMAFastUnderSlow == False:
        return response

    stc = ind.get_stc(data=coinTicker.closes, period=60, fastLength=27, slowLength=50)
    isSTC = (stc[-1] > stc[-2]) and (stc[-1] <= 20)
    if isSTC == False:
        return response


    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker,
                                      period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    targetPrice = 0.0
    targetRatio = 0.0
    if TARGET_TYPE == cons.TARGET_TYPE_PROFIT_RATIO:
        targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                     strategy=response.strategy)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = targetPrice
    response.targetRatio = round(targetRatio, 2)

    log(f" {symbol} UTBOT, HMASlow Slope Down, HMAFast Slow Up, HMAFast Unde HMASlow *S{response.strategy}*")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")
    log(f"          targetRatio: {response.targetRatio}")
    log(f"          isHMASlowSlopeDown: {isHMASlowSlopeDown} isHMAFastSlopeUp: {isHMAFastSlopeUp} isHMAFastUnderSlow: {isHMAFastUnderSlow}")

    return response


def control_strategy_66(session=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    """
    SMI Cross UP
    """
    response = StrategyResponse(signal=False, stopPrice=0.0, targetPrice=0.0, targetRatio=0.0, strategy=66)

    smi, smiSignal = ind.get_smi(high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                                 kLength=32, dLength=4, emaPeriod=2)
    isSMICrossOver = func.cross_over(smi, smiSignal)
    if isSMICrossOver == False:
        return response
    isSMISignal = smiSignal[-1] < -40.0
    if (isSMISignal == False):
        return response

    # hma = ind.get_hma(data=coinTicker.closes, period=HMA_SLOW_PERIOD)
    # isHMASlopeUp = hma[-1] > hma[-2]
    #
    # if isHMASlopeUp == True:
    #     isSMISignal = smiSignal[-1] < -50.0
    #     if (isSMISignal == False):
    #         return response
    # else:
    #     isSMISignal = smiSignal[-1] < -70.0
    #     if (isSMISignal == False):
    #         return response

    stopPrice = get_stop_price_by_atr(coinTicker=coinTicker,
                                      period=ATR_PERIOD, multiplier=ATR_MULTIPLIER)
    stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)

    targetPrice = 0.0
    targetRatio = 0.0
    if TARGET_TYPE == cons.TARGET_TYPE_PROFIT_RATIO:
        targetPrice, targetRatio = get_profit_target(price=coinTicker.closes[-1], stopPrice=stopPrice,
                                                     strategy=response.strategy)

    response.signal = True
    response.stopPrice = stopPrice
    response.targetPrice = targetPrice
    response.targetRatio = round(targetRatio, 2)

    log(f" {symbol} SMI Cross Over *S{response.strategy}*")
    log(f"          Price: {coinTicker.closes[-1]} ")
    log(f"          stopPrice: {stopPrice} Ratio:{round(func.calculate_ratio(coinTicker.closes[-1], stopPrice),3)}")
    log(f"          targetRatio: {response.targetRatio}")
    log(f"          isSMICrossOver: {isSMICrossOver} smiCur: {smi[-1]} signalCur: {smiSignal[-1]}")

    return response

#endregion

def control_buy_signal(dbCursor=None, session=None, url=None, symbol=None, coinTicker=None, tickSize=None) -> (StrategyResponse):
    # response = control_strategy_57(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response

    # response = control_strategy_58(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response

    # response = control_strategy_59(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response

    # response = control_strategy_60(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response

    # response = control_strategy_61(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response

    # response = control_strategy_62(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response

    response = control_strategy_63(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if response.signal == True:
        return response

    # response = control_strategy_64(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response
    #
    # response = control_strategy_65(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    # if response.signal == True:
    #     return response

    response = control_strategy_66(session=session, symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
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
            coinTicker = TickerData(opens=df['open'].to_numpy(dtype=float),
                                    highs=df['high'].to_numpy(dtype=float),
                                    lows=df['low'].to_numpy(dtype=float),
                                    closes=df['close'].to_numpy(dtype=float),
                                    volumes=df['volume'].to_numpy(dtype=float),
                                    dates=df['date'].to_numpy(dtype=datetime))

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

            periodTimestamps = coinTicker.dates[-1]
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
            log(f" {SIDE_BUY} {coinSymbol} Price:{askPrice} Stop:{buyResponse.stopPrice} Target:%{buyResponse.targetRatio} Strategy:{buyResponse.strategy}")
            log(f" ********** ")
            alarm()

            telegramMessage = f"{coinSymbol}\n{SIDE_BUY} S{buyResponse.strategy} {CANDLE_INTERVAL}\nPrice:{askPrice}"
            tlg.sendNotification(connSession=connSession, notification=telegramMessage)

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
            coinTicker = TickerData(opens=df['open'].to_numpy(dtype=float),
                                    highs=df['high'].to_numpy(dtype=float),
                                    lows=df['low'].to_numpy(dtype=float),
                                    closes=df['close'].to_numpy(dtype=float),
                                    volumes=df['volume'].to_numpy(dtype=float),
                                    dates=df['date'].to_numpy(dtype=datetime))

            periodTimestamps = coinTicker.dates[-1]
            candlePeriodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            """ STOP kontrolleri en son işlem görmüş (close) fiyat ile yapılır """

            isStop = False
            isProfit = False
            if TARGET_TYPE == cons.TARGET_TYPE_PROFIT_RATIO:
                isProfit = profit_control(currentPrice=coinTicker.closes[-1], buyingPrice=buyPrice,
                                          targetProfit=profitTarget, targetPrice=targetPrice, strategy=strategy)

            isStop = stop_control(currentPrice=coinTicker.closes[-1], stopPrice=stopPrice)
            if (isStop == True) or (isProfit == True):
            # if (isStop == True):
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

                    telegramMessage = f"{coinSymbol}\n{SIDE_SELL} S{strategy} {period}\nBuy:{buyPrice} Sell:{bidPrice}\nKAR:{round(kar,3)} (%{round(yuzde, 2)})"
                    tlg.sendNotification(connSession=connSession, notification=telegramMessage)

                else:
                    zarar = buyTotal - sellTotal
                    yuzde = (zarar * 100) / sellTotal
                    log(f" {SIDE_SELL} {coinSymbol} Buy: {buyTotal} Sell: {sellTotal} price:{bidPrice} strategy:{strategy} ZARAR: {zarar} (%{round(yuzde, 2)}) <<<")

                    telegramMessage = f"{coinSymbol}\n{SIDE_SELL} S{strategy} {period}\nBuy:{buyPrice} Sell:{bidPrice}\nZARAR:{round(zarar,3)} (%{round(yuzde, 2)})"
                    tlg.sendNotification(connSession=connSession, notification=telegramMessage)
                # alarm()
                log(f" ************* ")
                continue
            # end if (isStop is True) or (isProfit is True):

            if minPrice is None:
                minPrice = buyPrice

            if coinTicker.closes[-1] < minPrice:
                minPrice = coinTicker.closes[-1]
                update_profit_target(dbCursor=dbCursor, symbol=coinSymbol, profitTarget=profitTarget, maxPrice=maxPrice, minPrice=minPrice)
                log(f"  * UPDATE MIN PRICE * {coinSymbol} Buy:{buyPrice} Min Price:{minPrice} Stop:{stopPrice}")

            if maxPrice is None:
                maxPrice = buyPrice

            if coinTicker.closes[-1] > maxPrice :
                maxPrice = coinTicker.closes[-1]
                maxPriceRatio = round((maxPrice - buyPrice) / buyPrice * 100, 3)
                update_profit_target(dbCursor=dbCursor, symbol=coinSymbol, profitTarget=profitTarget, maxPrice=maxPrice, minPrice=minPrice)
                log(f"  * UPDATE MAX PRICE * {coinSymbol} Buy:{buyPrice} Max Price:{maxPrice} Max Profit %:{maxPriceRatio} Target:{profitTarget}")

                # if TARGET_TYPE == cons.TARGET_TYPE_STOP_TRAILING:
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

            # if str(currentPeriodTime) != str(candlePeriodTime):
            #     candleBars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle,
            #                                  symbol=coinSymbol, interval=CANDLE_INTERVAL, limit=CANDLE_COUNT)
            #     if (candleBars is None) or (len(candleBars) < CANDLE_COUNT):
            #         continue
            #
            #     df = convert_dataframe(bars=candleBars)
            #     dates = df['date'].to_numpy(dtype=datetime)
            #     opens = df['open'].to_numpy(dtype=float)
            #     highs = df['high'].to_numpy(dtype=float)
            #     lows = df['low'].to_numpy(dtype=float)
            #     closes = df['close'].to_numpy(dtype=float)
            #     volumes = df['volume'].to_numpy(dtype=float)
            #
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
        coinTicker = TickerData(opens=df['open'].to_numpy(dtype=float),
                                highs=df['high'].to_numpy(dtype=float),
                                lows=df['low'].to_numpy(dtype=float),
                                closes=df['close'].to_numpy(dtype=float),
                                volumes=df['volume'].to_numpy(dtype=float),
                                dates=df['date'].to_numpy(dtype=datetime))

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