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

BTC_SIGNAL = False
BTC_CANDLE_INTERVAL = "15m"
BTC_CANDLE_COUNT = 11
BTC_SLOPE_PERIOD = 10
BTC_LSMA_PERIOD = 6

CANDLE_INTERVAL = "15m"     # Okunacak mum verisi periyodu
CANDLE_COUNT = 11          # Okunacak mum verisi adeti
LR_PERIOD = 10
LSMA_PERIOD = 6
BOLLINGER_PERIOD = 10
AVERAGE_CANDLE_COUNT = 10   # Ortalaması hesaplanacak mum adeti
ATR_PERIOD = 6
ATR_STOP_FACTOR = float(0.5)
MAX_AMOUNT_LIMIT = float(100)
LIMIT_FACTOR = float(5)
COMMISSION_RATE = float(0.002)

PROFIT_STRATEGY_10 = float(2.0)
PROFIT_STRATEGY_13 = float(2.0)
PROFIT_STRATEGY_14 = float(2.0)
# VOLUME_WEIGHT = float(30000.0)
# STRATEGY_2 = 2
# USE_PROFIT_RANGE_2 = True
# PROFIT_STRATEGY_2 = float(1.5)
#
# STRATEGY_4 = 4
# USE_PROFIT_RANGE_4 = False
# PROFIT_STRATEGY_4 = float(0.75)
#
# STRATEGY_5 = 5
# USE_PROFIT_RANGE_5 = True
# PROFIT_STRATEGY_5 = float(0.60)
#
# STRATEGY_7 = 7
# USE_PROFIT_RANGE_7 = False
# PROFIT_STRATEGY_7 = float(0.50)
#
# PROFIT_PERCENTAGE = float(1.5)      # Kazanç yüzdesi

IS_LOG = True
IS_PRINT = True
IS_ALARM = True

TEST_NAME = "STRATEJI 14"
glbExplanation = ""
logging.basicConfig(filename="debug_test_lr_signal", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )


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
    trade.mfi = mfi
    trade.fastk = fastk
    trade.slowd = slowd
    trade.pboll = pboll
    trade.strategy = strategy

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

def stop_control(currentPrice=None, stopPrice=None) -> (bool):
    if currentPrice < stopPrice:
        log(f"   STOP StopPrice: {stopPrice} CurrentPrice: {currentPrice}")
        return True
    return False


def lr_stop_control(coinTicker=None, stopPrice=None, buyPrice=None, tickSize=None) -> (bool):
    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=2.2,
                                                        standard_deviation_down_factor=2.2)
    ups = up.to_numpy(dtype=float)
    downs = down.to_numpy(dtype=float)
    upPrice = func.round_tick_size(price=ups[-1], tick_size=tickSize)
    downPrice = func.round_tick_size(price=downs[-1], tick_size=tickSize)
    closePrice = coinTicker.closes[-1]
    prev1Low = coinTicker.lows[-2]

    # Eğim negatife dönmüş ise STOP yapılır
    if slope < 0:
        if closePrice < stopPrice:
            log(f"   === STOP slope NEGATIVE:{slope} closePrice:{closePrice} < stopPrice: {stopPrice}")
            return True
        if closePrice < downPrice:
            log(f"   === STOP slope NEGATIVE:{slope} closePrice:{closePrice} < downPrice:{downPrice}")
            return True

    if slope > 0:
        lsma = ind.get_lsma(data=coinTicker.closes, period=LSMA_PERIOD)
        curLSMA = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
        prev1LSMA = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
        prev2LSMA = func.round_tick_size(price=lsma[-3], tick_size=tickSize)

        if prev2LSMA < prev1LSMA and curLSMA < prev1LSMA:
            log(f"   === STOP slope POSITIVE: prev2LSMA:{prev2LSMA} ve curLSMA:{curLSMA} < prev1LSMA:{prev1LSMA} ")
            return True
        if closePrice < downPrice:
            if closePrice < prev1Low:
                log(f"   === STOP slope POSITIVE: down:{downPrice} > close:{closePrice} < prev1Low:{prev1Low}")
                return True
        if closePrice > upPrice:
            log(f"   === STOP slope POSITIVE: Close:{closePrice} > upPrice:{upPrice}")
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


def profit_control(currentPrice=None, buyingPrice=None, strategy=None) -> (bool):
    if strategy == 10:
        if currentPrice > buyingPrice:
            profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
            if profit >= PROFIT_STRATEGY_10:
                log(f"   PROFIT STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                return True
    if strategy == 13:
        if currentPrice > buyingPrice:
            profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
            if profit >= PROFIT_STRATEGY_13:
                log(f"   PROFIT STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                return True
    if strategy == 14:
        if currentPrice > buyingPrice:
            profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
            if profit >= PROFIT_STRATEGY_14:
                log(f"   PROFIT STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                return True
    return False


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


def control_btc(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    curColor = ind.get_candle_color(open=coinTicker.opens[-1], close=coinTicker.closes[-1])
    prev1Color = ind.get_candle_color(open=coinTicker.opens[-2], close=coinTicker.closes[-2])
    prev2Color = ind.get_candle_color(open=coinTicker.opens[-3], close=coinTicker.closes[-3])

    lsma = ind.get_lsma(data=coinTicker.closes, period=BTC_LSMA_PERIOD)
    curLSMA = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    prev1LSMA = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    prev2LSMA = func.round_tick_size(price=lsma[-3], tick_size=tickSize)

    slope, average, intercept = ind.get_linear_regression_slope(prices=coinTicker.closes, period=BTC_SLOPE_PERIOD)

    # Eğim negatif iken, LSMA dipten dönüş ise TRUE
    if slope < 0:
        if BTC_SIGNAL is True:
            if curLSMA > prev1LSMA:
                return True
        if BTC_SIGNAL is False:
            if (prev2LSMA > prev1LSMA) and (curLSMA > prev1LSMA):
                # log(f" ++++++ BTC TRUE 1 (dipten donus) ++++++ ")
                return True
            if curLSMA > prev1LSMA > prev2LSMA:
                return True
        return False

    # Eğim pozitif ise TRUE
    if slope > 0:
        if BTC_SIGNAL is False:
            if curLSMA < prev1LSMA:
                return False
        if BTC_SIGNAL is True:
            if (prev2LSMA < prev1LSMA) and (curLSMA < prev1LSMA):
                # log(f" ++++++ BTC FALSE 2 (üstten dönüş) ++++++ ")
                return False
            if curLSMA < prev1LSMA < prev2LSMA:
                return False
        return True

    # Yukarıdaki kontrollerde BTC_SIGNAL i değiştirecek bir durum oluşmamışsa eski durum korunur.
    return BTC_SIGNAL


def control_strategy_10(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=2.2,
                                                        standard_deviation_down_factor=2.2)
    downs = down.to_numpy(dtype=float)
    centers = center.to_numpy(dtype=float)
    curLRDown = func.round_tick_size(price=downs[-1], tick_size=tickSize)

    curClose = coinTicker.closes[-1]

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=6)
    curRSI = rsi[-1]
    prevRSI = rsi[-2]

    if slope >= 0:
        if curClose < curLRDown:
            # if prevRSI < 50:
            # if curRSI > prevRSI:
            log(f" {symbol} curClose < curLRDown: {curClose} < {curLRDown} slope:{slope} BTC_SIGNAL:{BTC_SIGNAL}")
            # log(f"          prevRSI < 50: {prevRSI} ")
            # log(f" +++      curRSI > prevRSI: {curRSI} > {prevRSI} ")
            glbExplanation = f" curClose < curLRDown: {curClose} < {curLRDown} slope:{slope}"
            return True
    return False


def control_strategy_11(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    up, center, down, slope = ind.get_linear_regression(prices=coinTicker.closes, period=LR_PERIOD,
                                                        standard_deviation_up_factor=2.2,
                                                        standard_deviation_down_factor=2.2)
    global glbExplanation
    glbExplanation = ""

    downs = down.to_numpy(dtype=float)
    centers = center.to_numpy(dtype=float)
    ups = up.to_numpy(dtype=float)
    curLRDown = func.round_tick_size(price=downs[-1], tick_size=tickSize)
    prev1LRDown = func.round_tick_size(price=downs[-2], tick_size=tickSize)
    curLRCenter = func.round_tick_size(price=centers[-1], tick_size=tickSize)
    curLRUp = func.round_tick_size(price=ups[-1], tick_size=tickSize)

    curClose = coinTicker.closes[-1]
    curOpen = coinTicker.opens[-1]
    prev1Close = coinTicker.closes[-2]
    prev1Open = coinTicker.opens[-2]
    curLow = coinTicker.lows[-1]
    prev1Low = coinTicker.lows[-2]

    curCandleColor = ind.get_candle_color(open=curOpen, close=curClose)
    prev1CandleColor = ind.get_candle_color(open=prev1Open, close=prev1Close)

    if slope < 0:
        rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=6)
        curRSI = rsi[-1]
        prevRSI = rsi[-2]
        if (prevRSI < 25) and (prev1CandleColor == cons.CANDLE_RED) and (curCandleColor == cons.CANDLE_GREEN):
            if prev1Low < prev1LRDown:
                if curRSI > prevRSI:
                    lrFactor = 4
                    if BTC_SIGNAL is True:
                        lrFactor = 2
                    heightLR = (curLRUp - curLRDown)/lrFactor
                    if curClose < (curLRDown + heightLR):
                        log(f" {symbol} prevRSI < 25: {prevRSI} slope:{slope} BTC_SIGNAL: {BTC_SIGNAL}")
                        log(f"          prev1Low < prev1LRDown: {prev1Low} < {prev1LRDown} ")
                        log(f"          curRSI > prevRSI: {curRSI} > {prevRSI} ")
                        log(f" +++      curClose < curLRDown + heightLR3: {curClose} < {curLRDown + heightLR} Factor: {lrFactor}")
                        glbExplanation = f" prevRSI:{prevRSI} prev1Close < prev1LRDown: {prev1Close} < {prev1LRDown} slope:{slope}"
                        return True
    return False


def control_strategy_12(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    curOpen = coinTicker.opens[-1]
    curClose = coinTicker.closes[-1]
    curHigh = coinTicker.highs[-1]
    curLow = coinTicker.lows[-1]

    curCandleColor = ind.get_candle_color(open=curOpen, close=curClose)
    if curCandleColor == cons.CANDLE_RED:
        return False

    start = CANDLE_COUNT - AVERAGE_CANDLE_COUNT - 1
    end = CANDLE_COUNT - 1
    avgVolume = coinTicker.volumes[start:end].mean()
    curVolume = coinTicker.volumes[-1]

    if curVolume > avgVolume * 10:
        log(f" {symbol} curVolume > avgVolume*30: {curVolume} > {avgVolume}")
        hlLevel = curLow + ((curHigh - curLow)/4)
        log(f" ---      curClose < hlLevel: {curClose} < {hlLevel} ")
        if curClose <= hlLevel:
            log(f" +++      curClose <= hlLevel: {curClose} <= {hlLevel} ")
            glbExplanation = f" {symbol} curVolume > avgVolume*30: {curVolume} > {avgVolume} curClose <= hlLevel: {curClose} <= {hlLevel} "
            return True
    return False


def control_strategy_13(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    curClose = coinTicker.closes[-1]
    prev1High = coinTicker.highs[-2]
    prev1Low = coinTicker.lows[-2]

    curOpen = coinTicker.opens[-1]
    curClose = coinTicker.closes[-1]
    curColor = ind.get_candle_color(open=curOpen, close=curClose)

    prev1Open = coinTicker.opens[-2]
    prev1Close = coinTicker.closes[-2]
    prev1Color = ind.get_candle_color(open=prev1Open, close=prev1Close)

    prev2Open = coinTicker.opens[-3]
    prev2Close = coinTicker.closes[-3]
    prev2Color = ind.get_candle_color(open=prev2Open, close=prev2Close)

    prev3Open = coinTicker.opens[-4]
    prev3Close = coinTicker.closes[-4]
    prev3Color = ind.get_candle_color(open=prev3Open, close=prev3Close)

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=6)
    curRSI = rsi[-1]
    prev1RSI = rsi[-2]
    prev2RSI = rsi[-3]
    prev3RSI = rsi[-4]

    lsma = ind.get_lsma(data=coinTicker.closes, period=BTC_LSMA_PERIOD)
    curLSMA = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    prev1LSMA = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    prev2LSMA = func.round_tick_size(price=lsma[-3], tick_size=tickSize)
    prev3LSMA = func.round_tick_size(price=lsma[-4], tick_size=tickSize)

    cv = coinTicker.closes * coinTicker.volumes
    prev1CV = cv[-2]
    prev2CV = cv[-3]
    prev3CV = cv[-4]

    if prev1LSMA > prev2LSMA < prev3LSMA:
        if (prev1RSI >= prev2RSI >= prev3RSI):
            if prev3RSI < 15:
                if prev1Low <= curClose <= prev1High:
                    log(f" {symbol} LSMA: {prev1LSMA} > {prev2LSMA} < {prev3LSMA} ")
                    log(f"          RSI: {prev1RSI} >= {prev2RSI}>= {prev3RSI} ")
                    log(f"          prev3RSI < 15: {prev3RSI} ")
                    log(f" +++      prev1Low <= curClose <= prev1High : {prev1Low} <= {curClose} <= {prev1High} ")
                    glbExplanation = f" LSMA Dipten dönüş: {prev1LSMA} > {prev2LSMA} < {prev3LSMA} RSI Yükseliş: {prev1RSI}>{prev2RSI}>{prev3RSI} "
                    return True
    # if (prev1Color == cons.CANDLE_GREEN) and (prev2Color == cons.CANDLE_GREEN) and (prev3Color == cons.CANDLE_GREEN) \
    #         and (curColor == cons.CANDLE_GREEN):
    #     if prev1Low <= curClose <= prev1High:
    #         log(f" {symbol} Prev 3 Candle GREEN prev1Low <= curClose <= prev1High: {prev1Low} <= {curClose} <= {prev1High} ")
    #         if (prev1RSI >= prev2RSI >= prev3RSI) and (prev1RSI <= 50):
    #             log(f"           RSI: {prev1RSI} >= {prev2RSI}>= {prev3RSI} AND prev1RSI <= 50: {prev1RSI}")
    #             if prev1CV >= prev2CV >= prev3CV:
    #                 log(f" +++      C*V: {prev1CV} >= {prev2CV}>= {prev3CV} ")
    #                 glbExplanation = f" Prev 3 Candle GREEN prev1Low <= curClose <= prev1High: {prev1Low}<={curClose}<={prev1High} RSI: {prev1RSI}>{prev2RSI}>{prev3RSI} "
    #                 return True
    return False


def control_strategy_14(symbol=None, coinTicker=None, tickSize=None) -> (bool):
    global glbExplanation
    glbExplanation = ""

    curClose = coinTicker.closes[-1]
    prev1High = coinTicker.highs[-2]
    prev1Low = coinTicker.lows[-2]

    rsi = ind.get_rsi(prices=coinTicker.closes, timePeriod=6)
    curRSI = rsi[-1]
    prev1RSI = rsi[-2]
    prev2RSI = rsi[-3]
    prev3RSI = rsi[-4]

    lsma = ind.get_lsma(data=coinTicker.closes, period=BTC_LSMA_PERIOD)
    curLSMA = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    prev1LSMA = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    prev2LSMA = func.round_tick_size(price=lsma[-3], tick_size=tickSize)
    prev3LSMA = func.round_tick_size(price=lsma[-4], tick_size=tickSize)

    if curLSMA > prev1LSMA < prev2LSMA:
        if prev2RSI < 15:
            if prev1Low <= curClose <= prev1High:
                log(f" {symbol} LSMA: {curLSMA} > {prev1LSMA} < {prev2LSMA} ")
                log(f"          prev2RSI < 15: {prev2RSI} ")
                log(f" +++      prev1Low <= curClose <= prev1High : {prev1Low} <= {curClose} <= {prev1High} ")
                glbExplanation = f" LSMA: {curLSMA} > {prev1LSMA} < {prev2LSMA} prev2RSI < 15: {prev2RSI} "
                return True
    return False


def control_buy_signal(symbol=None, coinTicker=None, tickSize=None) -> (bool, int):
    if BTC_SIGNAL is True:
        signal = control_strategy_10(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
        if signal is True:
            return True, 10

    signal = control_strategy_11(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal is True:
        return True, 11

    signal = control_strategy_12(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal is True:
        return True, 12

    signal = control_strategy_13(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal is True:
        return True, 13

    signal = control_strategy_14(symbol=symbol, coinTicker=coinTicker, tickSize=tickSize)
    if signal is True:
        return True, 14

    return False, 0


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

            # if BTC_SIGNAL == False:
            #     continue

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

            coinTicker = TickerData(opens=opens, highs=highs, lows=lows, closes=closes, volumes=volumes, dates=dates)

            global glbExplanation
            glbExplanation = ""
            buySignal, strategy = control_buy_signal(symbol=coinSymbol, coinTicker=coinTicker, tickSize=tickSize)
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

            periodTimestamps = dates[-1]
            periodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            """ Stop fiyatı belirlenir """
            stopPrice, stopHeight = get_stop_price(stopType=cons.DEFAULT_STOP_TYPE,
                                                   coinTicker=coinTicker, tickSize=tickSize)

            glbExplanation = f"Buy Price:{askPrice} {glbExplanation}"
            buyLot = MAX_AMOUNT_LIMIT / askPrice
            buyLot = func.round_step_size(quantity=buyLot, step_size=stepSize)
            buyAmount = buyLot * askPrice
            buyCommission = buyAmount * COMMISSION_RATE

            insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                               price=askPrice, buyLot=buyLot, buyAmount=buyAmount, buyCommission=buyCommission,
                               stopPrice=stopPrice, stopType=cons.DEFAULT_STOP_TYPE,
                               stopHeight=stopHeight, sellTarget=None, period=CANDLE_INTERVAL,
                               periodTime=periodTime, currentPeriodTime=periodTime, signalName=None,
                               explanation=glbExplanation,
                               mfi=None, fastk=None, slowd=None, pboll=None, strategy=strategy)
            log(f"{SIDE_BUY} {coinSymbol} stop:{stopPrice} {glbExplanation} strategy:{strategy}")
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

            if (str(buyPeriodTime) == str(candlePeriodTime)) and (stopType == cons.STOP_TYPE_PREVIOUS_LOW):
                # Coin in alındığı periyot içinde low değeri stopPrice ın altına düşerse
                # yeni stopPrice low[-1] yapılır
                if stopPrice > lows[-1]:
                    newStopPrice = lows[-1]
                    stopChangeCount += 1

                    update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                                         currentPeriodTime=candlePeriodTime,
                                                         stopChangeCount=stopChangeCount)
                    log(f"  *** STOP UPDATE *** {coinSymbol} Buy:{buyPrice} OldStop:{stopPrice} NewStop:{newStopPrice} %:{round((newStopPrice - buyPrice) / buyPrice * 100, 3)}")
                    continue

            coinTicker = TickerData(opens=opens, highs=highs, lows=lows, closes=closes, volumes=volumes, dates=dates)
            """ STOP kontrolleri en son işlem görmüş (close) fiyat ile yapılır """
            if (strategy == 10) or (strategy == 12) or (strategy == 13) or (strategy == 14):
                isStop = stop_control(currentPrice=closes[-1], stopPrice=stopPrice)
            else:
                isStop = lr_stop_control(coinTicker=coinTicker, stopPrice=stopPrice, buyPrice=buyPrice,
                                         tickSize=tickSize)
            # isStop = stop_control(currentPrice=closes[-1], stopPrice=stopPrice)
            isProfit = profit_control(currentPrice=closes[-1], buyingPrice=buyPrice, strategy=strategy)

            """ Stop olmuş ise veya kar alma noktasına gelmiş ise satış yapılır. """
            # if isStop is True:
            if (isStop is True) or (isProfit is True):
                """ Coin in anlık tahta fiyatı okunur. """
                tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                                   url=url_book_ticker,
                                                                                   symbol=coinSymbol)
                if tickerStatus is False:
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
                # if bidPrice > buyPrice:
                if sellTotal > buyTotal:
                    kar = sellTotal - buyTotal
                    yuzde = (kar * 100) / buyTotal
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {buyTotal} Sell: {sellTotal} price:{bidPrice} strategy:{strategy} KAR: {kar} (%{round(yuzde, 2)}) >>>")
                else:
                    zarar = buyTotal - sellTotal
                    yuzde = (zarar * 100) / sellTotal
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {buyTotal} Sell: {sellTotal} price:{bidPrice} strategy:{strategy} ZARAR: {zarar} (%{round(yuzde, 2)}) <<<")

                continue
            # end if (isStop is True) or (isProfit is True):

            # strategy = 13 ise yeni stop belirlenmez. İlk stop ile devam edilir.
            # if (strategy == 13) or (strategy == 14):
            #     continue

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

                # Strategy 13 ve 14 için maliyet stopPrice ın üstünde ise newStopPrice güncellenmez
                if (strategy == 10) or (strategy == 13) or (strategy == 14):
                    cost = (buyAmount + buyCommission) + (buyAmount + buyCommission)*(COMMISSION_RATE + 0.001)
                    if stopPrice > cost:
                        continue

                stopChangeCount += 1

                update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                                     currentPeriodTime=candlePeriodTime,
                                                     stopChangeCount=stopChangeCount)
                log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {buyPrice} New Stop: {newStopPrice} Fark: {round((newStopPrice - buyPrice), 8)} %:{round((newStopPrice - buyPrice) / buyPrice * 100, 3)}")
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
                                                                           stopPrice=stopPrice,
                                                                           tickSize=tickSize)
                if isTrailingStopChange is True:
                    stopChangeCount += 1

                    update_stop_price(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                      stopChangeCount=stopChangeCount)
                    log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {buyPrice} New Stop: {newStopPrice} fark: {newStopPrice - buyPrice} %:{round((newStopPrice - buyPrice)/buyPrice*100, 3)}")
            # end stopType == cons.STOP_TYPE_TRAILING

            if stopType == cons.STOP_TYPE_LR:
                periodTimestamps = dates[-1]
                candlePeriodTime = datetime.fromtimestamp(periodTimestamps / 1000)

                """ Period değişmemiş ise stopPrice değiştirilmez """
                if str(currentPeriodTime) == str(candlePeriodTime):
                    continue

                slope, average, intercept = ind.get_linear_regression_slope(prices=closes, period=LR_PERIOD)
                if slope > 0:
                    """ Period değişmiş ise stopPrice önceki mumun low değeri yapılır. """
                    newStopPrice = lows[-2]
                    """ Yeni stop fiyatı, eskisinden küçük ise stop fiyat değiştirilmez """
                    if newStopPrice <= stopPrice:
                        continue

                    stopChangeCount += 1

                    update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                                         currentPeriodTime=candlePeriodTime,
                                                         stopChangeCount=stopChangeCount)
                    log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {buyPrice} New Stop: {newStopPrice} Fark: {round((newStopPrice - buyPrice), 8)} %:{round((newStopPrice - buyPrice) / buyPrice * 100, 3)}")
            # end stopType == cons.STOP_TYPE_LR
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

        BTC_SIGNAL = control_btc(symbol=coinSymbol, coinTicker=coinTicker, tickSize=tickSize)

        time.sleep(3)
    # end while True:


def notify(connSession=None):
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
                kar = record["kar"]
                zarar = record["zarar"]
                fark = record["fark"]
                topKar += kar
                topZarar += zarar
                topFark += fark
                message = message + f" (S{strategy}) -> {kar} - {zarar} = *{fark}*\n"
        message = message + f" Toplam: {topKar} - {topZarar} = *{topFark}*\n"
        r = tlg.sendNotification(connSession=connSession, notification=message)

    schedule.every().hour.at(":00").do(job)
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
