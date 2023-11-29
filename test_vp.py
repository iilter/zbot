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
from matplotlib import pyplot as plt
import seaborn as sb
import plotly.express as px

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

CANDLE_INTERVAL = "1m"      # Okunacak mum verisi periyodu
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

TEST_NAME = "VOLUME_PROFILE"
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

    if currentPrice > buyingPrice:
        profit = func.calculate_ratio(currentPrice, buyingPrice)

        if BTC_RED_ALERT == True:
            log(f"   BTC_RED_ALERT True: {BTC_RED_ALERT}")
            log(f"        PROFIT BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
            log(f"        Sell PROFIT > 0 - targetProfit: %{targetProfit} profit: %{profit}")
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
    hist, bins =ind.get_vp(open=coinTicker.opens, high=coinTicker.highs, low=coinTicker.lows, close=coinTicker.closes,
                           volume=coinTicker.volumes, histogramCount=24, tickSize=tickSize)

    data = {'X': bins,
            'Y': hist}
    df = pd.DataFrame(data)

    dv = {'close': coinTicker.closes,
          'volume': coinTicker.volumes}
    dvf = pd.DataFrame(dv)
    px.histogram(dvf, x='volume', y='close', nbins=90, orientation='h').show()

    kde_factor = 0.05
    num_samples = 500
    kde = stats.gaussian_kde(coinTicker.closes, weights=coinTicker.volumes, bw_method=kde_factor)

    sb.barplot(data=df, x='X', y='Y')
    # sb.histplot(data=df, x='X', y='Y', kde=True)
    # plt.bar(df['X'], df['Y'])

    plt.xticks(rotation='vertical')
    # labeling
    title = f"{symbol} Volume Profile"
    plt.title(title, fontsize=25)
    plt.xlabel('Prices', fontsize=20)
    plt.ylabel('Volumes', fontsize=20)

    plt.show()

    return False, float(0.0)

#endregion

def control_buy_signal(dbCursor=None, session=None, url=None, symbol=None, coinTicker=None, tickSize=None) -> (bool, float, int):
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
    # thread_notify = Thread(name='notify', target=notify, args=(connSession, ), daemon=True)
    thread_sell = Thread(name='sell', target=sell, args=(connSession, ), daemon=True)
    thread_btc = Thread(name='btc', target=btc, args=(connSession, ), daemon=True)
    thread_buy = Thread(name='buy', target=buy, args=(connSession,))

    # thread_notify.start()
    thread_sell.start()
    thread_btc.start()
    thread_buy.start()

    # thread_notify.join()
    thread_sell.join()
    thread_btc.join()
    thread_buy.join()


main()