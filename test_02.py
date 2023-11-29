import json
import requests
import pandas as pd
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

import notify as tlg
import logging
import winsound

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP = 2

CANDLE_INTERVAL = "15m"     # Okunacak mum verisi periyodu
CANDLE_COUNT = 30           # Okunacak mum verisi adeti
STANDARD_DEVIATION = 2.0
ATR_PERIOD = 14
SMA_PERIOD = 12
LSMA_PERIOD = 12
PROFIT_PERCENTAGE = 2
STOPLOSS_PERCENTAGE = 0.5
MFI_PERIOD = 12
MFI_LOW_THRESHOLD = 20

TRAILING_STOP_FACTOR_FOR_UP = float(0.7)
TRAILING_STOP_FACTOR_FOR_DOWN = float(0.4)

IS_LOG = True
IS_PRINT = True
IS_ALARM = False

glbExplanation = ""
logging.basicConfig(filename="debug_test02", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )


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


def get_candle_data(dbCursor=None, session=None, url=None, symbol=None, interval=None, limit=None):
    candle = candleClass()
    candle.dbCursor = dbCursor
    bars = candle.getDataWithSession(session=session, url=url, symbol=symbol, candleInterval=interval, limit=limit)
    return bars


def insert_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, stopPrice=None,
                       stopType=None, stopHeight=None,
                       sellTarget=None, period=None, periodTime=None, signalName=None, explanation=None,
                       mfi=None, fastk=None, slowd=None, pboll=None, strategy=None):
    trade = tradeClass()
    trade.symbol = symbol
    trade.period = period
    trade.period_time = periodTime
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


def get_stop_price(stopType=None, high=None, low=None, close=None, tickSize=None):
    stopPrice = None
    stopHeight = None
    if stopType == cons.STOP_TYPE_PREVIOUS_LOW:
        stopPrice = low[-2]

    if stopType == cons.STOP_TYPE_TRAILING:
        atr = ind.get_atr(highPrices=high,
                          lowPrices=low,
                          closePrices=close,
                          period=ATR_PERIOD)
        atrIndicator = func.round_tick_size(price=atr[-2], tick_size=tickSize)
        prevLow = low[-2]
        slope, average, intercept = ind.get_linear_regression_slope(prices=close, period=ATR_PERIOD)
        if slope > 0.0:
            stopPrice = prevLow - (atrIndicator * TRAILING_STOP_FACTOR_FOR_UP)
        else:
            stopPrice = prevLow - (atrIndicator * TRAILING_STOP_FACTOR_FOR_DOWN)

        """ 
        Stop fiyatının coin in minumum artım miktarına (tick size) göre ayarlanır.
        Minimum artım miktarından fazla olan digitler silinir.
        Örnek: Stop: 0.2346328 ise ve tick_size: 0.0001 ise stop: 0.2346 yapılır.
        """
        # residualValue = stopPrice % tickSize
        # stopPrice = stopPrice - residualValue
        stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
        stopHeight = prevLow - stopPrice
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
        return True
    return False


def sell_control(currentPrice=None, buyPrice=None, closePrices=None, tickSize=None,
                 buyPeriodTime=None, candlePeriodTime=None):
    profit = buyPrice + (buyPrice * PROFIT_PERCENTAGE) / 100
    loss = buyPrice - (buyPrice * STOPLOSS_PERCENTAGE) / 100

    if currentPrice > profit:
        log(f"=== Profit: {profit} buyPrice: {buyPrice} bidPrice: {currentPrice}")
        return True

    if (currentPrice < loss) and (str(buyPeriodTime) != str(candlePeriodTime)):
        log(f"=== Loss: {loss} buyPrice: {buyPrice} bidPrice: {currentPrice} bP:{buyPeriodTime} cP:{candlePeriodTime}")
        return True

    fast = ind.get_lsma(data=closePrices, period=LSMA_PERIOD)
    slow = ind.sma(data=closePrices, period=SMA_PERIOD)
    currFast = func.round_tick_size(price=fast[-1], tick_size=tickSize)
    prevFast = func.round_tick_size(price=fast[-2], tick_size=tickSize)
    currSlow = func.round_tick_size(price=slow[-1] , tick_size=tickSize)
    prevSlow = func.round_tick_size(price=slow[-2], tick_size=tickSize)

    if (prevFast > prevSlow) and (currFast < currSlow):
        log(f"=== Aşağı Kesti: Fast: {currFast} Slow: {currSlow}")
        return True

    return False


def control_buy_signal(symbol=None, opens=None, high=None,
                       low=None, close=None, volume=None, tickSize=None):
    buySignal = False
    strategy = None

    fast = ind.get_lsma(data=close, period=LSMA_PERIOD)
    slow = ind.sma(data=close, period=SMA_PERIOD)

    currFast = func.round_tick_size(price=fast[-1], tick_size=tickSize)
    prevFast = func.round_tick_size(price=fast[-2], tick_size=tickSize)
    currSlow = func.round_tick_size(price=slow[-1] , tick_size=tickSize)
    prevSlow = func.round_tick_size(price=slow[-2], tick_size=tickSize)

    if (prevFast < prevSlow) and (currFast > currSlow):
        buySignal = True

    if buySignal is True:
        global glbExplanation
        glbExplanation = f"prevFast: {prevFast} prevSlow: {prevSlow} currFast: {currFast} currSlow: {currSlow}"
        strategy = 4

    return buySignal, strategy


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
        message = f"Hesap Özeti\n {kar} - {zarar} = *{fark}*"
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
        symbolRows = symbol.readAll(dbCursor=dbCursor, exchangeId=1, quoteAsset='USDT')
        if symbolRows is None:
            continue

        for symbolRow in symbolRows:
            item = json.loads(symbolRow[0])
            coinSymbol = item['symbol']
            tickSize = float(item['tick_size'])
            stepSize = float(item['step_size'])

            # if coinSymbol != 'MATICUSDT':
            #     continue
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
            volumes = df['volume']

            """ Coine ait işlemde olan bir kayıt var ise tekrar alış yapılmaz """
            isPosition, positionRow = position_control(dbCursor=dbCursor, symbol=coinSymbol, status=STATUS_BUY)
            if isPosition is True:
                continue

            opens = open_prices.to_numpy(dtype=float)
            high = high_prices.to_numpy(dtype=float)
            low = low_prices.to_numpy(dtype=float)
            close = closing_prices.to_numpy(dtype=float)
            volume = volumes.to_numpy(dtype=float)
            date = date_kline.to_numpy(dtype=datetime)

            buySignal, strategy = control_buy_signal(symbol=coinSymbol,
                                                     opens=opens,
                                                     high=high,
                                                     low=low,
                                                     close=close,
                                                     volume=volume,
                                                     tickSize=tickSize)
            if buySignal is False:
                continue

            tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                               url=url_book_ticker,
                                                                               symbol=coinSymbol)
            if tickerStatus is False:
                continue

            global glbExplanation
            periodTimestamps = date[-1]
            periodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            # TODO: Burada ALIM işlemi yapılacak.
            #       Alım işlemi yapıldıktan sonra stop fiyatı belirlenir
            stopPrice = None # func.round_tick_size(price=askPrice * 1.01, tick_size=tickSize)

            glbExplanation = f"price:{askPrice} {glbExplanation}"
            insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                               price=askPrice, stopPrice=None, stopType=cons.DEFAULT_STOP_TYPE,
                               stopHeight=None, sellTarget=None, period=CANDLE_INTERVAL,
                               periodTime=periodTime, signalName=None, explanation=glbExplanation,
                               mfi=None, fastk=None, slowd=None, pboll=None, strategy=strategy)
            # TODO: Aşağıdaki satırlar silinecek
            # log(f"{SIDE_BUY} {coinSymbol} {glbExplanation} height:{stopHeight} %{round((stopHeight*100/stopPrice), 2)} strateji: {strategy}")
            log(f"{SIDE_BUY} {coinSymbol} {glbExplanation} strategy: {strategy}")

        # end for symbolRow in symbolRows:
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
            positionPeriodTime = position['period_time']
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
                                         limit=CANDLE_COUNT)
            if (candleBars is None) or (len(candleBars) < 2):
                continue

            df = convert_dataframe(bars=candleBars)
            close_prices = df['close']
            date_kline = df['date']

            close = close_prices.to_numpy(dtype=float)
            date = date_kline.to_numpy(dtype=datetime)

            periodTimestamps = date[-1]
            candlePeriodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            """ Coin in anlık tahta fiyatı okunur. """
            tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                               url=url_book_ticker,
                                                                               symbol=coinSymbol)
            if tickerStatus is False:
                continue

            """ STOP kontrolleri bidPrice (tahtadaki anlık alış fiyatı) ile yapılır """

            isStop = sell_control(currentPrice=bidPrice, buyPrice=position['buy_price'],
                                  closePrices=close, tickSize=tickSize, buyPeriodTime=positionPeriodTime,
                                  candlePeriodTime=candlePeriodTime)
            if isStop is True:
                # TODO: Burada SATIM işlemi yapılacak
                # Satış yapılmış gibi satış kaydı update edilir.
                update_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_SELL,
                                   price=bidPrice, explanation=expl, oldStatus=position['status'],
                                   newStatus=STATUS_STOP)
                # TODO: LOG kayıtları silinecek
                if bidPrice > position['buy_price']:
                    kar = ((bidPrice - position['buy_price']) * 100) / (position['buy_price'])
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} KAR: %{round(kar, 2)}  >>>")
                else:
                    zarar = ((position['buy_price'] - bidPrice) * 100) / bidPrice
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} ZARAR: %{round(zarar, 2)}  <<<")

            # end if isStop is True:

        # end for item in positionRecords:
    # end while True:

""" LSMA ve SMA indikatörleri ile test programı """
def main():
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"========== TEST START TIME: {t} ============="
    log(msg=msg)
    msg = f"========== test_02 (SMA ve LSMA ile) ============="
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
