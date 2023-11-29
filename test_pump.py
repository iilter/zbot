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

CANDLE_INTERVAL = "1h"     # Okunacak mum verisi periyodu
CANDLE_COUNT = 6          # Okunacak mum verisi adeti
VOLUME_PERCENTAGE = 10
PROFIT_PERCENTAGE = 5      # Kazanç yüzdesi
MAX_AMOUNT_LIMIT = 100
LIMIT_FACTOR = 5

IS_LOG = True
IS_PRINT = True
IS_ALARM = False

glbExplanation = ""
logging.basicConfig(filename="debug_test03", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )


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


def stop_control(currentPrice=None, stopPrice=None):
    if currentPrice < stopPrice:
        log(f"   STOP StopPrice: {stopPrice} CurrentPrice: {currentPrice}")
        return True
    return False

def profit_control(currentPrice=None, buyingPrice=None):
    if currentPrice > buyingPrice:
        profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
        if profit >= PROFIT_PERCENTAGE:
            log(f"   PROFIT STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
            return True
    return False

def control_buy_signal(symbol=None, opens=None, high=None,
                       low=None, close=None, volume=None, tickSize=None):
    currOpen = opens[-1]
    currClose = close[-1]
    currCandleColor = ind.get_candle_color(open=currOpen, close=currClose)
    if currCandleColor == cons.CANDLE_RED:
        return False, None

    prevHigh = high[-2]
    if currClose <= prevHigh:
        return False, None

    len = volume.size - 1
    avgVolume = volume[:len].mean()
    currVolume = volume[-1]
    prevVolume = volume[-2]
    if avgVolume == 0:
        return False, None
    if currVolume <= avgVolume:
        return False, None

    volumeIncrease = ((currVolume - avgVolume) * 100) / avgVolume

    if volumeIncrease > VOLUME_PERCENTAGE:
        global glbExplanation
        glbExplanation = f"avgVolume: {round(avgVolume, 2)} currVolume: {round(currVolume,2)} volume %{round(volumeIncrease, 2)}"
        return True, volumeIncrease

    return False, None


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

            opens = open_prices.to_numpy(dtype=float)
            high = high_prices.to_numpy(dtype=float)
            low = low_prices.to_numpy(dtype=float)
            close = closing_prices.to_numpy(dtype=float)
            volume = volumes.to_numpy(dtype=float)
            date = date_kline.to_numpy(dtype=datetime)

            buySignal, volIncrease = control_buy_signal(symbol=coinSymbol, opens=opens, high=high, low=low,
                                                        close=close, volume=volume, tickSize=tickSize)
            if buySignal is False:
                continue

            """ Stop fiyatı bir önceki mumun low değeri yapılır. """
            stopPrice = low[-2]
            strategy = 5
            message = f"PUMP\n{coinSymbol} price: {close[-1]} Increase: *{round(volIncrease, 2)}*"
            log(f"{SIDE_BUY} {coinSymbol} stop:{stopPrice} {glbExplanation} strategy: {strategy}")
            # r = tlg.sendNotification(connSession=connSession, notification=message)

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
            currentPeriodTime = position['current_period_time']
            expl = position['explanation']

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

            date = date_kline.to_numpy(dtype=datetime)
            low = low_prices.to_numpy(dtype=float)
            close = close_prices.to_numpy(dtype=float)

            periodTimestamps = date[-1]
            candlePeriodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            """ STOP kontrolleri en son işlem görmüş (close) fiyat ile yapılır """
            isStop = stop_control(currentPrice=close[-1], stopPrice=position['stop_price'])
            isProfit = profit_control(currentPrice=close[-1], buyingPrice=position['buy_price'])

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
                # TODO: LOG kayıtları silinecek
                if bidPrice > position['buy_price']:
                    kar = ((bidPrice - position['buy_price']) * 100) / (position['buy_price'])
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} KAR: %{round(kar, 2)}  >>>")
                else:
                    zarar = ((position['buy_price'] - bidPrice) * 100) / bidPrice
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} ZARAR: %{round(zarar, 2)}  <<<")

                continue
            # end if isStop is True:

            """ Yeni stop fiyatı belirlenir """

            """ Period değişmemiş ise stopPrice değiştirilmez """
            if str(currentPeriodTime) == str(candlePeriodTime):
                continue

            """ Period değişmiş ise stopPrice önceki mumun close değeri yapılır. """
            newStopPrice = low[-2]
            stopPrice = position['stop_price']
            """ Yeni stop fiyatı, eskisinden küçük ise stop fiyat değiştirilmez """
            if newStopPrice <= stopPrice:
                continue

            stopChangeCount = position['stop_change_count']
            stopChangeCount += 1

            update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                                 currentPeriodTime=candlePeriodTime,
                                                 stopChangeCount=stopChangeCount)
            # TODO: print silinecek
            log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {position['buy_price']} New Stop: {newStopPrice} fark: {round((newStopPrice - position['buy_price']), 8)} %:{round((newStopPrice - position['buy_price']) / position['buy_price'] * 100, 3)}")

        # end for item in positionRecords:
    # end while True:

""" PUMP yapan coinleri bulma amaçlı test programı """
def main():
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"========== TEST START TIME: {t} ============="
    log(msg=msg)
    msg = f"========== test_03 (Volume) Pump yapan coinleri bulma amaçlı ============="
    log(msg=msg)
    msg = f"========== CANDLE INTERVAL: {CANDLE_INTERVAL} ============="
    log(msg=msg)

    connSession = requests.session()

    # thread_notify = Thread(name='notify', target=notify, args=(connSession, ), daemon=True)
    # thread_sell = Thread(name='sell', target=sell, args=(connSession, ), daemon=True)
    thread_buy = Thread(name='buy', target=buy, args=(connSession,))

    # thread_notify.start()
    # thread_sell.start()
    thread_buy.start()

    # thread_notify.join()
    # thread_sell.join()
    thread_buy.join()


main()
