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

import notify as tlg
import logging
import winsound

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP = 2

TEST_NAME = "2 li Kural (İniş ve Dönüş)"
CANDLE_INTERVAL = "15m"     # Okunacak mum verisi periyodu
CANDLE_COUNT = 10           # Okunacak mum verisi adeti
AVERAGE_CANDLE_COUNT = 6    # Ortalaması hesaplanacak mum adeti
RATE_THRESHOLD = float(200)
MAX_AMOUNT_LIMIT = float(100)
LIMIT_FACTOR = float(5)
SMA_PERIOD = 6
LSMA_PERIOD = 6
ATR_PERIOD = 6
ATR_STOP_FACTOR = float(0.5)
USE_PROFIT_RANGE = False
PROFIT_PERCENTAGE = float(5)      # Kazanç yüzdesi

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


def get_stop_price(stopType=None, opens=None, highs=None, lows=None, closes=None, tickSize=None):
    stopPrice = None
    stopHeight = None
    if stopType == cons.STOP_TYPE_PREVIOUS_LOW:
        stopPrice = lows[-2]
        if stopPrice > lows[-1]:
            stopPrice = lows[-1]

    if stopType == cons.STOP_TYPE_TRAILING:
        atr = ind.get_atr(highPrices=highs, lowPrices=lows, closePrices=closes, period=ATR_PERIOD)
        atrIndicator = func.round_tick_size(price=atr[-2], tick_size=tickSize)
        stopPrice = lows[-2]
        if ATR_STOP_FACTOR > 0:
            stopPrice = stopPrice - (atrIndicator * ATR_STOP_FACTOR)
        else:
            stopPrice = stopPrice - atrIndicator

        """ 
        Stop fiyatının coin in minumum artım miktarına (tick size) göre ayarlanır.
        Minimum artım miktarından fazla olan digitler silinir.
        Örnek: Stop: 0.2346328 ise ve tick_size: 0.0001 ise stop: 0.2346 yapılır.
        """
        # residualValue = stopPrice % tickSize
        # stopPrice = stopPrice - residualValue
        stopPrice = func.round_tick_size(price=stopPrice, tick_size=tickSize)
        stopHeight = lows[-2] - stopPrice
        stopHeight = func.round_tick_size(price=stopHeight, tick_size=tickSize)
        log(f"   Stop Price: {stopPrice} Stop Height: {stopHeight} Under Low: %{round((stopHeight/stopPrice)*100, 3)}")

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
        log(f"   STOP StopPrice: {stopPrice} CurrentPrice: {currentPrice}")
        return True
    return False


def profit_control(currentPrice=None, buyingPrice=None):
    if USE_PROFIT_RANGE is True:
        if currentPrice > buyingPrice:
            profit = ((currentPrice - buyingPrice) * 100) / buyingPrice
            if profit >= PROFIT_PERCENTAGE:
                log(f"   PROFIT STOP BuyingPrice: {buyingPrice} CurrentPrice: {currentPrice}")
                return True
    return False


def control_buy_signal(symbol=None, opens=None, highs=None,
                       lows=None, closes=None, volumes=None, tickSize=None):
    """
    1. KURAL
    - LSMA inişte
    - MA inişte
    - MA, LSMA nın üstünde
    - 1 önceki mum YEŞİL
    - Mevcut kapanış, MA nın altında
    - Mevcut kapanış, 1 önceki kapanışın üstünde
    2. KURAL
    - LSMA dipten dönüş
    - MA inişte
    - 1 önceki mum YEŞİL
    - Mevcut kapanış, MA nın altında
    - Mevcut kapanış, 1 önceki kapanışın üstünde
    """

    ohlc = (opens + highs + lows + closes) / 4
    lsma = ind.get_lsma(data=ohlc, period=LSMA_PERIOD)
    sma = ind.sma(data=ohlc, period=SMA_PERIOD)

    currLSMA = func.round_tick_size(price=lsma[-1], tick_size=tickSize)
    prev1LSMA = func.round_tick_size(price=lsma[-2], tick_size=tickSize)
    prev2LSMA = func.round_tick_size(price=lsma[-3], tick_size=tickSize)
    currMA = func.round_tick_size(price=sma[-1], tick_size=tickSize)
    prev1MA = func.round_tick_size(price=sma[-2], tick_size=tickSize)

    prev1CandleColor = ind.get_candle_color(open=opens[-2], close=closes[-2])
    prev2CandleColor = ind.get_candle_color(open=opens[-3], close=closes[-3])

    currClose = closes[-1]
    prev1Open = opens[-2]
    prev1Close = closes[-2]
    prev2Close = closes[-3]
    prev1High = highs[-2]

    global glbExplanation
    """
    1. KURAL
    - LSMA inişte
    - MA inişte
    - MA, LSMA nın üstünde
    - 1 önceki mum YEŞİL
    - Mevcut kapanış, 1 önceki kapanışın üstünde
    - Mevcut kapanış, MA nın altında
    """
    if (currLSMA < prev1LSMA) and (currMA < prev1MA):
        if (currMA > currLSMA) and (prev1MA > prev1LSMA):
            if prev1CandleColor == cons.CANDLE_GREEN:
                if currClose > prev1High:
                    if currClose < currMA:
                        glbExplanation = f"1.Kural(İNİŞ): CurPrice: {currClose} prevHigh: {prev1High} MA prev, cur: {prev1MA}, {currMA}  LSMA prev, cur: {prev1LSMA}, {currLSMA}"
                        return True

    """
    2. KURAL
    - LSMA dipten dönüş
    - MA inişte
    - MA, LSMA nın üstünde
    - Mevcut kapanış, MA nın altında
    """
    if (prev2LSMA > prev1LSMA) and (currLSMA > prev1LSMA):
        if currMA < prev1MA:
            if currMA > currLSMA:
            # if prev1CandleColor == cons.CANDLE_GREEN:
                if currClose < currMA:
#                    if currClose > prev1Close:
                    glbExplanation = f"2.Kural(DÖNÜŞ): CurPrice: {currClose} prevClose: {prev1Close} MA prev, cur:  {prev1MA}, {currMA} LSMA prev, cur: {prev1LSMA}, {currLSMA}"
                    return True

    return False


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
        message = f"{TEST_NAME}: {CANDLE_INTERVAL}\n {kar} - {zarar} = *{fark}*"
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

            buySignal = control_buy_signal(symbol=coinSymbol, opens=opens, highs=highs, lows=lows, closes=closes,
                                           volumes=volumes, tickSize=tickSize)
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

            global glbExplanation
            periodTimestamps = dates[-1]
            periodTime = datetime.fromtimestamp(periodTimestamps / 1000)

            """ Stop fiyatı belirlenir """
            stopPrice, stopHeight = get_stop_price(stopType=cons.DEFAULT_STOP_TYPE, opens=opens,
                                                   highs=highs, lows=lows, closes=closes, tickSize=tickSize)

            glbExplanation = f"price:{askPrice} {glbExplanation}"
            insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                               price=askPrice, stopPrice=stopPrice, stopType=cons.DEFAULT_STOP_TYPE,
                               stopHeight=stopHeight, sellTarget=None, period=CANDLE_INTERVAL,
                               periodTime=periodTime, currentPeriodTime=periodTime, signalName=None,
                               explanation=glbExplanation,
                               mfi=None, fastk=None, slowd=None, pboll=None, strategy=3)
            log(f"{SIDE_BUY} {coinSymbol} stop:{stopPrice} {glbExplanation}")

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
            stopType = position['stop_type']
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

            dates = date_kline.to_numpy(dtype=datetime)
            lows = low_prices.to_numpy(dtype=float)
            closes = close_prices.to_numpy(dtype=float)

            """ STOP kontrolleri en son işlem görmüş (close) fiyat ile yapılır """
            isStop = stop_control(currentPrice=closes[-1], stopPrice=position['stop_price'])
            isProfit = profit_control(currentPrice=closes[-1], buyingPrice=position['buy_price'])

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

                if bidPrice > position['buy_price']:
                    kar = ((bidPrice - position['buy_price']) * 100) / (position['buy_price'])
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} KAR: %{round(kar, 2)}  >>>")
                else:
                    zarar = ((position['buy_price'] - bidPrice) * 100) / bidPrice
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} ZARAR: %{round(zarar, 2)}  <<<")

                continue
            # end if isStop is True:

            """ Yeni stop fiyatı belirlenir """
            if stopType == cons.STOP_TYPE_PREVIOUS_LOW:
                periodTimestamps = dates[-1]
                candlePeriodTime = datetime.fromtimestamp(periodTimestamps / 1000)

                """ Period değişmemiş ise stopPrice değiştirilmez """
                if str(currentPeriodTime) == str(candlePeriodTime):
                    continue

                """ Period değişmiş ise stopPrice önceki mumun low değeri yapılır. """
                newStopPrice = lows[-2]
                stopPrice = position['stop_price']
                """ Yeni stop fiyatı, eskisinden küçük ise stop fiyat değiştirilmez """
                if newStopPrice <= stopPrice:
                    continue

                stopChangeCount = position['stop_change_count']
                stopChangeCount += 1

                update_stop_price_and_current_period(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                                     currentPeriodTime=candlePeriodTime,
                                                     stopChangeCount=stopChangeCount)
                log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {position['buy_price']} New Stop: {newStopPrice} fark: {round((newStopPrice - position['buy_price']), 8)} %:{round((newStopPrice - position['buy_price']) / position['buy_price'] * 100, 3)}")
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
                                                                           buyPrice=position['buy_price'],
                                                                           stopPrice=position['stop_price'])
                if isTrailingStopChange is True:
                    stopChangeCount = position['stop_change_count']
                    stopChangeCount += 1

                    update_stop_price(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                      stopChangeCount=stopChangeCount)
                    log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {position['buy_price']} New Stop: {newStopPrice} fark: {round((newStopPrice - position['buy_price']), 8)} %:{round((newStopPrice - position['buy_price'])/position['buy_price']*100, 3)}")
                    position['stopPrice'] = newStopPrice
            # end stopType == cons.STOP_TYPE_TRAILING


        # end for item in positionRecords:
    # end while True:

""" İkili Kural (LSMA İniş ve Dönüş durumunda) """
def main():
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"========== TEST START TIME: {t} ============="
    log(msg=msg)
    msg = f"========== {Path(__file__).stem} Mevcut mum ve önceki 2 mum YEŞİL ve fiyat MA nın altında ============="
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
