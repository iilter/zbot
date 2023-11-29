import pandas as pd
import json
from datetime import datetime
import requests
from threading import Thread
import time
import numpy as np
from botclass import BinanceCandlePrice as candleClass
from botclass import BinanceSymbol as symbolClass
from botclass import BinanceBookTicker as tickerClass
from botclass import Trade as tradeClass
import indicator as ind
import botfunction as func
import winsound

import logging

SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
CANDLE_INTERVAL = "15m"  # Okunacak mum verisi periyodu
CANDLE_COUNT = 40  # Okunacak mum verisi adeti
BOLLINGER_BAND_PERIOD = 20  # Bollinger band hesaplama uzunluğu (mum bar sayısı)
MINIMUM_BOLLINGER_HEIGHT = 5  # Bollinger alt ve üst bant arası yükseklik (yüzde olarak)
LINEAR_REGRESSION_PERIOD = 30
PERCENT_LR_THRESHOLD = 0
ATR_PERIOD = 14
AO_SHORT_PERIOD = 5
AO_LONG_PERIOD = 34
STOCHRSI_PERIOD = 14
STOCHRSI_SLOW_PERIOD = 3
STOCHRSI_FAST_PERIOD = 3
FASTK_THRESHOLD = 20
SLOWD_THRESHOLD = 20
STANDARD_DEVIATION = 2.0  # Bollinger band alt ve üst bandlar için standart sapma değeri
STANDARD_DEVIATION_UP = 2.0  # Trend YUKARI iken kullanılacak standart sapma değeri
STANDARD_DEVIATION_DOWN = 2.5  # Trend AŞAĞI iken kullanılacak standart sapma değeri
TARGET_DOWN_TO_CENTER = 'DOWN_TO_CENTER'
TARGET_DOWN_TO_UP = 'DOWN_TO_UP'
TARGET_CENTER_TO_UP = 'CENTER_TO_UP'
DOWN_CENTER_RATIO = float(0.15)
CENTER_UP_RATIO = float(0.05)
DOWN_UP_RATIO = float(0.10)
STOP_LIMIT_PERCENTAGE = 1
#MAXIMUM_STOP_PERCENTAGE = float(1.5)
TRAILING_STOP_FACTOR_FOR_UP = float(0.6)
TRAILING_STOP_FACTOR_FOR_DOWN = float(0.4)
STATUS_BUY = 0
STATUS_SELL = 1
STATUS_STOP = 2

IS_LOG = True
IS_PRINT = True
IS_ALARM = False

URL_TELEGRAM = 'https://api.telegram.org/bot'
TOKEN = '5474334107:AAEceU3EUiINChLTunuTsZ6CZN-udB3e_EY'

glbExplanation = ""
logging.basicConfig(filename="debug", level=logging.INFO, format='(%(threadName)-5s) %(message)s', )

def convert_dataframe(bars=None):
    # İlk 6 beş kolon muhafaza edilir, "date", "open", "high", "low", "close", "volume"
    for line in bars:
        del line[6:]

    df = pd.DataFrame(bars, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    return df

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

def position_existence_control(dbCursor=None, symbol=None, status=None):
    trade = tradeClass()
    row = trade.readTrade(dbCursor=dbCursor, symbol=symbol, status=status)
    if (row is None) or (len(row) <= 0):
        return False, row
    return True, row

def insert_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, stopPrice=None, stopHeight=None,
                       sellTarget=None, period=None, periodTime=None, signalName=None, explanation=None):
    trade = tradeClass()
    trade.symbol = symbol
    trade.period = period
    trade.period_time = periodTime
    trade.explanation = explanation
    trade.buy_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    trade.buy_price = price
    trade.stop_price = stopPrice
    trade.stop_height = stopHeight
    trade.stop_change_count = 0
    trade.buy_signal_name = signalName
    trade.sell_target = sellTarget
    trade.status = STATUS_BUY

    trade.addTrade(dbCursor=dbCursor)

def update_trade_table(dbCursor=None, symbol=None, buySell=None, price=None, signalName=None,
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

def control_percent_LR_buy_signal(percentLR=None, threshold=None):
    currentPercentLR = percentLR[-1]
    if currentPercentLR < threshold:
        return True
    return False

def control_stochrsi_buy_signal(fastK=None, slowD=None):
    if (fastK < FASTK_THRESHOLD) and (slowD < SLOWD_THRESHOLD) and (slowD < fastK):
        return True
    return False

def control_stochastic_buy_signal(fastK=None, slowD=None):
    if (fastK < 20) and (slowD < 20) and (slowD < fastK):
        return True
    return False

def control_buy_signal(openPrices=None, highPrices=None, lowPrices=None, closePrices=None, volumes=None, tickSize=None):
    buySignal = False
    sellTarget = None
    stopPrice = None

    percentLR = ind.get_percent_linear_regression(prices=closePrices.to_numpy(dtype=float),
                                                  period=LINEAR_REGRESSION_PERIOD,
                                                  standard_deviation_up_factor=STANDARD_DEVIATION_UP,
                                                  standard_deviation_down_factor=STANDARD_DEVIATION_DOWN)

    percentLR *= 100.0

    percentLRBuySignal = control_percent_LR_buy_signal(percentLR=percentLR, threshold=PERCENT_LR_THRESHOLD)

    if percentLRBuySignal is False:
        return buySignal, sellTarget, stopPrice

    fastK, slowD = ind.get_stochrsi(prices=closePrices.to_numpy(dtype=float),
                                    timePeriod=STOCHRSI_PERIOD,
                                    slowKPeriod=STOCHRSI_FAST_PERIOD,
                                    slowDPeriod=STOCHRSI_SLOW_PERIOD)

    stochrsiBuySignal = control_stochrsi_buy_signal(fastK=fastK[-1], slowD=slowD[-1])

    # TODO: Aşağıdaki kısım glbexplanation sonra silinecek (aşağıdaki kısım)
    if (percentLRBuySignal is True) and (stochrsiBuySignal is True):
        buySignal = True
        sellTarget = TARGET_DOWN_TO_UP

        high = highPrices.to_numpy(dtype=float)
        low = lowPrices.to_numpy(dtype=float)
        close = closePrices.to_numpy(dtype=float)

        atr = ind.get_atr(highPrices=high,
                          lowPrices=low,
                          closePrices=close,
                          period=ATR_PERIOD)
        atrIndicator = round(float(atr[-1]), 8)
        currentClosePrice = round(close[-1], 8)
        stopPrice = currentClosePrice - (atrIndicator * TRAILING_STOP_FACTOR_FOR_UP)
        """ 
        Stop fiyatının coin in minumum artım miktarına göre ayarlanması.
        Minimum artım miktarından fazla olan digitler silinir.
        Örnek: Stop: 0.2346328 ise ve tick_size: 0.0001 ise stop: 0.2346 yapılır.
        """
        residualValue = stopPrice % tickSize
        stopPrice = stopPrice - residualValue

        global glbExplanation
        glbExplanation= f"pLR: {round(percentLR[-1],2)} fast: {round(fastK[-1], 2)} slow: {round(slowD[-1], 2)}"

    return buySignal, sellTarget, stopPrice

def stop_control(currentPrice=None, stopPrice=None):
    if currentPrice < stopPrice:
        return True

    return False

def trailing_stop_control(stopHeight=None, currentPrice=None, buyPrice=None, stopPrice=None):
    stopChange = False
    newStopPrice = None

    if (currentPrice > stopPrice):
        difference = round((currentPrice - stopPrice), 8)
        if difference > stopHeight:
            newStopPrice = currentPrice - stopHeight
            newStopPrice = round(newStopPrice, 8)
            stopChange = True

    return stopChange, newStopPrice

def log(msg=None):
    if IS_LOG is True:
        logging.info(msg=msg)
    if IS_PRINT is True:
        print(msg, flush=True)
    if IS_ALARM is True:
        alarm()

def alarm():
    frequency = 2000
    duration = 1000
    winsound.Beep(frequency, duration)

def buy(connSession=None):
    log(f"BUY Thread Start")

    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_candle = binanceConfig["url_historical"] + binanceConfig["url_candle_historical"]
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    symbol = symbolClass()
    symbolRows = symbol.readAll(dbCursor=dbCursor, exchangeId=1, quoteAsset='USDT')
    if (symbolRows is not None) and (len(symbolRows) > 0):
        while True:

            for symbolRow in symbolRows:
                item = json.loads(symbolRow[0])
                coinSymbol = item['symbol']
                tickSize = float(item['tick_size'])

                # coinSymbol = 'FTMUSDT'

                """ Coin mum verileri okunur """
                bars = get_candle_data(dbCursor=dbCursor, session=connSession, url=url_candle, symbol=coinSymbol,
                                       interval=CANDLE_INTERVAL,
                                       limit=CANDLE_COUNT)

                """ MUM verileri okunamamış ise işlem yapılmaz """
                if (bars is None) or (len(bars) < CANDLE_COUNT):
                    continue

                df = convert_dataframe(bars=bars)
                date_kline = df['date']
                open_prices = df['open']
                high_prices = df['high']
                low_prices = df['low']
                closing_prices = df['close']
                volumes = df['volume']

                isPosition, positionRow = position_existence_control(dbCursor=dbCursor,
                                                                     symbol=coinSymbol,
                                                                     status=STATUS_BUY)

                """ Coine ait işlemde olan bir kayıt var ise satılmadan tekrar alış yapılmaz """
                if isPosition is True:
                    continue

                """ ALIM sinyali olup olmadığı kontrol edilir """
                buySignal, sellTarget, stopPrice = control_buy_signal(openPrices=open_prices,
                                                                      highPrices=high_prices,
                                                                      lowPrices=low_prices,
                                                                      closePrices=closing_prices,
                                                                      volumes=volumes,
                                                                      tickSize=tickSize)
                """ ALIM sinyali yoksa sonraki COIN """
                if buySignal is False:
                    continue

                stopPrice = round(stopPrice, 8)
                """ Coin in anlık tahta fiyatı okunur. """
                tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                                   url=url_book_ticker,
                                                                                   symbol=coinSymbol)
                if tickerStatus is False:
                    continue

                """ ALIM işlemi yapılır. ALIM işlemi askPrice (tahtadaki üstteki satış fiyatı) ile yapılır """
                # TODO: Alım işlemi yapılacak
                global glbExplanation

                curIndex = date_kline.size - 1
                periodTimetamps = date_kline[curIndex]
                periodTime = datetime.fromtimestamp((periodTimetamps / 1000))

                stopHeight = float(closing_prices[curIndex]) - stopPrice
                stopHeight = round(stopHeight, 8)

                glbExplanation = f"price:{askPrice} stop: {stopPrice} {glbExplanation}"
                insert_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_BUY,
                                   price=askPrice, stopPrice=stopPrice, stopHeight=stopHeight,
                                   sellTarget=sellTarget, period=CANDLE_INTERVAL, periodTime=periodTime,
                                   signalName='BOLL', explanation=glbExplanation)
                # TODO: Aşağıdaki satırlar silinecek
                log(f"{SIDE_BUY} {coinSymbol} {glbExplanation} height:{stopHeight} %{stopHeight*100/stopPrice}")

            # end for symbolRow in symbolRows:
        # end while True:

def sell(connSession=None):
    log(f"SELL Thread Start")

    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_book_ticker = binanceConfig["url_base"] + binanceConfig["url_book_ticker"]

    while True:
        """ Alınmış durumdaki (trade.status = 0) kayıtlar okunur """
        trade = tradeClass()
        positionRecords = trade.readTradeAll(dbCursor=dbCursor, status=STATUS_BUY)
        if (positionRecords is None):
            continue

        for item in positionRecords:
            position = json.loads(item[0])
            coinSymbol = position['symbol']

            """ Coin in anlık tahta fiyatı okunur. """
            tickerStatus, bidPrice, bidQty, askPrice, askQty = get_ticker_info(session=connSession,
                                                                               url=url_book_ticker,
                                                                               symbol=coinSymbol)
            if tickerStatus is False:
                continue

            """ STOP kontrolleri bidPrice (tahtadaki anlık alış fiyatı) ile yapılır """
            isStop = stop_control(currentPrice=bidPrice, stopPrice=position['stop_price'])

            if isStop is True:
                # TODO: Burada SATIM işlemi yapılacak
                # Satış yapılmış gibi satış kaydı update edilir.
                update_trade_table(dbCursor=dbCursor, symbol=coinSymbol, buySell=SIDE_SELL,
                                   price=bidPrice, oldStatus=position['status'],
                                   newStatus=STATUS_STOP)
                # TODO: LOG kayıtları silinecek
                if bidPrice > position['buy_price']:
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} KAR: {bidPrice - position['buy_price']} >>>")
                else:
                    log(f"{SIDE_SELL} {coinSymbol} Buy: {position['buy_price']} Sell: {bidPrice} ZARAR: {bidPrice - position['buy_price']} <<<")
                continue
            # end if isStop is True:

            """ Trailing Stop (iz süren stop) kontrolleri """
            isTrailingStopChange, newStopPrice = trailing_stop_control(stopHeight=position['stop_height'],
                                                                       currentPrice=bidPrice,
                                                                       buyPrice=position['buy_price'],
                                                                       stopPrice=position['stop_price'])
            if isTrailingStopChange is True:
                """ 
                Mevcut fiyat alış fiyatının üzerine çıktığı anda stopPrice = buyPrice yapılarak
                zarar azaltılmaya çalışıldı. (zaten stopPrice > buyPrice ise yapılmaz) 
                Bu işlemi bir kere yapması için stopChangeCount kullanıldı.
                """
                target = position['sell_target']
                buyPrice = position['buy_price']
                stopChangeCount = position['stop_change_count']

                # TODO: Stop belirlemede kural;
                #   aşağı trend ise atr * TRAILING_STOP_FACTOR_FOR_DOWN
                #   yukarı trend ise atr * TRAILING_STOP_FACTOR_FOR_UP
                stopChangeCount = stopChangeCount + 1

                update_stop_price(dbCursor=dbCursor, symbol=coinSymbol, stopPrice=newStopPrice,
                                  stopChangeCount=stopChangeCount)
                # TODO: print silinecek
                log(f"  *** STOP UPDATE *** {coinSymbol} Buy: {position['buy_price']} New Stop: {newStopPrice} fark: {round((newStopPrice - position['buy_price']), 8)}")
                # TODO: Burada kayıt yeniden okunabilir.
                #       Vakit kaybı olmasın diye daha önce okunan kayıtta stop_price değiştirildi.
                #       Aşağıdaki satır silinecek
                position['stopPrice'] = newStopPrice

            # end if isTrailingStopChange is True:

        # end for item in positionRecords:
    # end while True:

def getChatId(connSession=None):
    url = URL_TELEGRAM + TOKEN + '/getUpdates'
    response = connSession.get(url)
    r = response.json()
    chatId = r['result'][0]['message']['chat']['id']
    return str(chatId)

def sendNotification(connSession=None, notification=None):
    bot_chatID = getChatId(connSession=connSession)
    msg = f"{notification}"
    send_text = URL_TELEGRAM + TOKEN + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + msg
    response = connSession.get(send_text)
    return response.json

def readSummary(dbCursor=None):
    trade = tradeClass()
    res = trade.readProfitSummary(dbCursor=dbCursor)
    return res

def notify(connSession=None):
    db = func.connectDB()
    dbCursor = db.cursor()

    while True:
        row = readSummary(dbCursor=dbCursor)
        record = json.loads(row[0])
        kar = record["kar"]
        zarar = record["zarar"]
        fark = record["fark"]
        message = f"Hesap Özeti (LR)\n {kar} - {zarar} = *{fark}*"
        r = sendNotification(connSession=connSession, notification=message)
        time.sleep(3600)

def main():
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    str = f"========== TEST START TIME: {t} ============="
    log(msg=str)
    str = f"========== LINEAR REGRESSION + STOCHRSI ============="
    log(msg=str)
    str = f"========== STANDART DEVIATION DOWN: {STANDARD_DEVIATION_DOWN} UP: {STANDARD_DEVIATION_UP} ============="
    log(msg=str)
    str = f"========== CANDLE INTERVAL:{CANDLE_INTERVAL} ============="
    log(msg=str)

    connSession = requests.session()

    thread_notify = Thread(name='notify', target = notify, args = (connSession, ), daemon=True)
    thread_sell = Thread(name='sell', target = sell, args = (connSession, ), daemon=True)
    thread_buy = Thread(name='buy', target=buy, args=(connSession,))

    thread_notify.start()
    thread_sell.start()
    thread_buy.start()

    thread_notify.join()
    thread_sell.join()
    thread_buy.join()

main()