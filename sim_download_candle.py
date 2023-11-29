import requests
import pandas as pd
from datetime import datetime

import botfunction as func
from botclass import BinanceCandlePrice as getCandleClass
from simulationclass import SimCandleClass as simCandleClass

COIN_SYMBOL = 'FTMUSDT'
CANDLE_INTERVAL = "2h"     # Okunacak mum verisi periyodu
CANDLE_COUNT = 500         # Okunacak mum verisi adeti

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


def get_candles(dbCursor=None, session=None, url=None, symbol=None, period=None, limit=None):
    getCandle = getCandleClass()
    getCandle.dbCursor = dbCursor
    bars = getCandle.getDataWithSession(session=session, url=url, symbol=symbol, candleInterval=period, limit=limit)
    return bars


def save_candles(dbCursor=None, symbol=None, period=None, dates=None, opens=None,
                highs=None, lows=None, closes=None, volumes=None):
    candle = simCandleClass()
    candle.symbol = symbol
    candle.period = period
    candle.dates = dates
    candle.opens = opens
    candle.highs = highs
    candle.lows = lows
    candle.closes = closes
    candle.volumes = volumes
    candle.addData(dbCursor=dbCursor)

def main():
    connSession = requests.session()
    db = func.connectDB()
    dbCursor = db.cursor()

    binanceConfig = func.readConfig(filename="config.ini", section="binance")
    url_candle = binanceConfig["url_historical"] + binanceConfig["url_candle_historical"]

    bars = get_candles(dbCursor=dbCursor, session=connSession, url=url_candle, symbol=COIN_SYMBOL,
                       period=CANDLE_INTERVAL, limit=CANDLE_COUNT)
    if bars is None:
        exit(0)

    df = convert_dataframe(bars=bars)
    date_kline = df['date']
    open_prices = df['open']
    high_prices = df['high']
    low_prices = df['low']
    closing_prices = df['close']
    volumes = df['volume']

    opens = open_prices.to_numpy(dtype=float)
    highs = high_prices.to_numpy(dtype=float)
    lows = low_prices.to_numpy(dtype=float)
    closes = closing_prices.to_numpy(dtype=float)
    volumes = volumes.to_numpy(dtype=float)
    dates = date_kline.to_numpy(dtype=datetime)

    for ix in range(len(bars)):
        save_candles(dbCursor=dbCursor, symbol=COIN_SYMBOL, period=CANDLE_INTERVAL,
                     dates=datetime.fromtimestamp(dates[ix] / 1000),
                     opens=opens[ix],
                     highs=highs[ix], lows=lows[ix], closes=closes[ix], volumes=volumes[ix])

main()