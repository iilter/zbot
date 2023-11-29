from datetime import datetime
from enum import Enum
from dataclasses import dataclass

import numpy as np
import requests
import mariadb
import errorclass as error
import error_helper

@dataclass
class TickerData():
    opens: np.ndarray
    highs: np.ndarray
    lows: np.ndarray
    closes: np.ndarray
    volumes: np.ndarray
    dates: np.ndarray

@dataclass
class StrategyResponse():
    signal: bool
    stopPrice: float
    targetPrice: float
    targetRatio: float
    strategy: int


class BinanceExchangeInfo:
    def __init__(self,
                 dbCursor=None,
                 symbol=None,
                 baseAsset=None,
                 quoteAsset=None,
                 baseAssetName=None,
                 exchangeId=1,
                 status=0,
                 updateDate=datetime.now().strftime("%Y-%m-%d"),
                 updateTime=datetime.now().strftime("%H:%M:%S")
                 ):
        self.dbCursor = dbCursor
        self.symbol = symbol  # ETHBTC
        self.baseAsset = baseAsset  # ETH
        self.quoteAsset = quoteAsset  # BTC
        self.baseAssetName = baseAssetName  # ethereum
        self.exchangeId = exchangeId  # 1 (Binance)
        self.status = status
        self.updateDate = updateDate
        self.updateTime = updateTime

    def getData(self, url=None):
        dbCursor = self.dbCursor

        try:
            response = requests.get(url)
            response.raise_for_status()
            records = response.json()
            return records['symbols']
        #            return records['result']['data']
        except requests.exceptions.HTTPError as errh:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errh.response.status_code
            log.errorMessage = errh.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Http Error: " + errh.response.url
            log.addData()
        except requests.exceptions.ConnectionError as errc:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errc.response.status_code
            log.errorMessage = errc.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Error Connecting: " + errc.response.url
            log.addData()
        except requests.exceptions.Timeout as errt:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errt.response.status_code
            log.errorMessage = errt.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Timeout Error: " + errt.response.url
            log.addData()
        except requests.exceptions.RequestException as err:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = err.response.status_code
            log.errorMessage = err.response.text
            log.moduleName = type(self).__name__
            log.explanation = err.response.url
            log.addData()

    def addData(self):
        try:
            self.dbCursor.execute(
                "INSERT INTO coin (symbol, base_asset, quote_asset, base_asset_name, "
                " exchange_id, status, update_date, update_time) "
                "VALUES (?, ?, ?, ?, ? ,?, ?, ?)",
                (self.symbol, self.baseAsset, self.quoteAsset, self.baseAssetName,
                 self.exchangeId, self.status, self.updateDate, self.updateTime))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO coin"
            log.addData()

    def readAllSymbol(self):
        try:
            self.dbCursor.execute("SELECT JSON_OBJECT('symbol', symbol) FROM coin WHERE status=1")
            rows = self.dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "SELECT all coin"
            log.addData()

    def readAllCoinAsset(self, exchangeId):
        try:
            self.dbCursor.execute(
                "SELECT JSON_OBJECT('symbol', symbol, 'base', base_asset, 'quote', quote_asset) "
                "FROM symbol WHERE STATUS=? AND exchange_id=?",
                (0, exchangeId)
            )

            rows = self.dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "SELECT all coin"
            log.addData()


class BinanceSymbol:
    def __init__(self,
                 symbol=None,
                 exchange_id=None,
                 base_asset=None,
                 base_asset_precision=None,
                 quote_asset=None,
                 quote_precision=None,
                 quote_asset_precision=None,
                 symbol_status=None,
                 order_types=None,
                 iceberg_allowed=None,
                 oco_allowed=None,
                 quote_order_qty_market_allowed=None,
                 allow_trailing_stop=None,
                 cancel_replace_allowed=None,
                 is_spot_trading_allowed=None,
                 is_margin_trading_allowed=None,
                 permissions=None,
                 min_price=None,
                 max_price=None,
                 tick_size=None,
                 min_lot=None,
                 max_lot=None,
                 step_size=None,
                 min_notional=None,
                 status=None,
                 market_group=None,
                 update_time=None,
                 ):
        self.symbol = symbol
        self.exchange_id = exchange_id
        self.base_asset = base_asset
        self.base_asset_precision = base_asset_precision
        self.quote_asset = quote_asset
        self.quote_precision = quote_precision
        self.quote_asset_precision = quote_asset_precision
        self.symbol_status = symbol_status
        self.order_types = order_types
        self.iceberg_allowed = iceberg_allowed
        self.oco_allowed = oco_allowed
        self.quote_order_qty_market_allowed = quote_order_qty_market_allowed
        self.allow_trailing_stop = allow_trailing_stop
        self.cancel_replace_allowed = cancel_replace_allowed
        self.is_spot_trading_allowed = is_spot_trading_allowed
        self.is_margin_trading_allowed = is_margin_trading_allowed
        self.permissions = permissions
        self.min_price = min_price
        self.max_price = max_price
        self.tick_size = tick_size
        self.min_lot = min_lot
        self.max_lot = max_lot
        self.step_size = step_size
        self.min_notional = min_notional
        self.status = status
        self.market_group = market_group
        self.update_time = update_time

    @staticmethod
    def getData(url=None):
        try:
            response = requests.get(url)
            response.raise_for_status()
            records = response.json()
            return records['symbols']
        except requests.exceptions.RequestException as err:
            helper = error_helper.ErrorHelper()
            helper.status_code = err.response.status_code
            helper.code = err.errno
            helper.msg = err.response.reason
            helper.module = 'Symbol getData'
            helper.url = err.response.url
            helper.addData()

    def addData(self, dbCursor=None):
        self.update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            dbCursor.execute(
                "INSERT INTO symbol (symbol, exchange_id, base_asset, base_asset_precision, quote_asset, "
                "quote_precision, quote_asset_precision, symbol_status, order_types, iceberg_allowed, oco_allowed, "
                "quote_order_qty_market_allowed, allow_trailing_stop, cancel_replace_allowed, "
                "is_spot_trading_allowed, is_margin_trading_allowed, permissions, min_price, max_price, tick_size, "
                "min_lot, max_lot, step_size, min_notional, status, market_group, update_time) "
                "VALUES (?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.symbol, self.exchange_id, self.base_asset, self.base_asset_precision, self.quote_asset,
                 self.quote_precision, self.quote_asset_precision, self.symbol_status, self.order_types,
                 self.iceberg_allowed, self.oco_allowed, self.quote_order_qty_market_allowed, self.allow_trailing_stop,
                 self.cancel_replace_allowed, self.is_spot_trading_allowed,self.is_margin_trading_allowed,
                 self.permissions, self.min_price, self.max_price, self.tick_size, self.min_lot, self.max_lot,
                 self.step_size, self.min_notional, self.status, self.market_group, self.update_time))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            helper = error_helper.ErrorHelper()
            helper.code = e.errno
            helper.msg = e.errmeg
            helper.module = 'Symbol addData'
            helper.explanation = "INSERT INTO SYMBOL"
            helper.addData()

    @staticmethod
    def readAll(dbCursor=None, exchangeId=None, quoteAsset=None):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT('symbol', symbol, 'base', base_asset, 'quote', quote_asset, "
                "'min_price', min_price, 'max_price', max_price, 'tick_size', tick_size, 'min_lot', min_lot, "
                "'max_lot', max_lot, 'step_size', step_size, 'min_notional', min_notional ) "
                "FROM symbol WHERE exchange_id=? AND quote_asset=? AND STATUS=? AND  symbol_status=? "
                " AND permissions LIKE ? ",
                (exchangeId, quoteAsset, 0, 'TRADING', '%SPOT%')
            )
            rows = dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            helper = error_helper.ErrorHelper()
            helper.code = e.errno
            helper.msg = e.errmeg
            helper.module = 'Symbol readAll'
            helper.explanation = "SELECT SYMBOL"
            helper.addData()

    @staticmethod
    def readAllByGroup(dbCursor=None, exchangeId=None, quoteAsset=None, marketGroup=None):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT('symbol', symbol, 'base', base_asset, 'quote', quote_asset, "
                "'min_price', min_price, 'max_price', max_price, 'tick_size', tick_size, 'min_lot', min_lot, "
                "'max_lot', max_lot, 'step_size', step_size, 'min_notional', min_notional ) "
                "FROM symbol WHERE exchange_id=? AND quote_asset=? AND STATUS=? AND  symbol_status=? "
                " AND permissions LIKE ? AND market_group = ? ",
                (exchangeId, quoteAsset, 0, 'TRADING', '%SPOT%', marketGroup)
            )
            rows = dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            helper = error_helper.ErrorHelper()
            helper.code = e.errno
            helper.msg = e.errmeg
            helper.module = 'Symbol readAll'
            helper.explanation = "SELECT SYMBOL"
            helper.addData()

    @staticmethod
    def readOne(dbCursor=None, exchangeId=None, symbol=None):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT('symbol', symbol, 'base', base_asset, 'quote', quote_asset, "
                "'min_price', min_price, 'max_price', max_price, 'tick_size', tick_size, 'min_lot', min_lot, "
                "'max_lot', max_lot, 'step_size', step_size, 'min_notional', min_notional ) "
                "FROM symbol WHERE exchange_id=? AND symbol=? AND STATUS=? AND  symbol_status=? "
                " AND permissions LIKE ? ",
                (exchangeId, symbol, 0, 'TRADING', '%SPOT%')
            )
            row = dbCursor.fetchone()
            return row
        except mariadb.Error as e:
            helper = error_helper.ErrorHelper()
            helper.code = e.errno
            helper.msg = e.errmeg
            helper.module = 'Symbol readOne'
            helper.explanation = "SELECT SYMBOL"
            helper.addData()

    @staticmethod
    def readExist(dbCursor=None, exchangeId=None, symbol=None):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT('symbol', symbol, 'status', status, 'market_group', market_group ) "
                "FROM symbol WHERE exchange_id=? AND symbol=? ",
                (exchangeId, symbol)
            )
            row = dbCursor.fetchone()
            return row
        except mariadb.Error as e:
            helper = error_helper.ErrorHelper()
            helper.code = e.errno
            helper.msg = e.errmeg
            helper.module = 'Symbol readExist'
            helper.explanation = "SELECT SYMBOL"
            helper.addData()

    @staticmethod
    def delete(dbCursor=None, exchangeId=None, symbol=None):
        try:
            dbCursor.execute(
                "DELETE FROM symbol WHERE exchange_id=? AND symbol=? ",
                (exchangeId, symbol)
            )
        except mariadb.Error as e:
            helper = error_helper.ErrorHelper()
            helper.code = e.errno
            helper.msg = e.errmeg
            helper.module = 'Symbol delete'
            helper.explanation = "DELETE SYMBOL"
            helper.addData()


class TriangularPair:
    def __init__(self,
                 dbCursor=None,
                 exchangeId=None,
                 aPairSymbol=None,
                 aPairBase=None,
                 aPairQuote=None,
                 bPairSymbol=None,
                 bPairBase=None,
                 bPairQuote=None,
                 cPairSymbol=None,
                 cPairBase=None,
                 cPairQuote=None,
                 status=None
                 ):
        self.dbCursor = dbCursor
        self.exchangeId = exchangeId
        self.aPairSymbol = aPairSymbol
        self.aPairBase = aPairBase
        self.aPairQuote = aPairQuote
        self.bPairSymbol = bPairSymbol
        self.bPairBase = bPairBase
        self.bPairQuote = bPairQuote
        self.cPairSymbol = cPairSymbol
        self.cPairBase = cPairBase
        self.cPairQuote = cPairQuote
        self.status = status

    def addTriangularPair(self):
        try:
            self.dbCursor.execute(
                "INSERT INTO triangular_pair (exchange_id, pair_a_symbol, pair_a_base, pair_a_quote, "
                "pair_b_symbol, pair_b_base, pair_b_quote, pair_c_symbol, pair_c_base, pair_c_quote, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.exchangeId, self.aPairSymbol, self.aPairBase, self.aPairQuote, self.bPairSymbol,
                 self.bPairBase, self.bPairQuote, self.cPairSymbol, self.cPairBase, self.cPairQuote, self.status))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO triangular_pair"
            log.addData()

    def readAllPair(self, exchangeId=None, refSymbol=None):
        try:
            self.dbCursor.execute(
                "SELECT JSON_OBJECT('pair_a_symbol', pair_a_symbol, 'pair_b_symbol', pair_b_symbol, "
                "'pair_c_symbol', pair_c_symbol) "
                "FROM triangular_pair WHERE exchange_id=? and pair_a_quote=? and status=0 ",
                (exchangeId, refSymbol)
            )
            rows = self.dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "SELECT all triangular_pair"
            log.addData()


class BinanceSymbolPriceTicker:
    def __init__(self,
                 dbCursor=None,
                 priceId=None,
                 priceTime=None,
                 symbol=None,
                 price=None,
                 sourceExchange="binance",
                 status=None,
                 updateDate=None,
                 updateTime=None
                 ):
        self.dbCursor = dbCursor
        self.priceId = priceId
        self.priceTime = priceTime
        self.symbol = symbol
        self.price = price
        self.sourceExchange = sourceExchange
        self.status = status
        self.updateDate = updateDate
        self.updateTime = updateTime

    def getData(self, url=None):
        dbCursor = self.dbCursor
        prm = {'symbol': self.symbol}

        try:
            response = requests.get(url, params=prm)
            response.raise_for_status()
            records = response.json()
            return records
        #            return records['result']['data']
        except requests.exceptions.HTTPError as errh:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errh.response.status_code
            log.errorMessage = errh.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Http Error: " + errh.response.url
            log.addData()
        except requests.exceptions.ConnectionError as errc:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errc.response.status_code
            log.errorMessage = errc.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Error Connecting: " + errc.response.url
            log.addData()
        except requests.exceptions.Timeout as errt:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errt.response.status_code
            log.errorMessage = errt.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Timeout Error: " + errt.response.url
            log.addData()
        except requests.exceptions.RequestException as err:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = err.response.status_code
            log.errorMessage = err.response.text
            log.moduleName = type(self).__name__
            log.explanation = err.response.url
            log.addData()

    def addData(self):
        self.priceTime = datetime.now()
        self.updateDate = datetime.now().strftime("%Y-%m-%d")
        self.updateTime = datetime.now().strftime("%H:%M:%S")
        self.status = 1
        try:
            self.dbCursor.execute(
                "INSERT INTO current_price ( price_time, symbol, price, source_exchange, "
                "  status, update_date, update_time) "
                "VALUES (?, ?, ?, ? ,?, ?, ?)",
                (self.priceTime, self.symbol, self.price, self.sourceExchange,
                 self.status, self.updateDate, self.updateTime))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO current_price"
            log.addData()


class BinanceBookTicker:
    def __init__(self,
                 dbCursor=None,
                 symbol=None,
                 symbols=None,
                 bidPrice=None,
                 bidQty=None,
                 askPrice=None,
                 askQty=None
                 ):
        self.dbCursor = dbCursor
        self.symbol = symbol
        self.symbols = symbols
        self.bidPrice = bidPrice
        self.bidQty = bidQty
        self.askPrice = askPrice
        self.askQty = askQty

    def getData(self, url=None):
        dbCursor = self.dbCursor

        prm = url
        if self.symbols is not None:
            prm = prm + '?symbols=['
            itemLen = len(self.symbols)
            for ix in range(itemLen):
                prm = prm + '"' + self.symbols[ix] + '"'
                if ix < itemLen - 1:
                    prm = prm + ','
            prm = prm + ']'

        try:
            # response = requests.get(url, params=prm)
            response = requests.get(url=prm)
            response.raise_for_status()
            records = response.json()
            return records
        #            return records['result']['data']
        except requests.exceptions.HTTPError as errh:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errh.response.status_code
            log.errorMessage = errh.response.reason
            log.moduleName = type(self).__name__
            log.explanation = "Http Error: " + errh.response.url
            log.addData()
        except requests.exceptions.ConnectionError as errc:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errc.response.status_code
            log.errorMessage = errc.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Error Connecting: " + errc.response.url
            log.addData()
        except requests.exceptions.Timeout as errt:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errt.response.status_code
            log.errorMessage = errt.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Timeout Error: " + errt.response.url
            log.addData()
        except requests.exceptions.RequestException as err:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = err.response.status_code
            log.errorMessage = err.response.text
            log.moduleName = type(self).__name__
            log.explanation = err.response.url
            log.addData()

    def getDataWithSession(self, session=None, url=None):
        dbCursor = self.dbCursor

        prm = url
        if self.symbols is not None:
            prm = prm + '?symbols=['
            itemLen = len(self.symbols)
            for ix in range(itemLen):
                prm = prm + '"' + self.symbols[ix] + '"'
                if ix < itemLen - 1:
                    prm = prm + ','
            prm = prm + ']'

        try:
            # response = requests.get(url, params=prm)
            response = session.get(url=prm)
            response.raise_for_status()
            records = response.json()
            return records
        #            return records['result']['data']
        except requests.exceptions.HTTPError as errh:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errh.response.status_code
            log.errorMessage = errh.response.reason
            log.moduleName = type(self).__name__
            log.explanation = "Http Error: " + errh.response.url
            log.addData()
        except requests.exceptions.ConnectionError as errc:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errc.response.status_code
            log.errorMessage = errc.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Error Connecting: " + errc.response.url
            log.addData()
        except requests.exceptions.Timeout as errt:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errt.response.status_code
            log.errorMessage = errt.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Timeout Error: " + errt.response.url
            log.addData()
        except requests.exceptions.RequestException as err:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = err.response.status_code
            log.errorMessage = err.response.text
            log.moduleName = type(self).__name__
            log.explanation = err.response.url
            log.addData()


class BinanceCandlePrice:
    def __init__(self,
                 dbCursor=None,
                 start_time=None,
                 close_time=None,
                 symbol=None,
                 candle_interval=None,
                 first_trade_id=None,
                 last_trade_id=None,
                 open_price=None,
                 close_price=None,
                 high_price=None,
                 low_price=None,
                 base_asset_volume=None,
                 number_of_trades=None,
                 quote_asset_volume=None,
                 taker_buy_base_asset_volume=None,
                 taker_buy_quote_asset_volume=None,
                 ignore_info=None,
                 source_exchange="binance",
                 update_time=None
                 ):
        self.dbCursor = dbCursor
        self.start_time = start_time
        self.close_time = close_time
        self.symbol = symbol
        self.candle_interval = candle_interval
        self.first_trade_id = first_trade_id
        self.last_trade_id = last_trade_id
        self.open_price = open_price
        self.close_price = close_price
        self.high_price = high_price
        self.low_price = low_price
        self.base_asset_volume = base_asset_volume
        self.number_of_trades = number_of_trades
        self.quote_asset_volume = quote_asset_volume
        self.taker_buy_base_asset_volume = taker_buy_base_asset_volume
        self.taker_buy_quote_asset_volume = taker_buy_quote_asset_volume
        self.ignore_info = ignore_info
        self.source_exchange = source_exchange
        self.update_time = update_time

    def addData(self):
        self.update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.dbCursor.execute(
                "INSERT INTO candle_price (start_time, close_time, symbol, candle_interval, first_trade_id, "
                "  last_trade_id, open_price, close_price, high_price, low_price, base_asset_volume, number_of_trades,"
                "  quote_asset_volume, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, "
                "  ignore_info, source_exchange, update_time) "
                "VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.start_time, self.close_time, self.symbol, self.candle_interval, self.first_trade_id,
                 self.last_trade_id, self.open_price, self.close_price, self.high_price, self.low_price,
                 self.base_asset_volume, self.number_of_trades, self.quote_asset_volume,
                 self.taker_buy_base_asset_volume, self.taker_buy_quote_asset_volume, self.ignore_info,
                 self.source_exchange, self.update_time))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO candle_price"
            log.addData()

    def getData(self, url=None, symbol=None, candleInterval=None, startTime=None, endTime=None, limit=None):
        dbCursor = self.dbCursor
        prm = {}
        if (startTime is None) and (endTime is None) and (limit is not None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   # 'startTime': startTime,
                   # 'endTime': endTime,
                   'limit': limit
                   }
        if (startTime is not None) and (endTime is None) and (limit is not None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   'startTime': startTime,
                   'limit': limit
                   }
        if (startTime is not None) and (endTime is None) and (limit is None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   'startTime': startTime
                   }
        if (startTime is not None) and (endTime is not None) and (limit is None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   'startTime': startTime,
                   'endTime': endTime
                   }
        if (startTime is not None) and (endTime is not None) and (limit is not None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   'startTime': startTime,
                   'endTime': endTime,
                   'limit': limit
                   }

        try:
            response = requests.get(url, params=prm)
            response.raise_for_status()
            records = response.json()
            return records
        #            return records['result']['data']
        except requests.exceptions.HTTPError as errh:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = errh.response.status_code
            log.errorMessage = errh.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Http Error: " + errh.response.url
            log.addData()
        except requests.exceptions.ConnectionError as errc:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = errc.response.status_code
            log.errorMessage = errc.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Error Connecting: " + errc.response.url
            log.addData()
        except requests.exceptions.Timeout as errt:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = errt.response.status_code
            log.errorMessage = errt.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Timeout Error: " + errt.response.url
            log.addData()
        except requests.exceptions.RequestException as err:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = err.response.status_code
            log.errorMessage = err.response.text + symbol
            log.moduleName = type(self).__name__
            log.explanation = err.response.url
            log.addData()

    def getDataWithSession(self, session=None, url=None, symbol=None, candleInterval=None, startTime=None, endTime=None, limit=None):
        dbCursor = self.dbCursor
        prm = {}
        if (startTime is None) and (endTime is None) and (limit is not None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   # 'startTime': startTime,
                   # 'endTime': endTime,
                   'limit': limit
                   }
        if (startTime is not None) and (endTime is None) and (limit is not None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   'startTime': startTime,
                   'limit': limit
                   }
        if (startTime is not None) and (endTime is None) and (limit is None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   'startTime': startTime
                   }
        if (startTime is not None) and (endTime is not None) and (limit is None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   'startTime': startTime,
                   'endTime': endTime
                   }
        if (startTime is not None) and (endTime is not None) and (limit is not None):
            prm = {'symbol': symbol,
                   'interval': candleInterval,
                   'startTime': startTime,
                   'endTime': endTime,
                   'limit': limit
                   }

        try:
            response = session.get(url, params=prm)
            response.raise_for_status()
            records = response.json()
            return records
        #            return records['result']['data']
        except requests.exceptions.HTTPError as errh:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = errh.response.status_code
            log.errorMessage = errh.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Http Error: " + errh.response.url
            log.addData()
        except requests.exceptions.ConnectionError as errc:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = errc.response.status_code
            log.errorMessage = errc.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Error Connecting: " + errc.response.url
            log.addData()
        except requests.exceptions.Timeout as errt:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = errt.response.status_code
            log.errorMessage = errt.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Timeout Error: " + errt.response.url
            log.addData()
        except requests.exceptions.RequestException as err:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = err.response.status_code
            log.errorMessage = err.response.text + symbol
            log.moduleName = type(self).__name__
            log.explanation = err.response.url
            log.addData()

    def fetchWithLimit(self, symbol=None, candleInterval=None, startTime=None, limit=None):
        try:
            self.dbCursor.execute(
                "SELECT * FROM "
                "(SELECT JSON_OBJECT('start_time', start_time, 'close_time', close_time, 'symbol', symbol, "
                "'open_price', open_price, 'close_price', close_price, 'high_price', high_price, "
                "'low_price', low_price,"
                "'base_asset_volume', base_asset_volume, 'number_of_trades', number_of_trades) "
                "FROM candle_price "
                "WHERE symbol=? AND candle_interval=? AND start_time<=? "
                "ORDER BY start_time LIMIT ?) tbl "
                " ORDER BY tbl.start_time asc",
                (symbol, candleInterval, startTime, limit)
            )
            rows = self.dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "fetchWithLimit candle_price"
            log.addData()

    def fetchWithLimitDf(self, symbol=None, candleInterval=None, startTime=None, limit=None):
        try:
            self.dbCursor.execute(
                " SELECT * FROM "
                " (SELECT start_time, close_time, symbol, open_price, close_price, high_price, "
                " low_price, base_asset_volume, number_of_trades "
                "FROM candle_price "
                "WHERE symbol=? AND candle_interval=? AND start_time<=? "
                "ORDER BY start_time desc LIMIT ?) tbl"
                " ORDER BY tbl.start_time asc",
                (symbol, candleInterval, startTime, limit)
            )
            rows = self.dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "fetchWithLimitDf candle_price"
            log.addData()

    def fetchMinDate(self, symbol=None, candleInterval=None):
        try:
            self.dbCursor.execute(
                "SELECT min(start_time) FROM candle_price WHERE symbol=? AND candle_interval=?",
                (symbol, candleInterval,)
            )
            result = self.dbCursor.fetchone()
            return result[0]
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "fetchMinDate candle_price"
            log.addData()

    def fetchMaxCloseTime(self, symbol=None, candleInterval=None):
        try:
            self.dbCursor.execute(
                "SELECT max(close_time) FROM candle_price WHERE symbol=? AND candle_interval=?",
                (symbol, candleInterval,)
            )
            result = self.dbCursor.fetchone()
            return result[0]
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "fetchMaxCloseTime candle_price"
            log.addData()

    def fetchCount(self, symbol=None, candleInterval=None):
        try:
            self.dbCursor.execute(
                "SELECT count(*) FROM candle_price WHERE symbol=? AND candle_interval=?",
                (symbol, candleInterval,)
            )
            result = self.dbCursor.fetchone()
            return result[0]
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "fetchCount candle_price"
            log.addData()

    def fetchStartRow(self, symbol=None, candleInterval=None, startTime=None, limit=None):
        try:
            self.dbCursor.execute(
                "SELECT JSON_OBJECT('start_time', start_time, 'close_time', close_time, 'symbol', symbol, "
                "'open_price', open_price, 'close_price', close_price, 'high_price', high_price, "
                "'low_price', low_price,"
                "'base_asset_volume', base_asset_volume, 'number_of_trades', number_of_trades) "
                "FROM candle_price "
                "WHERE symbol=? AND candle_interval=? AND start_time>=? "
                "ORDER BY start_time LIMIT ?, 1 ",
                (symbol, candleInterval, startTime, limit - 1)
            )
            result = self.dbCursor.fetchone()
            return result
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "fetchStartRow candle_price"
            log.addData()

    def fetchAll(self, symbol=None, candleInterval=None, startTime=None):
        try:
            self.dbCursor.execute(
                "SELECT JSON_OBJECT('start_time', start_time, 'close_time', close_time, 'symbol', symbol, "
                "'open_price', open_price, 'close_price', close_price, 'high_price', high_price, "
                "'low_price', low_price,"
                "'base_asset_volume', base_asset_volume, 'number_of_trades', number_of_trades) "
                "FROM candle_price "
                "WHERE symbol=? AND candle_interval=? AND start_time>=? "
                "ORDER BY start_time ",
                (symbol, candleInterval, startTime)
            )
            rows = self.dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = self.dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "fetchAll candle_price"
            log.addData()


class BinanceOrderBook:
    def getData(self, dbCursor=None, url=None, symbol=None, limit=None):
        prm = {}
        if limit is not None:
            prm = {'symbol': symbol, 'limit': limit}
        if limit is None:
            prm = {'symbol': symbol}

        try:
            response = requests.get(url, params=prm)
            response.raise_for_status()
            records = response.json()
            return records
            # return records['symbols']
        except requests.exceptions.HTTPError as errh:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errh.response.status_code
            log.errorMessage = errh.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Http Error: " + errh.response.url
            log.addData()
        except requests.exceptions.ConnectionError as errc:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errc.response.status_code
            log.errorMessage = errc.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Error Connecting: " + errc.response.url
            log.addData()
        except requests.exceptions.Timeout as errt:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errt.response.status_code
            log.errorMessage = errt.response.text
            log.moduleName = type(self).__name__
            log.explanation = "Timeout Error: " + errt.response.url
            log.addData()
        except requests.exceptions.RequestException as err:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = err.response.status_code
            log.errorMessage = err.response.text
            log.moduleName = type(self).__name__
            log.explanation = err.response.url
            log.addData()


class Trade:
    def __init__(self,
                 symbol=None,
                 period=None,
                 period_time=None,
                 buy_date=None,
                 buy_price=None,
                 buy_lot=None,
                 buy_amount=None,
                 buy_commission=None,
                 stop_price=None,
                 stop_type=None,
                 stop_height=None,
                 stop_change_count=None,
                 sell_target=None,
                 sell_date=None,
                 sell_price=None,
                 sell_lot=None,
                 sell_amount=None,
                 sell_commission=None,
                 status=None,
                 buy_signal_name=None,
                 sell_signal_name=None,
                 current_period_time=None,
                 explanation=None,
                 profit_target=None,
                 strategy=None,
                 btc_signal=None,
                 btc_inner_signal=None,
                 btc_red_alert=None,
                 btc_green_alert=None,
                 target_line=None,
                 target_price=None,
                 slope=None,
                 max_price=None,
                 min_price=None
                 ):
        self.symbol=symbol
        self.period=period
        self.period_time=period_time
        self.buy_date=buy_date
        self.buy_price=buy_price
        self.buy_lot=buy_lot
        self.buy_amount=buy_amount
        self.buy_commission=buy_commission
        self.stop_price=stop_price
        self.stop_type=stop_type
        self.stop_height=stop_height
        self.stop_change_count=stop_change_count
        self.sell_target=sell_target
        self.sell_date=sell_date
        self.sell_price=sell_price
        self.sell_lot=sell_lot
        self.sell_amount=sell_amount
        self.sell_commission=sell_commission
        self.status=status
        self.buy_signal_name=buy_signal_name
        self.sell_signal_name=sell_signal_name
        self.current_period_time=current_period_time
        self.explanation=explanation
        self.profit_target=profit_target
        self.strategy=strategy
        self.btc_signal=btc_signal
        self.btc_inner_signal=btc_inner_signal
        self.btc_red_alert=btc_red_alert
        self.btc_green_alert=btc_green_alert
        self.target_line=target_line
        self.target_price=target_price
        self.slope=slope
        self.max_price=max_price
        self.min_price=min_price

    def addTrade(self, dbCursor=None):
        # self.upddate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            dbCursor.execute(
                "INSERT INTO trade (symbol, period, period_time, buy_date, buy_price, buy_lot, buy_amount, "
                "buy_commission, stop_price, stop_type, stop_height, stop_change_count, sell_target, sell_date, "
                "sell_price, sell_lot, sell_amount, sell_commission, status, buy_signal_name, "
                "sell_signal_name, current_period_time, explanation, profit_target, strategy, "
                "btc_signal, btc_inner_signal, btc_red_signal, btc_green_signal, target_line,"
                "target_price, slope, max_price, min_price) "
                "VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.symbol, self.period, self.period_time, self.buy_date, self.buy_price, self.buy_lot,
                 self.buy_amount, self.buy_commission, self.stop_price, self.stop_type, self.stop_height,
                 self.stop_change_count, self.sell_target, self.sell_date, self.sell_price, self.sell_lot,
                 self.sell_amount, self.sell_commission, self.status, self.buy_signal_name, self.sell_signal_name,
                 self.current_period_time, self.explanation, self.profit_target, self.strategy,
                 self.btc_signal, self.btc_inner_signal, self.btc_red_alert, self.btc_green_alert,
                 self.target_line, self.target_price, self.slope, self.max_price, self.min_price))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO trade"
            log.addData()

    def readProfitSummary(self, dbCursor=None):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT ('strategy', strategy, 'kar', ROUND(kar,2), 'zarar', ROUND(zarar, 2), "
                " 'fark', ROUND((kar - zarar) ,2) )"
                "FROM ( "
                "   SELECT strategy,"
                "          NVL(ABS(SUM(case when fark >= 0 then fark end)),0) kar,"
                "          NVL(ABS(SUM(case when fark < 0 then fark END)),0) zarar"
                "   FROM ( "
                "      SELECT strategy, ((sell_amount - sell_commission) - (buy_amount + buy_commission)) fark"
                "        FROM trade WHERE status = 2"
                "   ) t GROUP BY strategy "
                ") summary"
            )
            rows = dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "SELECT trade"
            log.addData()

    def readTrade(self, dbCursor=None, symbol=None, status=None):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT('symbol', symbol, 'period', period, 'period_time', period_time, "
                "'buy_date', buy_date, 'buy_price', buy_price, "
                "'buy_lot', buy_lot, 'buy_amount', buy_amount, 'buy_commission', buy_commission, "
                "'stop_price', stop_price, 'stop_type', stop_type, 'stop_height', stop_height, "
                "'stop_change_count', stop_change_count,"
                "'sell_target', sell_target, 'sell_date', sell_date, 'sell_price', sell_price, 'sell_lot', "
                "sell_lot, 'sell_amount', sell_amount, 'sell_commission', sell_commission, 'status', status, "
                "'buy_signal_name', buy_signal_name, 'sell_signal_name', sell_signal_name, "
                "'current_period_time', current_period_time,'explanation', explanation, "
                "'profit_target', profit_target, 'strategy', strategy, 'target_line', target_line, "
                "'target_price', target_price, 'slope', slope, 'max_price', max_price, 'min_price', min_price) "
                "FROM trade WHERE symbol=? AND status=? ",
                (symbol, int(status))
            )
            row = dbCursor.fetchone()
            return row
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "SELECT trade"
            log.addData()

    def readTradeAll(self, dbCursor=None, status=None):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT('symbol', symbol, 'period', period, 'period_time', period_time, "
                "'buy_date', buy_date, 'buy_price', buy_price, "
                "'buy_lot', buy_lot, 'buy_amount', buy_amount, 'buy_commission', buy_commission, "
                "'stop_price', stop_price, 'stop_type', stop_type, 'stop_height', stop_height, "
                "'stop_change_count', stop_change_count, "
                "'sell_target', sell_target, 'sell_date', sell_date, 'sell_price', sell_price, 'sell_lot', sell_lot, "
                "'sell_amount', sell_amount, 'sell_commission', sell_commission, 'status', status, "
                "'buy_signal_name', buy_signal_name, 'sell_signal_name', sell_signal_name, "
                "'current_period_time', current_period_time, 'explanation', explanation, "
                "'profit_target', profit_target, 'strategy', strategy, 'target_line', target_line, "
                "'target_price', target_price, 'slope', slope, 'max_price', max_price, 'min_price', min_price) "
                "FROM trade WHERE status=0"
            )
            rows = dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "SELECT ALL trade"
            log.addData()

    def updateTrade(self, dbCursor=None, symbol=None, oldStatus=None, newStatus=None):
        try:
            dbCursor.execute("UPDATE trade SET sell_date=?, sell_price=?, sell_lot=?, sell_amount=?, "
                             "sell_commission=?, status=?, sell_signal_name=? "
                             "WHERE symbol=? and status=?",
                             (self.sell_date, self.sell_price, self.sell_lot, self.sell_amount, self.sell_commission,
                              newStatus, self.sell_signal_name,
                              symbol, oldStatus))

        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "UPDATE trade"
            log.addData()

    def updateStopPrice(self, dbCursor=None, symbol=None, stopPrice=None, stopChangeCount=None, status=None):
        try:
            dbCursor.execute("UPDATE trade SET stop_price=?, stop_change_count=? "
                             "WHERE symbol=? and status=?",
                             (stopPrice, stopChangeCount, symbol, status))
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "UPDATE trade"
            log.addData()

    def updateStopPriceAndCurrentPeriod(self, dbCursor=None, symbol=None, stopPrice=None,
                                        currentPeriodTime=None, stopChangeCount=None, status=None):
        try:
            dbCursor.execute("UPDATE trade SET stop_price=?, current_period_time=?, stop_change_count=? "
                             "WHERE symbol=? and status=?",
                             (stopPrice, currentPeriodTime, stopChangeCount, symbol, status))
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "UPDATE trade"
            log.addData()

    def updateProfitTarget(self, dbCursor=None, symbol=None, profitTarget=None, maxPrice=None, minPrice=None, status=None):
        try:
            dbCursor.execute("UPDATE trade SET profit_target=?, max_price=?, min_price=? "
                             "WHERE symbol=? and status=?",
                             (profitTarget, maxPrice, minPrice, symbol, status))
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "UPDATE trade profit"
            log.addData()


class Cuzdan:
    def __init__(self,
                 id=None,
                 sira=None,
                 coin=None,
                 kullanilabilir=None,
                 bloke=None,
                 upddate=None):
        self.id = id
        self.sira = sira
        self.coin = coin
        self.kullanilabilir = kullanilabilir
        self.bloke = bloke
        self.upddate = upddate

    def addData(self, dbCursor=None):
        self.upddate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            dbCursor.execute(
                "INSERT INTO cuzdan (id, sira, coin, kullanilabilir, bloke, upddate) "
                "VALUES (?, ?, ?, ? ,?, ?)",
                (self.id, self.sira, self.coin, self.kullanilabilir, self.bloke, self.upddate))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO cuzdan"
            log.addData()


class TestArbitraj:
    def __init__(self,
                 test_id=None,
                 borsa_id=None,
                 apair=None,
                 abase=None,
                 aquote=None,
                 abid_price=None,
                 aask_price=None,
                 abid_qty=None,
                 aask_qty=None,
                 bpair=None,
                 bbase=None,
                 bquote=None,
                 bbid_price=None,
                 bask_price=None,
                 bbid_qty=None,
                 bask_qty=None,
                 cpair=None,
                 cbase=None,
                 cquote=None,
                 cbid_price=None,
                 cask_price=None,
                 cbid_qty=None,
                 cask_qty=None,
                 yontem=None,
                 capraz=None,
                 islemTutar=None,
                 komisyon=None,
                 kar=None,
                 prg=None,
                 islzaman=None,
                 zamanstr=None):
        self.test_id = test_id
        self.borsa_id = borsa_id
        self.apair = apair
        self.abase = abase
        self.aquote = aquote
        self.abid_price = abid_price
        self.aask_price = aask_price
        self.abid_qty = abid_qty
        self.aask_qty = aask_qty
        self.bpair = bpair
        self.bbase = bbase
        self.bquote = bquote
        self.bbid_price = bbid_price
        self.bask_price = bask_price
        self.bbid_qty = bbid_qty
        self.bask_qty = bask_qty
        self.cpair = cpair
        self.cbase = cbase
        self.cquote = cquote
        self.cbid_price = cbid_price
        self.cask_price = cask_price
        self.cbid_qty = cbid_qty
        self.cask_qty = cask_qty
        self.yontem = yontem
        self.capraz = capraz
        self.islemTutar = islemTutar
        self.komisyon = komisyon
        self.kar = kar
        self.prg = prg
        self.islzaman = islzaman
        self.zamanstr = zamanstr

    def addData(self, dbCursor=None):
        # self.islzaman = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            dbCursor.execute(
                "INSERT INTO testarb (test_id, borsa_id, apair, abase, aquote, abid_price, aask_price, abid_qty, "
                "aask_qty, bpair, bbase, bquote, bbid_price, bask_price, bbid_qty, bask_qty, "
                "cpair, cbase, cquote, cbid_price, cask_price, cbid_qty, cask_qty, "
                "yontem, capraz, islem_tutar, komisyon, kar, prg, islzaman, zamanstr) "
                "VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.test_id, self.borsa_id, self.apair, self.abase, self.aquote, self.abid_price, self.aask_price,
                 self.abid_qty, self.aask_qty, self.bpair, self.bbase, self.bquote, self.bbid_price, self.bask_price,
                 self.bbid_qty, self.bask_qty, self.cpair, self.cbase, self.cquote, self.cbid_price, self.cask_price,
                 self.cbid_qty, self.cask_qty, self.yontem, self.capraz, self.islemTutar, self.komisyon,
                 self.kar, self.prg, self.islzaman, self.zamanstr))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO testarb"
            log.addData()


class Test:
    def __init__(self,
                 test_id=None,
                 candle_interval=None,
                 symbol=None,
                 candle_start_time=None,
                 candle_close_time=None,
                 description=None,
                 rsi=None,
                 lr=None,
                 update_time=None):
        self.test_id = test_id
        self.candle_interval = candle_interval
        self.symbol = symbol
        self.candle_start_time = candle_start_time
        self.candle_close_time = candle_close_time
        self.description = description
        self.rsi = rsi
        self.lr = lr
        self.update_time = update_time

    def addData(self, dbCursor=None):
        self.update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            dbCursor.execute(
                "INSERT INTO test (test_id, candle_interval, symbol, candle_start_time, "
                "  candle_close_time, description, rsi, lr, update_time) "
                "VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?)",
                (self.test_id, self.candle_interval, self.symbol, self.candle_start_time, self.candle_close_time,
                 self.description, self.rsi, self.lr, self.update_time))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO test"
            log.addData()

    def fetchAll(self, dbCursor, testId):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT('test_id', test_id, 'candle_interval', candle_interval, 'symbol', symbol, "
                " 'candle_start_time', candle_start_time, 'candle_close_time', candle_close_time, "
                " 'description', description, 'rsi', rsi, 'lr', lr) "
                "FROM test "
                "WHERE test_id = ? "
                "ORDER BY test_id, symbol, candle_start_time",
                (testId,)
            )
            rows = dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "fetchAll test"
            log.addData()

    def fetchAllAlarm(self, dbCursor, testId, rsiLevel, lrLevel):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT('test_id', t.test_id, 'symbol', t.symbol, 'candle_interval', t.candle_interval, "
                " 'candle_start_time', t.candle_start_time, 'candle_close_time', t.candle_close_time, "
                " 'open_price', c.open_price, 'close_price', c.close_price, 'rsi', t.rsi, 'lr', t.lr, "
                " 'low_lr', ((t.lr - c.close_price) / t.lr) * 100) "
                "FROM test t, candle_price c "
                "WHERE t.test_id = ? "
                "  AND t.symbol = c.symbol "
                "  AND t.candle_interval = c.candle_interval "
                "  AND t.candle_start_time = c.start_time "
                "  AND t.candle_close_time = c.close_time "
                "  AND t.rsi < ? "
                "  AND c.close_price > c.open_price "
                "  AND t.lr > c.close_price "
                "  AND ((t.lr - c.close_price) / t.lr) * 100 > ? "
                "ORDER BY t.test_id, t.symbol, t.candle_interval, t.candle_start_time",
                (testId, rsiLevel, lrLevel)
            )
            result = dbCursor.fetchall()
            return result
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "fetchAll test"
            log.addData()


class Simulation:
    def __init__(self,
                 symbol=None,
                 test_id=None,
                 status=None,
                 candle_start_time=None,
                 candle_close_time=None,
                 quantity=None,
                 buy_price=None,
                 buy_amount=None,
                 buy_commission=None,
                 sell_price=None,
                 sell_amount=None,
                 sell_commission=None,
                 profit=None,
                 loss=None,
                 update_time=None):
        self.symbol = symbol
        self.test_id = test_id
        self.status = status
        self.candle_start_time = candle_start_time
        self.candle_close_time = candle_close_time
        self.quantity = quantity
        self.buy_price = buy_price
        self.buy_amount = buy_amount
        self.buy_commission = buy_commission
        self.sell_price = sell_price
        self.sell_amount = sell_amount
        self.sell_commission = sell_commission
        self.profit = profit
        self.loss = loss
        self.update_time = update_time

    def addData(self, dbCursor=None):
        self.update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            dbCursor.execute(
                "INSERT INTO simulation (symbol, test_id, status, candle_start_time, candle_close_time, "
                "  quantity, buy_price, buy_amount, buy_commission, sell_price, sell_amount, sell_commission, "
                "  profit, loss, update_time) "
                "VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.symbol, self.test_id, self.status, self.candle_start_time, self.candle_close_time,
                 self.quantity, self.buy_price, self.buy_amount, self.buy_commission, self.sell_price,
                 self.sell_amount, self.sell_commission, self.profit, self.loss, self.update_time))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO simulation"
            log.addData()


class CounterType(Enum):
    TEST_COUNTER = 1
    CUZDAN_COUNTER = 2


class Counter:
    def __init__(self,
                 counter=None):
        self.counter = counter

    def getCounter(self, dbCursor=None, counterType=None):
        counterID = CounterType(counterType)
        # print(f"counter {member.name} - {member.value}")
        try:
            dbCursor.execute(
                "SELECT counter FROM counter WHERE id=?",
                (counterID.value,)
            )
            result = dbCursor.fetchone()
            updateTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if result is None:
                counter = 1
                dbCursor.execute(
                    "INSERT INTO counter (id, name, counter, update_time) "
                    "VALUES (?, ?, ?, ?)",
                    (counterID.value, counterID.name, counter, updateTime))
            else:
                counter = result[0] + 1
                dbCursor.execute("UPDATE counter SET counter = ?, update_time = ? WHERE id=?",
                                 (counter, updateTime, counterID.value))
            return counter
        except mariadb.Error as e:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "getCounter"
            log.addData()
