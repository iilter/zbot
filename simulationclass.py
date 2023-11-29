import mariadb
import errorclass as error
import error_helper


class SimCandleClass:
    def __init__(self,
                 symbol=None,
                 period=None,
                 dates=None,
                 opens=None,
                 highs=None,
                 lows=None,
                 closes=None,
                 volumes=None
                 ):
        self.symbol = symbol
        self.period = period
        self.dates = dates
        self.opens = opens
        self.highs = highs
        self.lows = lows
        self.closes = closes
        self.volumes = volumes

    def addData(self, dbCursor=None):
        try:
            dbCursor.execute(
                "INSERT INTO sim_candle (symbol, period, date, open, high, low, close, volume) "
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?) ",
                (self.symbol, self.period, self.dates, self.opens, self.highs, self.lows, self.closes, self.volumes))
        except mariadb.Error as e:
            # print(f"Error: {e.errno} {e.errmsg}")
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = e.errno
            log.errorMessage = e.errmsg
            log.moduleName = type(self).__name__
            log.explanation = "INSERT INTO sim_candle "
            log.addData()

    @staticmethod
    def readAll(dbCursor=None, symbol=None, period=None):
        try:
            dbCursor.execute(
                "SELECT JSON_OBJECT('symbol', symbol, 'period', period, 'date', date, "
                "'open', open, 'high', high, 'low', low, 'close', close, 'volume', volume) "
                "FROM sim_candle WHERE symbol=? AND period=? ",
                (symbol, period)
            )
            rows = dbCursor.fetchall()
            return rows
        except mariadb.Error as e:
            helper = error_helper.ErrorHelper()
            helper.code = e.errno
            helper.msg = e.errmeg
            helper.module = 'sim_candle readAll'
            helper.explanation = "SELECT sim_candle"
            helper.addData()
