from datetime import datetime
import errorclass as error
import requests
import mariadb


class ExchangeInfo:
    def __init__(self,
                 dbCursor=None,
                 symbol=None,
                 baseAsset=None,
                 quoteAsset=None,
                 baseAssetName=None,
                 exchangeId=3,  # BTCTURK
                 status=0,
                 updateDate=datetime.now().strftime("%Y-%m-%d"),
                 updateTime=datetime.now().strftime("%H:%M:%S")
                 ):
        self.dbCursor = dbCursor
        self.symbol = symbol  # ETHBTC
        self.baseAsset = baseAsset  # ETH
        self.quoteAsset = quoteAsset  # BTC
        self.baseAssetName = baseAssetName  # ethereum
        self.exchangeId = exchangeId  # 3 (BTCTURK)
        self.status = status
        self.updateDate = updateDate
        self.updateTime = updateTime

    def getData(self, url=None):
        dbCursor = self.dbCursor

        try:
            response = requests.get(url)
            response.raise_for_status()
            records = response.json()
            return records['data']['symbols']
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


class OrderBook:
    def __init__(self,
                 dbCursor=None,
                 symbol=None,
                 bidPrice=None,
                 bidQty=None,
                 askPrice=None,
                 askQty=None
                 ):
        self.dbCursor = dbCursor
        self.symbol = symbol
        self.bidPrice = bidPrice
        self.bidQty = bidQty
        self.askPrice = askPrice
        self.askQty = askQty

    def getData(self, url=None):
        dbCursor = self.dbCursor

        prm = {'pairSymbol': self.symbol,
               'limit': 1
               }
        try:
            response = requests.get(url, params=prm)
            #            response = requests.get(url=prm)
            response.raise_for_status()
            records = response.json()
            return records['data']
        #            return records['result']['data']
        except requests.exceptions.HTTPError as errh:
            log = error.ErrorLog()
            log.dbCursor = dbCursor
            log.errorNo = errh.response.status_code
            log.errorMessage = errh.response.text
            log.moduleName = type(self).__name__
            if errh.response.status_code == 429:
                log.explanation = "Too many request: " + errh.response.url
            else:
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
