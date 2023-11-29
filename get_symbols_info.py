# BINANCE borsasında işlem gören symbol bilgilerini çeker.
# Tarih: 05.07.2022
import json
import sys
import mariadb
import click as click

import botfunction as func
from botclass import BinanceSymbol as symbolClass

"""
Binance borsasında işlem gören symbol (coin) listesini çeker SYMBOL tablosuna yazar
"""


def main():
    kayitSayisi = 0
    exchangeId = 1  # Binance
    db = func.connectDB()
    dbCursor = db.cursor()
    binanceConfig = func.readConfig(filename="config.ini", section="binance")

    url_api = binanceConfig["url_base"] + binanceConfig["url_exchangeinfo"]

    symbol = symbolClass()
    response = symbol.getData(url_api)
    if (response is not None) and (len(response) > 0):
        # print(f"{response}")
        # print(f"{len(response)}")
        for item in response:
            if item["status"] == "TRADING":
                symbol.symbol = item["symbol"]
                symbol.exchange_id = exchangeId
                symbol.base_asset = item["baseAsset"]
                symbol.base_asset_precision = item["baseAssetPrecision"]
                symbol.quote_asset = item["quoteAsset"]
                symbol.quote_precision = item["quotePrecision"]
                symbol.quote_asset_precision = item["quoteAssetPrecision"]
                symbol.symbol_status = item["status"]

                orderTypes = ','.join(item["orderTypes"])
                symbol.order_types = orderTypes

                symbol.iceberg_allowed = 1 if item["icebergAllowed"] is True else 0
                symbol.oco_allowed = 1 if item["ocoAllowed"] is True else 0
                symbol.quote_order_qty_market_allowed = 1 if item["quoteOrderQtyMarketAllowed"] is True else 0
                symbol.allow_trailing_stop = 1 if item["allowTrailingStop"] is True else 0
                symbol.cancel_replace_allowed = 1 if item["cancelReplaceAllowed"] is True else 0
                symbol.is_spot_trading_allowed = 1 if item["isSpotTradingAllowed"] is True else 0
                symbol.is_margin_trading_allowed = 1 if item["isMarginTradingAllowed"] is True else 0

                permission = ','.join(item["permissions"])
                symbol.permissions = permission

                # Filtre bilgileri ayrıştırılır
                filters = item["filters"]
                for filterItem in filters:
                    if filterItem["filterType"] == "PRICE_FILTER":
                        symbol.min_price = float(filterItem["minPrice"])
                        symbol.max_price = float(filterItem["maxPrice"])
                        symbol.tick_size = float(filterItem["tickSize"])
                    if filterItem["filterType"] == "LOT_SIZE":
                        symbol.min_lot = float(filterItem["minQty"])
                        symbol.max_lot = float(filterItem["maxQty"])
                        symbol.step_size = float(filterItem["stepSize"])
                    if filterItem["filterType"] == "NOTIONAL":
                        symbol.min_notional = float(filterItem["minNotional"])
                symbol.status = 0
                symbol.market_group = 0

                response = symbol.readExist(dbCursor=dbCursor, exchangeId=exchangeId, symbol=symbol.symbol)
                if (response is not None):
                    item = json.loads(response[0])
                    symbol.status = item["status"]
                    symbol.market_group = item["market_group"]
                    symbol.delete(dbCursor=dbCursor, exchangeId=exchangeId, symbol=symbol.symbol)

                # Symbol bilgileri SYMBOL tablosuna yazılır.
                symbol.addData(dbCursor=dbCursor)

                kayitSayisi = kayitSayisi + 1

    print(f"Kayıt Sayısı: {kayitSayisi}")

    db.commit()
    db.close()


main()
