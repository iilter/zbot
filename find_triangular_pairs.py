import json
import botfunction as func
from botclass import BinanceExchangeInfo as symbolClass
from botclass import TriangularPair as pairClass


# Üçlü arbitraj çiftlerini bulma
# borsaId = 1 - Binance
#         = 3 - BTCTURK

def main():
    db = func.connectDB()
    dbCursor = db.cursor()

    borsaId = 1
    symbol = symbolClass()
    symbol.dbCursor = dbCursor
    symbolRows = symbol.readAllCoinAsset(exchangeId=borsaId)

    if (symbolRows is None) or (len(symbolRows) == 0):
        exit(0)

    adet = 0
    pair = pairClass()
    pair.dbCursor = dbCursor
    for aPairRow in symbolRows:
        aPair = json.loads(aPairRow[0])
        aPairBase = aPair["base"]
        aPairQuote = aPair["quote"]
        aPairSymbol = aPair["symbol"]

        if aPair["base"] == "AGIX" and aPairQuote == "USDT":
            a=0
        if aPairQuote == "USDT":
            for bPairRow in symbolRows:
                bPair = json.loads(bPairRow[0])
                bPairBase = bPair["base"]
                bPairQuote = bPair["quote"]
                bPairSymbol = bPair["symbol"]

                if aPairBase == bPairQuote:
                    for cPairRow in symbolRows:
                        cPair = json.loads(cPairRow[0])
                        cPairBase = cPair["base"]
                        cPairQuote = cPair["quote"]
                        cPairSymbol = cPair["symbol"]

                        if (bPairBase == cPairBase) and (aPairQuote == cPairQuote):
                            print(f"Pair A: {aPairSymbol} Pair B: {bPairSymbol} Pair C: {cPairSymbol}")
                            pair.exchangeId = borsaId
                            pair.aPairSymbol = aPairSymbol
                            pair.aPairBase = aPairBase
                            pair.aPairQuote = aPairQuote
                            pair.bPairSymbol = bPairSymbol
                            pair.bPairBase = bPairBase
                            pair.bPairQuote = bPairQuote
                            pair.cPairSymbol = cPairSymbol
                            pair.cPairBase = cPairBase
                            pair.cPairQuote = cPairQuote
                            pair.status = 0
                            pair.addTriangularPair()
                            adet = adet + 1

    print(f"Bitti adet {adet}")


main()
