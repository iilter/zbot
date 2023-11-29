import numpy
import numpy as np
import pandas as pd
from pandas_ta.volatility import kc
import pandas_ta as pta
import talib
from talib import MA_Type
import math
from scipy import stats
from numba import njit
import vectorbt as vbt
from ta.trend import STCIndicator
import plotly.express as px

import botfunction as func
import constant as cons


def get_sma_list(prices, period):
    """
    Hareketli ortalama
      prices: Fiyat bilgileri listesi
      period: Kaç tane mum verisi kullanılacağını belirtir.
      list array alır ve list array döner
    """
    return prices.rolling(period).mean()


# numpy array döner.
def get_sma(data, period=14):
    pl = []
    sm = []
    for ix in range(len(data)):
        pl.append(float(data[ix]))
        if (len(pl)) >= period:
            sm.append(sum(pl) / period)
            pl = pl[1:]
    return np.array(sm, dtype=float)


def get_ema(data, period=14):
    ema = talib.EMA(data, timeperiod=period)
    return ema


def get_lsma(data, period=25):
    pl = []
    lr = []
    for i in range(len(data)):
        pl.append(float(data[i]))
        if len(pl) >= period:
            sum_x = 0.0
            sum_y = 0.0
            sum_xy = 0.0
            sum_xx = 0.0
            sum_yy = 0.0
            for a in range(1, len(pl)+1):
                sum_x += a
                sum_y += pl[a-1]
                sum_xy += (pl[a-1] * a)
                sum_xx += (a*a)
                sum_yy += (pl[a-1] * pl[a-1])
            m = ((sum_xy - sum_x * sum_y / period) / (sum_xx - sum_x * sum_x / period))
            b = sum_y / period - m * sum_x / period
            lr.append(m * period + b)
            pl = pl[1:]
    return lr


def get_bolinger_bands(data, period, std_dev):
    dataSeries = pd.Series(data)
    u, c, d = talib.BBANDS(dataSeries, timeperiod=period, nbdevup=std_dev, nbdevdn=std_dev, matype=0)
    up = u.to_numpy(dtype=float)
    center = c.to_numpy(dtype=float)
    down = d.to_numpy(dtype=float)
    return up, center, down


def get_percent_bollinger(prices=None, period=20, standard_deviation=2):
    """ Bollinger %B indikatörü """
    up, center, down = get_bolinger_bands(data=prices, period=period, std_dev=standard_deviation)

    fUp = up.to_numpy(dtype=float)
    fDown = down.to_numpy(dtype=float)

    length = period - 1
    fUp = fUp[-length:]
    fUp = np.where(np.isnan(fUp), 0, fUp)

    fDown = fDown[-length:]
    fDown = np.where(np.isnan(fDown), 0, fDown)

    prices = prices[-length:]
    prices = np.where(np.isnan(prices), 0, prices)

    np.seterr(all='ignore')
    try:
        percentBollinger = np.divide((prices - fDown), (fUp - fDown))
    except ZeroDivisionError:
        return None
    except:
        return None

    percentBollinger = np.where(np.isnan(percentBollinger), 0, percentBollinger)
    return percentBollinger


def get_awesome_oscillator(highPrices, lowPrices, shortPeriod=5, longPeriod=34):
    medianPrices = (highPrices + lowPrices) / 2
#    median = prices.rolling(2).median()
    short = get_sma_list(medianPrices, shortPeriod)
    long = get_sma_list(medianPrices, longPeriod)

    ao = short - long
    ao = ao.to_frame(name='ao')
    ao['short'] = short
    ao['long'] = long
    return ao

# def get_trend_slope(prices, period):
#     # prices eleman sayısı, period ile eşit olmaz ise linregress hataya verdiği için
#     # local değişkende eleman sayısının period ile aynı olması sağlandı.
#     localPrices = prices
#     if prices.size > period:
#         delLength = prices.size - period
#         # ilk len eleman silinir
#         localPrices = prices[delLength:]
#
#     xs = np.array([ix for ix in range(0, period)], dtype=np.int)
#
#     # y ekseni : localPrices
#     # x ekseni : xs
#     res = stats.linregress(xs, localPrices)
#     return res.slope

def get_linear_regression(prices=None, period=100,
                          standard_deviation_up_factor=None, standard_deviation_down_factor=None):
    """ regresyon doğrusu üzerinde x eksenindeki koordinata karşılık gelen y ekseni koordinatını döner """
    def get_point_on_line(x=None, slope=None, intercept=None):
        return (slope * x) + intercept

    """ 
    y ekseni olarak prices 
    x ekseni olarak 0, 1, 2, ...
    yapılır
    """
    ys = prices
    if ys.size > period:
        """ period uzunluğundan fazla olan kısım silinir """
        delLength = ys.size - period
        ys = ys[delLength:]

    xs = np.array([ix for ix in range(0, ys.size)], dtype=np.int)


    slope, average, intercept = get_linear_regression_slope(ys, ys.size)

    center = np.array(get_point_on_line(xs, slope=slope, intercept=intercept))
    channelCenter = pd.Series(center, dtype=float)
    channelCenter = round(channelCenter, 8)

    stdDev = calculate_deviation(prices=ys,
                                 period=period,
                                 slope=slope,
                                 average=average,
                                 intercept=intercept)

    """
    Trendin yönü YUKARI ise kanal yüksekliğinin daha dar olması için standard sapma değeri daha küçük tutulur.
    Trendin yönü AŞAĞI ise kanal yüksekliğinin daha geniş olması için standard sapma değeri daha büyük tutulur.
    Yani; aşağı yönlü trendde daha aşağıda fiyatta işleme girmeye çalışılır. 
    Bu nedenle kanal genişletilerek kanal alt bandı daha aşağı taşınır ki fiyat onun altına düştüğünde
    işleme girilsin.
    """
    if slope > 0:
        channelUp = channelCenter + (stdDev * standard_deviation_up_factor)
        channelDown = channelCenter - (stdDev * standard_deviation_up_factor)
    else:
        channelUp = channelCenter + (stdDev * standard_deviation_down_factor)
        channelDown = channelCenter - (stdDev * standard_deviation_down_factor)

    up = channelUp.to_numpy(dtype=float)
    center = channelCenter.to_numpy(dtype=float)
    down = channelDown.to_numpy(dtype=float)
    return up, center, down, slope


def calculate_deviation(prices=None, period=None, slope=None, average=None, intercept=None):
    stdDevAcc = float(0.0)
    periods = period - 1
    val = intercept # periods = 0 ın yani lr center başlangıç noktası
    """
    Kapanış fiyatı ile lineer regresyon doğrusu arasındaki farkların standart sapması hesaplanır.
    val değişkeni: iterasyon boyunca lineer regrasyon doğrusu üzerindeki koordinat
    """
    for jx in range(periods):
        price = prices[jx]
        price -= val
        stdDevAcc += price * price
        val += slope

    stDev = math.sqrt(stdDevAcc / periods)
    return stDev


def get_percent_linear_regression(prices=None, period=100, standard_deviation_up_factor=None,
                                  standard_deviation_down_factor=None):
    up, center, down, slope = get_linear_regression(prices=prices,
                                                    period=period,
                                                    standard_deviation_up_factor=standard_deviation_up_factor,
                                                    standard_deviation_down_factor=standard_deviation_down_factor)
    fPrices = prices
    fUp = up.to_numpy(dtype=float)
    fDown = down.to_numpy(dtype=float)

    """ dizi uzunlukları farklı ise eşitlenir. """
    if fPrices.size > fUp.size:
        delLen = fPrices.size - fUp.size
        fPrices = fPrices[delLen:]

    percentLR = (fPrices - fDown) / (fUp - fDown)
    return percentLR


def get_linear_regression_slope(prices=None, period=None):
    ys = prices
    if ys.size > period:
        """ period uzunluğundan fazla olan kısım silinir """
        delLength = ys.size - period
        ys = ys[delLength:]
        prices = ys

    sumX = float(0.0)
    sumY = float(0.0)
    sumX_sqr = float(0.0)
    sumXY = float(0.0)
    ix = float(0.0)
    for val in prices:
        price = val
        per = ix + 1.0
        sumX = sumX + per
        sumY = sumY + val
        sumX_sqr = sumX_sqr + (per * per)
        sumXY = sumXY + (val * per)
        ix = ix + 1
    slope = ((period * sumXY) - (sumX * sumY)) / ((period * sumX_sqr) - (sumX * sumX))
    slope2 = (period * sumXY - sumX * sumY) / (period * sumX_sqr - sumX * sumX)
    average = sumY / period
    intercept= average - slope * sumX / period + slope

    return slope, average, intercept


def get_slope_scipy(data=None, period=None):
    ys = data
    if ys.size > period:
        """ period uzunluğundan fazla olan kısım silinir """
        delLength = ys.size - period
        ys = ys[delLength:]

    xs = np.array([ix for ix in range(0, ys.size)], dtype=np.int)

    slope, intercept, r_value, p_value, std_err = stats.linregress(xs,ys)
    return slope, intercept


def get_linear_regression_talib(data, period=100):
    lr = talib.LINEARREG(data, period)
    return lr


def get_slope_talib(data, period=90):
    slope = talib.LINEARREG_SLOPE(data, timeperiod=period)
    return slope


def get_stochastic(high=None, low=None, close=None, timePeriod=14, slowKPeriod=3, slowDPeriod=3):
    fastK, slowD = talib.STOCH(high=high, low=low, close=close,
                               fastk_period=timePeriod, slowk_period=slowKPeriod,
                               slowk_matype=0, slowd_period=slowDPeriod, slowd_matype=0)
    return fastK, slowD


def get_smi(high=None, low=None, close=None, kLength=3, dLength=3, emaPeriod=10):
    """
    Stochastic Momentum Index (SMI)
    :return:
    """
    def ema_ema(source=None, length=None):
        dd = get_ema(data=source, period=length)
        return get_ema(data=dd, period=length)

    highestHigh = pd.Series(high).rolling(window=kLength).max()
    lowestLow = pd.Series(low).rolling(window=kLength).min()
    diff = highestHigh - lowestLow
    relativeDiff = close - (highestHigh + lowestLow) / 2

    smi = 200 * (ema_ema(source=relativeDiff.to_numpy(dtype=float), length=dLength) / ema_ema(source=diff.to_numpy(dtype=float), length=dLength))
    signal = get_ema(data=smi, period=emaPeriod)

    return smi, signal

def get_rsi(prices=None, timePeriod=14):
    rsi = talib.RSI(prices, timeperiod=timePeriod)
    return rsi


def get_stochrsi(prices=None, timePeriod=None, slowKPeriod=None, slowDPeriod=None):
    rsi = get_rsi(prices=prices, timePeriod=timePeriod)
    """ rsi dizisinden sıfırlar kaldırılır """
    rsi = rsi[~np.isnan(rsi)]

    fastK, slowD = get_stochastic(high=rsi, low=rsi, close=rsi,
                                  timePeriod=timePeriod, slowKPeriod=slowKPeriod, slowDPeriod=slowDPeriod)

    return fastK, slowD


def get_mfi(high=None, low=None, close=None, volume=None, period=14):
    """ Money Flow Index (MFI) """
    typical_price = (close + high + low) / 3
    money_flow = typical_price * volume

    positive_flow = []
    negative_flow = []

    for ix in range(1, len(typical_price)):
        if typical_price[ix] > typical_price[ix-1]:
            positive_flow.append(money_flow[ix-1])
            negative_flow.append(0.0)
        elif typical_price[ix] < typical_price[ix-1]:
            negative_flow.append(money_flow[ix-1])
            positive_flow.append(0.0)
        else:
            positive_flow.append(0.0)
            negative_flow.append(0.0)

    # Get all of the positive and negative money flows within the time period
    positive_mf = []
    negative_mf = []

    for ix in range(period-1, len(positive_flow)):
        first = ix + 1 - period
        last = ix + 1
        positive_mf.append(sum(positive_flow[first : last]))

    for ix in range(period-1, len(negative_flow)):
        first = ix + 1 - period
        last = ix + 1
        negative_mf.append(sum(negative_flow[first : last]))

    # Calculate the money flow index
    # money_flow_ratio = np.array(positive_mf) / np.array(negative_mf)
    # mfi = 100 - (100/(1 + money_flow_ratio))
    mfi = 100 * (np.array(positive_mf) / (np.array(positive_mf) + np.array(negative_mf)))
    return mfi


def get_mfi_talib(high=None, low=None, close=None, volume=None, period=14):
    mfi = talib.MFI(high, low, close, volume, timeperiod=period)
    return mfi


def get_adx(high=None, low=None, close=None, period=14):
    adx = talib.ADX(high, low, close, timeperiod=period)
    return adx


def get_dmi(high=None, low=None, close=None, period=14):
    di_plus = talib.PLUS_DI(high, low, close, timeperiod=period)
    di_minus = talib.MINUS_DI(high, low, close, timeperiod=period)
    return di_plus, di_minus


def get_adx_dmi(high=None, low=None, close=None, period=14):
    adx = talib.ADX(high, low, close, timeperiod=period)
    di_plus = talib.PLUS_DI(high, low, close, timeperiod=period)
    di_minus = talib.MINUS_DI(high, low, close, timeperiod=period)
    return adx, di_plus, di_minus


def get_candle_color(open=None, close=None):
    if open > close:
        return cons.CANDLE_RED
    if close >= open:
        return cons.CANDLE_GREEN


def get_candles_color(open=None, close=None):
    colors = [get_candle_color(open=open[ix], close=close[ix]) for ix in range(0, len(close) -1)]
    return colors


# def get_vwap(data=None, volume=None, period=None):
#     ix = 0
#     totalTp = 0
#     totalVol = 0
#     start = len(data) - period
#     end = len(data)
#     datas = data[start:end]
#     volumes = volume[start:end]
#     vwap = []
#
#     for item in datas:
#         totalTp += item * volumes[ix]
#         totalVol += volumes[ix]
#         if totalVol > float(0.0):
#             vwap.append(totalTp / totalVol)
#         ix += 1
#     return vwap

def get_vwma(data=None, volume=None, period=None):
    """ Volume Weighted Moving Average """

    dv = data * volume
    dvSMA = get_sma(data=dv, period=period)
    vlSMA = get_sma(data=volume, period=period)
    vwma = dvSMA / vlSMA
    return vwma


def get_wma(data=None, period=None):
    wma = talib.WMA(data, timeperiod=period)
    return wma


def get_hma(data=None, period=None):
    h1 = 2 * get_wma(data, period=period/2)
    h2 = get_wma(data, period=period)
    hma = get_wma(data=(h1 - h2), period=int(np.sqrt(period)))
    return hma


def get_macd(data=None, fastPeriod=None, slowPeriod=None, signalPeriod=None):
    macd, macdSignal, macdHist = talib.MACD(data, fastperiod=fastPeriod, slowperiod=slowPeriod, signalperiod=signalPeriod)
    return macd, macdSignal, macdHist


def get_roc(data=None, period=None):
    roc = talib.ROC(data, timeperiod=period)
    return roc


def get_sar(high=None, low=None, acceleration=None, maximum=None):
    sar = talib.SAR(high=high, low=low, acceleration=acceleration, maximum=maximum)
    return sar


def get_engulfing_pattern(open=None, high=None, low=None, close=None):
    pattern = talib.CDLENGULFING(open, high, low, close)
    return pattern


def get_morningstar_pattern(open=None, high=None, low=None, close=None):
    morningStar = talib.CDLMORNINGSTAR(open, high, low, close, penetration=0)
    return morningStar


def get_tsi(data=None, long=25, short=13, signal=13):
    ls = []
    for ix in range(1, len(data)):
        ls.append(data[ix] - data[ix-1])

    df = pd.DataFrame(ls, columns=['diff'])
    diff = df['diff']
    abs_diff = abs(diff)

    diff_smoothed = diff.ewm(span=long, adjust=False).mean()
    diff_double_smoothed = diff_smoothed.ewm(span=short, adjust=False).mean()
    abs_diff_smoothed = abs_diff.ewm(span=long, adjust=False).mean()
    abs_diff_double_smoothed = abs_diff_smoothed.ewm(span=short, adjust=False).mean()

    tsi = (diff_double_smoothed / abs_diff_double_smoothed) * 100
    signal = tsi.ewm(span=signal, adjust=False).mean()
    return tsi.to_numpy(dtype=float), signal.to_numpy(dtype=float)


def get_min_max(minData=None, maxData=None, period=90):
    dataLow = minData[-(period+1):-1]
    dataHigh = maxData[-(period+1):-1]
    minimum = min(dataLow)
    maximum = max(dataHigh)

    return minimum, maximum


def get_vp(open=None, high=None, low=None, close=None, volume=None, barCount=90, histogramCount=20,
           valueAreaPercentage=70, tickSize=None):
    lengthClose = len(close)
    if lengthClose < barCount:
        return False

    dataOpen = open[-(barCount+1):-1]
    dataHigh = high[-(barCount+1):-1]
    dataLow = low[-(barCount+1):-1]
    dataClose = close[-(barCount+1):-1]
    dataVolume = volume[-(barCount+1):-1]

    # data = {'close':dataClose, 'volume':dataVolume}
    # df = pd.DataFrame(data)
    # px.histogram(df, x='volume', y='close', nbins=50, orientation='h').show()

    bins = np.linspace(min(dataLow), max(dataHigh), histogramCount)

    # minRange = min(dataLow)
    # maxRange = max(dataHigh)
    # histStep = (maxRange - minRange) / (histogramCount)

    # binArray = []
    # binArray.append(minRange)
    # val = minRange
    # for ix in range(0, histogramCount):
    #     val += histStep
    #     binArray.append(val)
    # bins = np.array(binArray)

    for ix in range(0, len(bins)):
        bins[ix] = func.round_tick_size(bins[ix], tick_size=tickSize)

    histArray = [float(0.0) for i in range(histogramCount)]

    for jx in range(0, len(dataClose)):
        barHeight = dataHigh[jx] - dataLow[jx]
        price = dataClose[jx]
        vol = 0.0
        if dataHigh[jx] != dataLow[jx]:
            buyVol = 0.0
            sellVol = 0.0
            if dataClose[jx] >= dataOpen[jx]:
                # buyVol = dataVolume[jx] * (dataClose[jx] - dataLow[jx]) / barHeight
                buyVol = dataVolume[jx]
            else:
                # sellVol = dataVolume[jx] * (dataHigh[jx] - dataClose[jx]) / barHeight
                sellVol = dataVolume[jx]
            vol = buyVol + sellVol

        for hx in range(0, histogramCount-1):
            prev = bins[hx]
            next = bins[hx+1]
            if (price >= prev) and (price < next):
                histArray[hx] += vol
                break

    hist = np.array(histArray)
    return hist, bins


def get_heikin_ashi(open=None, high=None, low=None, close=None):
    """
    Heikin-Ashi Mumları (HA)
    Hesaplama:
        HA_OPEN[0] = (open[0] + close[0]) / 2
        HA_CLOSE = (open + high + low + close) / 4

        for i > 1 in len(close):
            HA_OPEN = (HA_OPEN[i−1] + HA_CLOSE[i−1]) / 2

        HA_HIGH = MAX(HA_OPEN, HA_HIGH, HA_CLOSE)
        HA_LOW = MIN(HA_OPEN, HA_LOW, HA_CLOSE)
    """
    df = pd.DataFrame({
        "HA_open": 0.5 * (open[0] + close[0]),
        "HA_high": high,
        "HA_low": low,
        "HA_close": 0.25 * (open + high + low + close),
    })

    m = len(close)
    for i in range(1, m):
        df["HA_open"][i] = 0.5 * (df["HA_open"][i - 1] + df["HA_close"][i - 1])

    df["HA_high"] = df[["HA_open", "HA_high", "HA_close"]].max(axis=1)
    df["HA_low"] = df[["HA_open", "HA_low", "HA_close"]].min(axis=1)

    haClose = df["HA_close"].to_numpy(dtype=float)
    haOpen = df["HA_open"].to_numpy(dtype=float)
    haHigh = df["HA_high"].to_numpy(dtype=float)
    haLow = df["HA_low"].to_numpy(dtype=float)

    return haOpen, haHigh, haLow, haClose


def get_smoothed_heikin_ashi(open=None, high=None, low=None, close=None, period=None, averageType=cons.AVERAGE_EMA):
    if averageType == cons.AVERAGE_SMA:
        avgOpen = get_sma(data=open, period=period)
        avgHigh = get_sma(data=high, period=period)
        avgLow = get_sma(data=low, period=period)
        avgClose = get_sma(data=close, period=period)
    elif averageType == cons.AVERAGE_HMA:
        avgOpen = get_hma(data=open, period=period)
        avgHigh = get_hma(data=high, period=period)
        avgLow = get_hma(data=low, period=period)
        avgClose = get_hma(data=close, period=period)
    else:
        avgOpen = get_ema(data=open, period=period)
        avgHigh = get_ema(data=high, period=period)
        avgLow = get_ema(data=low, period=period)
        avgClose = get_ema(data=close, period=period)

    """ remove nan values """
    avgOpen = avgOpen[np.isfinite(avgOpen)]
    avgHigh = avgHigh[np.isfinite(avgHigh)]
    avgLow = avgLow[np.isfinite(avgLow)]
    avgClose = avgClose[np.isfinite(avgClose)]

    haOpen, haHigh, haLow, haClose = get_heikin_ashi(open=avgOpen, high=avgHigh, low=avgLow, close=avgClose)
    return haOpen, haHigh, haLow, haClose


def get_chandelier(open=None, high=None, low=None, close=None, period=22, multiplier=2):
    atr = get_atr(highPrices=high, lowPrices=low, closePrices=close, period=period) * multiplier
    # cl = pd.Series(high).rolling(window=period).max().to_numpy(dtype=float)
    # cs = pd.Series(low).rolling(window=period).min().to_numpy(dtype=float)
    cl = pd.Series(close).rolling(window=period).max().to_numpy(dtype=float)
    cs = pd.Series(close).rolling(window=period).min().to_numpy(dtype=float)

    chandelierLong = cl - atr
    chandelierShort = cs + atr
    return chandelierLong, chandelierShort


def get_volume_average(volumes=None, period=20):
    avg = pd.Series(volumes).rolling(window=period).mean().to_numpy(dtype=float)
    return avg


def get_atr(highPrices=None, lowPrices=None, closePrices=None, period=14):
    atr = talib.ATR(highPrices, lowPrices, closePrices, timeperiod=period)
    return atr


def get_median_price(high=None, low=None):
    #return (high + low) / 2
    return talib.MEDPRICE(high, low)


def get_basic_bands(averagePrice, atr, multiplier):
    matr = multiplier * atr
    upper = averagePrice + matr
    lower = averagePrice - matr
    return upper, lower

@njit
def get_final_bands(close, upper, lower):
    trend = np.full(close.shape, np.nan)
    direction = np.full(close.shape, 1)
    long = np.full(close.shape, np.nan)
    short = np.full(close.shape, np.nan)

    for i in range(1, close.shape[0]):
        if close[i] > upper[i - 1]:
            direction[i] = 1
        elif close[i] < lower[i - 1]:
            direction[i] = -1
        else:
            direction[i] = direction[i - 1]
            if direction[i] > 0 and lower[i] < lower[i - 1]:
                lower[i] = lower[i - 1]
            if direction[i] < 0 and upper[i] > upper[i - 1]:
                upper[i] = upper[i - 1]

        if direction[i] > 0:
            trend[i] = long[i] = lower[i]
        else:
            trend[i] = short[i] = upper[i]

    return trend, direction, long, short


def get_supertrend(high=None, low=None, close=None, period=20, multiplier=2):
    averagePrice = get_median_price(high=high, low=low)
    atr = get_atr(highPrices=high, lowPrices=low, closePrices=close, period=period)
    upper, lower = get_basic_bands(averagePrice=averagePrice, atr=atr, multiplier=multiplier)
    return get_final_bands(close=close, upper=upper, lower=lower)


def get_srsi(data=None, period=10):
    """
    Smoothed relative strength index
    """
    rData = data[::-1]  # reverse array
    smooth23 = [(rData[ix] + 2*rData[ix+1] + 2*rData[ix+2] + rData[ix+3])/ 6 for ix in range(0, len(rData)-3)]

    sr = []
    loopEnd = len(smooth23) - period
    for ix in range(0, loopEnd):
        cu23 = 0
        cd23 = 0
        first = ix
        end = ix + period
        for jx in range(first, end):
            if smooth23[jx] > smooth23[jx+1]:
                cu23 = cu23 + (smooth23[jx] - smooth23[jx+1])
            if smooth23[jx] < smooth23[jx+1]:
                cd23 = cd23 + (smooth23[jx+1] - smooth23[jx])

        if (cu23 + cd23) > 0:
            sr.append(cu23/(cu23 + cd23))
        else:
            sr.append(0)

    srsi = sr[::-1] # reverse array
    return np.array(srsi)


def get_squeeze_momentum(high=None, low=None, close=None, BBPeriod=20, BBMultFactor=2, KCPeriod=20, KCMultFactor=1.5, stdDev=0, useTrueRange=True):
    """
    Squeeze Momentum Indicator
    :param high:
    :param low:
    :param close:
    :param BBPeriod: Bollinger Band Period (Default Value: 20, Maximum Value: 50, Minimum Value: 1)
    :param BBMultFactor: Default Value: 2, Maximum Value:10, Minumum Value: 0.1
    :param KCPeriod: Keltner Channel Period (Default Value: 20, Maximum Value: 50, Minimum Value: 1)
    :param KCMultFactor: Default Value: 1.5, Maximum Value:10, Minumum Value: 0.1
    :param stdDev: Standard Deviation
    :param useTrueRange: True/False
    :return:
    """

    upperBB, centerBB, lowerBB = get_bolinger_bands(data=close, period=BBPeriod, std_dev=stdDev)

    """ Keltner Channels"""
    ma = get_sma(data=close, period=KCPeriod)
    if useTrueRange == True:
        range = talib.TRANGE(high, low, close)
    else:
        range = high - low

    rangema = get_sma(data=range, period=KCPeriod)
    upperKC = ma + rangema * KCMultFactor
    lowerKC = ma - rangema * KCMultFactor

    highest = pd.Series(high).rolling(KCPeriod).max()
    highest_high = highest.to_numpy()
    lowest = pd.Series(low).rolling(KCPeriod).min()
    lowest_low = lowest.to_numpy()

    avgHL = (highest_high + lowest_low) / 2
    avgHL = avgHL[-len(ma):]
    avg = (avgHL + ma) / 2

    lrData = close[-len(ma):] - avg
    sqzVal = get_linear_regression_talib(data=lrData, period=KCPeriod)

    extractLowerBB = lowerBB[-len(lowerKC):]
    extractUpperBB = upperBB[-len(upperKC):]
    sqzOn = ((extractLowerBB > lowerKC) & (extractUpperBB < upperKC))
    sqzOff = ((extractLowerBB < lowerKC) & (extractUpperBB > upperKC))
    noSqz = (sqzOn == False) & (sqzOff == False)

    # İhtiyaç olursa sqzOn, sqzOff, noSqz değerleri de döndürülebilir. (Tradingview sıfır çizgisindeki x işaretleri)
    # sqzVal trandingview daki histogram değerleri
    return sqzVal


def get_ut_bot_alerts(high=None, low=None, close=None, sensivity=1, period=10, useHeikin=False):
    def atrTrailingStopItem(close, prevClose, prevAtr, nLoss):
        if close > prevAtr and prevClose > prevAtr:
            return max(prevAtr, close - nLoss)
        elif close < prevAtr and prevClose < prevAtr:
            return min(prevAtr, close + nLoss)
        elif close > prevAtr:
            return close - nLoss
        else:
            return close + nLoss

    atr = get_atr(highPrices=high, lowPrices=low, closePrices=close, period=period)
    nLoss = atr * sensivity

    atrTrailingStop = numpy.empty(len(atr))
    atrTrailingStop[:] = numpy.nan
    atrTrailingStop[0] = 0.0

    for ix in range(1, len(atr)):
        atrTrailingStop[ix] = atrTrailingStopItem(close=close[ix], prevClose=close[ix-1],
                                                  prevAtr=atrTrailingStop[ix-1], nLoss=nLoss[ix])

    # Calculating signals
    ema = vbt.MA.run(close, 1, short_name='EMA', ewm=True)

    above = ema.ma_crossed_above(atrTrailingStop)
    below = ema.ma_crossed_below(atrTrailingStop)

    buy = (close > atrTrailingStop) & (above == True)
    sell = (close < atrTrailingStop) & (below == True)

    return buy.to_numpy(), sell.to_numpy()


def get_nadaraya_watson_envelope(data=None, bandWidth=8, multiplier=3):
    k = 2
    y = []
    src = data
    # ..............#
    up = []
    dn = []
    up_signal = []
    dn_signal = []
    up_temp = 0
    dn_temp = 0
    # .................#
    upper_band = []
    lower_band = []
    upper_band_signal = []
    lower_band_signal = []

    sum_e = 0
    for i in range(len(data)):
        sum = 0
        sumw = 0
        for j in range(len(data)):
            w = math.exp(-(math.pow(i - j, 2) / (bandWidth * bandWidth * 2)))
            sum += src[j] * w
            sumw += w
        y2 = sum / sumw
        sum_e += abs(src[i] - y2)
        y.insert(i, y2)
    mae = sum_e / len(data) * multiplier

    for i in range(len(data)):
        y2 = y[i]
        y1 = y[i - 1]

        if y[i] > y[i - 1]:
            up.insert(i, y[i])
            if up_temp == 0:
                up_signal.insert(i, data[i])
            else:
                up_signal.insert(i, np.nan)
            up_temp = 1
        else:
            up_temp = 0
            up.insert(i, np.nan)
            up_signal.insert(i, np.nan)

        if y[i] < y[i - 1]:
            dn.insert(i, y[i])
            if dn_temp == 0:
                dn_signal.insert(i, data[i])
            else:
                dn_signal.insert(i, np.nan)
            dn_temp = 1
        else:
            dn_temp = 0
            dn.insert(i, np.nan)
            dn_signal.insert(i, np.nan)

        upper_band.insert(i, y[i] + mae)
        lower_band.insert(i, y[i] - mae)
        if data[i] > upper_band[i]:
            upper_band_signal.insert(i, data[i])
        else:
            upper_band_signal.insert(i, np.nan)

        if data[i] < lower_band[i]:
            lower_band_signal.insert(i, data[i])
        else:
            lower_band_signal.insert(i, np.nan)

    Nadaraya_Watson = pd.DataFrame({
        "Buy": up,
        "Sell": dn,
        "BUY_Signal": up_signal,
        "Sell_Signal": dn_signal,
        "Upper_Band": upper_band,
        "Lower_Band": lower_band,
        "Upper_Band_signal": upper_band_signal,
        "Lower_Band_Signal": lower_band_signal
    })
    return Nadaraya_Watson


def get_stc(data=None, period=12, fastLength=26, slowLength=50):
    seriesData = pd.Series(data)
    stc = STCIndicator(seriesData, window_slow=slowLength, window_fast=fastLength, cycle=period).stc()
    return stc.to_numpy()