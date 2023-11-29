""" Candle Type """
CANDLE_GREEN = 1
CANDLE_RED = 2
""" Stop type """
STOP_TYPE_NONE = None
STOP_TYPE_TRAILING = 1
STOP_TYPE_PREVIOUS_LOW = 2
STOP_TYPE_LR = 3
STOP_TYPE_VOLUME = 4
STOP_TYPE_PROFIT = 5
DEFAULT_STOP_TYPE = STOP_TYPE_PREVIOUS_LOW
""" Average Type """
AVERAGE_SMA = 1
AVERAGE_EMA = 2
AVERAGE_HMA = 3
""" Target Type """
TARGET_TYPE_STOP_TRAILING = 1   # Fiyat yükseldikçe stop yükseltilir. Fiyat stopun altına düşerse satılır.
TARGET_TYPE_PROFIT_RATIO = 2    # Fiyat alış anında belirlenen hedef kar oranına ulaştığında satılır.
""" SuperTrend Type """
SUPERTREND_TYPE_LONG = 1
SUPERTREND_TYPE_SHORT = -1