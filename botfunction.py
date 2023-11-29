import logging
import sys
import traceback
from configparser import ConfigParser
import mariadb
from decimal import Decimal
from typing import Union, Optional, Dict

def connectDB():
    # Read database section from config.ini
    dbConfig = readConfig(filename="config.ini", section="database")

    # Connect to Database
    try:
        dbConnection = mariadb.connect(
            user=dbConfig["user"],
            password=dbConfig["password"],
            host=dbConfig["host"],
            port=int(dbConfig["port"]),
            database=dbConfig["database"],
            autocommit=True
        )
        return dbConnection
    except mariadb.Error as ex:
        logTraceback(ex)
        sys.exit(1)


def readConfig(filename="config.ini", section=None):
    try:
        parser = ConfigParser()
        if not (parser.read(filename)):
            raise Exception("{0} file not found".format(filename))

        parameters = {}
        if parser.has_section(section):
            items = parser.items(section)
            for item in items:
                parameters[item[0]] = item[1]
        else:
            raise Exception('{0} not found in the {1} file'.format(section, filename))
        return parameters
    except Exception as ex:
        logTraceback(ex)
        sys.exit(1)


def round_step_size(quantity: Union[float, Decimal], step_size: Union[float, Decimal]) -> float:
    """
    step_size adetin (miktarın) arttırılıp, azaltılabileceği minimum aralıktır.
    Adet step-size ın bir katı olmalıdır.

    Bu fonksiyon adeti step_size ın katı olarak ayarlamak (yuvarlamak) için kullanılır.

    :param quantity: Adet bilgisi (required)
    :param step_size: Adetin arttırlıp azaltılabileceği min değer (required)
    :return: float
    """
    quantity = Decimal(str(quantity))
    return float(quantity - quantity % Decimal(str(step_size)))


def round_tick_size(price: Union[float, Decimal], tick_size: Union[float, Decimal]) -> float:
    """
    tick_size fiyatın arttırılıp, azaltılabileceği minimum aralıktır.
    Fiyat tick-size ın bir katı olmalıdır.

    Bu fonksiyon fiyatı tick_size ın katı olarak ayarlamak (yuvarlamak) için kullanılır.

    :param price: Fiyat bilgisi (required)
    :param tick_size: Fiyatın arttırlıp azaltılabileceği min değer (required)
    :return: float
    """
    price = Decimal(str(price))
    return float(price - price % Decimal(str(tick_size)))


def calculate_ratio(num1: Union[float, Decimal], num2: Union[float, Decimal]) -> float:
    n1 = Decimal(str(num1))
    n2 = Decimal(str(num2))

    return float(((n1 - n2) * 100) / n2)


def cross_over(source1, source2) -> bool:
    num1Cur = float(source1[-1])
    num1Prev = float(source1[-2])
    num2Cur = float(source2[-1])
    num2Prev = float(source2[-2])
    ret = (num1Prev < num2Prev) and (num1Cur > num2Cur)
    return ret


def cross_under(source1, source2) -> bool:
    num1Cur = float(source1[-1])
    num1Prev = float(source1[-2])
    num2Cur = float(source2[-1])
    num2Prev = float(source2[-2])
    ret = (num1Prev > num2Prev) and (num1Cur < num2Cur)
    return ret


def above(above, under, length) -> bool:
    if above.size < length:
        return False
    if under.size < length:
        return False

    start = length * (-1)
    for ix in range(start, 0):
        if float(under[ix]) > float(above[ix]):
            return False
    return True


def below_curve(source, curve, length) -> bool:
    if source.size < length:
        return False
    if curve.size < length:
        return False

    start = length * (-1)
    for ix in range(start, 0):
        if float(source[ix]) > float(curve[ix]):
            return False
    return True


def below_level(source, level, length) -> bool:
    ls = source.size
    if ls < length:
        return False

    if ls == 1:
        if (source > level):
            return False
        else:
            return True
    else:
        start = length * (-1)
        for ix in range(start, 0):
            if float(source[ix]) > level:
                return False
    return True


def deep_turn(source) -> bool:
    num1 = float(source[-1])
    num2 = float(source[-2])
    num3 = float(source[-3])
    return(num1 > num2 < num3)


def peak_turn(source) -> bool:
    num1 = float(source[-1])
    num2 = float(source[-2])
    num3 = float(source[-3])
    return(num1 < num2 > num3)


def slope(source) -> bool:
    # eğim pozitif ise True, negatif ise False döner
    cur = float(source[-1])
    prev = float(source[-2])
    return(cur > prev)

def logTraceback(ex, ex_traceback=None):
    logging.basicConfig(filename='error.log',
                        filemode='a',
                        format='%(asctime)s: %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    if ex_traceback is None:
        ex_traceback = ex.__traceback__
    tb_lines = traceback.format_exception(ex.__class__, ex, ex.__traceback__)
    tb_text = ''.join(tb_lines)
    logging.exception(tb_text)


def logError(message):
    logging.basicConfig(filename='error.log',
                        filemode='a',
                        format='%(asctime)s: %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    logging.error(message)
