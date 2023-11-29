import hashlib
import hmac
import time
from typing import Dict, Optional
from urllib.parse import urlencode
import requests
from decimal import Decimal

import error_helper


class Order:
    API_URL = 'https://api.binance.{}/api'
    API_TESTNET_URL = 'https://testnet.binance.vision/api'
    MARGIN_API_URL = 'https://api.binance.{}/sapi'
    WEBSITE_URL = 'https://www.binance.{}'
    FUTURES_URL = 'https://fapi.binance.{}/fapi'
    FUTURES_TESTNET_URL = 'https://testnet.binancefuture.com/fapi'
    FUTURES_DATA_URL = 'https://fapi.binance.{}/futures/data'
    FUTURES_DATA_TESTNET_URL = 'https://testnet.binancefuture.com/futures/data'
    FUTURES_COIN_URL = "https://dapi.binance.{}/dapi"
    FUTURES_COIN_TESTNET_URL = 'https://testnet.binancefuture.com/dapi'
    FUTURES_COIN_DATA_URL = "https://dapi.binance.{}/futures/data"
    FUTURES_COIN_DATA_TESTNET_URL = 'https://testnet.binancefuture.com/futures/data'
    OPTIONS_URL = 'https://vapi.binance.{}/vapi'
    OPTIONS_TESTNET_URL = 'https://testnet.binanceops.{}/vapi'
    PUBLIC_API_VERSION = 'v1'
    PRIVATE_API_VERSION = 'v3'
    MARGIN_API_VERSION = 'v1'
    FUTURES_API_VERSION = 'v1'
    FUTURES_API_VERSION2 = "v2"
    OPTIONS_API_VERSION = 'v1'

    ORDER_STATUS_NEW = 'NEW'
    ORDER_STATUS_PARTIALLY_FILLED = 'PARTIALLY_FILLED'
    ORDER_STATUS_FILLED = 'FILLED'
    ORDER_STATUS_CANCELED = 'CANCELED'
    ORDER_STATUS_PENDING_CANCEL = 'PENDING_CANCEL'
    ORDER_STATUS_REJECTED = 'REJECTED'
    ORDER_STATUS_EXPIRED = 'EXPIRED'

    SIDE_BUY = 'BUY'
    SIDE_SELL = 'SELL'

    ORDER_TYPE_LIMIT = 'LIMIT'
    ORDER_TYPE_MARKET = 'MARKET'
    ORDER_TYPE_STOP_LOSS = 'STOP_LOSS'
    ORDER_TYPE_STOP_LOSS_LIMIT = 'STOP_LOSS_LIMIT'
    ORDER_TYPE_TAKE_PROFIT = 'TAKE_PROFIT'
    ORDER_TYPE_TAKE_PROFIT_LIMIT = 'TAKE_PROFIT_LIMIT'
    ORDER_TYPE_LIMIT_MAKER = 'LIMIT_MAKER'

    TIME_IN_FORCE_GTC = 'GTC'  # Good till cancelled
    TIME_IN_FORCE_IOC = 'IOC'  # Immediate or cancel
    TIME_IN_FORCE_FOK = 'FOK'  # Fill or kill

    ORDER_RESP_TYPE_ACK = 'ACK'
    ORDER_RESP_TYPE_RESULT = 'RESULT'
    ORDER_RESP_TYPE_FULL = 'FULL'

    def __init__(self,
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 requests_params: Optional[Dict[str, str]] = None,
                 tld: str = 'com',
                 testnet: bool = False):
        self.API_KEY = api_key
        self.API_SECRET = api_secret
        self.request_params = requests_params
        self.tld = tld
        self.testnet = testnet

        self.API_URL = self.API_URL.format(tld)
        self.headers = {
            'X-MBX-APIKEY': self.API_KEY
        }

    def _generate_signature(self, **params) -> str:
        query_string = urlencode(params)
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    def _create_api_uri(self, path: str, signed: bool = True, version: str = PUBLIC_API_VERSION) -> str:
        url = self.API_URL
        """
        testnet = True ise test url ine bağlanılır
                = False ise gerçek ortama bağlanılır
        """
        if self.testnet:
            url = self.API_TESTNET_URL
        v = self.PRIVATE_API_VERSION if signed else version
        return url + '/' + v + '/' + path

    def _post_request(self, path, signed=False, version=PUBLIC_API_VERSION, **params):
        url = self._create_api_uri(path, signed, version)
        """
        Üç tip güvenlik uç noktası vardır
        NONE: Serbestçe erişilebilir (signed = False)
        USER_STREAM ve MARKET DATA: API_KEY ile erişelebilir (signed = True)
        TRADE ve USER_DATA: API_KEY ve API_SECRET (signature) ile erişilebilir (signed = True)
        """
        if signed:
            timestamp = int(time.time() * 1000)
            params.update({
                'timestamp': timestamp
            })
            signature = self._generate_signature(**params)
            params.update({
                'signature': signature
            })
        # Call request
        try:
            response = requests.post(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as err:
            self._handle_response(status_code=response.status_code, error=err)

    def _get_request(self, path, signed=False, version=PUBLIC_API_VERSION, **params):
        url = self._create_api_uri(path, signed, version)
        if signed:
            timestamp = int(time.time() * 1000)
            params.update({
                'timestamp': timestamp
            })
            signature = self._generate_signature(**params)
            params.update({
                'signature': signature
            })
        # Call request
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as err:
            self._handle_response(status_code=response.status_code, error=err)

    @staticmethod
    def _handle_response(status_code, error):
        rsp = error.response.json()
        # Hatanın kaydedilmesi
        helper = error_helper.ErrorHelper()
        helper.status_code = status_code
        helper.code = rsp['code']
        helper.msg = rsp['msg']
        helper.url = error.response.url
        helper.addData()

    """
    MARKET Endpoints
    """

    def create_order(self, **params):
        return self._post_request(path='order', signed=True, **params)

    def order_limit(self, timeInForce=TIME_IN_FORCE_FOK, **params):
        params.update({
            'type': self.ORDER_TYPE_LIMIT,
            'timeInForce': timeInForce
        })
        return self.create_order(**params)

    def order_limit_buy(self, timeInForce=TIME_IN_FORCE_FOK, **params):
        params.update({
            'side': self.SIDE_BUY
        })
        return self.order_limit(timeInForce=timeInForce, **params)

    def order_limit_sell(self, timeInForce=TIME_IN_FORCE_FOK, **params):
        params.update({
            'side': self.SIDE_SELL
        })
        return self.order_limit(timeInForce=timeInForce, **params)

    def order_market(self, **params):
        params.update({
            'type': self.ORDER_TYPE_MARKET
        })
        return self.create_order(**params)

    def order_market_buy(self, **params):
        params.update({
            'side': self.SIDE_BUY
        })
        return self.order_market(**params)

    def order_market_sell(self, **params):
        params.update({
            'side': self.SIDE_SELL
        })

        return self.order_market(**params)

    """
    USER DATA Endpoints
    """

    def get_account(self, **params):
        rsp = self._get_request(path='account', signed=True, **params)
        y = []
        # Hesapta sıfır bakiyeli olanlar silinir
        for balance in rsp['balances']:
            free = float(balance['free'])
            lock = float(balance['locked'])
            if free != 0.0 or lock != 0.0:
                y.append(balance)
        rsp['balances'] = y
        return rsp
