# coding: UTF-8
import time
import ccxt as ccxt
import numpy as np
import pickle
import pandas as pd
from datetime import datetime
import json, requests
import inspect
import os
import math
from decimal import Decimal

from ccxt.base.errors import ExchangeError
from ccxt.base.errors import AuthenticationError
from ccxt.base.errors import PermissionDenied
from ccxt.base.errors import ArgumentsRequired
from ccxt.base.errors import BadRequest
from ccxt.base.errors import BadSymbol
from ccxt.base.errors import InsufficientFunds
from ccxt.base.errors import InvalidOrder
from ccxt.base.errors import OrderNotFound
from ccxt.base.errors import CancelPending
from ccxt.base.errors import DuplicateOrderId
from ccxt.base.errors import RateLimitExceeded
from ccxt.base.errors import ExchangeNotAvailable
from ccxt.base.decimal_to_precision import TICK_SIZE
from ccxt.base.precise import Precise

ftx = ccxt.ftx()

markets = ftx.public_get_markets()["result"]
df_moa = pd.DataFrame()
for i in range(len(markets)):
    df_moa.loc[i, "symbol"] = markets[i]["name"]
    df_moa.loc[i, "minProvideSize"] = markets[i]["minProvideSize"]
df_moa.to_csv("moa.csv")

class Config:
    def __init__(self):
        self.traded_symbol = "BTC-PERP"
        self.analysis_time = 60*60*24
        self.entry_amount_usd = 1500
        self.mid_exit_unit_usd = 100
        self.mid_exit_cost = 0.07/100
        self.max_leverage = 20
        self.minimum_profit_rate = 0.002
    def minimum_order_amount(self, traded_symbol):
        df_moa = pd.read_csv("moa.csv", index_col = 0)
        moa = df_moa[df_moa["symbol"] == traded_symbol].values[0][1]
        return(moa)
    def entry_amount(self, traded_symbol):
        entry_amount_usd = self.entry_amount_usd
        lp = float(pd.read_pickle("{}_last_price.pkl".format(traded_symbol)))
        entry_amount = entry_amount_usd/lp
        return(entry_amount)
    def mid_exit_unit(self, traded_symbol):
        mid_exit_unit_usd = self.mid_exit_unit_usd
        lp = float(pd.read_pickle("{}_last_price.pkl".format(traded_symbol)))
        mid_exit_unit = mid_exit_unit_usd/lp
        return(mid_exit_unit)
    def minimum_price_stride():
        pass
