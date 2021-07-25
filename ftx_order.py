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
from ftx_config import Config

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

class MyFTX(ccxt.ftx):
    def nonce(self):
        return(self.milliseconds())

def trades_reporter(url, buy_price, sell_price, buy_amount, sell_amount, entry_side, asset, duration, entry_ld, exit_pr, t2, t4, score, mid_exit_done):
    today = datetime.today()
    headline = "data is from {0}/{1}/{2} {3}:{4}:{5}".format(today.year, today.month, today.day, today.hour, today.minute, today.second)
    send_data = {
      "blocks": [
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(buy_price)
          }
        },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(sell_price)
          }
        },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(buy_amount)
          }
        },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(sell_amount)
          }
        },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": entry_side
          }
        },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(asset)
          }
        },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(duration)
          }
         },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(entry_ld)
          }
         },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(exit_pr)
          }
         },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(t2)
          }
         },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(t4)
          }
         },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(score)
          }
         },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(mid_exit_done)
          }
         },
        {
          "type": "section",
          "text": {
            "type": "mrkdwn",
            "text": str(datetime.today())
          }
        }
      ]
    }

    res = requests.post(url, headers={'Content-Type': 'application/json'}, data = json.dumps(send_data))
    print(res.text)

url = "url"

ftx = MyFTX({"apiKey":"apiKey", "secret":"secret"})

def location(depth=0):
  frame = inspect.currentframe().f_back
  return os.path.basename(frame.f_code.co_filename), frame.f_code.co_name, frame.f_lineno

def ceil_floor_general(price, minimum_price_stride_order):
    if "." in str(price):
        if minimum_price_stride_order <= 0:
            effective_number = float(str(price)[:str(price).find(".")+(-1*minimum_price_stride_order)+1])
        elif minimum_price_stride_order > 0:
            if str(price).find(".")-1 >= minimum_price_stride_order:
                effective_number = float(str(price)[:str(price).find(".")+(-1*minimum_price_stride_order)]+str(0)*minimum_price_stride_order)
            elif str(price).find(".")-1 < minimum_price_stride_order:
                effective_number = 0
    elif "." not in str(price):
        effective_number = price
    else:
        print("unexpected")
    excess = price - effective_number
    if excess == 0:
        ceil_price = effective_number
        floor_price = effective_number
    elif excess > 0:
        ceil_price = float(Decimal(str(effective_number)) + Decimal(str(math.pow(10, minimum_price_stride_order))))
        floor_price = effective_number
    else:
        print("unexpected")
    return(float(ceil_price), float(floor_price))

upper_boundary = 10000000000
lower_boundary = 0
traded_symbol = Config().traded_symbol
minimum_order_amount = Config().minimum_order_amount(traded_symbol)

accounting_currency = "USD"
exit_initiated = False
entried = False
wait_forever = False
circuit_break_amount = 240
dormant_time = 900
dormant = False
mid_exit_done = False
initial_position_size = 0

while True:
    try:
        if pd.read_pickle("{}_if_optimized.pkl".format(traded_symbol)) == True:
            print("optimized")
            break
        else:
            print("waiting for optimization...")
            time.sleep(1)
    except Exception as e:
        print(e)
        time.sleep(1)

while True:
    # you do not trade if score < minimum_profit_rate:
    while True:
        try:
            print("checking score")
            score = pd.read_pickle("{}_score.pkl".format(traded_symbol))
            print("{0}_score {1}".format(traded_symbol, score))
            lp = float(pd.read_pickle("{}_last_price.pkl".format(traded_symbol)))
            minimum_profit_rate = Config().minimum_profit_rate
            bea = Config().entry_amount(traded_symbol)
            sea = Config().entry_amount(traded_symbol)
            print("lp : {}".format(lp))
            print("minimum_profit_rate : {}".format(minimum_profit_rate))
            print("bea : {}".format(bea))
            minimum_score = (lp*bea)*minimum_profit_rate
            bea = Config().entry_amount(traded_symbol)
            sea = Config().entry_amount(traded_symbol)
            mid_exit_unit = Config().mid_exit_unit(traded_symbol)
            break
        except Exception as e:
            print("error in row number : {}".format(location()[2]))
            print(e)
            time.sleep(1)
    if score < minimum_score and entried == False:
        print("{0} score is {1} and less than minimum_score of {2}".format(traded_symbol, score, minimum_score))
        time.sleep(15)
        continue
    while True:
        print("initial phase")
        index_unch = True
        time.sleep(0.1)
        try:
            positions = ftx.private_get_positions(params = {'showAvgPrice':True})["result"]
            for p in positions:
                if p["future"] == traded_symbol:
                    if float(p["size"]) == 0 and entried == True:
                        print("reporting trade pnl")
                        entried = False
                        exit_initiated = False
                        while True:
                            try:
                                balances_list = ftx.private_get_wallet_balances()["result"]
                                for b in balances_list:
                                    if b["coin"] == accounting_currency:
                                        print(b)
                                        # if asset > float(b["total"]):
                                        #     print("you lost, you gotta wait for dormant time to re-optimize")
                                        #     dormant = True
                                        asset = float(b["total"])
                                        break
                                fills = ftx.private_get_fills(params = {"market":traded_symbol})["result"]
                                for f in fills:
                                    if f["side"] == "buy":
                                        buy_price = f["price"]
                                        buy_amount = f["size"]
                                        buy_id = float(f["id"])
                                        break
                                    else:
                                        buy_price = 0
                                        buy_amount = 0
                                        buy_id = 0
                                for f in fills:
                                    if f["side"] == "sell":
                                        sell_price = f["price"]
                                        sell_amount = f["size"]
                                        sell_id = float(f["id"])
                                        break
                                    else:
                                        sell_price = 0
                                        sell_amount = 0
                                        sell_id = 0
                                if buy_id < sell_id:
                                    entry_side = "buy"
                                elif buy_id > sell_id:
                                    entry_side = "sell"
                                break
                            except Exception as e:
                                print("error in row number : {}".format(location()[2]))
                                print(e)
                                time.sleep(1)
                        print("entry_side : {}".format(entry_side))
                        while True:
                            try:
                                score = pd.read_pickle("{}_score.pkl".format(traded_symbol))
                                entry_ld = pd.read_pickle("{}_optimized_entry_limit_delta.pkl".format(traded_symbol))
                                exit_pr = pd.read_pickle("{}_optimized_exit_price_ratio.pkl".format(traded_symbol))
                                t2 = pd.read_pickle("{}_optimized_t2.pkl".format(traded_symbol))
                                t4 = pd.read_pickle("{}_optimized_t4.pkl".format(traded_symbol))
                                break
                            except Exception as e:
                                print("error in row number : {}".format(location()[2]))
                                print(e)
                                time.sleep(1)
                        duration = int(time.time() - exit_initiation_time)
                        trades_reporter(url = url, buy_price = buy_price, sell_price = sell_price, buy_amount = buy_amount, sell_amount = sell_amount, entry_side = entry_side, asset = asset, duration = duration, entry_ld = entry_ld, exit_pr = exit_pr, t2 = t2, t4 = t4, score = score, mid_exit_done = mid_exit_done)
                        print("reported")
                        if dormant == True:
                            time.sleep(dormant_time)
                            dormant = False
                        if asset < circuit_break_amount:
                            print("circuit_break")
                            exit()
                    # get initial position size to check if entried or not
                    initial_position_size = float(p["size"])
                    initial_position_side = p["side"]
                    if initial_position_size != 0:
                        initial_position_entry_price = float(p["recentAverageOpenPrice"])
                    elif initial_position_size == 0:
                        initial_position_entry_price = np.nan
                    break
                elif p["future"] != traded_symbol:
                    initial_position_size = initial_position_size
            balances_list = ftx.private_get_wallet_balances()["result"]
            for b in balances_list:
                if b["coin"] == accounting_currency:
                    print(asset)
                    print(float(b["total"]))
                    asset = float(b["total"])
                    break
            order_book = ftx.public_get_markets_market_name_orderbook(params = {"market_name":traded_symbol})["result"]
            best_bid = float(order_book["bids"][0][0])
            best_ask = float(order_book["asks"][0][0])
            break
        except Exception as e:
            print("error in row number : {}".format(location()[2]))
            print(e)
            time.sleep(1)
    mid_exit_done = False
    if initial_position_size < minimum_order_amount:
        if best_ask >= lower_boundary and best_bid <= upper_boundary:
            print("will buy and sell for entry")
            while True:
                try:
                    bld_entry = -1*abs(pd.read_pickle("{}_optimized_entry_limit_delta.pkl".format(traded_symbol)))
                    bld_entry = bld_entry*best_bid
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
            buy_entry_price = best_bid + bld_entry
            buy_entry_amount = bea
            while True:
                try:
                    print(buy_entry_price)
                    print(buy_entry_amount)
                    buy_entry = ftx.private_post_orders(params = {"market":traded_symbol, "side":"buy", "price":buy_entry_price, "type":"limit", "size":buy_entry_amount, "postOnly":True})
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
            while True:
                try:
                    sld_entry = abs(pd.read_pickle("{}_optimized_entry_limit_delta.pkl".format(traded_symbol)))
                    sld_entry = sld_entry*best_ask
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
            sell_entry_price = best_ask + sld_entry
            sell_entry_amount = sea
            while True:
                try:
                    sell_entry = ftx.private_post_orders(params = {"market":traded_symbol, "side":"sell", "price":sell_entry_price, "type":"limit", "size":sell_entry_amount, "postOnly":True})
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
        elif best_ask < lower_boundary:
            print("will buy for entry")
            while True:
                try:
                    bld_entry = -1*abs(pd.read_pickle("{}_optimized_entry_limit_delta.pkl".format(traded_symbol)))
                    bld_entry = bld_entry*best_bid
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
            buy_entry_price = best_bid + bld_entry
            buy_entry_amount = bea
            while True:
                try:
                    buy_entry = ftx.private_post_orders(params = {"market":traded_symbol, "side":"buy", "price":buy_entry_price, "type":"limit", "size":buy_entry_amount, "postOnly":True})
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
        elif best_bid > upper_boundary:
            print("will sell for entry")
            while True:
                try:
                    sld_entry = abs(pd.read_pickle("{}_optimized_entry_limit_delta.pkl".format(traded_symbol)))
                    sld_entry = sld_entry*best_ask
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
            sell_entry_price = best_ask + sld_entry
            sell_entry_amount = sea
            while True:
                try:
                    sell_entry = ftx.private_post_orders(params = {"market":traded_symbol, "side":"sell", "price":sell_entry_price, "type":"limit", "size":sell_entry_amount, "postOnly":True})
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
        else:
            print("unexpected market")
        time.sleep(1)
        while True:
            try:
                positions = ftx.private_get_positions()["result"]
                for p in positions:
                    if p["future"] == traded_symbol:
                        position_size = float(p["size"])
                        break
                    elif p["future"] != traded_symbol:
                        position_size = 0
                break
            except Exception as e:
                print("error in row number : {}".format(location()[2]))
                print(e)
                time.sleep(1)
        if initial_position_size < position_size:
            print("entried, will cancel orders and focus on exit")
            while True:
                try:
                    ftx.private_delete_orders(params = {"market":traded_symbol})
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
            print("go back to initial phase")
            continue
        elif initial_position_size >= position_size:
            while True:
                while True:
                    try:
                        open_orders = ftx.private_get_orders(params = {"market":traded_symbol})["result"]
                        break
                    except Exception as e:
                        print("error in row number : {}".format(location()[2]))
                        print(e)
                        time.sleep(1)
                if len(open_orders) == 0:
                    print("no open orders")
                    print("go back to initial phase because there is no open positions")
                    break
                elif len(open_orders) >= 1:
                    print("open orders detected")

                    if wait_forever == True:

                        while True:
                            try:
                                order_book = ftx.public_get_markets_market_name_orderbook(params = {"market_name":traded_symbol})["result"]
                                best_bid = float(order_book["bids"][0][0])
                                best_ask = float(order_book["asks"][0][0])
                                break
                            except Exception as e:
                                print("error in row number : {}".format(location()[2]))
                                print(e)
                                time.sleep(1)
                        for o in open_orders:
                            if o["side"] == "buy":
                                if best_bid + bld_entry != float(o["price"]):
                                    print("buy index changed")
                                    index_unch = False
                                    break
                            elif o["side"] == "sell":
                                if best_ask + sld_entry != float(o["price"]):
                                    print("sell index changed")
                                    index_unch = False
                                    break
                        if index_unch == False:
                            print("index moved and will delete orders")
                            while True:
                                try:
                                    ftx.private_delete_orders(params = {"market":traded_symbol})
                                    break
                                except Exception as e:
                                    print("error in row number : {}".format(location()[2]))
                                    print(e)
                                    time.sleep(1)
                            print("go back to initial phase")
                            break
                        elif index_unch == True:
                            print("index is unch, will collect open orders and orderbook and judge again")
                            continue

                    elif wait_forever == False:

                        entry_started = time.time()
                        print("entry detection started")

                        while True:
                            try:
                                entry_detect_time = pd.read_pickle("{}_optimized_t2.pkl".format(traded_symbol))
                                break
                            except Exception as e:
                                print("error in row number : {}".format(location()[2]))
                                print(e)
                                time.sleep(1)

                        while time.time() - entry_started <= entry_detect_time:
                            time.sleep(1)
                            print(time.time() - entry_started)
                            print(entry_detect_time)
                            while True:
                                try:
                                    positions = ftx.private_get_positions()["result"]
                                    for p in positions:
                                        if p["future"] == traded_symbol:
                                            position_size = float(p["size"])
                                            break
                                        elif p["future"] != traded_symbol:
                                            position_size = 0
                                    break
                                except Exception as e:
                                    print("error in row number : {}".format(location()[2]))
                                    print(e)
                                    time.sleep(1)
                            if initial_position_size < position_size:
                                # this means your entry orders were posted and filled
                                print("entried in entry detection phase, will cancel orders and focus on exit")
                                break
                        print("entry detection phase finished")
                        print("will delete orders and go back to initial phase")
                        while True:
                            try:
                                ftx.private_delete_orders(params = {"market":traded_symbol})
                                break
                            except Exception as e:
                                print("error in row number : {}".format(location()[2]))
                                print(e)
                                time.sleep(1)

    elif initial_position_size >= minimum_order_amount:
        print("exit phase")
        if exit_initiated == False:
            exit_initiation_time = time.time()
            exit_initiated == True
            entried = True
        if initial_position_side == "buy":
            while True:
                try:
                    sld_exit_pr = pd.read_pickle("{}_optimized_exit_price_ratio.pkl".format(traded_symbol))
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
            sld_exit = abs(bld_entry*sld_exit_pr)
            sell_exit_price = max(initial_position_entry_price + sld_exit, best_ask)
            print("sell exit price : {}".format(sell_exit_price))
            print(best_ask)
            print(sld_exit)
            while True:
                try:
                    sell_exit = ftx.private_post_orders(params = {"market":traded_symbol, "side":"sell", "price":sell_exit_price, "type":"limit", "size":initial_position_size, "postOnly":True})
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
        elif initial_position_side == "sell":
            while True:
                try:
                    bld_exit_pr = pd.read_pickle("{}_optimized_exit_price_ratio.pkl".format(traded_symbol))
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
            bld_exit = -1*abs(sld_entry*bld_exit_pr)
            buy_exit_price = min(initial_position_entry_price + bld_exit, best_bid)
            print("buy exit price : {}".format(buy_exit_price))
            print(initial_position_entry_price)
            print(best_bid)
            print(bld_exit)
            while True:
                try:
                    buy_exit = ftx.private_post_orders(params = {"market":traded_symbol, "side":"buy", "price":buy_exit_price, "type":"limit", "size":initial_position_size, "postOnly":True})
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
        time.sleep(1)
        while True:
            try:
                positions = ftx.private_get_positions()["result"]
                for p in positions:
                    if p["future"] == traded_symbol:
                        position_size = float(p["size"])
                        break
                    elif p["future"] != traded_symbol:
                        position_size = 0
                break
            except Exception as e:
                print("error in row number : {}".format(location()[2]))
                print(e)
                time.sleep(1)
        if position_size < minimum_order_amount:
            print("exited while waiting for 1 second, will cancel orders and report pnl")
            while True:
                try:
                    ftx.private_delete_orders(params = {"market":traded_symbol})
                    break
                except Exception as e:
                    print("error in row number : {}".format(location()[2]))
                    print(e)
                    time.sleep(1)
            print("go back to initial phase")
            continue

        elif position_size >= minimum_order_amount:
            print("no exit or partially filled")
            while True:
                while True:
                    try:
                        open_orders = ftx.private_get_orders(params = {"market":traded_symbol})["result"]
                        break
                    except Exception as e:
                        print("error in row number : {}".format(location()[2]))
                        print(e)
                        time.sleep(1)
                if len(open_orders) == 0:
                    print("no open orders")
                    print("go back to initial phase because there is no open positions")
                    break
                elif len(open_orders) >= 1:
                    print("open orders detected")

                    if wait_forever == True:

                        while True:
                            try:
                                order_book = ftx.public_get_markets_market_name_orderbook(params = {"market_name":traded_symbol})["result"]
                                best_bid = float(order_book["bids"][0][0])
                                best_ask = float(order_book["asks"][0][0])
                                break
                            except Exception as e:
                                print("error in row number : {}".format(location()[2]))
                                print(e)
                                time.sleep(1)
                        for o in open_orders:
                            print(o)
                            if o["side"] == "buy":
                                if min(initial_position_entry_price + bld_exit, best_bid) != float(o["price"]):
                                    print("buy index changed")
                                    print(min(initial_position_entry_price + bld_exit, best_bid))
                                    print(float(o["price"]))
                                    index_unch = False
                                    break
                            elif o["side"] == "sell":
                                if max(initial_position_entry_price + sld_exit, best_ask) != float(o["price"]):
                                    print("sell index changed")
                                    print(max(initial_position_entry_price + sld_exit, best_ask))
                                    print(float(o["price"]))
                                    index_unch = False
                                    break
                        if index_unch == False:
                            print("index moved and will delete orders")
                            while True:
                                try:
                                    ftx.private_delete_orders(params = {"market":traded_symbol})
                                    break
                                except Exception as e:
                                    print("error in row number : {}".format(location()[2]))
                                    print(e)
                                    time.sleep(1)
                            print("go back to initial phase")
                            break
                        elif index_unch == True:
                            print("index is unch, will collect open orders and orderbook and judge again")
                            continue

                    elif wait_forever == False:

                        exit_started = time.time()
                        print("exit detection started")

                        while True:
                            try:
                                exit_detect_time = pd.read_pickle("{}_optimized_t4.pkl".format(traded_symbol))
                                break
                            except Exception as e:
                                print("error in row number : {}".format(location()[2]))
                                print(e)
                                time.sleep(1)

                        while time.time() - exit_started <= exit_detect_time:
                            time.sleep(1)
                            print(time.time() - exit_started)
                            print(exit_detect_time)
                            while True:
                                try:
                                    positions = ftx.private_get_positions()["result"]
                                    for p in positions:
                                        if p["future"] == traded_symbol:
                                            position_size = float(p["size"])
                                            break
                                        elif p["future"] != traded_symbol:
                                            position_size = 0
                                    break
                                except Exception as e:
                                    print("error in row number : {}".format(location()[2]))
                                    print(e)
                                    time.sleep(1)
                            if position_size < minimum_order_amount:
                                print("exited while waiting for exit_detect_time, will cancel orders and report pnl")
                                while True:
                                    try:
                                        ftx.private_delete_orders(params = {"market":traded_symbol})
                                        break
                                    except Exception as e:
                                        print("error in row number : {}".format(location()[2]))
                                        print(e)
                                        time.sleep(1)
                                print("go back to initial phase")
                                break
                        print("will mid exit")
                        while True:
                            try:
                                ftx.private_delete_orders(params = {"market":traded_symbol})
                                time.sleep(1)
                                positions = ftx.private_get_positions()["result"]
                                for p in positions:
                                    if p["future"] == traded_symbol:
                                        position_size = float(p["size"])
                                        if p["side"] == "buy":
                                            mid_exit_side = "sell"
                                        elif p["side"] == "sell":
                                            mid_exit_side = "buy"
                                        break
                                    elif p["future"] != traded_symbol:
                                        position_size = 0
                                break
                            except Exception as e:
                                print("error in row number : {}".format(location()[2]))
                                print(e)
                                time.sleep(1)
                        while position_size >= minimum_order_amount:
                            mid_exit_amount = min(position_size, mid_exit_unit)
                            while True:
                                try:
                                    mid_exit = ftx.private_post_orders(params = {"market":traded_symbol, "side":mid_exit_side, "price":None, "type":"market", "size":mid_exit_amount, "postOnly":False})
                                    position_size -= mid_exit_amount
                                    break
                                except Exception as e:
                                    print("error in row number : {}".format(location()[2]))
                                    print(e)
                                    time.sleep(1)
                            time.sleep(1)
                            print(mid_exit)
                            print("mid exit ordered")
                        print("mid exit done")
                        mid_exit_done = True
                        time.sleep(1)
                        break
        elif initial_position_size < position_size:
            print("unexpeted, your positions are not supposed to become bigger in exit phase")
