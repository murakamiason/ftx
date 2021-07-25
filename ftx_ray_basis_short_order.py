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

class MyFTX(ccxt.ftx):
    def nonce(self):
        return(self.milliseconds())

ftx = MyFTX({"apiKey":"apiKey", "secret":"secret"})

def location(depth=0):
  frame = inspect.currentframe().f_back
  return os.path.basename(frame.f_code.co_filename), frame.f_code.co_name, frame.f_lineno

spot = "RAY/USD"
future = "RAY-PERP"

minimum_order_amount = 1
entry_unit = 1
full_amount = 5
target_entry_rate = 1.0005

balances = ftx.private_get_wallet_all_balances()["result"]["main"]
positions = ftx.private_get_positions()["result"]
for b in balances:
    if b["coin"] == spot.replace("/USD", ""):
        spot_balance = float(b["total"])
        break
for p in positions:
    if p["future"] == future:
        future_position = float(p["netSize"])
        break

while spot_balance + minimum_order_amount <= full_amount:
    time.sleep(1)
    while True:
        try:
            spot_order_book = ftx.fetchOrderBook(symbol = spot)
            future_order_book = ftx.fetchOrderBook(symbol = future)
            break
        except Exception as e:
            print(e)
            time.sleep(1)
    available_entry_rate = spot_order_book["asks"][0][0]/future_order_book["bids"][0][0]
    print("available entry rate : {}".format(available_entry_rate))
    if available_entry_rate <= target_entry_rate:
        spot_available_amount = spot_order_book["bids"][0][1]
        future_available_amount = future_order_book["asks"][0][1]
        entry_order_amount = min(full_amount - spot_balance, min(entry_unit, spot_available_amount, future_available_amount))
        print("entry order amount : {}".format(entry_order_amount))
        while True:
            try:
                spot_buy_entry = ftx.private_post_orders(params = {"market":spot, "side":"buy", "price":None, "type":"market", "size":entry_order_amount, "postOnly":False})
                break
            except Exception as e:
                print(e)
                time.sleep(1)
        while True:
            try:
                future_sell_entry = ftx.private_post_orders(params = {"market":future, "side":"sell", "price":None, "type":"market", "size":entry_order_amount, "postOnly":False})
                break
            except Exception as e:
                print(e)
                time.sleep(1)
        time.sleep(1)
        while True:
            try:
                balances = ftx.private_get_wallet_all_balances()["result"]["main"]
                for b in balances:
                    if b["coin"] == spot.replace("/USD", ""):
                        spot_balance = float(b["total"])
                        break
                break
            except Exception as e:
                print(e)
                time.sleep(1)
        print("spot balance : {}".format(spot_balance))
print("exit phase initiated")
