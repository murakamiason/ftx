import time
from datetime import datetime
import ccxt as ccxt
import numpy as np
import math
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import copy
import inspect
import os
from ftx_config import Config

def location(depth=0):
  frame = inspect.currentframe().f_back
  return os.path.basename(frame.f_code.co_filename), frame.f_code.co_name, frame.f_lineno

traded_symbol = Config().traded_symbol

ftx = ccxt.ftx()
# plesae note that you specify datetime in japan and it is converted to datetime in UCT
time_unit = 10
freq = 1/20
milestone_percentage_unit = 0.001
analysis_time = Config().analysis_time
refresh_interval = 5
saved = False
pd.to_pickle(False, "{}_if_loaded.pkl".format(traded_symbol))

end_time = datetime.today().timestamp() - analysis_time
last_end_time = datetime.today().timestamp()
remanining_time = (last_end_time - end_time)/time_unit*freq
initial_remaining_time = remanining_time

cnt = 1
traded_symbol = Config().traded_symbol
print("initiate fetching trade history of {0} from {1} JST".format(traded_symbol, datetime.fromtimestamp(end_time)))

hist_trades = []

ts = time.time()
while end_time < time.time():
    while True:
        try:
            trades = ftx.public_get_markets_market_name_trades(params = {"market_name":traded_symbol, "limit":100, "end_time":end_time})["result"]
            break
        except Exception as e:
            print("error in row number : {}".format(location()[2]))
            print(e)
            time.sleep(1)
    hist_trades.extend(trades)
    end_time += time_unit
    remanining_time = (last_end_time - end_time)/time_unit*freq
    progress_rate = 1 - remanining_time/initial_remaining_time
    if progress_rate > milestone_percentage_unit*cnt:
        print("remanining_time:{}".format(int(remanining_time)))
        print("progress_rate:{}%".format(int(10000*progress_rate)/100))
        cnt += 1
    time.sleep(freq)
print("now accululated data to present time")
# you get historical trades for analysis time to present time
hist_trades = pd.DataFrame(hist_trades)[pd.DataFrame(hist_trades)["time"].apply(lambda x : x[:26]).apply(lambda x : datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f')).apply(lambda x : x.timestamp()) >= time.time() - analysis_time]
hist_trades = pd.DataFrame(hist_trades)[pd.DataFrame(hist_trades)["id"].duplicated() == False].reset_index(drop = True)
while True:
    try:
        pd.to_pickle(len(hist_trades), "{}_len_of_data.pkl".format(traded_symbol))
        hist_trades.to_csv("{}_executions.csv".format(traded_symbol))
        print("1st executions saved")
        pd.to_pickle(True, "{}_if_loaded.pkl".format(traded_symbol))
        print(len(hist_trades))
        break
    except Exception as e:
        print("error in row number : {}".format(location()[2]))
        print(e)
        time.sleep(1)
while True:
    while True:
        try:
            # get new trades in the market
            trades = ftx.public_get_markets_market_name_trades(params = {"market_name":traded_symbol, "limit":100})["result"]
            # irregular data check
            try:
                tmp = pd.DataFrame(trades)["time"].apply(lambda x : x[:26]).apply(lambda x : datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f'))
            except Exception as e:
                print("error in row number : {}".format(location()[2]))
                print(e)
                print("irregular time")
                time.sleep(1)
                continue
            break
        except Exception as e:
            print("error in row number : {}".format(location()[2]))
            print(e)
            time.sleep(1)
    hist_trades = pd.concat([hist_trades, pd.DataFrame(trades)])
    hist_trades = hist_trades[hist_trades["time"].apply(lambda x : x[:26]).apply(lambda x : datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f')).apply(lambda x : x.timestamp()) >= time.time() - analysis_time]
    hist_trades = pd.DataFrame(hist_trades)[pd.DataFrame(hist_trades)["id"].duplicated() == False].reset_index(drop = True)
    while True:
        try:
            pd.to_pickle(len(hist_trades), "{}_len_of_data.pkl".format(traded_symbol))
            hist_trades.to_csv("{}_executions.csv".format(traded_symbol))
            print(len(hist_trades))
            print("data refreshed")
            # save last price to calculate entry_ld
            last_price = hist_trades.sort_values(by = "id", ascending = False).reset_index(drop = True).loc[0, "price"]
            pd.to_pickle(last_price, "{}_last_price.pkl".format(traded_symbol))
            if saved == False and datetime.today().hour == 0:
                hist_trades.to_csv("{0}_executions_{1}_{2}.csv".format(traded_symbol, traded_symbol.replace("/", "-"), pd.DataFrame(trades)["time"].apply(lambda x : x[:10])[0]))
                print("data saved")
                saved = True
            elif saved == True and datetime.today().hour != 0:
                saved = False
            break
        except Exception as e:
            print("error in row number : {}".format(location()[2]))
            print(e)
            time.sleep(1)
    time.sleep(refresh_interval)
    pd.to_pickle(time.time(), "{}_refreshed_time.pkl".format(traded_symbol))
