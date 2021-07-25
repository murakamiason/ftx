import time
import datetime
import ccxt as ccxt
import numpy as np
import math
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import copy
import inspect
import os

ftx = ccxt.ftx()

def location(depth=0):
  frame = inspect.currentframe().f_back
  return os.path.basename(frame.f_code.co_filename), frame.f_code.co_name, frame.f_lineno

# plesae note that you specify datetime in japan and it is converted to datetime in UTC
time_unit = 5
year_f = 2021
month_f = 7
day_f = 1
hour_f = 8
minute_f = 0
freq = 1/20
milestone_percentage_unit = 0.001

end_datetime = datetime.datetime(year = year_f, month = month_f, day = day_f, hour = hour_f, minute = minute_f)
end_time = end_datetime.timestamp()
last_end_time = datetime.datetime.today().timestamp()

remanining_time = (last_end_time - end_time)/time_unit*freq
initial_remaining_time = remanining_time
cnt = 1
symbol = input("please specify symbol in FTX to retrieve trade history : ")
print("inititate fetching trade history of {0} from {1} JST".format(symbol, end_datetime))

hist_trades = []
current_date  = pd.DataFrame(ftx.public_get_markets_market_name_trades(params = {"market_name":symbol, "limit":100, "end_time":end_time})["result"])["time"].apply(lambda x : x[:10])[0]

ts = time.time()
while end_time < last_end_time:
    while True:
        try:
            trades = ftx.public_get_markets_market_name_trades(params = {"market_name":symbol, "limit":100, "end_time":end_time})["result"]
            break
        except Exception as e:
            print("error in row number : {}".format(location()[2]))
            print(e)
            time.sleep(1)
    hist_trades.extend(trades)
    latest_date = pd.DataFrame(trades)["time"].apply(lambda x : x[:10])[0]
    if current_date  != latest_date:
        print("curent_date : {}".format(current_date))
        print("latest_date : {}".format(latest_date))
        hist_trades  = pd.DataFrame(hist_trades)[pd.DataFrame(hist_trades)["id"].duplicated() == False].reset_index(drop = True)
        df_old = hist_trades[hist_trades["time"].apply(lambda x : x[:10]) == current_date].reset_index(drop = True)
        df_old.to_csv("executions_{0}_{1}.csv".format(symbol.replace("/", "-"), current_date))
        hist_trades = hist_trades[hist_trades["time"].apply(lambda x : x[:10]) == latest_date].reset_index(drop = True)
        hist_trades = hist_trades.to_dict(orient = "records")
        print("saved executions {0} {1}".format(symbol.replace("/", "-"), current_date))
        current_date = latest_date
    end_time += time_unit
    remanining_time = (last_end_time - end_time)/time_unit*freq
    progress_rate = 1 - remanining_time/initial_remaining_time
    if progress_rate > milestone_percentage_unit*cnt:
        print("remanining_time:{}".format(int(remanining_time)))
        print("progress_rate:{}%".format(int(10000*progress_rate)/100))
        cnt += 1
    time.sleep(freq)
print("finished")
