import time
import datetime
import ccxt as ccxt
import numpy as np
import math
import pickle
import pandas as pd
import matplotlib.pyplot as plt

day_len = 22
symbol_list = ["BTC-PERP", "BTC-0924", "BTC-1231"]
exe_dict = {}
best_exe_dict = {}
year_list = [2021 for i in range(day_len)]
month_list = [7 for i in range(day_len)]
day_list = [1 + i for i in range(day_len)]
date_list = []

for i in range(day_len):
    y = str(year_list[i])
    if month_list[i] <= 9:
        m = "0" + str(month_list[i])
    elif month_list[i] >= 10:
        m = str(month_list)
    if day_list[i] <= 9:
        d = "0" + str(day_list[i])
    elif day_list[i] >= 10:
        d = str(day_list[i])
    date = y + "-" + m + "-" + d
    date_list.append(date)

for s in symbol_list:
    for i in range(len(date_list)):
        if i == 0:
            df_exe_ftx = pd.read_csv("executions_{}_{}_{}.csv".format(s, "ftx", date_list[i]), index_col = 0)
        elif i >= 1:
            df_exe_ftx = pd.concat([df_exe_ftx, pd.read_csv("executions_{}_{}_{}.csv".format(s, "ftx", date_list[i]), index_col = 0)])

    df_exe_ftx["ts"] = df_exe_ftx["time"].apply(lambda x : x[:19]).apply(lambda x : datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')).apply(lambda x : x.timestamp())
    df_exe_ftx = df_exe_ftx.drop(["liquidation", "time"], axis = 1)
    df_exe_ftx = df_exe_ftx.rename(columns = {"size":"quantity", "side":"taker_side"})
    df_exe_ftx = df_exe_ftx.reindex(columns = ["ts", "id", "price", "quantity", "taker_side"])
    df_exe_ftx = df_exe_ftx.sort_values(by = "ts").reset_index(drop = True)
    exe_dict[s] = df_exe_ftx
    print(s)

for s in symbol_list:
    s_best_exe = exe_dict[s].groupby(["ts"]).apply(lambda x : np.nanmax(x.values[:,2]))
    df_best_exe = pd.DataFrame([s_best_exe.index, s_best_exe]).T
    columns = ["ts", "best_price_{}".format(s)]
    df_best_exe.columns = columns
    best_exe_dict[s] = df_best_exe

df_merge = pd.merge(best_exe_dict["BTC-PERP"], best_exe_dict["BTC-0924"], on = "ts")
df_merge = pd.merge(df_merge, best_exe_dict["BTC-1231"], on = "ts")

df_merge["1231_PERP_basis_ratio"] = df_merge["best_price_BTC-1231"] - df_merge["best_price_BTC-PERP"]
df_merge["1231_PERP_basis_ratio"] = df_merge["1231_PERP_basis_ratio"]/df_merge["best_price_BTC-PERP"]

df_merge["0924_PERP_basis_ratio"] = df_merge["best_price_BTC-0924"] - df_merge["best_price_BTC-PERP"]
df_merge["0924_PERP_basis_ratio"] = df_merge["0924_PERP_basis_ratio"]/df_merge["best_price_BTC-PERP"]

df_merge["1231_0924_basis_ratio"] = df_merge["best_price_BTC-1231"] - df_merge["best_price_BTC-0924"]
df_merge["1231_0924_basis_ratio"] = df_merge["1231_0924_basis_ratio"]/df_merge["best_price_BTC-PERP"]

df_merge["1231_0924_PERP_basis_ratio_ratio"] = df_merge["1231_PERP_basis_ratio"]/df_merge["0924_PERP_basis_ratio"]
# 外れ値排除
df_merge["1231_0924_PERP_basis_ratio_ratio"] = df_merge["1231_0924_PERP_basis_ratio_ratio"][df_merge["1231_0924_PERP_basis_ratio_ratio"] <= 4]
df_merge["1231_0924_PERP_basis_ratio_ratio"] = df_merge["1231_0924_PERP_basis_ratio_ratio"][df_merge["1231_0924_PERP_basis_ratio_ratio"] >=1]

ts_dict = {}
ts_dict["1231"] = datetime.datetime(2021,12,31).timestamp()
ts_dict["0924"] = datetime.datetime(2021,9,24).timestamp()

df_merge["duration_1231"] = df_merge["ts"].apply(lambda x : ts_dict["1231"] - x)
df_merge["duration_0924"] = df_merge["ts"].apply(lambda x : ts_dict["0924"] - x)
df_merge["duration_ratio_1231_0924"] = df_merge["duration_1231"]/df_merge["duration_0924"]

plt.plot(df_merge["1231_0924_PERP_basis_ratio_ratio"])
plt.plot(df_merge["duration_ratio_1231_0924"])
plt.show()

df_merge = df_merge.reset_index(drop = True)
df_merge["pl"] = np.nan

columns = df_merge.columns
column_n_dict = dict()
for i in range(len(columns)):
    column_n_dict[columns[i]] = i
columns
merge = df_merge.values

target_rr_diff = 0.7
target_pl = 0
fly_position = 0
rrt0 = np.nan
# you assume you take order for every trade
trading_cost_per_trade = -0.07/100

for i in range(len(merge)):
    expected_mtm = merge[i, column_n_dict["best_price_BTC-PERP"]]*merge[i, column_n_dict["0924_PERP_basis_ratio"]]*target_rr_diff
    expected_total_trading_cost = 2 * trading_cost_per_trade * (merge[i, column_n_dict["best_price_BTC-1231"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-0924"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-PERP"]])
    expected_pl = (expected_mtm + expected_total_trading_cost)/merge[i, column_n_dict["best_price_BTC-PERP"]]
    if merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]] >= merge[i, column_n_dict["duration_ratio_1231_0924"]] + target_rr_diff/2 and expected_pl >= target_pl:
        if fly_position == 0:
            rrt0 = merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]
            fly_position = 1
            total_trading_cost = trading_cost_per_trade * (merge[i, column_n_dict["best_price_BTC-1231"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-0924"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-PERP"]])
        elif fly_position == -1:
            mtm = merge[i, column_n_dict["best_price_BTC-PERP"]]*merge[i, column_n_dict["0924_PERP_basis_ratio"]]*(merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]] - rrt0)
            fly_position = 0
            total_trading_cost += trading_cost_per_trade * (merge[i, column_n_dict["best_price_BTC-1231"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-0924"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-PERP"]])
            print("total trading_cost : {}".format(total_trading_cost))
            print("mtm : {}".format(mtm))
            print("pl : {}".format(mtm + total_trading_cost))
            merge[i, column_n_dict["pl"]] = mtm + total_trading_cost
            print("fly short exit")
    elif merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]] <= merge[i, column_n_dict["duration_ratio_1231_0924"]] - target_rr_diff/2 and expected_pl >= target_pl:
        if fly_position == 0:
            rrt0 = merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]
            fly_position = -1
            total_trading_cost = trading_cost_per_trade * (merge[i, column_n_dict["best_price_BTC-1231"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-0924"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-PERP"]])
        elif fly_position == 1:
            mtm = -1*merge[i, column_n_dict["best_price_BTC-PERP"]]*merge[i, column_n_dict["0924_PERP_basis_ratio"]]*(merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]] - rrt0)
            fly_position = 0
            total_trading_cost += trading_cost_per_trade * (merge[i, column_n_dict["best_price_BTC-1231"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-0924"]] + merge[i, column_n_dict["1231_0924_PERP_basis_ratio_ratio"]]*merge[i, column_n_dict["best_price_BTC-PERP"]])
            print("total trading_cost : {}".format(total_trading_cost))
            print("mtm : {}".format(mtm))
            print("pl : {}".format(mtm + total_trading_cost))
            merge[i, column_n_dict["pl"]] = mtm + total_trading_cost
            print("fly long exit")

df_merge = pd.DataFrame(merge)
df_merge.columns = columns

# check pl from simulation
plt.plot(df_merge.dropna(subset = ["pl"])["pl"])
plt.plot(df_merge.dropna(subset = ["pl"])["pl"].cumsum())
