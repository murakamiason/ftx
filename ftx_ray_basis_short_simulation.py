import time
import datetime
import ccxt as ccxt
import numpy as np
import math
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import copy

ftx = ccxt.ftx()
spot = "RAY/USD"
future = "RAY-PERP"
entry_price_hist = True

t1 = 1
t6 = 1

df_exe = pd.read_csv("executions_{}_2021-07-01.csv".format(spot.replace("/", "-")), index_col = 0)
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-02.csv".format(spot.replace("/", "-")), index_col = 0)])
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-03.csv".format(spot.replace("/", "-")), index_col = 0)])
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-04.csv".format(spot.replace("/", "-")), index_col = 0)])
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-05.csv".format(spot.replace("/", "-")), index_col = 0)])
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-06.csv".format(spot.replace("/", "-")), index_col = 0)])

df_exe["ts"] = df_exe["time"].apply(lambda x : x[:26]).apply(lambda x : datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f')).apply(lambda x : x.timestamp())
df_exe = df_exe.drop(["liquidation", "time"], axis = 1)
df_exe = df_exe.rename(columns = {"size":"quantity", "side":"taker_side"})
df_exe = df_exe.reindex(columns = ["ts", "id", "price", "quantity", "taker_side"])

df_exe["ts"] = df_exe["ts"].apply(lambda x : int(x))

exe = df_exe.values
drop_list = list()
for i in range(len(exe)):
    if isinstance(exe[i][2], str) == True:
        print("str type price detected")
        drop_list.append(i)
df_exe = df_exe.drop(drop_list)

df_buy = df_exe[df_exe["taker_side"] == "buy"]
df_sell = df_exe[df_exe["taker_side"] == "sell"]

df_buy_best = df_buy.groupby(["ts"]).apply(lambda x : np.nanmax(x.values[:,2]))
df_buy_best = pd.DataFrame([df_buy_best.index, df_buy_best]).T
columns = ["ts", "buy_best_price"]
df_buy_best.columns = columns

df_sell_best = df_sell.groupby(["ts"]).apply(lambda x : np.nanmin(x.values[:,2]))
df_sell_best = pd.DataFrame([df_sell_best.index, df_sell_best]).T
columns = ["ts", "sell_best_price"]
df_sell_best.columns = columns

df_buy_vwap = df_buy.groupby(["ts"]).apply(lambda x : np.average(a = x.values[:,2], weights = x.values[:,3]))
df_buy_vwap = pd.DataFrame([df_buy_vwap.index, df_buy_vwap]).T
columns = ["ts", "buy_vwap"]
df_buy_vwap.columns = columns

df_sell_vwap = df_sell.groupby(["ts"]).apply(lambda x : np.average(a = x.values[:,2], weights = x.values[:,3]))
df_sell_vwap = pd.DataFrame([df_sell_vwap.index, df_sell_vwap]).T
columns = ["ts", "sell_vwap"]
df_sell_vwap.columns = columns

df_vwap = pd.merge(df_buy_vwap, df_sell_vwap, how = "outer")
df_best = pd.merge(df_buy_best, df_sell_best, how = "outer")

df_prices = pd.merge(df_best, df_vwap, how = "outer")

ts = df_prices["ts"].values
ts_list = list(range(int(ts[0]), int(ts[len(ts)-1]+1)))

df_prices = pd.merge(pd.DataFrame(ts_list).rename(columns = {0:"ts"}), df_prices, how = "outer").sort_values(by = "ts", ascending = True).reset_index(drop = True)

df_prices["referred_buy_best_price"] = np.nan
df_prices["referred_sell_best_price"] = np.nan
df_prices["referred_buy_vwap"] = np.nan
df_prices["referred_sell_vwap"] = np.nan

columns = df_sell.columns
column_n_dict_sell = dict()
for i in range(len(columns)):
    column_n_dict_sell[columns[i]] = i
sell = df_sell.values
columns = df_sell.columns

columns = df_buy.columns
column_n_dict_buy = dict()
for i in range(len(columns)):
    column_n_dict_buy[columns[i]] = i
buy = df_buy.values

columns = df_prices.columns
column_n_dict = dict()
for i in range(len(columns)):
    column_n_dict[columns[i]] = i
prices = df_prices.values

index_list = ["buy_best_price", "sell_best_price"]
referred_index_list = ["referred_" + i for i in index_list]
for index in index_list:
    print(index)
    for i in range(len(prices)):
        ri = np.nan
        if i+1-t1 >= 1:
            for j in range(i-t1+1):
                if math.isnan(prices[i-t1-j, column_n_dict[index]]) == False:
                    ri = prices[i-t1-j, column_n_dict[index]]
                    break
        prices[i, column_n_dict["referred_{}".format(index)]] = ri
df_prices = pd.DataFrame(prices)
df_prices.columns = columns

df_prices = df_prices.dropna(subset = referred_index_list).reset_index(drop = True)

# process perp data

df_exe = pd.read_csv("executions_{}_2021-07-01.csv".format(future.replace("/", "-")), index_col = 0)
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-02.csv".format(future.replace("/", "-")), index_col = 0)])
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-03.csv".format(future.replace("/", "-")), index_col = 0)])
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-04.csv".format(future.replace("/", "-")), index_col = 0)])
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-05.csv".format(future.replace("/", "-")), index_col = 0)])
df_exe = pd.concat([df_exe, pd.read_csv("executions_{}_2021-07-06.csv".format(future.replace("/", "-")), index_col = 0)])

df_exe["ts"] = df_exe["time"].apply(lambda x : x[:26]).apply(lambda x : datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f')).apply(lambda x : x.timestamp())
df_exe = df_exe.drop(["liquidation", "time"], axis = 1)
df_exe = df_exe.rename(columns = {"size":"quantity", "side":"taker_side"})
df_exe = df_exe.reindex(columns = ["ts", "id", "price", "quantity", "taker_side"])

df_exe["ts"] = df_exe["ts"].apply(lambda x : int(x))

exe = df_exe.values
drop_list = list()
for i in range(len(exe)):
    if isinstance(exe[i][2], str) == True:
        print("str type price detected")
        drop_list.append(i)
df_exe = df_exe.drop(drop_list)

df_buy_perp = df_exe[df_exe["taker_side"] == "buy"]
df_sell_perp = df_exe[df_exe["taker_side"] == "sell"]

df_buy_best = df_buy_perp.groupby(["ts"]).apply(lambda x : np.nanmax(x.values[:,2]))
df_buy_best = pd.DataFrame([df_buy_best.index, df_buy_best]).T
columns = ["ts", "buy_best_price"]
df_buy_best.columns = columns

df_sell_best = df_sell_perp.groupby(["ts"]).apply(lambda x : np.nanmin(x.values[:,2]))
df_sell_best = pd.DataFrame([df_sell_best.index, df_sell_best]).T
columns = ["ts", "sell_best_price"]
df_sell_best.columns = columns

df_buy_vwap = df_buy_perp.groupby(["ts"]).apply(lambda x : np.average(a = x.values[:,2], weights = x.values[:,3]))
df_buy_vwap = pd.DataFrame([df_buy_vwap.index, df_buy_vwap]).T
columns = ["ts", "buy_vwap"]
df_buy_vwap.columns = columns

df_sell_vwap = df_sell_perp.groupby(["ts"]).apply(lambda x : np.average(a = x.values[:,2], weights = x.values[:,3]))
df_sell_vwap = pd.DataFrame([df_sell_vwap.index, df_sell_vwap]).T
columns = ["ts", "sell_vwap"]
df_sell_vwap.columns = columns

df_vwap = pd.merge(df_buy_vwap, df_sell_vwap, how = "outer")
df_best = pd.merge(df_buy_best, df_sell_best, how = "outer")
df_prices_perp = pd.merge(df_best, df_vwap, how = "outer")

ts = df_prices_perp["ts"].values
ts_list = list(range(int(ts[0]), int(ts[len(ts)-1]+1)))

df_prices_perp = pd.merge(pd.DataFrame(ts_list).rename(columns = {0:"ts"}), df_prices_perp, how = "outer").sort_values(by = "ts", ascending = True).reset_index(drop = True)

df_prices_perp["referred_buy_best_price"] = np.nan
df_prices_perp["referred_sell_best_price"] = np.nan
df_prices_perp["referred_buy_vwap"] = np.nan
df_prices_perp["referred_sell_vwap"] = np.nan

columns = df_sell_perp.columns
column_n_dict_sell = dict()
for i in range(len(columns)):
    column_n_dict_sell[columns[i]] = i
sell_perp = df_sell_perp.values
columns = df_sell_perp.columns

columns = df_buy_perp.columns
column_n_dict_buy_perp = dict()
for i in range(len(columns)):
    column_n_dict_buy_perp[columns[i]] = i
buy_perp = df_buy_perp.values

columns = df_prices_perp.columns
column_n_dict = dict()
for i in range(len(columns)):
    column_n_dict[columns[i]] = i
prices_perp = df_prices_perp.values

index_list = ["buy_best_price", "sell_best_price"]
referred_index_list = ["referred_" + i for i in index_list]
for index in index_list:
    print(index)
    for i in range(len(prices_perp)):
        ri = np.nan
        if i+1-t1 >= 1:
            for j in range(i-t1+1):
                if math.isnan(prices_perp[i-t1-j, column_n_dict[index]]) == False:
                    ri = prices_perp[i-t1-j, column_n_dict[index]]
                    break
        prices_perp[i, column_n_dict["referred_{}".format(index)]] = ri
df_prices_perp = pd.DataFrame(prices_perp)
df_prices_perp.columns = columns

df_prices_perp = df_prices_perp.dropna(subset = referred_index_list).reset_index(drop = True)

plt.plot(df_prices["referred_buy_best_price"])
plt.plot(df_prices_perp["referred_buy_best_price"])
plt.show()

df_merge = pd.merge(df_prices, df_prices_perp, on = "ts").reset_index(drop = True)

referred_buy_best_price_x_dict = df_merge.referred_buy_best_price_x.to_dict()
referred_sell_best_price_y_dict = df_merge.referred_sell_best_price_y.to_dict()
df_merge["index_val"] = df_merge.index

df_merge["referred_index"] = df_merge.index_val.apply(lambda x : referred_buy_best_price_x_dict[x]/referred_sell_best_price_y_dict[x])

plt.plot(df_merge["referred_index"])
plt.hist(df_merge["referred_index"])
df_merge["referred_index"].describe()

df_tmp = copy.deepcopy(df_merge)

target_entry_list = [1, 1.001]
df_summary = pd.DataFrame()

for te in target_entry_list:
    df_merge = copy.deepcopy(df_tmp)
    target_entry_rate = te
    df_merge["entried"] = False
    df_merge["entried"] = df_merge["referred_index"] <= target_entry_rate
    print("entry ratio : {}".format((df_merge["entried"] == True).sum()/len(df_merge)))
    columns = list(df_merge.columns)
    column_n_dict = {}
    for c in columns:
        column_n_dict[c] = columns.index(c)

    if entry_price_hist == True:
        df_merge["entry_rate_assumed"] = df_merge["referred_index"]

    elif entry_price_hist == False:

        df_merge_bbp_x_drop_na = df_merge.dropna(subset = ["buy_best_price_x"])
        df_merge_sbp_y_drop_na = df_merge.dropna(subset = ["sell_best_price_y"])
        merge_bbp_x_drop_na = df_merge_bbp_x_drop_na.values
        merge_sbp_y_drop_na = df_merge_sbp_y_drop_na.values

        df_entry = df_merge[df_merge["entried"] == True].reset_index(drop = True)
        df_entry.index_val = df_entry.index

        def get_future_price(index, array, side):
            if len(array[array[:,column_n_dict["ts"]] > index]) >= 1:
                return(array[array[:,column_n_dict["ts"]] > index][0, column_n_dict[side]])
            else:
                np.nan

        df_entry["buy_entry_price_assumed"] = df_entry["ts"].apply(lambda x : get_future_price(x, merge_bbp_x_drop_na, "buy_best_price_x"))
        df_entry["sell_entry_price_assumed"] = df_entry["ts"].apply(lambda x : get_future_price(x, merge_sbp_y_drop_na, "sell_best_price_y"))

        df_merge = pd.merge(df_merge, df_entry[["ts", "buy_entry_price_assumed", "sell_entry_price_assumed"]], on = "ts", how = "outer")

        a_dict = df_merge.buy_entry_price_assumed.to_dict()
        b_dict = df_merge.sell_entry_price_assumed.to_dict()
        df_merge["index_val"] = df_merge.index

        df_merge["entry_rate_assumed"] = df_merge.index_val.apply(lambda x : a_dict[x]/b_dict[x])

    df_merge["pl"] = np.nan
    df_merge["spot_sell_price"] = np.nan
    df_merge["future_buy_price"] = np.nan
    df_merge["exit_rate"] = np.nan
    df_merge["entry_rate"] = np.nan

    columns = list(df_merge.columns)
    column_n_dict = {}
    for c in columns:
        column_n_dict[c] = columns.index(c)

    profit_ratio_list = [0.005 + 0.001*i for i in range(10)]
    t2_list = [7 + 5*i for i in range(1)]
    t4_list = [0]

    end_index = len(df_merge)-1
    score = 0
    trial = 0

    initial_ts = df_merge.loc[0, "ts"]

    for pr in profit_ratio_list:
        for t2 in t2_list:
            for t4 in t4_list:
                cost = -0.07/100*3
                order_amount = 1
                print("target_entry_rate : {}".format(te))
                print("pr :{}".format(pr))
                print("t2 : {}".format(t2))
                st = time.time()
                df_merge["pl"] = np.nan
                df_merge["spot_sell_price"] = np.nan
                df_merge["future_buy_price"] = np.nan
                df_merge["exit_rate"] = np.nan
                df_merge["entry_rate"] = np.nan
                merge = df_merge.values
                entried = False
                exited = False
                position = 0
                start_index = 0
                trial += 1

                st = time.time()

                while start_index+t2+t6 <= end_index:
                    if merge[start_index, column_n_dict["entried"]] == True and position == 0:
                        entried = True
                        position = order_amount
                        entry_rate_assumed = merge[start_index, column_n_dict["entry_rate_assumed"]]
                    if entried == True:
                        spot_sell_price = merge[start_index, column_n_dict["referred_buy_best_price_y"]]*(1+pr)
                        try:

                            a = buy[:,column_n_dict_buy["ts"]] >= initial_ts + start_index
                            b = buy[:,column_n_dict_buy["ts"]] <= initial_ts + start_index + t2
                            buy_scope = buy[a==b]
                            future_max_buy_price_entry = np.nanmax(buy_scope[:, column_n_dict_buy["price"]])
                        except ValueError as e:
                            future_max_buy_price_entry = np.nan
                        except Exception as e:
                            print(e)
                            future_max_buy_price_entry = np.nan

                        if spot_sell_price < future_max_buy_price_entry:
                            try:
                                future_buy_price = buy_perp[buy_perp[:,column_n_dict_buy_perp["ts"]] >= initial_ts + start_index + t2][1, column_n_dict_buy_perp["price"]]
                            except Exception as e:
                                future_buy_price = np.nan
                            if math.isnan(future_buy_price) == False:
                                merge[start_index, column_n_dict["spot_sell_price"]] = spot_sell_price
                                merge[start_index, column_n_dict["future_buy_price"]] = future_buy_price
                                merge[start_index, column_n_dict["exit_rate"]] = spot_sell_price/future_buy_price
                                merge[start_index, column_n_dict["entry_rate"]] = entry_rate_assumed
                                pl = spot_sell_price/future_buy_price - entry_rate_assumed + cost
                                merge[start_index, column_n_dict["pl"]] = pl
                                entried = False
                                position = 0
                                entry_rate_assumed = np.nan
                                start_index += t2+t4
                            else:
                                start_index += t2+t4
                        else:
                            start_index += t2
                    elif entried == False:
                        start_index += 1

                df_result = copy.deepcopy(pd.DataFrame(merge))
                df_result.columns = columns
                df_result["ts_datetime"] = df_result["ts"].apply(lambda x : datetime.datetime.fromtimestamp(x))
                df_result.dropna(subset = ["pl"])
                total_pnl = df_result.dropna(subset = ["pl"])["pl"].sum()
                df_summary.loc[te, pr] = total_pnl
                print("total pnl : {}".format(total_pnl))
                print("pl hist")
                plt.hist(df_result.dropna(subset = ["pl"])["pl"])
                plt.show()
                print("pl plot")
                plt.plot(df_result.dropna(subset = ["pl"])["pl"])
                plt.show()
                print("cumsum of pl plot")
                plt.plot(df_result.dropna(subset = ["pl"])["pl"].cumsum())
                plt.show()
                print("processing time : {}".format(time.time() - st))

import seaborn as sns
sns.heatmap(df_summary)
