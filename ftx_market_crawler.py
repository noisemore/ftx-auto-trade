# ---------------------------------------------------------------------------------------- #

import requests
import pandas as pd
import datetime
from datetime import timezone
import numpy as np
import os
import math
import os.path

#import plotly.graph_objects as go

# ---------------------------------------------------------------------------------------- #

# 顯示所有欄位
pd.set_option('display.max_column', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_seq_items', None)
pd.set_option('display.max_colwidth', 500)
pd.set_option('expand_frame_repr', False)

# ---------------------------------------------------------------------------------------- #

# 定義K棒時間
# resolution_1M = 60
# resolution_15M = 900
# resolution_1H = 3600
# resolution_4H = 14400
# resolution_1D = 86400
resolution = {"1m": 60, "5m": 300, '15m': 900, '30m': 1800, "1h": 3600, '2h': 7200, "4h": 14400, "1d": 86400}

# ---------------------------------------------------------------------------------------- #

# Datetime轉Unix
def start_dt_to_unix(y,m,d):
    start_dt = datetime.datetime(y, m, d)
    start_unix_ts = int(start_dt.timestamp())
    return(start_unix_ts)

def end_dt_to_unix(y,m,d):
    end_dt = datetime.datetime(y, m, d)
    end_unix_ts = int(end_dt.timestamp())
    return(end_unix_ts)

# ---------------------------------------------------------------------------------------- #

def get_ftx_historial_market(symbol, kline, start_time, end_time):
    
    if os.path.isdir('history'):
        print("history file already created")
    else:
        os.mkdir('history')
        print('create history file')

    market_url = 'https://ftx.com/api/markets/{}/candles?resolution={}&start_time={}&end_time={}'.format(symbol, kline, start_time, end_time) # 設定URL
    
    try: # 除錯
        request_data = requests.get(market_url, timeout = 15) # 對API要求資訊

    except Exception as e:

        print('ERROR', e)

        return None
    
    historical_data = None

    if request_data.status_code == 200: # 除錯

        request_data_json = request_data.json() # 資訊轉成JSON

        if 'error' in request_data_json: # 除錯
            print ("ERROR MESSAGE:{}".format(request_data_json['error']))

        else: # 沒出現錯誤繼續程序

            historical_data = pd.DataFrame(request_data_json['result']) # 從API裡面拿出叫做'result'裡面的資料

            historical_data['time'] = pd.to_datetime(historical_data['time'], unit='ms') # 校正UNIX顯示時間   

            historical_data.drop(['startTime'], axis = 1, inplace=True) # 把開始時間拿掉

            historical_data['volume'] = historical_data['volume'].apply(lambda x: '{:.5f}'.format(x)) # 完整顯示volume float

            historical_data.set_index('time', inplace=True) # 把time當索引

            historical_data.insert(5,'market',symbol)

            # historical_data.head(5) # 顯示最一開始的幾筆資料

            # historical_data.tail(5) # 顯示最後的幾筆資料
    else:
        print ("ERROR MESSAGE:{}".format(request_data.status_code))
    
    return historical_data

# ---------------------------------------------------------------------------------------- #

def main():

    symbol = "FTT/USD"
    kline = resolution['15m']
    start_time = start_dt_to_unix(2021,1,20)
    end_time = end_dt_to_unix(2021,1,21)

    historical_data = get_ftx_historial_market(symbol, kline, start_time, end_time)

    print(historical_data)

# ---------------------------------------------------------------------------------------- #

# 定義 kline 的 resolution 存檔用

    if kline == 60:
        kline = "1m"

    elif kline == 300:
        kline = "5m"

    elif kline == 900:
        kline = "15m"

    elif kline == 1800:
        kline = "30m"

    elif kline == 3600:
        kline = "1h"

    elif kline == 7200:
        kline = "2h"

    elif kline == 14400:
        kline = "4h"

    else:
        kline = "1d"

# ---------------------------------------------------------------------------------------- #

    symbol = symbol.replace("/","-").upper() # 存檔時不能有 "/"

    filename = './history/{}-{}-data.csv'.format(symbol, kline)

    if  os.path.isfile(filename):
        temp_historical_data = pd.read_csv(filename,index_col = "time",parse_dates = ["time"])
        if historical_data.index not in temp_historical_data.index:
            historical_data = historical_data.append(temp_historical_data)
            temp_historical_data.to_csv(filename)
            print("已更新到最新資料")
        else:
            print("已是最新資料，無需更新")
    else:
        historical_data.to_csv(filename)
        print("此為新資料，已創建csv檔")
    #historical_data.to_csv(filename)
# ---------------------------------------------------------------------------------------- #

main()
