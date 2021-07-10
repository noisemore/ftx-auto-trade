# ---------------------------------------------------------------------------------------- #

import os.path
import requests
import pandas as pd
import datetime
from datetime import datetime
import time
import numpy as np
import os
import os.path

# ---------------------------------------------------------------------------------------- #

# 顯示所有欄位

pd.set_option('display.max_column', None)

pd.set_option('display.max_rows', None)

pd.set_option('display.max_seq_items', None)

pd.set_option('display.max_colwidth', 500)

pd.set_option('expand_frame_repr', False)

# ---------------------------------------------------------------------------------------- #

resolution = {"1m": 60, "5m": 300, '15m': 900, '30m': 1800, "1h": 3600, '2h': 7200, "4h": 14400, "1d": 86400} # 轉換時間秒數

# ---------------------------------------------------------------------------------------- #

# Datetime轉Unix

def start_dt_to_unix(y,m,d):

    start_dt = datetime(y, m, d)

    start_unix_ts = int(start_dt.timestamp())

    return(start_unix_ts)

def end_dt_to_unix(y,m,d):

    end_dt = datetime(y, m, d)

    end_unix_ts = int(end_dt.timestamp())

    return(end_unix_ts)

# ---------------------------------------------------------------------------------------- #

def get_ftx_historial_data(symbol, kline, start_time):
    
    if os.path.isdir('history'):

        print("歷史資料資料夾已創建")

    else:

        os.mkdir('history')

        print('創建歷史資料資料夾')

    market_url = 'https://ftx.com/api/markets/{}/candles?resolution={}&start_time={}&end_time={}'.format(symbol, kline, start_time, start_time + 432000) # 設定URL
    
    try: # 除錯

        request_data = requests.get(market_url, timeout = 15) # 對API要求資訊

    except Exception as e:

        print('ERROR', e)

        return None
    
    # historical_data = None # 為什麼我要放這一行?

    if request_data.status_code == 200: # 除錯

        request_data_json = request_data.json() # 資訊轉成JSON

        if 'error' in request_data_json: # 除錯

            print ("ERROR MESSAGE:{}".format(request_data_json['error']))

        else: # 沒出現錯誤繼續程序

            historical_data = pd.DataFrame(request_data_json['result']) # 從API裡面拿出叫做'result'裡面的資料

            historical_data['time'] = pd.to_datetime(historical_data['time'], unit='ms') # 校正UNIX顯示時間   

            historical_data.drop(['startTime'], axis = 1, inplace=True) # 把開始時間拿掉

            historical_data['volume'] = historical_data['volume'].apply(lambda x: '{:.5f}'.format(x)) # 完整顯示volume float

            historical_data['volume'] = historical_data['volume'].astype(float) # 改volume 的 type

            historical_data.set_index('time', inplace=True) # 把time當索引

            # historical_data.index = pd.DatetimeIndex(historical_data.index) # 為什麼我要放這一行?

            historical_data.insert(5,'market',symbol) # 加入 pair 欄位,方便看
         
            # historical_data.head(5) # 顯示最一開始的幾筆資料

            # historical_data.tail(5) # 顯示最後的幾筆資料

        # ===================== 以下開始資料存儲 ========================== #
        
        if kline == 60: interval = "1m" # 定義 kline 的 resolution 存檔用

        elif kline == 300: interval = "5m"

        elif kline == 900: interval = "15m"

        elif kline == 1800:interval = "30m"

        elif kline == 3600:interval = "1h"

        elif kline == 7200:interval = "2h"

        elif kline == 14400:interval = "4h"

        else:interval = "1d"
    
        pair = symbol.replace("/","-").upper() # 存檔時不能有 "/"

        filename = './history/{}-{}-data.csv'.format(pair, interval) # 存檔的檔名

        if  os.path.isfile(filename):

            temp_historical_data = pd.read_csv(filename,index_col = "time",parse_dates = ["time"]) # 讀檔案並將 index 改為 time

            new_historical_data = temp_historical_data.append(historical_data) # 新檔案加到舊檔案

            new_historical_data.sort_values(by='time', inplace = True, ascending = True) # 資料排序(順序)

            new_historical_data = new_historical_data[~new_historical_data.index.duplicated(keep='first')] # 刪除重複資料

            new_historical_data.to_csv(filename) # 新資料存儲

            print("已更新到最新資料")

            # else:

            #     print("已是最新資料，無需更新")

        else:

            historical_data.to_csv(filename) # 新的 pair 資料存儲

            print("此為新資料，已創建csv檔")

        # ======================= 資料存儲完成 ============================ #

    else:
        
        print ("ERROR MESSAGE:{}".format(request_data.status_code))
    
    return historical_data

# ---------------------------------------------------------------------------------------- #

def get_ftx_all_historial_data(symbol, kline, start_time):

    if kline == 60: interval = "1m" # 定義 kline 的 resolution 存檔用

    elif kline == 300: interval = "5m"

    elif kline == 900: interval = "15m"

    elif kline == 1800:interval = "30m"

    elif kline == 3600:interval = "1h"

    elif kline == 7200:interval = "2h"

    elif kline == 14400:interval = "4h"

    else : interval = "1d"

    pair = symbol.replace("/","-").upper() # 存檔時不能有 "/"

    now = datetime.now().date()

    unix_now = time.mktime(now.timetuple())

    unix_now = int(unix_now)

    print(unix_now)

    filename = './history/{}-{}-data.csv'.format(pair, interval) # 存檔的檔名

    if os.path.isfile(filename):

        temp_historical_data = pd.read_csv(filename,index_col = "time",parse_dates = ["time"])

        temp_historical_data_last = temp_historical_data.tail(1)

        # temp_historical_data_last = temp_historical_data_last.head(1)

        last_historical_data_time = temp_historical_data_last.index
        
        last_historical_data_time = last_historical_data_time.astype(np.int64)

        last_historical_data_time = last_historical_data_time // 10**9
        
        last_historical_data_time = last_historical_data_time[0]

        start_time = last_historical_data_time - 28800

        print(start_time)

        while start_time <= unix_now:

            get_ftx_historial_data(symbol,kline,start_time)

            start_time += 432000

    else:

        while start_time <= unix_now:

            get_ftx_historial_data(symbol,kline,start_time)

            start_time += 432000

    return(start_time)

# ---------------------------------------------------------------------------------------- #



# ======================= Function Test ============================ #

symbol = "BTC/USD"

kline = resolution['15m']

start_time = start_dt_to_unix(2019,7,21)

# end_time = start_time + 86400

get_ftx_all_historial_data(symbol, kline, start_time)

# print(historical_data)

# ---------------------------------------------------------------------------------------- #
