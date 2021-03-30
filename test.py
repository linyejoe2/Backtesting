# IMPORTS
import pandas as pd
from backtesting import Backtest
from backtesting.lib import SignalStrategy
from binance.client import Client

import math
import os.path
import time

from datetime import timedelta, datetime
# from dateutil import parser
# from tqdm import tqdm_notebook #(Optional, used for progress-bars)
# import config
# import backtrader as bt


binance_api_key = '[REDACTED]'    #Enter your own API-key here
binance_api_secret = '[REDACTED]' #Enter your own API-secret here

binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)

binsizes = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "2h":120, "4h": 240, "6h": 360, "8h": 480, "1d": 1440}

def minutes_of_new_data(symbol, kline_size, data, source):
    if len(data) > 0:  old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance": old = datetime.strptime('1 Jan 2017', '%d %b %Y')
    if source == "binance": new = pd.to_datetime(binance_client.get_klines(symbol=symbol, interval=kline_size)[-1][0], unit='ms')
    return old, new

def get_all_binance(symbol, kline_size, save=True, update=True):
    
    # 創資料夾
    if not os.path.exists('history'):
        os.mkdir('history')
        
    if not os.path.exists(os.path.join('history', 'crypto')):
        os.mkdir(os.path.join('history', 'crypto'))
    
    # 如果資料夾裡沒有檔案的話 才進下一步下載
    filename = os.path.join('history', 'crypto', '%s-%s-data.csv' % (symbol, kline_size))
    if os.path.isfile(filename):
        data_df = pd.read_csv(filename, index_col='Timestamp', parse_dates=True)
    else:
        data_df = pd.DataFrame()
    
    if update == False:
        data_df.columns = data_df.columns.str.capitalize()
        data_df.index = pd.to_datetime(data_df.index)
        return data_df

        
    # find time period
    #oldest_point, newest_point = minutes_of_new_data(symbol, kline_size, data_df, source = "binance")
    
    # 設定時間長度 
    if not data_df.empty:
        oldest_point = data_df.index[-1].to_pydatetime()
    else:
        oldest_point = datetime.strptime('1 Jan 2017', '%d %b %Y')
    newest_point = datetime.now()
    print(oldest_point, newest_point)
    
    delta_min = (newest_point - oldest_point).total_seconds()/60 # 轉換成Unix時間
    available_data = math.ceil(delta_min/binsizes[kline_size])
    
    # print some info
    if oldest_point == datetime.strptime('1 Jan 2017', '%d %b %Y'): print('Downloading all available %s data for %s. Be patient..!' % (kline_size, symbol))
    else: print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (delta_min, symbol, available_data, kline_size))
    
    # download kbars
    klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime("%d %b %Y %H:%M:%S"))
    
    # processing
    data = pd.DataFrame(klines, columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time', 'Quote_av', 'Trades', 'Tb_base_av', 'Tb_quote_av', 'Ignore' ])

    data.Timestamp = pd.to_datetime(data.Timestamp, unit='ms')
    data.set_index('Timestamp', inplace=True)
    data = data[~data.index.duplicated(keep='last')]
    data = data.astype(float)
    
    # combine dataframe
    if len(data_df) > 0:
        data_df = data_df.append(data)
    else:
        data_df = data
        
    data_df = data_df[~data_df.index.duplicated(keep='last')]
    
    assert data_df.index.duplicated().sum() == 0
    # save data   
    if save: data_df.to_csv(filename)
    print('All caught up..!')
    return data_df