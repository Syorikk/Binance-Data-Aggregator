import os
import sys
import requests
import pandas as pd
import pprint
from datetime import datetime

def fetch_binance_data(symbol, start_date, end_date):
    # Преобразование строки дат в datetime объекты
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    #print(f"{start_dt=}, {end_dt=}") # Done | No errors here
    # Преобразование в миллисекунды для API
    start_timestamp = int(start_dt.timestamp() * 1000)
    end_timestamp = int(end_dt.timestamp() * 1000)
    #print(f"{start_timestamp=}, {end_timestamp=}")
    # Запросик
    url = 'https://api.binance.com/api/v3/klines'
    response = requests.get(url, params = {
        'symbol': symbol,
        'interval': '1d',
        'startTime': start_timestamp,
        'endTime': end_timestamp
        })
    # print(response)
    # pprint.pprint(response.json())
    # print(response.status_code)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API error: {response.status_code}")


data:dict = fetch_binance_data(symbol='ETHUSDT', start_date='2025-11-01', end_date='2025-11-13')

columns = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore"
]
df = pd.DataFrame(data, columns=columns)#columns=['ВРЕМЯ_ОТКРЫТИЯ', 'ОТКРЫТИЕ', 'МАКСИМУМ', 'МИНИМУМ', 'ЗАКРЫТИЕ', 'ОБЪЕМ', 'ВРЕМЯ_ЗАКРЫТИЯ', 'СРЕДНЯЯ_ЦЕНА'])
#df = df.rename()
print(df.head(5).to_string())

