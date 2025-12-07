import sys
import requests
import pandas as pd
from datetime import datetime

def main():
    if len(sys.argv) != 4:
        print(f'Используйте скрипт правильно - <скрипт.py> <BTC> <%Y-%m-%d> <%Y-%m-%d>')
        sys.exit()

    ticker = sys.argv[1].upper()

    # Добавляю USDT
    if not ticker.endswith('USDT'):
        symbol = ticker + 'USDT'
    else:
        symbol = ticker

    start_date = sys.argv[2]
    end_date = sys.argv[3]

    raw_data = fetch_binance_data(symbol, start_date, end_date)
    df_detail, df_summary = transform_data(raw_data)
    save_to_excel(df_detail, df_summary, symbol, start_date, end_date)

def fetch_binance_data(symbol, start_date, end_date):

    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        print("Ошибка: дата должна быть в формате YYYY-MM-DD")
        sys.exit()
    if start_dt >= end_dt:
        print("Ошибка: start_date должна быть раньше end_date")
        sys.exit()

    # Преобразование в миллисекунды для API
    start_timestamp = int(start_dt.timestamp() * 1000)
    end_timestamp = int(end_dt.timestamp() * 1000)

    # Запросик
    url = 'https://api.binance.com/api/v3/klines'
    response = requests.get(url, params = {
        'symbol': symbol,
        'interval': '1d',
        'startTime': start_timestamp,
        'endTime': end_timestamp
        })

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API error: {response.status_code}")

def transform_data(raw_data):
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

    df = pd.DataFrame(raw_data, columns=columns)

    # Переименование колонок на русский
    df = df.rename(columns={
        "open_time": "ВРЕМЯ_ОТКРЫТИЯ",
        "open": "ОТКРЫТИЕ",
        "high": "МАКСИМУМ",
        "low": "МИНИМУМ",
        "close": "ЗАКРЫТИЕ",
        "volume": "ОБЪЕМ",
        "close_time": "ВРЕМЯ_ЗАКРЫТИЯ",
        "quote_asset_volume": "ОБЪЕМ КОТИРУЕМЫХ АКТИВОВ",
        "number_of_trades": "КОЛИЧЕСТВО_СДЕЛОК",
        "taker_buy_base_asset_volume": "ОБЪЕМ_ПОКУПКИ_БАЗОВОГО_АКТИВА",
        "taker_buy_quote_asset_volume": "ОБЪЕМ_ПОКУПКИ_КВОТНОГО_АКТИВА",
        "ignore": "ИГНОР"
    })

    # Преобразование временных метки в читаемый формат datetime, для exel
    df['ВРЕМЯ_ОТКРЫТИЯ'] = pd.to_datetime(df['ВРЕМЯ_ОТКРЫТИЯ'], unit='ms')
    df['ВРЕМЯ_ЗАКРЫТИЯ'] = pd.to_datetime(df['ВРЕМЯ_ЗАКРЫТИЯ'], unit='ms')

    # Вычисление колонки средней цены
    df['СРЕДНЯЯ_ЦЕНА'] = (df['МАКСИМУМ'].astype(float) + df['МИНИМУМ'].astype(float)) / 2

    # Приведение числовых колонок к float
    numeric_columns = [
        "ОТКРЫТИЕ",
        "МАКСИМУМ",
        "МИНИМУМ",
        "ЗАКРЫТИЕ",
        "ОБЪЕМ",
        "ОБЪЕМ КОТИРУЕМЫХ АКТИВОВ",
        "ОБЪЕМ_ПОКУПКИ_БАЗОВОГО_АКТИВА",
        "ОБЪЕМ_ПОКУПКИ_КВОТНОГО_АКТИВА"
    ]
    df[numeric_columns] = df[numeric_columns].astype(float)

    # Создание сводного DataFrame (df_summary)

    # Создание колонки дата для группировки
    df["ДАТА"] = df["ВРЕМЯ_ОТКРЫТИЯ"].dt.date

    # Группировка по дате
    df_summary = df.groupby('ДАТА').agg(
        СРЕДНЕЕ_ЗАКРЫТИЕ=('ЗАКРЫТИЕ', 'mean'),
        МИН_ЗАКРЫТИЕ=('ЗАКРЫТИЕ', 'min'),
        МАКС_ЗАКРЫТИЕ=('ЗАКРЫТИЕ', 'max'),
        СРЕДНЯЯ_ЦЕНА_СВЕЧИ=('СРЕДНЯЯ_ЦЕНА', 'mean'),
        СУММАРНЫЙ_ОБЪЕМ=('ОБЪЕМ', 'sum')
    ).reset_index()

    df_detail = df.copy()
    return df_detail, df_summary

def save_to_excel(df_detail, df_summary, symbol, start_date, end_date):
    filename = f"binance_{symbol}_{start_date}_{end_date}.xlsx"

    # Excel writer с движком openpyxl
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        df_detail.to_excel(writer, sheet_name="Детали", index=False)
        df_summary.to_excel(writer, sheet_name="Сводка", index=False)

    print(f"Файл успешно сохранён: {filename}")

if __name__ == "__main__":
    main()