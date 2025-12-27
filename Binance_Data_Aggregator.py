import sys
import pandas as pd
import requests
from datetime import datetime, timedelta

def main():
    if len(sys.argv) != 4:
        print("Используйте скрипт правильно: <script.py> <BTC> <YYYY-MM-DD> <YYYY-MM-DD>")
        sys.exit()

    ticker = sys.argv[1].upper()
    symbol = ticker if ticker.endswith("USDT") else f"{ticker}USDT"

    start_date = sys.argv[2]
    end_date = sys.argv[3]

    raw_data = fetch_binance_data(symbol, start_date, end_date)
    df_detail, df_summary = transform_data(raw_data)
    save_to_excel(df_detail, df_summary, symbol, start_date, end_date)


def fetch_binance_data(symbol, start_date, end_date):
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    except ValueError:
        print("Ошибка: дата должна быть в формате YYYY-MM-DD")
        sys.exit()

    if start_dt >= end_dt:
        print("Ошибка: start_date должна быть раньше end_date")
        sys.exit()

    params = {
        "symbol": symbol,
        "interval": "1d",
        "startTime": int(start_dt.timestamp() * 1000),
        "endTime": int(end_dt.timestamp() * 1000),
    }

    response = requests.get("https://api.binance.com/api/v3/klines", params=params)

    if response.status_code != 200:
        raise Exception(f"API error: {response.status_code}")

    return response.json()


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
        "ignore",
    ]

    df = pd.DataFrame(raw_data, columns=columns)

    df = df.rename(
        columns={
            "open_time": "ВРЕМЯ_ОТКРЫТИЯ",
            "open": "ОТКРЫТИЕ",
            "high": "МАКСИМУМ",
            "low": "МИНИМУМ",
            "close": "ЗАКРЫТИЕ",
            "volume": "ОБЪЕМ",
            "close_time": "ВРЕМЯ_ЗАКРЫТИЯ",
            "quote_asset_volume": "ОБЪЕМ_КОТИРУЕМЫХ_АКТИВОВ",
            "number_of_trades": "КОЛИЧЕСТВО_СДЕЛОК",
            "taker_buy_base_asset_volume": "ОБЪЕМ_ПОКУПКИ_БАЗОВОГО_АКТИВА",
            "taker_buy_quote_asset_volume": "ОБЪЕМ_ПОКУПКИ_КВОТНОГО_АКТИВА",
            "ignore": "ИГНОР",
        }
    )

    df["ВРЕМЯ_ОТКРЫТИЯ"] = pd.to_datetime(df["ВРЕМЯ_ОТКРЫТИЯ"], unit="ms")
    df["ВРЕМЯ_ЗАКРЫТИЯ"] = pd.to_datetime(df["ВРЕМЯ_ЗАКРЫТИЯ"], unit="ms")

    numeric_columns = [
        "ОТКРЫТИЕ",
        "МАКСИМУМ",
        "МИНИМУМ",
        "ЗАКРЫТИЕ",
        "ОБЪЕМ",
        "ОБЪЕМ_КОТИРУЕМЫХ_АКТИВОВ",
        "ОБЪЕМ_ПОКУПКИ_БАЗОВОГО_АКТИВА",
        "ОБЪЕМ_ПОКУПКИ_КВОТНОГО_АКТИВА",
    ]
    df[numeric_columns] = df[numeric_columns].astype(float)

    df["СРЕДНЯЯ_ЦЕНА"] = (df["МАКСИМУМ"] + df["МИНИМУМ"]) / 2
    df["ДАТА"] = df["ВРЕМЯ_ОТКРЫТИЯ"].dt.date

    df_summary = (
        df.groupby("ДАТА")
        .agg(
            СРЕДНЕЕ_ЗАКРЫТИЕ=("ЗАКРЫТИЕ", "mean"),
            МИН_ЗАКРЫТИЕ=("ЗАКРЫТИЕ", "min"),
            МАКС_ЗАКРЫТИЕ=("ЗАКРЫТИЕ", "max"),
            СРЕДНЯЯ_ЦЕНА_СВЕЧИ=("СРЕДНЯЯ_ЦЕНА", "mean"),
            СУММАРНЫЙ_ОБЪЕМ=("ОБЪЕМ", "sum"),
        )
        .reset_index()
    )

    return df.copy(), df_summary


def save_to_excel(df_detail, df_summary, symbol, start_date, end_date):
    filename = f"binance_{symbol}_{start_date}_{end_date}.xlsx"

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        df_detail.to_excel(writer, sheet_name="Детали", index=False)
        df_summary.to_excel(writer, sheet_name="Сводка", index=False)

    print(f"Файл успешно сохранён: {filename}")


if __name__ == "__main__":
    main()