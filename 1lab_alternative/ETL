import requests
import pandas as pd
import sqlite3
import json
import os
from datetime import datetime

DB_NAME = "market_data.db"
RAW_DATA_DIR = "raw_market_data"

if not os.path.exists(RAW_DATA_DIR):
    os.makedirs(RAW_DATA_DIR)

# Тикеры
MOEX_TICKERS = ['SBER', 'GAZP', 'LKOH', 'YNDX', 'ROSN']
COINGECKO_IDS = ['bitcoin', 'ethereum', 'tether']

# 1. EXTRACT

def extract_moex():
    """Скачиваем данные MOEX"""
    print("[EXTRACT] Запрос к MOEX...")
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{RAW_DATA_DIR}/moex_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        return filename
    except Exception as e:
        print(f"Ошибка MOEX: {e}")
        return None


def extract_crypto():
    """Скачиваем крипту через CoinGecko"""
    print("[EXTRACT] Запрос к CoinGecko...")

    ids_str = ",".join(COINGECKO_IDS)
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids_str}&vs_currencies=usd"

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{RAW_DATA_DIR}/crypto_{timestamp}.json"

        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        return filename
    except Exception as e:
        print(f"Ошибка Crypto API: {e}")
        return None

# 2. TRANSFORM

def transform_moex(filepath):
    print(f"[TRANSFORM] Обработка MOEX: {filepath}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            raw = json.load(f)

        columns = raw['marketdata']['columns']
        data = raw['marketdata']['data']

        df = pd.DataFrame(data, columns=columns)

        df = df[df['SECID'].isin(MOEX_TICKERS)].copy()

        result = pd.DataFrame()
        result['ticker'] = df['SECID']

        result['price'] = pd.to_numeric(df['LAST']).fillna(0)

        result['currency'] = 'RUB'
        result['type'] = 'Stock'
        result['source'] = 'MOEX'

        return result

    except Exception as e:
        print(f"Ошибка парсинга MOEX: {e}")
        return pd.DataFrame()


def transform_crypto(filepath):
    print(f"[TRANSFORM] Обработка Crypto: {filepath}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        rows = []
        for coin_id, values in data.items():
            rows.append({
                'ticker': coin_id.upper(),
                'price': values['usd'],
                'currency': 'USD',
                'type': 'Crypto',
                'source': 'CoinGecko'
            })

        return pd.DataFrame(rows)

    except Exception as e:
        print(f"Ошибка парсинга Crypto: {e}")
        return pd.DataFrame()


# 3. LOAD

def load_db(df):
    if df.empty:
        print("[LOAD] Нет данных.")
        return

    df['updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(DB_NAME)
    print(f"[LOAD] Запись {len(df)} строк в БД...")

    df.to_sql('quotes', conn, if_exists='append', index=False)

    conn.close()


if __name__ == "__main__":
    # 1. MOEX
    f_moex = extract_moex()
    df_moex = transform_moex(f_moex) if f_moex else pd.DataFrame()

    # 2. CRYPTO
    f_crypto = extract_crypto()
    df_crypto = transform_crypto(f_crypto) if f_crypto else pd.DataFrame()

    # 3. MERGE & LOAD
    full_df = pd.concat([df_moex, df_crypto], ignore_index=True)

    print("\nИТОГОВАЯ ТАБЛИЦА")
    print(full_df)

    load_db(full_df)
