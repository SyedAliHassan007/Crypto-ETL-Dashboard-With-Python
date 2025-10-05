# etl.py
import requests
import pandas as pd
import sqlite3
import os
from datetime import datetime

# zoneinfo available in Python 3.9+. If not available, remove timezone usage or install backports.
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

LOG_PATH = "./logs/code_log.txt"
EXCHANGE_CSV = "./input/exchange_rate.csv"
DB_NAME = "crypto_data.db"
TABLE_NAME = "crypto_prices"

def log_progress(message):
    """Log messages (also print)."""
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    if ZoneInfo:
        ts = datetime.now(ZoneInfo("Asia/Karachi")).strftime("%Y-%m-%d %H:%M:%S")
    else:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a") as f:
        f.write(f"{ts}: {message}\n")
    print(f"{ts}: {message}")

# ---------- Step 1: Extract ----------
def extract():
    """
    Extract live prices from CoinGecko simple price API.
    Returns a DataFrame where each row is a crypto and columns are currency codes (pkr, usd, ...)
    """
    log_progress("Extract: started")
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,dogecoin,solana",      # change list if you want
        "vs_currencies": "pkr,usd,eur,gbp,inr,aud"     # requested currencies (lowercase)
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        # Convert to DataFrame: rows = crypto, cols = currencies
        df = pd.DataFrame(data).T.reset_index().rename(columns={"index": "crypto"})
        log_progress("Extract: completed successfully")
        return df
    except Exception as e:
        log_progress(f"Extract: failed - {e}")
        return pd.DataFrame()

# ---------- Step 2: Transform ----------
def transform(df, csv_path=EXCHANGE_CSV):
    """
    Transform using exchange rates CSV.
    CSV must contain Currency,Rate where Rate is multiplier from PKR to target currency:
      e.g. USD,0.0036  means 1 PKR = 0.0036 USD
    This function looks for the PKR price column in df and creates new columns:
      price_in_USD, price_in_EUR, ...
    """
    log_progress("Transform: started")

    if df.empty:
        log_progress("Transform: aborted (empty DataFrame)")
        return df

    if not os.path.exists(csv_path):
        log_progress(f"Transform: exchange CSV not found at {csv_path}")
        return df

    try:
        rates = pd.read_csv(csv_path, index_col=0).to_dict()['Rate']
    except Exception as e:
        log_progress(f"Transform: failed to read CSV - {e}")
        return df

    # normalize keys to lowercase for comparison
    rates = {k.lower(): v for k, v in rates.items()}

    # find PKR column in extracted df
    pkr_col = None
    for c in df.columns:
        if c.lower() == "pkr" or "pkr" in c.lower():
            pkr_col = c
            break

    if pkr_col is None:
        log_progress("Transform: PKR column not found in DataFrame")
        return df

    # create converted columns
    for cur, rate in rates.items():
        # skip if rate is not numeric
        try:
            rate = float(rate)
        except Exception:
            log_progress(f"Transform: skipping invalid rate for {cur}")
            continue
        new_col = f"price_in_{cur.upper()}"
        df[new_col] = (df[pkr_col] * rate).round(8)  # keep 8 decimal places for small currencies

    # add timestamp
    if ZoneInfo:
        df['last_updated'] = datetime.now(ZoneInfo("Asia/Karachi")).isoformat()
    else:
        df['last_updated'] = datetime.now().isoformat()

    log_progress("Transform: completed")
    return df

# ---------- Step 3: Load ----------
def load_to_sqlite(df, db_name=DB_NAME, table_name=TABLE_NAME):
    log_progress("Load: started")
    if df.empty:
        log_progress("Load: aborted (empty DataFrame)")
        return
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        log_progress(f"Load: completed. Data written to {db_name} -> {table_name}")
    except Exception as e:
        log_progress(f"Load: failed - {e}")

# ---------- Run full ETL ----------
def run_etl():
    df = extract()
    df2 = transform(df)
    load_to_sqlite(df2)
    return df2

if __name__ == "__main__":
    # run ETL when executed directly
    df_final = run_etl()
    print("\nFinal DataFrame:\n", df_final)
