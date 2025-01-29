import requests
import psycopg2
import polars as pl
from datetime import datetime

db_config = {
    'dbname': 'mydatabase',
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'port': 5432
}

connection = psycopg2.connect(**db_config)
print("Connected!")

API_KEY = "HLBTLZGODES6IH5J" 
SYMBOL = "IBM"
INTERVAL = "5min"
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={SYMBOL}&interval={INTERVAL}&apikey={API_KEY}'

r = requests.get(url)
data = r.json()

time_series_key = f"Time Series ({INTERVAL})"
if time_series_key not in data:
    print("Error: Invalid API response or rate limit exceeded.")
    exit()

time_series = data[time_series_key]

records = []
for timestamp, values in time_series.items():
    dt_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    formatted_timestamp = dt_obj.strftime("%Y-%m-%d %H:%M")

    records.append({
        "symbol": SYMBOL,
        "timestamp": formatted_timestamp,
        "open": float(values["1. open"]),
        "high": float(values["2. high"]),
        "low": float(values["3. low"]),
        "close": float(values["4. close"]),
        "volume": int(values["5. volume"])
    })

df = pl.DataFrame(records)
print(df)

try:
    cursor = connection.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        timestamp TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
        open DECIMAL(10,4),
        high DECIMAL(10,4),
        low DECIMAL(10,4),
        close DECIMAL(10,4),
        volume BIGINT,
        UNIQUE(symbol, timestamp)  -- Avoid duplicate entries
    );
    """
    cursor.execute(create_table_query)
    connection.commit()
    print("Table ensured!")

    insert_query = """
    INSERT INTO stock_data (symbol, timestamp, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (symbol, timestamp) DO NOTHING;
    """
    
    values = [
        (row["symbol"], row["timestamp"], row["open"], row["high"], row["low"], row["close"], row["volume"])
        for row in df.iter_rows(named=True)
    ]

    from psycopg2.extras import execute_values
    execute_values(cursor, insert_query, values)

    connection.commit()
    print("Data inserted successfully!")

except Exception as e:
    print("Error:", e)

finally:
    cursor.close()
    connection.close()
