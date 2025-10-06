from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def return_last_90d_price(symbol,url):
    r = requests.get(url)
    data = r.json()
    daily_data = data.get("Time Series (Daily)", {})
    sorted_dates = sorted(daily_data.keys(), reverse=True)

    results = []
    for d in sorted_dates[:90]:
        results.append({
            "date": d,
            **daily_data[d]
        })
    print(f"Fetched {len(results)} rows from API for {symbol}")
    return results

@task
def load_to_snowflake(records, symbol):
    target_table = "DAILY_PRICES"
    con = return_snowflake_conn()
    try:
        con.execute("BEGIN;")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
              DATE DATE,
              OPEN FLOAT,
              HIGH FLOAT,
              LOW FLOAT,
              CLOSE FLOAT,
              VOLUME NUMBER(38,0),
              SYMBOL VARCHAR,
              PRIMARY KEY (SYMBOL, DATE)
            );
        """)
        con.execute(f"DELETE FROM {target_table}")
        for r in records:
            date   = r['date']
            open_  = r['1. open']
            high   = r['2. high']
            low    = r['3. low']
            close  = r['4. close']
            volume = r['5. volume']
            sql = (
                f"INSERT INTO {target_table} (DATE,OPEN,HIGH,LOW,CLOSE,VOLUME,SYMBOL) "
                f"VALUES ('{date}', '{open_}','{high}','{low}','{close}','{volume}','{symbol}')"
            )
            con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print("Transaction rolled back due to error:", e)
        raise
with DAG(
    dag_id = '90_days_price',
    start_date = datetime(2025,10,5),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "DAILY_PRICES"
    api_key = Variable.get("AlphaVantageAPI")
    symbol = "KMB"
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    data = return_last_90d_price(symbol,url)
    lines = load_to_snowflake(data, symbol)  
