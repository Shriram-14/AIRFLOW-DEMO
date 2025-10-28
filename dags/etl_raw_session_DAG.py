from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import logging
import snowflake.connector


def return_snowflake_conn():
    # Initialize the SnowflakeHook with your connection name
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def create_table_load(cursor):
    try:
        cursor.execute("BEGIN")

        # Create tables in RAW_SCHEMA
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS RAW_SCHEMA.user_session_channel (
                userId INT NOT NULL,
                sessionId VARCHAR(32) PRIMARY KEY,
                channel VARCHAR(32) DEFAULT 'direct'
            );
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS RAW_SCHEMA.session_timestamp (
                sessionId VARCHAR(32) PRIMARY KEY,
                ts TIMESTAMP
            );
        ''')

        # Create stage and load CSVs from public S3 (read-only)
        cursor.execute('''
            CREATE OR REPLACE STAGE RAW_SCHEMA.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        ''')

        cursor.execute('''
            COPY INTO RAW_SCHEMA.user_session_channel
            FROM @RAW_SCHEMA.blob_stage/user_session_channel.csv;
        ''')
        cursor.execute('''
            COPY INTO RAW_SCHEMA.session_timestamp
            FROM @RAW_SCHEMA.blob_stage/session_timestamp.csv;
        ''')

        cursor.execute("COMMIT;")

        print("Tables created successfully")
        print("Data loaded successfully")

    except Exception as e:
        cursor.execute("COMMIT;")
        print("Error:", e)
        raise e


with DAG(
    dag_id='etl_raw_sessions',
    start_date=datetime(2024, 10, 27),
    catchup=False,
    tags=['ETL', 'RAW'],
    schedule='45 2 * * *',
) as dag:
    cursor = return_snowflake_conn()
    create_table_load(cursor)
