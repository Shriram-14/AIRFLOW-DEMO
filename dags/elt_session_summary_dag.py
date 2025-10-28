from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import logging
import snowflake.connector


def return_snowflake_conn():
    # Use your Airflow Snowflake connection
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(schema, table, select_sql, primary_key=None):
    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        # Create/replace temp table with CTAS
        sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # Primary key uniqueness check on temp table
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt
              FROM {schema}.temp_{table}
              GROUP BY 1
              HAVING COUNT(1) > 1
              ORDER BY cnt DESC
              LIMIT 1
            """
            print(sql)
            cur.execute(sql)
            dup_row = cur.fetchone()
            if dup_row is not None:
                print("Primary key uniqueness failed:", dup_row)
                raise Exception(f"Primary key uniqueness failed: {dup_row}")

            # Optional stronger check directly on the join SQL
            print(f"Checking duplicates for {primary_key}...")
            cur.execute(f"""
              SELECT COUNT(*) FROM (
                SELECT {primary_key}
                FROM ({select_sql}) j
                GROUP BY {primary_key}
                HAVING COUNT(*) > 1
              )
            """)
            if int(cur.fetchone()[0]) > 0:
                raise Exception(f"Primary key duplicated in join result: {primary_key}")

        # Ensure final table exists (empty shell if first run)
        main_table_creation_if_not_exists_sql = f"""
          CREATE TABLE IF NOT EXISTS {schema}.{table} AS
          SELECT * FROM {schema}.temp_{table} WHERE 1=0;
        """
        cur.execute(main_table_creation_if_not_exists_sql)

        # Atomic swap temp into final
        swap_sql = f"ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"
        cur.execute(swap_sql)

        print("CTAS and SWAP completed successfully")

    finally:
        cur.close()


with DAG(
    dag_id='BuildELT_CTAS',
    start_date=datetime(2024, 10, 27),
    catchup=False,
    tags=['ELT'],
    schedule='45 2 * * *',  # daily at 02:45
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:

    schema = "ANALYTICS_SCHEMA"
    table = "session_summary"

    # Use snake_case column names that exist in your RAW tables
    select_sql = """
      SELECT
        u.USER_ID,
        u.SESSION_ID,
        u.CHANNEL,
        s.EVENT_TS,
        DATE_TRUNC('day',  s.EVENT_TS)  AS EVENT_DATE,
        DATE_TRUNC('week', s.EVENT_TS)  AS EVENT_WEEK
      FROM RAW_SCHEMA.USER_SESSION_CHANNEL u
      JOIN RAW_SCHEMA.SESSION_TIMESTAMP s
        ON u.SESSION_ID = s.SESSION_ID
    """

    run_ctas(schema, table, select_sql, primary_key='SESSION_ID')
