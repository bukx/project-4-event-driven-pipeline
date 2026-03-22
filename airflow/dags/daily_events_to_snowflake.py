"""Airflow DAG: Daily ETL — PostgreSQL events -> transform -> Snowflake."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {"owner": "data-platform", "depends_on_past": False, "retries": 2,
                "retry_delay": timedelta(minutes=5), "email_on_failure": True}

dag = DAG("daily_events_to_snowflake", default_args=default_args,
          schedule_interval="0 2 * * *", start_date=datetime(2024, 1, 1),
          catchup=False, tags=["etl", "snowflake"])

def extract_events(**ctx):
    pg = PostgresHook(postgres_conn_id="events_postgres")
    df = pg.get_pandas_df(
        "SELECT * FROM events WHERE timestamp >= %(s)s::timestamptz AND timestamp < %(e)s::timestamptz",
        parameters={"s": f"{ctx['ds']}T00:00:00Z", "e": f"{ctx['ds']}T23:59:59Z"})
    path = f"/tmp/events_{ctx['ds']}.parquet"
    df.to_parquet(path, index=False)
    ctx["ti"].xcom_push(key="parquet_path", value=path)
    ctx["ti"].xcom_push(key="row_count", value=len(df))

def transform_events(**ctx):
    import pandas as pd
    df = pd.read_parquet(ctx["ti"].xcom_pull(key="parquet_path"))
    df = df.drop_duplicates(subset=["event_id"])
    df["event_date"] = pd.to_datetime(df["timestamp"]).dt.date
    df["event_hour"] = pd.to_datetime(df["timestamp"]).dt.hour
    path = f"/tmp/events_{ctx['ds']}_transformed.parquet"
    df.to_parquet(path, index=False)
    ctx["ti"].xcom_push(key="transformed_path", value=path)
    ctx["ti"].xcom_push(key="transformed_count", value=len(df))

def load_to_snowflake(**ctx):
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    path = ctx["ti"].xcom_pull(key="transformed_path")
    sf = SnowflakeHook(snowflake_conn_id="snowflake_analytics")
    conn = sf.get_conn(); cursor = conn.cursor()
    try:
        cursor.execute(f"PUT file://{path} @events_stage AUTO_COMPRESS=TRUE")
        cursor.execute(f"COPY INTO analytics.events.raw_events FROM @events_stage/{path.split('/')[-1]} FILE_FORMAT=(TYPE=PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE")
    finally:
        cursor.close(); conn.close()

t1 = PythonOperator(task_id="extract", python_callable=extract_events, dag=dag)
t2 = PythonOperator(task_id="transform", python_callable=transform_events, dag=dag)
t3 = PythonOperator(task_id="load", python_callable=load_to_snowflake, dag=dag)
t1 >> t2 >> t3
