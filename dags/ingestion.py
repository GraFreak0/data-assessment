from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import datetime, timedelta
from sql import query_trip_data
from clickhouse_driver import Client
import sqlite3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clickhouse_to_sqlite_dag', default_args=default_args, schedule_interval=None)

def execute_query():
    host = 'github.demo.altinity.cloud'
    user = 'demo'
    password = 'demo'
    port = 9440
    database = 'default'
    
    client = Client(host=host, user=user, password=password, port=port, database=database, secure=True)
    result = client.execute(query_trip_data)  # Execute the SQL query
    return result

execute_query_task = PythonOperator(
    task_id='execute_query',
    python_callable=execute_query,
    dag=dag,
    provide_context=True,
)

create_table_sqlite_task = SqliteOperator(
    task_id='create_table_sqlite',
    sql=r"""
    CREATE TABLE IF NOT EXISTS trip_metrics (
        month text,
        sat_mean_trip_count real,
        sat_mean_fare_per_trip real,
        sat_mean_duration_per_trip real,
        sun_mean_trip_count real,
        sun_mean_fare_per_trip real,
        sun_mean_duration_per_trip real
    );
    """,
    dag=dag,
)

@dag.task(task_id="insert_sqlite_task")
def insert_sqlite_hook(**context):
    sqlite_hook = SqliteHook()
    ti = context['ti']
    results = ti.xcom_pull(task_ids="execute_query")
    target_fields = ['month', 'sat_mean_trip_count', 'sat_mean_fare_per_trip', 'sat_mean_duration_per_trip', 'sun_mean_trip_count', 'sun_mean_fare_per_trip', 'sun_mean_duration_per_trip']
    sqlite_hook.insert_rows(table='trip_metrics', rows=results, target_fields=target_fields)

execute_query_task >> create_table_sqlite_task >> insert_sqlite_hook()