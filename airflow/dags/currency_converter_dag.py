from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from services.currency_converter import run_currency_etl


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'currency-converter',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'currency', 'orders', 'eur']
) as dag:

    check_source = PostgresOperator(
        task_id='check_source_connection',
        postgres_conn_id='postgres_1',
        sql='SELECT 1'
    )

    check_target = PostgresOperator(
        task_id='check_target_connection',
        postgres_conn_id='postgres_2',
        sql='SELECT 1'
    )

    run_etl = PythonOperator(
        task_id='run_currency_conversion_etl',
        python_callable=run_currency_etl
    )

    [check_source, check_target] >> run_etl