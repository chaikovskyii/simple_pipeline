from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from services.generate_orders import generate_and_insert_orders


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'orders-generator',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    start_date=days_ago(0),
    catchup=False,
    max_active_runs=1,
    tags=['orders', 'data-generation']
) as dag:

    generate_orders_task = PythonOperator(
        task_id='generate_and_insert_orders',
        python_callable=generate_and_insert_orders
    )


generate_orders_task