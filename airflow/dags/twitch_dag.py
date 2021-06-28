import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from twitch_etl import run_twitch_etl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "catchup": False,
    "start_date": datetime.datetime(2021, 6, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


with DAG(
    "twitch_dag",
    default_args = default_args,
    description = "Top 30 Games and Top 100 Streams at the moment",
    schedule_interval = timedelta(hours= 1)
) as dag:

    run_etl = PythonOperator(
        task_id = "twitch_etl",
        python_callable = run_twitch_etl,
        dag = dag
    )