import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from twitch_info import run_twitch_info

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "catchup": False,
    "start_date": datetime.datetime(2021, 6, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


with DAG(
    "twitch_info_dag",
    default_args = default_args,
    description = "Relevant information from our Twitch API",
    schedule_interval = timedelta(days = 1)
) as dag:

    run_etl = PythonOperator(
        task_id = "twitch_info",
        python_callable = run_twitch_info,
        dag = dag
    )