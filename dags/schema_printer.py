# https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import pandas as pd

column_names = ["firstname", "lastname", "address", "city", "state", "postcode"]
input_path = "/Users/sathachaojaroenrat/projects/sea-dedp-2021/airflow/downloads/addresses.csv"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
}

with DAG("csv_schema_printer", default_args=default_args, catchup=False) as dag:
    file_sensor = FileSensor(
        task_id="poll_file",
        filepath=input_path,
        poke_interval=10,
        timeout=30,
    )

    printer = PythonOperator(
        task_id="print_schema",
        python_callable=lambda: print(pd.read_csv(input_path, names=column_names).info())
    )

    file_sensor >> printer