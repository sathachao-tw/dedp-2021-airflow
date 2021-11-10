# https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

output_path = "/Users/sathachaojaroenrat/projects/sea-dedp-2021/airflow/downloads/addresses.csv"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
}

with DAG("csv_download", default_args=default_args, catchup=False) as dag:
    download = BashOperator(
        task_id="download_csv",
        bash_command=f"curl https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv -o {output_path}"
    )

    trigger = TriggerDagRunOperator(
        task_id="printer_trigger",
        trigger_dag_id="csv_schema_printer"
    )

    download >> trigger
