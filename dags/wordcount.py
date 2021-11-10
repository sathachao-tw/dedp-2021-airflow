from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from uuid import uuid4


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('wordcount_dag', default_args=default_args) as dag:
    t2 = SparkSubmitOperator(
        task_id='count_words_spark_op',
        java_class="thoughtworks.wordcount.WordCount",
        application="/Users/sathachaojaroenrat/projects/sea-dedp-2021/transformations/target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar",
        application_args=["input/README.md", str(uuid4())]
    )

# spark_submit_wordcount_locally = f"""
#     spark-submit --class thoughtworks.wordcount.WordCount \\
#         --conf spark.sql.shuffle.partitions=1 \\
#         /Users/sathachaojaroenrat/projects/sea-dedp-2021/transformations/target/scala-2.12/tw-pipeline_2.12-0.1.0-SNAPSHOT.jar \\
#         input/README.md \\
#         {str(uuid4())}
# """


