from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG (
    dag_id = "my_first_dag",
    start_date=datetime(2023, 1, 17)
) as dag:
    task_1 = BashOperator(task_id="saludando", bash_command="echo 'hola mundo'")
