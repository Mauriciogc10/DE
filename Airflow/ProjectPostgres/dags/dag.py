from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from operators.PostgresFileOperator import PostgresFileOperator
import datetime

DATE=str(datetime.date.today()).replace('-','')

with DAG(
    dag_id="tecnica _postgres", 
    start_date= datetime (2023,2,17) , #schedule_interval='® 0 * * **
) as dag:
    task_1 = PostgresOperator(
        task_id="Crear_tabla",
        postgres conn id = "postgres_locahost",
        sql="""create table if not exists tecnica_ml (
                id varchar (30), 
                site_id varchar (30), 
                title varchar (50), 
                price varchar (10), 
                sold_quantity varchar (100), 
                thumbnail varchar (50), 
                created_date varchar (8), 
                primary key (id, created_date))

               )
            """
    ),
    task_2= BashOperator (
        task_id = "Consulting_API" ,
        bash_command = "python ../../../consult_api.py"
    ),
    task_3 = PostgresFileOperator(
        task_id = "Insert_Data",
        operation="write",
        config={"table_name": "tecnica_ml"}
    ),
    task_4 = PostgresFileOperator(
        task_id = "Read_Data",
        operation="read",
        config={"query": "SELECT * from tecnica_ml WHERE sold_quantity != 'null' and created_date = {DATE} and cast(price as decimal) * cast (sold_quantity as int) > 7000000"}
    )

    task_1>>>task_2>>task_3>>task_4