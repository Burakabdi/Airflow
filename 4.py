from airflow import DAG 
from datetime import timedelta
from airflow.utils.dates import days_ago
import time 
from airflow.operators.postgres_operator import PostgresOperator 
import json 


default_dag_args = { 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
    'project_id': 1 
}


table_creation = """
DROP TABLE IF EXIST public.employee
CREATE TABLE public.employee AS (id INT NOT NULL, name VARCHAR(250) , age INT);
"""

insert_data = """
INSERT INTO public.employee (id, name ,age) VALUES (1 , 'Stewie' , 3), (2, 'Peter' , 41), (3, 'Mag', 35);
"""


calculation_avg ="""
SELECT AVG(age) 
FROM public.employee;
"""


with DAG("postgress_dag_connection" , default_args = default_dag_args, schedule_interval= None, start_date= days_ago(3)) as dag_python:
    create_table = PostgresOperator(task_id = 'table_creation', sql = table_creation, postgress_conn_id = 'burak_postgres_local')
    insert_data = PostgresOperator(task_id = 'insertion_of_data', sql = insert_data, postgress_conn_id = 'burak_postgres_local')
    group_data = PostgresOperator(task_id = 'calculation_avg', sql = calculation_avg, postgress_conn_id = 'burak_postgres_local')

    

create_table >> insert_data >> group_data 