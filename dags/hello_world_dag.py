import logging
from time import sleep
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from logging import logging

def helloWorld():
    """ Print a Hello World for studies propourses """
    logging.info("Into the function")
    logging.info("Sleep call initiating...")
    sleep(5)
    logging.info("Sleep call ended...")
    logging.info("Printing Hello World...")
    print('Hello World')


with DAG(dag_id="hello_world_dag",
         start_date=datetime(2022,1,1),
         schedule_interval="@hourly",
         default_args={'owner': 'owner1'},
         catchup=False) as dag:
        
        # dummy_1 -> python_hello_world_task -> dummy_2 -> bash_hello_world_task
        dummy_1 = DummyOperator(task_id="dummy_operator_1")

        python_hello_world_task = PythonOperator(
            task_id="python_hello_world",
            python_callable=helloWorld)

        dummy_2 = DummyOperator(task_id="dummy_operator_2")

        bash_hello_world_task = BashOperator(
            task_id="bash_hello_world",
            bash_command='echo "bash_hello_world"')            
        

dummy_1 >> python_hello_world_task >> dummy_2 >> bash_hello_world_task
