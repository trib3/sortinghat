from airflow import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import time
import requests

args = {
    'owner': 'airflow',
    'start_date': datetime(2015,06,24),
}

dag = DAG(dag_id='get_posts', schedule_interval=timedelta(days=1), default_args=args)

def my_sleeping_function(random_base):
    '''This is a function that will run within the DAG execution'''
    time.sleep(random_base)

def call_watchdog(ds, **kwargs):
    r = requests.get('http://localhost:8000/media/outlet_posts?outlet_link=karliekloss&collect_posts=false')
    if (r.status_code != 200):
        raise AirflowException("request failed: " + r.status_code)
    return r.content

def store_posts(ds, **kwargs):
    la = "bobo" # kwargs['task_instance'].xcom_pull(task_ids='call_watchdog_task')
    file = open('lala.txt', 'a')
    file.write(la)
    file.close()

    return la

def print_context(ds, **kwargs):
    print(ds)
    return 'Whatever you return gets printed in the logs'

call = PythonOperator(
    task_id='call_watchdog_task',
    provide_context=True,
    python_callable=call_watchdog,
    dag=dag)

store = PythonOperator(
    task_id='store_task',
    provide_context=True,
    python_callable=store_posts,
    dag=dag)

date = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

store.set_upstream(call)
date.set_upstream(call)

if __name__ == "__main__":
    dag.run(start_date=datetime(year=2015, month=1, day=1))