"""
### Example HTTP operator and sensor
"""
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
import json

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('example_http_operator', default_args=default_args)

dag.doc_md = __doc__

# t1, t2 and t3 are examples of tasks created by instatiating operators
t1 = SimpleHttpOperator(
    task_id='post_op',
    http_conn_id='WATCHDOG',
    endpoint='media/outlet_posts',
    data=json.dumps({"outlet_link": "karliekloss", "collect_posts": "false"}),
    headers={"Content-Type": "application/json"},
    response_check=lambda response: True if len(response.json()) == 0 else False,
    dag=dag)

t5 = SimpleHttpOperator(
    task_id='post_op_formenc',
    http_conn_id='WATCHDOG',
    endpoint='nodes/url',
    data="outlet_link=karliekloss&collect_posts=false",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    dag=dag)

#sensor = HttpSensor(
#    task_id='http_sensor_check',
#    conn_id='http_default',
#    endpoint='api/v1.0/apps',
#    params={},
#    headers={"Content-Type": "application/json"},
#    response_check=lambda response: True if "collation" in response.content else False,
#    poke_interval=5,
#    dag=dag)

#t1.set_upstream(sensor)
t5.set_upstream(t1)