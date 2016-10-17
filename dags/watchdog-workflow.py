from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.operators.sensors import S3PrefixSensor
from airflow.models import DAG
from datetime import datetime, timedelta
import logging

args = {
    'owner': 'airflow',
    'start_date': datetime(2015,06,24),
}

dag = DAG(dag_id='watchdog_dag', schedule_interval=timedelta(days=1), default_args=args)


#def call_watchdog(ds, **kwargs):
#    r = requests.get('http://localhost:8000/media/outlet_posts?outlet_link=karliekloss&collect_posts=false')
#    if (r.status_code != 200):
#        raise AirflowException("request failed: " + r.status_code)
#    return r.content

def watchdog_trigger_call(ds, **kwargs):
    logging.info("watchdog_trigger_call")

def fb_solr_load_call(ds, **kwargs):
    logging.info("fb_solr_load_call")

def fb_dynamo_media_outlet_load_call(ds, **kwargs):
    logging.info("fb_dynamo_media_outlet_load_call")

def fb_s3_sorting_hat_call(ds, **kwargs):
    logging.info("fb_s3_sorting_hat_call")

def fb_postgres_load_call(ds, **kwargs):
    logging.info("fb_postgres_load_call")

def fb_dynamo_tag_load_call(ds, **kwargs):
    logging.info("fb_dynamo_tag_load_call")

watchdog_trigger = PythonOperator(
    task_id='watchdog_trigger_task',
    provide_context=True,
    python_callable=watchdog_trigger_call,
    dag=dag)

fb_request = SimpleHttpOperator(
    task_id='facebook_retrieve_posts',
    http_conn_id='http_watchdog_conn_id',
    method='GET',
    endpoint='/media/outlet_posts?outlet_link=karliekloss&collect_posts=false',
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.json()['success'],
    dag=dag)

fb_s3_raw_sensor = S3PrefixSensor(
    task_id = "fb_s3_raw_sensor_task",
    s3_conn_id = "s3_conn_id",
    bucket_name = "watchdog_posts",
    prefix="raw/facebook/{{year}}/{{month}}/{{day}}",
    dag=dag)

fb_s3_normalize_posts = S3FileTransformOperator(
    task_id='fb_s3_normalize_posts_task',
    source_s3_conn_id = "s3_conn_id",
    source_s3_key="raw/facebook/{{year}}/{{month}}/{{day}}",
    dest_s3_conn_id = "s3_conn_id",
    dest_s3_key="normalized/facebook/{{year}}/{{month}}/{{day}}",
    transform_script="fb_normalize.py",
    dag=dag)

fb_s3_normalize_sensor = S3PrefixSensor(
    task_id = "fb_s3_normalize_sensor_task",
    s3_conn_id = "s3_conn_id",
    bucket_name = "watchdog_posts",
    prefix="normalized/facebook/{{year}}/{{month}}/{{day}}",
    dag=dag)

fb_postgres_load = PythonOperator(
    task_id='fb_postgres_load_task',
    provide_context=True,
    python_callable=fb_postgres_load_call,
    dag=dag)

fb_solr_load = PythonOperator(
    task_id='fb_solr_load_task',
    provide_context=True,
    python_callable=fb_solr_load_call,
    dag=dag)

fb_dynamo_media_outlet_load = PythonOperator(
    task_id='fb_dynamo_media_outlet_load_task',
    provide_context=True,
    python_callable=fb_dynamo_media_outlet_load_call,
    dag=dag)

fb_s3_sorting_hat = PythonOperator(
    task_id='fb_s3_sorting_hat_task',
    provide_context=True,
    python_callable=fb_s3_sorting_hat_call,
    dag=dag)

fb_dynamo_tag_load = PythonOperator(
    task_id='fb_dynamo_tag_load_task',
    provide_context=True,
    python_callable=fb_dynamo_tag_load_call,
    dag=dag)

fb_request.set_upstream(watchdog_trigger)
fb_s3_raw_sensor.set_upstream(fb_request)
fb_s3_normalize_posts.set_upstream(fb_s3_raw_sensor)
fb_s3_normalize_sensor.set_upstream(fb_s3_normalize_posts)

fb_solr_load.set_upstream(fb_s3_normalize_sensor)
fb_dynamo_media_outlet_load.set_upstream(fb_s3_normalize_sensor)
fb_s3_sorting_hat.set_upstream(fb_s3_normalize_sensor)
fb_postgres_load.set_upstream(fb_s3_normalize_sensor)

fb_dynamo_tag_load.set_upstream(fb_s3_sorting_hat)


if __name__ == "__main__":
    dag.run(start_date=datetime(year=2015, month=1, day=1))