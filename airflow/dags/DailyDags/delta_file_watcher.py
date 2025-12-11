from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
import re
from airflow.models import Variable
import sys
sys.path.insert(0, '/opt/airflow')
from maestro.functionnality import functionnalities as fc


MINIO_CONFIG = Variable.get("MINIO_SECRET_CONFIG", deserialize_json=True)
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 9),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='delta_file_watcher',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    
    watch_process_delete = PythonOperator(
        task_id='list_and_process_new_files',
        python_callable=fc.Functionnalities.list_process_and_delete_files,
        op_kwargs={"MINIO_CONFIG":MINIO_CONFIG,"system":'delta'},
        provide_context=True
    )
    
    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="chaine_quotidienne_maroc_pwd",
        conf={
            "message": "Hello from parent",
            "execution_date": "{{ ti.xcom_pull(task_ids='list_and_process_new_files', key='execution_date') }}"
        },
        wait_for_completion=True,
        reset_dag_run=True,
        trigger_rule='none_failed_min_one_success'
    )
    
    watch_process_delete >> trigger_child