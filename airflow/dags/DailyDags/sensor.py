from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
import re

BUCKET_NAME = 'data'
ENDPOINT_URL = 'http://minio:9000'
ACCESS_KEY = 'admin'
SECRET_KEY = 'password'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

def extract_date_from_filename(filename):
    """Extract date from filename like delta_20240930.ctr"""
    match = re.search(r'delta_(\d{8})\.ctr', filename)
    if match:
        date_str = match.group(1)
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y%m%d')
    return None

def list_process_and_delete_files(**context):
    s3 = boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    # Get the list of all files
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    files = [obj['Key'] for obj in response.get('Contents', [])]
    
    # Get already processed files from XCom
    ti = context['ti']
    processed_files = ti.xcom_pull(task_ids='list_and_process_new_files', key='processed_files') or []
    new_files = list(set(files) - set(processed_files))
    
    if not new_files:
        print("✅ No new files to process.")
        raise AirflowSkipException("No files to process")
    
    # Extract execution_date from the first file (or modify logic as needed)
    execution_date = None
    for file_key in new_files:
        date_from_file = extract_date_from_filename(file_key)
        if date_from_file:
            execution_date = date_from_file
            break  # Use the first valid date found
    
    for file_key in new_files:
        print(f"🎯 Processing file: {file_key}")
        # Your processing logic here
        
        # Delete file after processing
        try:
            s3.delete_object(Bucket=BUCKET_NAME, Key=file_key)
            print(f"🗑️ Deleted file: {file_key}")
        except Exception as e:
            print(f"❌ Failed to delete {file_key}: {e}")
    
    # Save updated processed list and execution_date
    ti.xcom_push(key='processed_files', value=processed_files + new_files)
    ti.xcom_push(key='execution_date', value=execution_date)
    
    print(f"📅 Extracted execution_date: {execution_date}")

with DAG(
    dag_id='continuous_minio_watcher_with_delete',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    
    watch_process_delete = PythonOperator(
        task_id='list_and_process_new_files',
        python_callable=list_process_and_delete_files,
        provide_context=True
    )
    
    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="chaine_child",
        conf={
            "message": "Hello from parent",
            "execution_date": "{{ ti.xcom_pull(task_ids='list_and_process_new_files', key='execution_date') }}"
        },
        wait_for_completion=True,
        reset_dag_run=True,
        trigger_rule='none_failed_min_one_success'
    )
    
    watch_process_delete >> trigger_child