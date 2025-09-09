from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
import re
from airflow.models import Variable


class Functionnalities:

    def print_execution_date(**context):
        """Print the execution_date received from parent DAG"""
        # Get the execution_date from the DAG run configuration
        execution_date = context['dag_run'].conf.get('execution_date')
        message = context['dag_run'].conf.get('message')
        
        print("=" * 50)
        print("📅 EXECUTION DATE FROM FILE WATCHER")
        print("=" * 50)
        print(f"📅 Execution Date: {execution_date}")
        print(f"💬 Message: {message}")
        print(f"🔍 Full Configuration: {context['dag_run'].conf}")
        print("=" * 50)
        
        if execution_date:
            print(f"✅ Successfully received date: {execution_date}")
        else:
            print("❌ No execution_date found in configuration")

    def get_Xcom_Param(Param : str, **context):
        """Print the execution_date received from parent DAG"""
        # Get the execution_date from the DAG run configuration
        value = context['dag_run'].conf.get(Param)
        return value
    

    def get_Xcom_Params(params : list, **context) -> dict:
        """Print the execution_date received from parent DAG"""
        # Get the execution_date from the DAG run configuration
        values = dict()
        for item in params:
            values[item] = context['dag_run'].conf.get(item)
        print(values)
        return values
    @staticmethod
    # def extract_date_from_filename(system,filename):
    #     """Extract date from filename like delta_20240930.ctr"""
    #     #match = re.search(fr'{system}_(\d{8})\.ctr', filename)
    #     match = rf'{re.escape(system)}_(\d{{8}})\.ctr$'
    #     if match:
    #         date_str = match.group(1)
    #         return datetime.strptime(date_str, '%Y%m%d').strftime('%Y%m%d')
    #     return None
    def extract_date_from_filename(system: str, filename: str):
        """
        Extract YYYYMMDD from file names like:
          controls/delta_20240930.ctr
        Robust to prefixes, special chars in system, and extension case.
        """
        base = filename # strip any folder path
        # Build a raw f-string pattern, escape system, and anchor to end of name
        pattern = rf'{re.escape(system)}_(\d{{8}})\.ctr$'
        m = re.search(pattern, base, flags=re.IGNORECASE)
        if not m:
            return None
        date_str = m.group(1)
        # Validate it's a real date and normalize format
        try:
            return datetime.strptime(date_str, "%Y%m%d").strftime("%Y%m%d")
        except ValueError:
            return None
    @staticmethod
    def list_process_and_delete_files(MINIO_CONFIG,system,**context):
        BUCKET_NAME = MINIO_CONFIG.get("bucket_name", "data")
        ENDPOINT_URL = MINIO_CONFIG.get("endpoint_url", "http://minio:9000")
        ACCESS_KEY = MINIO_CONFIG.get("access_key", "admin")
        SECRET_KEY = MINIO_CONFIG.get("secret_key", "password")
        PATH_TO_DCO = MINIO_CONFIG.get("path_dco", "/opt/airflow/apps")
        s3 = boto3.client(
            's3',
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )
        with open(f"{PATH_TO_DCO}/dco.txt", "r") as f:
            dco_retrieved = f.read().strip()
        
        # Get the list of all files
        #response = s3.list_objects_v2(Bucket=BUCKET_NAME,Prefix="controls/")
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        #files = [obj['Key'] for obj in response.get('Contents', [])]
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].startswith(system)]

        
        # Get already processed files from XCom
        ti = context['ti']
        processed_files = ti.xcom_pull(task_ids='list_and_process_new_files', key='processed_files') or []
        new_files = list(set(files) - set(processed_files))
        
        if not new_files:
            print("✅ No new files to process.")
            raise AirflowSkipException("No files to process")
        



        # Extract execution_date from the first file (or modify logic as needed)
        execution_date = dco_retrieved
        for file_key in new_files:
            date_from_file = Functionnalities.extract_date_from_filename(system,file_key)
            if date_from_file:
                if not execution_date == date_from_file :
                    raise AirflowSkipException(f"⛔⛔⛔ Attention la date est erroné, la date attendu est {execution_date} ✅✅ , la date trouvé est : {date_from_file} ❌❌")
        
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