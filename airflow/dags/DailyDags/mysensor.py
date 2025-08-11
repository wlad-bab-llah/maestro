from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow')
from maestro.functionnality import functionnalities as fc

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# def print_execution_date(**context):
#     """Print the execution_date received from parent DAG"""
#     # Get the execution_date from the DAG run configuration
#     execution_date = context['dag_run'].conf.get('execution_date')
#     message = context['dag_run'].conf.get('message')
    
#     print("=" * 50)
#     print("📅 EXECUTION DATE FROM FILE WATCHER")
#     print("=" * 50)
#     print(f"📅 Execution Date: {execution_date}")
#     print(f"💬 Message: {message}")
#     print(f"🔍 Full Configuration: {context['dag_run'].conf}")
#     print("=" * 50)
    
#     if execution_date:
#         print(f"✅ Successfully received date: {execution_date}")
#     else:
#         print("❌ No execution_date found in configuration")

with DAG(
    dag_id='chaine_child',
    default_args=default_args,
    schedule_interval=None,  # This DAG is triggered by the parent
    catchup=False
) as dag:
    
    print_date_task = PythonOperator(
        task_id='print_execution_date_task',
        python_callable=fc.Functionnalities.get_Xcom_Params,
        op_kwargs={
            "params" : [
                "message",
                "execution_date"
            ]
        },
        provide_context=True
    )