from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import subprocess
import re


def run_spark(script_path, **context):
    result = subprocess.run(
        ["spark-submit", "--master", "local[*]", script_path],
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    print(output)

    match = re.search(r"Spark job status: (OK|KO)", output)
    status = match.group(1) if match else "UNKNOWN"
    print(f"==> Spark job status: {status}")

    if status == "KO":
        raise AirflowSkipException(f"Spark job returned KO")


with DAG(
    dag_id="test_remote_logging_spark",
    description="Test DAG - submit Spark app to verify remote logging to MinIO",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "spark", "logging"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    tasks = [
        ("spark_job_1", "/opt/airflow/apps/test_spark_logging.py"),
        ("spark_job_2", "/opt/airflow/apps/test_spark_logging.py"),
        ("spark_job_3", "/opt/airflow/apps/test_spark_logging.py"),
    ]

    prev = start
    for task_id, script in tasks:
        task = PythonOperator(
            task_id=task_id,
            python_callable=run_spark,
            op_kwargs={"script_path": script},
            trigger_rule="none_failed",
        )
        prev >> task
        prev = task

    prev >> end
