from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_remote_logging_spark",
    description="Test DAG - submit Spark app via BashOperator to verify remote logging to MinIO",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "spark", "logging"],
) as dag:

    submit_spark = BashOperator(
        task_id="spark_submit_show_databases",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "/opt/airflow/apps/test_spark_logging.py"
        ),
    )
