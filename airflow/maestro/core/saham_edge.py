from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG simple pour DCO à 18h
dag = DAG(
    dag_id='daily_dco_simple',
    description='Lance dco.sh chaque jour à 18h',
    schedule_interval='0 18 * * *',  # 18h00 tous les jours
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dco', 'daily']
)

# Tâche unique - Exécuter dco.sh
run_dco = BashOperator(
    task_id='run_dco_script',
    bash_command='bash /opt/airflow/apps/dco.sh',
    dag=dag
)
