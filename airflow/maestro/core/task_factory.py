"""MAESTRO Task Factory"""

from typing import Dict, Any, Optional
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from maestro.utils.logger import get_maestro_logger
from maestro.core.exceptions import TaskCreationError

logger = get_maestro_logger(__name__)

class TaskFactory:
    """Factory for creating different types of Airflow tasks"""
   
    def __init__(self):
        self.task_creators = {
            'spark': self._create_spark_task,
            'bash': self._create_bash_task,
            'default': self._create_spark_task
        }
   
    def create_task(self, dag: DAG, flux_name: str, vertical: str, phase: str, system: str, config: Dict[str, Any], flux_config: Optional[Dict[str, Any]] = None, task_type: str = 'spark') -> Any:
        """Create task based on type and configuration"""
        try:
            creator = self.task_creators.get(task_type, self.task_creators['default'])
            task = creator(dag=dag, flux_name=flux_name, vertical=vertical, phase=phase, system=system, config=config, flux_config=flux_config or {})
            logger.debug(f"✅ Created {task_type} task: {task.task_id}")
            return task
        except Exception as e:
            error_msg = f"Failed to create {task_type} task for flux {flux_name}: {e}"
            logger.error(f"❌ {error_msg}")
            raise TaskCreationError(error_msg) from e
   
    def _create_spark_task(self, dag: DAG, flux_name: str, vertical: str, phase: str, system: str, config: Dict[str, Any], flux_config: Dict[str, Any]) -> SparkSubmitOperator:
        task_id = self._generate_task_id(vertical, system, flux_name)
        spark_config = self._build_spark_config(flux_name, vertical, phase, system, config, flux_config)
        return SparkSubmitOperator(task_id=task_id, dag=dag, **spark_config)
   
    def _create_bash_task(self, dag: DAG, flux_name: str, vertical: str, phase: str, system: str, config: Dict[str, Any], flux_config: Dict[str, Any]) -> BashOperator:
        task_id = self._generate_task_id(vertical, system, flux_name)
        bash_command = self._build_bash_spark_command(flux_name, vertical, phase, system, config, flux_config)
        return BashOperator(task_id=task_id, dag=dag, bash_command=bash_command)
   
    def _generate_task_id(self, vertical: str, system: str, flux_name: str) -> str:
        return f"{vertical.lower()}_{system}_{flux_name.lower()}"
   
    #def _build_spark_config(self, flux_name: str, vertical: str, phase: str, system: str, config: Dict[str, Any], flux_config: Dict[str, Any]) -> Dict[str, Any]:
    def _build_spark_config(self, flux_name: str, vertical: str, phase: str, system: str, config: Dict[str, Any], flux_config: Dict[str, Any]) -> Dict[str, Any]:
        default_config = {
            'application': f'/opt/airflow/apps/ingestion/src/Main_Ingestion_Manager.py',
            'name': f'{flux_name} Processing Job',
            'conn_id': 'spark_default',
            'verbose': True,
            'conf': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.executor.memory': '2g',
                'spark.driver.memory': '1g',
                'spark.executor.cores': '2',
                'spark.executor.instances': '2'
            },
            'application_args': [
                '--flux_name', flux_name, '--country', config['country'],
                '--vertical', vertical,'--system', system
            ],
            'java_class': None, 'packages': None, 'exclude_packages': None,
            'repositories': None, 'total_executor_cores': None, 'executor_cores': None,
            'executor_memory': None, 'driver_memory': None, 'keytab': None,
            'principal': None, 'status_poll_interval': 1, 'spark_binary': 'spark-submit'
        }
       
        # if 'spark_config' in flux_config:
        #     spark_overrides = flux_config['spark_config']
        #     if 'conf' in spark_overrides:
        #         default_config['conf'].update(spark_overrides['conf'])
        #         del spark_overrides['conf']
        #     default_config.update(spark_overrides)
       
        return default_config
   
    def _build_bash_spark_command(self, flux_name: str, vertical: str, phase: str, system: str, config: Dict[str, Any], flux_config: Dict[str, Any]) -> str:
        cmd_parts = [
            "spark-submit", f"--name '{flux_name} Processing Job'",
            "--conf spark.sql.adaptive.enabled=true",
            "--conf spark.sql.adaptive.coalescePartitions.enabled=true",
            "--executor-memory 2g", "--driver-memory 1g", "--executor-cores 2", "--num-executors 2"
        ]
       
        if 'spark_config' in flux_config and 'conf' in flux_config['spark_config']:
            for key, value in flux_config['spark_config']['conf'].items():
                cmd_parts.append(f"--conf {key}={value}")
       
        cmd_parts.extend([
            f"/path/to/spark/jobs/{flux_name.lower()}_job.py",
            f"--flux_name {flux_name}", f"--country {config['country']}",
            f"--vertical {vertical}", f"--phase {phase}", f"--system {system}",
            "--execution_date {{ ds }}", "--dag_run_id {{ dag_run.run_id }}"
        ])
       
        return " \\\n    ".join(cmd_parts)
   
    def register_task_creator(self, task_type: str, creator_func):
        self.task_creators[task_type] = creator_func
        logger.info(f"📝 Registered custom task creator: {task_type}")

class EnhancedTaskFactory(TaskFactory):
    """Enhanced task factory with additional task types"""
   
    def __init__(self):
        super().__init__()
        self.task_creators.update({
            'spark_sql': self._create_spark_sql_task,
            'data_quality': self._create_data_quality_task,
            'notification': self._create_notification_task
        })
   
    def _create_spark_sql_task(self, dag: DAG, flux_name: str, vertical: str, phase: str, system: str, config: Dict[str, Any], flux_config: Dict[str, Any]) -> SparkSubmitOperator:
        task_id = self._generate_task_id(vertical, system, flux_name)
        spark_config = self._build_spark_config(flux_name, vertical, phase, system, config, flux_config)
        spark_config['conf'].update({
            'spark.sql.adaptive.advisoryPartitionSizeInBytes': '128MB',
            'spark.sql.adaptive.skewJoin.enabled': 'true'
        })
        spark_config['application_args'].extend(['--job_type', 'sql', '--sql_file', f'/path/to/sql/{flux_name.lower()}.sql'])
        return SparkSubmitOperator(task_id=task_id, dag=dag, **spark_config)
   
    def _create_data_quality_task(self, dag: DAG, flux_name: str, vertical: str, phase: str, system: str, config: Dict[str, Any], flux_config: Dict[str, Any]) -> SparkSubmitOperator:
        task_id = f"dq_{self._generate_task_id(vertical, system, flux_name)}"
        spark_config = self._build_spark_config(flux_name, vertical, phase, system, config, flux_config)
        spark_config['application'] = '/path/to/spark/jobs/data_quality_checker.py'
        spark_config['name'] = f'{flux_name} Data Quality Check'
        spark_config['application_args'].extend(['--job_type', 'data_quality', '--rules_file', f'/path/to/dq_rules/{flux_name.lower()}_rules.json'])
        return SparkSubmitOperator(task_id=task_id, dag=dag, **spark_config)
   
    def _create_notification_task(self, dag: DAG, flux_name: str, vertical: str, phase: str, system: str, config: Dict[str, Any], flux_config: Dict[str, Any]) -> BashOperator:
        task_id = f"notify_{self._generate_task_id(vertical, system, flux_name)}"
        notification_config = flux_config.get('notification', {})
        bash_command = f"echo 'Notification for {flux_name} completed'"
       
        if notification_config.get('type') == 'email':
            recipients = notification_config.get('recipients', ['admin@company.com'])
            bash_command = f'echo "Processing completed for {flux_name}" | mail -s "MAESTRO: {flux_name} Complete" {" ".join(recipients)}'
       
        return BashOperator(task_id=task_id, dag=dag, bash_command=bash_command)
