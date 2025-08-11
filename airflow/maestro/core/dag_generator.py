# maestro/core/dag_generator.py - VERSION CORRIGÉE
"""
MAESTRO DAG Generator - Version sans erreurs
"""
 
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from maestro.core.config_loader import ConfigLoader
from maestro.core.task_factory import TaskFactory
from maestro.utils.logger import get_maestro_logger
from maestro.core.exceptions import DAGGenerationError
 
logger = get_maestro_logger(__name__)
 
class MaestroDAGGenerator:
    """Core DAG generator for MAESTRO framework"""
    def __init__(self, config_loader: Optional[ConfigLoader] = None, task_factory: Optional[TaskFactory] = None):
        self.config_loader = config_loader or ConfigLoader()
        self.task_factory = task_factory or TaskFactory()
    def generate_dag(self, country: str, vertical: str, system_filter: Optional[str] = None, dag_params: Optional[Dict[str, Any]] = None) -> DAG:
        """Generate Airflow DAG from configuration"""
        try:
            logger.info(f"🚀 Generating DAG for {country}/{vertical}")
            config = self.config_loader.load_config(country, vertical)
            dag = self._create_dag_structure(config, system_filter, dag_params)
            tasks = self._generate_tasks(dag, config, system_filter)
            self._setup_dependencies(dag, tasks)
            logger.info(f"✅ DAG generated successfully: {dag.dag_id}")
            return dag
        except Exception as e:
            error_msg = f"Failed to generate DAG for {country}/{vertical}: {e}"
            logger.error(f"❌ {error_msg}")
            raise DAGGenerationError(error_msg) from e
    def _create_dag_structure(self, config: Dict[str, Any], system_filter: Optional[str], dag_params: Optional[Dict[str, Any]]) -> DAG:
        """Create basic DAG structure"""
        # Default DAG parameters
        default_params = {
            'owner': 'maestro-framework',
            'depends_on_past': False,
            'start_date': datetime(2025, 8, 8),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
        # Merge with custom parameters
        if dag_params:
            default_params.update(dag_params)
        # Generate DAG components
        dag_id = self._generate_dag_id(config, system_filter)
        description = self._generate_dag_description(config, system_filter)
        tags = self._generate_dag_tags(config, system_filter)
        # Create DAG
        dag = DAG(
            dag_id=dag_id,
            default_args=default_params,
            description=description,
            schedule_interval='@daily',
            catchup=False,
            max_active_runs=1,
            tags=tags
        )
        logger.info(f"🏗️ DAG structure created: {dag_id}")
        return dag
    def _generate_dag_id(self, config: Dict[str, Any], system_filter: Optional[str]) -> str:
        """Generate unique DAG ID"""
        base_id = f"chaine_quotidienne_{config['country'].lower()}"
        if system_filter:
            base_id += f"_{system_filter}"
        return base_id
    def _generate_dag_description(self, config: Dict[str, Any], system_filter: Optional[str]) -> str:
        """Generate DAG description"""
        desc = f"MAESTRO Pipeline pour {config['country']}"
        if system_filter:
            desc += f" (Système: {system_filter})"
        desc += " - Configuration externe"
        return desc
    def _generate_dag_tags(self, config: Dict[str, Any], system_filter: Optional[str]) -> List[str]:
        """Generate DAG tags"""
        tags = ['maestro', 'ingestion', config['country'].lower()]
        if system_filter:
            tags.append(system_filter)
        return tags
    def _generate_tasks(self, dag: DAG, config: Dict[str, Any], system_filter: Optional[str]) -> List[Any]:
        """Generate all tasks for the DAG"""
        tasks = []
        # Add start and end tasks
        start_task = DummyOperator(task_id='start_pipeline', dag=dag)
        end_task = DummyOperator(task_id='end_pipeline', dag=dag)
        tasks.extend([start_task, end_task])
        # Generate business tasks
        business_tasks = self._generate_business_tasks(dag, config, system_filter)
        tasks.extend(business_tasks)
        # Store references for dependency setup
        dag.start_task = start_task
        dag.end_task = end_task
        dag.business_tasks = business_tasks
        return tasks
    def _generate_business_tasks(self, dag: DAG, config: Dict[str, Any], system_filter: Optional[str]) -> List[Any]:
        """Generate business logic tasks"""
        business_tasks = []
        for vertical_name, vertical_config in config['verticals'].items():
            if vertical_config.get('disable', False):
                logger.info(f"🚫 Vertical {vertical_name} disabled, skipping")
                continue
            for phase_name, phase_config in vertical_config.get('phases', {}).items():
                if phase_config.get('disable', False):
                    logger.info(f"🚫 Phase {phase_name} disabled, skipping")
                    continue
                for system_name, system_config in phase_config.get('systeme_source', {}).items():
                    # Apply system filter if specified
                    if system_filter and system_name != system_filter:
                        continue
                    if system_config.get('disable', False):
                        logger.info(f"🚫 System {system_name} disabled, skipping")
                        continue
                    # Generate tasks for each flux in this system
                    system_tasks = self._generate_system_tasks(
                        dag, config, vertical_name, phase_name, system_name, system_config
                    )
                    business_tasks.extend(system_tasks)
        return business_tasks
    def _generate_system_tasks(self, dag: DAG, config: Dict[str, Any], vertical_name: str, phase_name: str, system_name: str, system_config: Dict[str, Any]) -> List[Any]:
        """Generate tasks for a specific system"""
        system_tasks = []
        for flux in system_config.get('flux', []):
            if flux.get('disable', False):
                logger.info(f"🚫 Flux {flux['name']} disabled, skipping")
                continue
            try:
                # Create task using factory
                task = self.task_factory.create_task(
                    dag=dag,
                    flux_name=flux['name'],
                    vertical=vertical_name,
                    phase=phase_name,
                    system=system_name,
                    config=config,
                    flux_config=flux
                )
                system_tasks.append(task)
                logger.info(f"✅ Task created: {task.task_id}")
            except Exception as e:
                logger.error(f"❌ Failed to create task for flux {flux['name']}: {e}")
                # Continue with other tasks instead of failing completely
                continue
        return system_tasks
    def _setup_dependencies(self, dag: DAG, tasks: List[Any]) -> None:
        """Setup task dependencies"""
        if not hasattr(dag, 'business_tasks') or not dag.business_tasks:
            # No business tasks, connect start to end directly
            dag.start_task >> dag.end_task
            logger.warning("⚠️ No business tasks found, empty pipeline")
            return
        # Setup dependencies: start -> all business tasks -> end
        dag.start_task >> dag.business_tasks >> dag.end_task
        logger.info(f"🔗 Dependencies configured: {len(dag.business_tasks)} business tasks")
    def generate_error_dag(self, error_message: str, dag_id: str = "maestro_error_dag", error_type: str = "configuration") -> DAG:
        """Generate error DAG when configuration fails"""
        default_args = {
            'owner': 'maestro-framework',
            'depends_on_past': False,
            'start_date': datetime(2025, 8, 8),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0
        }
        error_dag = DAG(
            dag_id=f"maestro_{error_type}_error",
            default_args=default_args,
            description=f"🚨 MAESTRO Error: {error_message}",
            schedule_interval=None,
            catchup=False,
            tags=['maestro', 'error', error_type, 'URGENT']
        )
        error_task = DummyOperator(
            task_id=f"{error_type}_error_requires_attention",
            dag=error_dag
        )
        logger.error(f"🚨 Error DAG generated: {error_dag.dag_id}")
        return error_dag