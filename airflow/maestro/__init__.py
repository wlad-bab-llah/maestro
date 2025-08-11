"""
MAESTRO — Metadata-Aware ETL Scheduling & Task Recovery Orchestrator
Version: 1.0.0
"""

__version__ = '1.0.0'
__title__ = 'MAESTRO'
__description__ = 'Metadata-Aware ETL Scheduling & Task Recovery Orchestrator'

from maestro.core.dag_generator import MaestroDAGGenerator
from maestro.core.config_loader import ConfigLoader
from maestro.core.task_factory import TaskFactory, EnhancedTaskFactory
from maestro.utils.logger import get_maestro_logger

__all__ = [
    'MaestroDAGGenerator',
    'ConfigLoader',
    'TaskFactory',
    'EnhancedTaskFactory',
    'get_maestro_logger'
]

def create_simple_dag(country: str, vertical: str, system_filter: str = None):
    """Fonction helper pour créer rapidement un DAG MAESTRO"""
    generator = MaestroDAGGenerator()
    return generator.generate_dag(country, vertical, system_filter)
