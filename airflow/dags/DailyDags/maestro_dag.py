# ============================================================================
# OPTION 1: CRÉATION ULTRA SIMPLE (1 ligne)
# ============================================================================
 
# my_simple_dag.py
"""
Création de DAG ultra simple avec MAESTRO
"""
 
import sys
sys.path.insert(0, '/opt/airflow')
from maestro import create_simple_dag


# maestro import create_simple_dag
# Créer ton DAG en UNE SEULE LIGNE !
dag = create_simple_dag(
    country="Maroc", 
    vertical="SAHAM_BANK", 
    system_filter="pwd"  # Optionnel: filtre pour système PWD uniquement,
)
