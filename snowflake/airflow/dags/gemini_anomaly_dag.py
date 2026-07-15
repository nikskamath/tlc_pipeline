"""
Airflow DAG to run the Gemini anomaly explanation script.
This DAG uses a simple BashOperator to execute the script in the container's Python
runtime so it inherits the same env vars (including GEMINI_API_KEY) and DB drivers.
"""
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Import n8n alert callbacks
sys.path.insert(0, "/opt/airflow/scripts")
try:
    from n8n_alerts import on_dag_success, on_dag_failure
except ImportError:
    on_dag_success = None
    on_dag_failure = None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='gemini_anomaly_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['ai', 'anomaly'],
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    run_gemini = BashOperator(
        task_id='run_gemini_anomaly_script',
        bash_command='python /opt/airflow/scripts/gemini_anomaly.py'
    )

    run_gemini
