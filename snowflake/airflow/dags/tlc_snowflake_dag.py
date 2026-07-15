"""
tlc_snowflake_dag.py
────────────────────
Full pipeline DAG. Runs inside a custom Docker image (Dockerfile.airflow)
with dbt Fusion 2.0.0-preview.190 (pinned in FUSION_VERSION.txt) and
Apache Airflow 2.9.3. The exact same Fusion version runs locally in
development and CI/CD.
"""

from __future__ import annotations
import os, sys, requests, logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# Import n8n alert callbacks
sys.path.insert(0, "/opt/airflow/scripts")
try:
    from n8n_alerts import on_dag_success, on_dag_failure
except ImportError:
    on_dag_success = None
    on_dag_failure = None

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/dbt"
N8N_WEBHOOK = os.environ.get("N8N_WEBHOOK_URL", "")
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _get_target_month(**context):
    logical_date = context["logical_date"]
    first = logical_date.replace(day=1)
    two_back = (first - timedelta(days=32)).replace(day=1)
    month = two_back.strftime("%Y-%m")
    context["ti"].xcom_push(key="target_month", value=month)
    return month


def _check_source(**context):
    month = context["ti"].xcom_pull(key="target_month", task_ids="get_target_month")
    url = f"{TLC_BASE}/yellow_tripdata_{month}.parquet"
    resp = requests.head(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    return "load_to_snowflake" if resp.status_code == 200 else "notify_failure"


def _load_to_snowflake(**context):
    month = context["ti"].xcom_pull(key="target_month", task_ids="get_target_month")
    sys.path.insert(0, "/opt/airflow/scripts")
    from load_to_snowflake import main
    main(month=month, load_zones_flag=False)


def _notify(status, **context):
    if not N8N_WEBHOOK:
        return
    month = context["ti"].xcom_pull(key="target_month", task_ids="get_target_month")
    try:
        requests.post(N8N_WEBHOOK, json={
            "status": status, "month": month,
            "dag_id": context["dag"].dag_id, "run_id": context["run_id"],
            "timestamp": datetime.utcnow().isoformat(),
        }, timeout=10)
    except Exception as e:
        log.warning(f"n8n notify failed: {e}")


with DAG(
    dag_id="tlc_snowflake_pipeline",
    default_args=DEFAULT_ARGS,
    description="NYC TLC -> Snowflake -> dbt monthly pipeline (version-locked)",
    schedule="0 6 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tlc", "snowflake", "dbt", "portfolio"],
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    get_target_month = PythonOperator(task_id="get_target_month", python_callable=_get_target_month)
    check_source = BranchPythonOperator(task_id="check_source", python_callable=_check_source)
    load_to_snowflake = PythonOperator(task_id="load_to_snowflake", python_callable=_load_to_snowflake)



    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir .",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --target prod",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir . --target prod",
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir . --target prod",
    )


    notify_success = PythonOperator(
        task_id="notify_success", python_callable=_notify,
        op_kwargs={"status": "success"}, trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    notify_failure = PythonOperator(
        task_id="notify_failure", python_callable=_notify,
        op_kwargs={"status": "failure"}, trigger_rule=TriggerRule.ONE_FAILED,
    )

    (
        get_target_month
        >> check_source
        >> load_to_snowflake
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> dbt_snapshot
        >> notify_success
    )
    [check_source, load_to_snowflake, dbt_deps, dbt_run, dbt_test] >> notify_failure
