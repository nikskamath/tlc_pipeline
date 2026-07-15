"""
tlc_databricks_dag.py
────────────────────
Full pipeline DAG. Runs inside the custom Docker image built from
Dockerfile.airflow, which has dbt-core==1.10.19 + dbt-databricks==1.10.19
pre-installed — the exact same versions as your local .venv.
"""

from __future__ import annotations
import os, sys, requests, logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

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
    return "load_to_databricks" if resp.status_code == 200 else "notify_failure"


def _load_to_databricks(**context):
    month = context["ti"].xcom_pull(key="target_month", task_ids="get_target_month")
    sys.path.insert(0, "/opt/airflow/scripts")
    from load_to_databricks import main
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
    dag_id="tlc_databricks_pipeline",
    default_args=DEFAULT_ARGS,
    description="NYC TLC -> Databricks -> dbt monthly pipeline (version-locked)",
    schedule="0 6 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tlc", "databricks", "dbt", "portfolio"],
) as dag:

    get_target_month = PythonOperator(task_id="get_target_month", python_callable=_get_target_month)
    check_source = BranchPythonOperator(task_id="check_source", python_callable=_check_source)
    load_to_databricks = PythonOperator(task_id="load_to_databricks", python_callable=_load_to_databricks)

    verify_versions = BashOperator(
        task_id="verify_versions",
        bash_command="python /opt/airflow/scripts/verify_versions.py",
    )

    # NOTE: Bronze/Silver notebooks must be triggered via Databricks Jobs API
    # or run manually in the Databricks UI before dbt_run executes, since they
    # require a Spark cluster that Airflow's container doesn't have.
    # For full automation, use the Databricks Airflow provider's
    # DatabricksSubmitRunOperator — see databricks_notebook_trigger.py

    run_bronze_silver = BashOperator(
        task_id="trigger_bronze_silver_notebooks",
        bash_command="python /opt/airflow/scripts/databricks_notebook_trigger.py",
    )

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
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir . --target prod",
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
        >> load_to_databricks
        >> verify_versions
        >> run_bronze_silver
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> dbt_docs
        >> notify_success
    )
    [check_source, load_to_databricks, verify_versions, run_bronze_silver, dbt_deps, dbt_run, dbt_test] >> notify_failure
