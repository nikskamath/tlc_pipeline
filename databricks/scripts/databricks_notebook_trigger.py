"""
databricks_notebook_trigger.py
────────────────────
Triggers the Bronze and Silver notebooks via the Databricks Jobs API
from inside Airflow. Requires the notebooks to already be uploaded to
your Databricks workspace at the paths below.
"""

import os
import time
import logging
import requests
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

for d in [Path(__file__).parent, Path(__file__).parent.parent]:
    if (d / ".env").exists():
        load_dotenv(d / ".env")
        break

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# Update these to match where you've uploaded the notebooks in your workspace
NOTEBOOK_PATHS = [
    "/Workspace/tlc_pipeline/01_bronze",
    "/Workspace/tlc_pipeline/02_silver",
]
CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID")  # set in .env


def run_notebook(path):
    log.info(f"Submitting notebook run: {path}")
    resp = requests.post(
        f"{HOST}/api/2.1/jobs/runs/submit",
        headers=HEADERS,
        json={
            "run_name": f"tlc-pipeline-{path.split('/')[-1]}",
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {"notebook_path": path},
        },
    )
    resp.raise_for_status()
    run_id = resp.json()["run_id"]
    log.info(f"Run submitted: run_id={run_id}")

    # Poll until complete
    while True:
        status = requests.get(
            f"{HOST}/api/2.1/jobs/runs/get",
            headers=HEADERS,
            params={"run_id": run_id},
        ).json()
        life_cycle = status["state"]["life_cycle_state"]
        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result_state = status["state"].get("result_state", "UNKNOWN")
            log.info(f"Run {run_id} finished: {life_cycle} / {result_state}")
            if result_state != "SUCCESS":
                raise RuntimeError(f"Notebook {path} failed: {status}")
            return
        log.info(f"Run {run_id} still {life_cycle}... waiting")
        time.sleep(15)


def main():
    for path in NOTEBOOK_PATHS:
        run_notebook(path)
    log.info("Bronze and Silver notebooks completed successfully.")


if __name__ == "__main__":
    main()
