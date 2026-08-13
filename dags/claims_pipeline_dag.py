"""
Healthcare Claims Pipeline
---------------------------
Orchestrates a dbt project that transforms raw claims data into a
provider-level claims mart, with a data-quality gate before the
mart is considered "ready."

Task flow:
    check_for_new_file
        -> dbt_seed        (loads raw CSV into the warehouse)
        -> dbt_run         (runs staging -> intermediate -> marts)
        -> dbt_test        (data quality gate)
        -> log_pipeline_complete
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

DBT_PROJECT_DIR = "/opt/airflow/dbt/claims_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt/claims_project"  # profiles.yml lives alongside the project in this setup

default_args = {
    "owner": "ramya",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def log_pipeline_complete(**context):
    ti = context["ti"]
    run_id = context["run_id"]
    print(f"[PIPELINE COMPLETE] run_id={run_id} finished successfully at {datetime.now()}")
    # In a real system this is where you'd push a metric to
    # Datadog/Prometheus, or write a row to an audit table.


with DAG(
    dag_id="healthcare_claims_pipeline",
    description="Orchestrates dbt seed -> run -> test for the claims mart",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "claims", "portfolio-project"],
) as dag:

    # Step 1: confirm the raw file is present before we do anything.
    # In this project it's a static seed, but this mirrors a real
    # pattern where you wait on an upstream drop (S3, SFTP, etc.)
    check_for_new_file = FileSensor(
        task_id="check_for_new_file",
        filepath=f"{DBT_PROJECT_DIR}/seeds/raw_claims.csv",
        poke_interval=10,
        timeout=60,
        mode="poke",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt seed --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    log_complete = PythonOperator(
        task_id="log_pipeline_complete",
        python_callable=log_pipeline_complete,
    )

    check_for_new_file >> dbt_seed >> dbt_run >> dbt_test >> log_complete