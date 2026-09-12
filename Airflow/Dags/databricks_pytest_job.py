from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)


with DAG(
    dag_id="databricks_pytest_job",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["databricks", "pytest"],
) as dag:

    run_pytest_job = DatabricksRunNowOperator(
        task_id="run_pytest_job",
        databricks_conn_id="databricks_default",
        job_id=284575767636795,
        wait_for_termination=True,
    )
