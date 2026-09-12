from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime

with DAG(
    dag_id="dbt_cloud_demo",
    start_date=datetime(2026, 7, 9),
    schedule=None,
    catchup=False,
) as dag:

    run_dbt = DbtCloudRunJobOperator(
        task_id="run_dbt_job",
        dbt_cloud_conn_id="dbt_cloud_airflow",
        job_id=70506183139345,
        wait_for_termination=True,
        check_interval=30,
        timeout=3600,
    )
