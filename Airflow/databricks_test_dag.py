from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook


def test_databricks_connection():
    hook = DatabricksHook(
        databricks_conn_id="databricks_default"
    )

    result = hook.test_connection()

    if not result[0]:
        raise Exception(f"Databricks connection failed: {result[1]}")

    print("Databricks connection successful!")
    print(result[1])


with DAG(
    dag_id="databricks_connection_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["databricks", "test"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_databricks_connection",
        python_callable=test_databricks_connection,
    )