from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)


default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="social_media_adf_ingestion",
    description="Trigger Azure Data Factory ingestion pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ADF", "Azure", "ADLS", "Social-Media"],
) as dag:

    trigger_adf = AzureDataFactoryRunPipelineOperator(
        task_id="trigger_adf_pipeline",
        azure_data_factory_conn_id="Azure_ADF_default",
        resource_group_name="rg-real-time-sentiment-project",
        factory_name="adf-real-time-sentiment-project",
        pipeline_name="social_media_ingestion",
        wait_for_termination=True,
    )
