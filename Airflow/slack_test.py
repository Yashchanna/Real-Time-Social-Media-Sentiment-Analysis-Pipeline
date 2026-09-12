from datetime import datetime

from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


with DAG(
    dag_id="slack_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["slack", "test"],
) as dag:

    send_slack = SlackWebhookOperator(
        task_id="send_slack",
        slack_webhook_conn_id="slack_alert_airflow",
        message="✅ Airflow → Slack connection is working!",
    )