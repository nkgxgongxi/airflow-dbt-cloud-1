from airflow import DAG
from datetime import datetime, timedelta
from astronomer.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

with DAG(
    dag_id="trigger_dbt_cloud_job",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["dbt", "cloud"],
) as dag:

    run_dbt_job = DbtCloudRunJobOperator(
        task_id="run_dbt_cloud_job",
        job_id=123456,  # <-- Replace with your dbt Cloud Job ID
        dbt_cloud_conn_id="dbt_cloud_default",  # <-- Airflow connection ID
        check_interval=10,  # polling interval
        timeout=600,  # timeout in seconds
        wait_for_termination=True,
    )
