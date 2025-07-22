from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from run_pipeline import run_pipeline
default_args = {
    "owner": "sahil",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="urjadrishti_dag",
    default_args=default_args,
    schedule_interval="0 7 */7 * *",  # every 7 days at 7 AM
    catchup=False,
    tags=["msedcl", "bq", "monthly"],
) as dag:

    run_pipeline_task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )