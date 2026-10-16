from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"

with DAG(
    dag_id="retail_spark_scala_etl",
    description="Run the retail batch ETL implemented in Scala and Apache Spark",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["spark", "scala", "retail"],
) as dag:
    run_retail_etl = BashOperator(
        task_id="run_retail_etl",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f'sbt "runMain com.retail.RetailBatchApp {PROJECT_DIR}"'
        ),
    )