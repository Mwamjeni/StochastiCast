from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": ["charlwiswa@gmail.com"],
}

dag = DAG(
    "stock_data_polygon_kafka_databricks",
    default_args=default_args,
    description='Market data ingestion pipeline from Polygon API to Databricks via Kafka',
    schedule_interval="0 3 * * *",  # runs daily at 3:00 AM UTC, which equals 6:00 AM in UTC+3
    catchup=False,
)

def run_script(script_name):
    """Execute external Python scripts."""
    subprocess.run(["python", f"C:/Users/BlvckMoon/OneDrive/Documents/GitHub/StochastiCast/src/{script_name}.py"], check=True)

fetch_task = PythonOperator(
    task_id="fetch_polygon_api_data",
    python_callable=lambda: run_script("kafka_producer"),
    dag=dag,
)

consume_task = PythonOperator(
    task_id="consume_kafka_topics_data_save_to_csv",
    python_callable=lambda: run_script("kafka_consumer"),
    dag=dag,
)
web_automated_ingestion_task = PythonOperator(
    task_id="kafka_output_csv_to_databricks_db",
    python_callable=lambda: run_script("kafka_csv_to_databricks_loader"),
    dag=dag,
)

fetch_task >> consume_task >> web_automated_ingestion_task