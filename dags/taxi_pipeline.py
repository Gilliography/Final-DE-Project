from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def aggregate_data():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS taxi_summary AS
        SELECT location, COUNT(*) as total_rides, AVG(fare) as avg_fare
        FROM taxi_rides
        GROUP BY location;
    """)

    conn.commit()
    conn.close()

with DAG(
    dag_id="taxi_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    task_aggregate = PythonOperator(
        task_id="aggregate_data",
        python_callable=aggregate_data
    )

run_etl