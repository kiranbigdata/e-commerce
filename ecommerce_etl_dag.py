from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG default arguments
default_args = {
    'owner': 'kiranteja',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ecommerce_etl',
    default_args=default_args,
    description='ETL pipeline for e-commerce data',
    schedule_interval='@hourly',  # Runs every hour
)

# Define the ETL Task
run_etl = BashOperator(
    task_id='run_etl_script',
    bash_command='python3 /Users/kiranteja/ecommerce_etl.py',
    dag=dag,
)

# Set task execution order
run_etl
