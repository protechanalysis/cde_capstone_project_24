from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from function_csv.test import create_postgres_table, load_csv_to_database

# setting bash scripts path
wik1 = 'bash /opt/airflow/dags/bash_scripts/wiki_log_file.sh '
wik2 = 'bash /opt/airflow/dags/bash_scripts/csv_deletion.sh '

# Default arguments
default_args = {
    'owner': 'Adewunmi',
    'start_date': datetime(2024, 10, 16)
}

# Define DAG
with DAG(
    dag_id='wiki_pageviews_dag',
    default_args=default_args,
    description='csv to postgres'
    
) as dag:
    
    fetching_data = bash_task = BashOperator(
        task_id="fetching_data",
        bash_command= wik1
    )

    creating_postgres_table = PythonOperator(
        task_id='creating_postgres_table',
        python_callable=create_postgres_table,
    )

    loading_data_to_postgres = PythonOperator(
        task_id='loading_data_to_postgres',
        python_callable=load_csv_to_database
    )

    csv_data_deleting = bash_task = BashOperator(
        task_id="csv_data_deleting",
        bash_command= wik2
    )

    # Task dependencies
    
    fetching_data >> creating_postgres_table >> loading_data_to_postgres >> csv_data_deleting