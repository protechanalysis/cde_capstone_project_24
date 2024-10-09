import os
import pandas as pd
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from school.include.function import load_arts, load_business, load_science


def load_csv(**kwargs):
    csv_dir = '/opt/airflow/dags/school/data/'  # Directory containing CSV files
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
    
    # Dictionary to store all CSV DataFrames
    csv_data_dict = {}
    
    for csv_file in csv_files:
        # Read CSV file into a DataFrame
        df = pd.read_csv(os.path.join(csv_dir, csv_file))
        
        # Store DataFrame in the dictionary with the file name as the key
        csv_data_dict[csv_file] = df.to_dict()
    
    # Push the dictionary containing all CSV DataFrames to XCom
    kwargs['ti'].xcom_push(key='csv_data_dict', value=csv_data_dict)

# Function to load DataFrames from XCom and upload them to S3
def load_to_s3(**kwargs):
    # Initialize S3 hook with connection ID
    s3 = S3Hook(aws_conn_id='simple_store')
    
    # Define S3 bucket name
    bucket_name = 'my-first-terraform-bucket-jeda'

    # Pull data from XCom
    ti = kwargs['ti']
    csv_data_dict = ti.xcom_pull(key='csv_data_dict', task_ids='load_csv')

    # Iterate over the dictionary and upload each DataFrame to S3
    for csv_file, csv_data in csv_data_dict.items():
        # Convert data back to DataFrame
        df = pd.DataFrame.from_dict(csv_data)
        
        # Save DataFrame to CSV in memory
        csv_buffer = df.to_csv(index=False)
        
        # Define S3 key (file name) for each CSV
        s3_key = csv_file
        
        # Upload the CSV to S3
        s3.load_string(
            string_data=csv_buffer,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )

# Define default arguments for the DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 7)
}

# Define the DAG
with DAG(
    dag_id='student_result',
    default_args=default_args,
    description='Load multiple CSV files to S3',
    catchup=False
) as dag:

    # Task 1: Load multiple CSV files and push to XCom
    loading_assessment = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv,
        provide_context=True
    )

    # Task 2: Load data from XCom and upload to S3
    loading_to_s3 = PythonOperator(
        task_id='load_to_s3',
        python_callable=load_to_s3,
        provide_context=True
    )

    # Task 3: Load data from S3 into Snowflake
    loading_arts_assessment = SnowflakeOperator(
        task_id='arts_assessment',
        sql=load_arts(),
        snowflake_conn_id='secondary'  
    )


    # Task 4: Load science data from S3 into Snowflake
    loading_science_assessment = SnowflakeOperator(
        task_id='science_assessment',
        sql=load_science(),
        snowflake_conn_id='secondary'  
    )

    # Task 5: Load business from S3 into Snowflake
    loading_business_assessment = SnowflakeOperator(
        task_id='business_assessment',
        sql=load_business(),
        snowflake_conn_id='secondary'
    )

    # Task dependencies
    loading_assessment >> loading_to_s3 
    loading_to_s3 >> [loading_arts_assessment,loading_science_assessment,loading_business_assessment]
