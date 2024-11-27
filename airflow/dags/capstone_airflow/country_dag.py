from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator 

from capstone_airflow.function.data_to_database import load_to_database
from capstone_airflow.function.extract_columns import column_selections
from capstone_airflow.function.extract_currency import (currency_code_symbol,
                                                        currency_name)
from capstone_airflow.function.extract_languages import languages
from capstone_airflow.function.extract_load_to_s3 import (
    country_api_request, country_to_s3_parquet)
from capstone_airflow.function.joining import tables_joining
from capstone_airflow.function.load_from_s3 import read_s3_parquet
from capstone_airflow.function.transformation import table_transformation
from capstone_airflow.notification.email_notification import task_fail_alert

default_args = {
    'owner': 'adewunmi',
    'depends_on_past': False,
    'on_failure_callback': task_fail_alert,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id="capstone",
    start_date=datetime(2024, 11, 25),
    default_args=default_args,
    schedule_interval=None
) as dag:

    get_request = PythonOperator(
        task_id="get_request_api",
        python_callable=country_api_request
    )

    write_to_s3 = PythonOperator(
        task_id="object_datalake",
        python_callable=country_to_s3_parquet
    )

    read_from_s3 = PythonOperator(
        task_id="read_from_datalake",
        python_callable=read_s3_parquet
    )

    column_required = PythonOperator(
        task_id="selecting_columns",
        python_callable=column_selections
    )

    code_symbol = PythonOperator(
        task_id="currency_code_sym",
        python_callable=currency_code_symbol
    )

    naming_currency = PythonOperator(
        task_id="currency_name",
        python_callable=currency_name
    )

    language = PythonOperator(
        task_id="language",
        python_callable=languages
    )

    joining = PythonOperator(
        task_id="tables_joining",
        python_callable=tables_joining
    )

    transformation = PythonOperator(
        task_id="table_tansformation",
        python_callable=table_transformation
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database
    )

get_request >> write_to_s3 >> read_from_s3
read_from_s3 >> [column_required, code_symbol, naming_currency, language]
[column_required, code_symbol, naming_currency, language] >> joining
joining >> transformation >> load_to_postgres
