from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from function.capstone_function import (column_selections, country_api_request,
                                        country_to_s3_parquet,
                                        extract_currency_code_symbol,
                                        extract_currency_name,
                                        extract_languages, load_to_database,
                                        read_s3_parquet, renaming_column,
                                        table_transformation, tables_joining
                                        )

default_args = {
    'owner': 'adewunmi',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='capstone',
    start_date=datetime(2024, 11, 17),
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
        task_id="filter_column",
        python_callable=column_selections
    )

    code_symbol = PythonOperator(
        task_id="currency_code_sym",
        python_callable=extract_currency_code_symbol
    )

    currency_name = PythonOperator(
        task_id="currency_name",
        python_callable=extract_currency_name
    )

    languages = PythonOperator(
        task_id="language",
        python_callable=extract_languages
    )

    together = PythonOperator(
        task_id="tables_joining",
        python_callable=tables_joining
    )

    transformation = PythonOperator(
        task_id="table_tansformation",
        python_callable=table_transformation
    )

    renaming = PythonOperator(
        task_id="renaming_columns",
        python_callable=renaming_column
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database
    )

get_request >> write_to_s3 >> read_from_s3
read_from_s3 >> [column_required, code_symbol, currency_name, languages]
[column_required, code_symbol, currency_name, languages] >> together
together >> transformation >> renaming >> load_to_postgres
