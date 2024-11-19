from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {'owner': 'adewunmi',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }


with DAG(
    dag_id="create_country_table",
    start_date=datetime(2024, 11, 18),
    default_args = default_args,
    schedule_interval=None
) as dag:


    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="rds_connect",
        sql="""
        CREATE TABLE country_data (
            country_name VARCHAR(255) not null,
            independence BOOLEAN,
            united_nation_members BOOLEAN,
            start_of_week VARCHAR(50),
            official_country_name VARCHAR(255),
            common_native_name VARCHAR(255),
            currency_code TEXT,
            currency_name TEXT,
            currency_symbol TEXT,
            country_code TEXT,
            capital VARCHAR(100),
            region VARCHAR(100),
            subregion VARCHAR(100),
            languages TEXT,
            area FLOAT,
            population BIGINT,
            continents VARCHAR(100),
            PRIMARY KEY (country_name)
        );
        """
    )

create_postgres_table