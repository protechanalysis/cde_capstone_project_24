import pandas as pd

from airflow.hooks.postgres_hook import PostgresHook

def create_postgres_table():
    pg_hook = PostgresHook(postgres_conn_id='post_s3')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS wiki_file (
        domain_name VARCHAR,
        page_title VARCHAR,
        view_count INTEGER,
        size NUMERIC,
        date DATE,
        hour INTEGER
    );
    """
    cursor.execute(create_table_query)
    pg_conn.commit()
    cursor.close()
    pg_conn.close()

    
def load_csv_to_database():
    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='post_s3')
    
    # Read CSV file into a pandas DataFrame
    csv_file_path = '/opt/airflow/dags/output.csv'
    df = pd.read_csv(csv_file_path, on_bad_lines='skip')

    # Get SQLAlchemy engine from PostgresHook
    engine = pg_hook.get_sqlalchemy_engine()

    # Load DataFrame to PostgreSQL table
    df.to_sql('wiki_file', engine, if_exists='append', index=False)

    

