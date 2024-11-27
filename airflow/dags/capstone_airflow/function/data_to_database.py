import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from capstone_airflow.function.transformation import table_transformation


def load_to_database():
    """
    This function loads a DataFrame into a PostgreSQL database.
    It uses Airflow's PostgresHook to connect to the database
    and SQLAlchemy for data insertion.
    """
    try:
        logging.info("Starting the database load process.")
        data = table_transformation()
        # Ensure the DataFrame is valid
        if data is None or data.empty:
            logging.error("The DataFrame is empty or None.")
            return None, "Error: The DataFrame is empty or None."

        logging.info("DataFrame validation successful. \
                     Database connection next.")

        # Connect to PostgreSQL using Airflow's PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="postgres_id")
        engine = postgres_hook.get_sqlalchemy_engine()

        # Define table name
        table_name = "country_data"  # Replace with your target table name

        logging.info(f"Loading data into the table '{table_name}' \
                     in the database.")

        # Load the DataFrame into the PostgreSQL table
        data.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",  # Replace the table if it exists
            index=False,  # Do not include the DataFrame's index as a column
        )

        logging.info(
            f"Data successfully loaded into the \
                     table '{table_name}'."
        )
        return "Data loaded successfully."

    except Exception as e:
        raise Exception(f"Error during database load process: {str(e)}")
