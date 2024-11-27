import logging
import pandas as pd

from capstone_airflow.function.extract_columns import column_selections
from capstone_airflow.function.extract_languages import languages
from capstone_airflow.function.extract_currency import (currency_code_symbol, currency_name)


def tables_joining():
    """
    This function joins multiple data tables (country information,
    currency data, and language data).
    It first selects columns, then joins the data from multiple sources
    and returns the combined table.
    """
    try:
        logging.info("Starting the table joining process.")

        # Fetch the individual tables
        country_column_select = column_selections()
        code_symbol = currency_code_symbol()
        name_currency = currency_name()
        language = languages()

        # Perform the joins
        logging.info("Joining tables.")

        country_currency = pd.merge(
            country_column_select, code_symbol, on="name.common",
            how="right"
        )
        country_currency_name = pd.merge(
            country_currency, name_currency, on="name.common", how="right"
        )
        country_language = pd.merge(
            country_currency_name, language, on="name.common", how="right"
        )

        logging.info("Tables joined successfully.")

        return country_language

    except Exception as e:
        raise Exception(f"Error during table joining: {str(e)}")
        # return None
