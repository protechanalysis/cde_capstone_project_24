import logging

import pandas as pd
from capstone_airflow.function.load_from_s3 import read_s3_parquet


def currency_code_symbol():
    """
    This function extracts the currency code,
    and symbol from the country_read function.
    """
    try:
        logging.info("Starting currency extraction process.")

        # Read data from S3
        country_read = read_s3_parquet()

        # Check if the data is empty or None
        if country_read is None or country_read.empty:
            logging.error("No data available to process.")
            return None, "Error: No data available to process."

        logging.info("Data loaded successfully from S3.")

        # Check if the necessary column 'name.common' is present in the data
        if "name.common" not in country_read.columns:
            logging.error("'name.common' column not found in data.")
            return None, "Error: 'name.common' column not found in data."

        # Filter and join currency symbol columns with country name
        logging.info(
            "Filtering currency symbol columns and joining with country names."
        )
        symbol = country_read.filter(items=["name.common"]).join(
            country_read.filter(regex="^currencies.*symbol$")
        )

        # Melt the data to get currency code and symbol
        logging.info("Melting the data to extract currency codes and symbols.")
        symbol_code = symbol.melt(
            id_vars=["name.common"],
            var_name="currency_code",
            value_name="currency_symbol",
        )

        # Remove rows with null values in the 'currency_symbol' column
        logging.info("Removing rows with null currency symbols.")
        syms = symbol_code[
            symbol_code["currency_symbol"].apply(lambda x: pd.notna(x))
        ].copy()

        # Clean up currency codes by splitting
        logging.info("Cleaning up currency codes.")
        syms["currency_code"] = syms["currency_code"].str.split(".").str[1]

        # Group by country name and aggregate the symbols and codes
        logging.info("Grouping by country & aggregating by symbols and codes.")
        sym_combine = (
            syms.groupby("name.common")
            .agg(
                {
                    "currency_symbol": lambda x: ", ".join(x),
                    "currency_code": lambda x: ", ".join(x),
                }
            )
            .reset_index()
        )

        logging.info("Currency extraction completed successfully.")
        return sym_combine

    except Exception as e:
        raise Exception(f"Error during currency extraction: {str(e)}")


def currency_name():
    """
    This function extracts the currency name from the country_read function.
    It reads data from S3, filters the relevant currency columns,
    processes them, and returns the results.
    """
    try:
        logging.info("Starting currency name extraction process.")

        # Read data from S3
        country_read = read_s3_parquet()

        # Ensure data is valid
        if country_read is None or country_read.empty:
            logging.error("No data available to process.")
            return None, "Error: No data available to process."

        logging.info("Data loaded successfully from S3.")

        # Filter the relevant columns for country names and currency names
        logging.info("Filtering country names and currency name columns.")
        cur_nam = country_read.filter(items=["name.common"]).join(
            country_read.filter(regex="^currencies.*name$")
        )

        # Melt the data to extract currency names
        logging.info("Melting data to extract currency names.")
        currencies_name = cur_nam.melt(
            id_vars=["name.common"], value_name="currency_name"
        )

        # Remove rows with null values in 'currency_name'
        logging.info("Removing rows with null currency names.")
        name_currency = currencies_name[
            currencies_name["currency_name"].apply(lambda x: pd.notna(x))
        ]

        # Select the relevant columns
        name_currencies = name_currency[["name.common", "currency_name"]]

        # Group the data by country and aggregate currency names
        logging.info("Grouping by country and aggregating currency names.")
        cur_combined = (
            name_currencies.groupby("name.common")
            .agg({"currency_name": lambda x: ", ".join(map(str, x))})
            .reset_index()
        )

        logging.info("Currency name extraction completed successfully.")
        return cur_combined

    except Exception as e:
        raise Exception(f"Error during currency name extraction: {str(e)}")
