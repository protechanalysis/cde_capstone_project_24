import logging
import pandas as pd

from capstone_airflow.function.load_from_s3 import read_s3_parquet


def languages():
    """
    This function extracts country language information from
    the country_read function.
    It reads data from S3, processes the relevant language columns,
    and returns the result.
    """
    try:
        logging.info("Starting language extraction process.")

        # Read data from S3
        country_read = read_s3_parquet()

        # Ensure data is valid
        if country_read is None or country_read.empty:
            logging.error("No data available to process.")
            return None

        logging.info("Data loaded successfully from S3.")

        # Filter relevant columns for country names and languages
        logging.info("Filtering country names and language columns.")
        language = country_read.filter(items=["name.common"]).join(
            country_read.filter(regex="^languages.*")
        )

        # Melt the data to extract language names
        logging.info("Melting data to extract languages.")
        language_code = language.melt(id_vars=["name.common"],
                                      value_name="languages")

        # Remove rows with null values in 'languages'
        logging.info("Removing rows with null language values.")
        languages_clean = language_code[
            language_code["languages"].apply(lambda x: pd.notna(x))
        ]

        # Select the relevant columns
        language_select = languages_clean[["name.common", "languages"]]

        # Group the data by country and aggregate languages
        logging.info("Grouping by country and aggregating languages.")
        languages = (
            language_select.groupby("name.common")
            .agg({"languages": lambda x: ", ".join(map(str, x))})
            .reset_index()
        )

        logging.info("Language extraction completed successfully.")
        return languages

    except Exception as e:
        raise Exception(f"Error during language extraction: {str(e)}")
