import logging

from capstone_airflow.function.load_from_s3 import read_s3_parquet


def column_selections():
    """
    This function selects the required columns from the S3 data.
    It first reads the data from the Parquet file in S3,
    and then selects specific columns.
    If any columns are missing, it handles the error gracefully.
    """
    try:
        logging.info("Starting column selection process.")

        # Read data from S3
        country_read = read_s3_parquet()

        # Ensure data is valid
        if country_read is None or country_read.empty:
            logging.error("No data available to process.")
            return None, "Error: No data available to process."

        logging.info("Data loaded successfully from S3.")

        # List of required columns
        required_columns = [
            "independent",
            "name.common",
            "unMember",
            "startOfWeek",
            "name.official",
            "name.nativeName.eng.common",
            "capital",
            "region",
            "area",
            "population",
            "continents",
            "idd.root",
            "idd.suffixes",
            "subregion"
        ]

        # Select the required columns
        dif = country_read[required_columns]

        logging.info("Column selection completed successfully.")
        return dif

    except Exception as e:
        logging.error(f"Error during column selection: {str(e)}")
