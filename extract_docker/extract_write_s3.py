import logging

import awswrangler as wr
import boto3
import pandas as pd
import requests

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


def country_api_request():
    """
    this function extract country data from a rest api
    """
    url = "https://restcountries.com/v3.1/all"
    data = None

    try:
        logging.info("GET request initiating")
        get_api = requests.get(url)

        # Check if the status code indicates success
        if get_api.status_code == 200:
            logging.info("API request connection is okay")

            # Try to retrieve and log JSON response data
            try:
                rest_data = get_api.json()
                data = pd.json_normalize(rest_data)
                logging.info("Response data received successfully")
            except ValueError:
                raise Exception("Failed to parse JSON response")
        else:
            raise Exception(
                f"API request failed with status code {get_api.status_code}"
            )

    except requests.ConnectionError as e:
        raise Exception(f"Connection error: {e}")
    except requests.RequestException as e:
        raise Exception(f"An unexpected error occurred: {e}")

    return data


def aws_session():
    """
    setting up aws boto3 session credentials
    """
    session = boto3.Session(
        aws_access_key_id='xxxxxxx',
        aws_secret_access_key='xxxxxxx',
        region_name="xxxxxxx",
    )
    return session


def country_to_s3_parquet():
    """
    This function fetches country data, processes it,
    and writes it to S3 in Parquet format.
    """
    try:
        # Fetch the country data (ensure this is a pandas DataFrame)
        country_data = country_api_request()

        # Check if the data is not None before proceeding
        if country_data is not None and not country_data.empty:
            # Write the data to S3 in Parquet format
            wr.s3.to_parquet(
                df=country_data,
                path="s3://bucket_path",
                boto3_session=aws_session(),
                mode="overwrite",
                dataset=True,
            )
            logging.info("Data successfully written to S3 in Parquet format.")
        else:
            logging.warning("No data available to write to S3.")

    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    try:
        country_to_s3_parquet()
    except Exception as main_exception:
        logging.critical(f"Pipeline failed: {main_exception}")