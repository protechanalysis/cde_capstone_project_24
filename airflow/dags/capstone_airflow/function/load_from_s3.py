import logging

import awswrangler as wr
from capstone_airflow.function.extract_load_to_s3 import aws_session

logging.basicConfig(format="%(asctime)s %(message)s")


def read_s3_parquet():
    """
    This function reads a Parquet file from S3,
    and returns the result as a Pandas DataFrame.
    """
    try:
        # Attempt to read the parquet file from S3
        read_result = wr.s3.read_parquet(
            path="s3://capstone-tourist/country/",
            boto3_session=aws_session,
            dataset=True,
        )

        # Log the result if reading was successful
        if not read_result.empty:
            logging.info("Successfully read Parquet file from S3.")
        else:
            logging.warning("The Parquet file is empty.")

    except Exception as e:
        raise Exception(f"Error reading Parquet from S3: {str(e)}")
    return read_result
