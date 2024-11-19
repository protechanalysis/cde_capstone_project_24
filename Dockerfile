FROM apache/airflow:2.10.1

# install extra requirements
RUN pip install --no-cache-dir awswrangler boto3