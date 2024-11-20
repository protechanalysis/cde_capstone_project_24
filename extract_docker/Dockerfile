FROM python:3.10-bookworm

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./extract_write_s3.py /app/extract_write_s3.py

ENTRYPOINT ["python", "extract_write_s3.py"]