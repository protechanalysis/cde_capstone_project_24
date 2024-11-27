### AIRFLOW SETUP

#### Navigate to the airflow/ directory:
```
cd airflow
```

### setup environment variable
```
# create .env file
touch .env

# copy into .env
AIRFLOW_UID=50000
```

### Script Overview
#### function folder:
- extract_load_to_s3.py:

    This script contains two function handles the initial data extraction from the REST Countries API and loads the data into S3 in parquet format using aws wrangler and boto3 session.

- load_from_s3.py:

    Reads data from the cloud object storage 

- extract_columns.py:

    Function extract the following columns: "independent",
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

-  extract_currency.py:

    Function extract currency name, code and symbol for each country.

-  extract_languages.py:

    Function extract languages for each country.

-  joining.py:

    Function merge all dataframe created into one.

-  transformation.py:

    Function contain data transformation such as column merging and dropping, columns renaming.

-  data_to_database.py:

    Function loads data in the Postgres database.

#### notification folder:
-  email_notification.py:

    Function send email notification of a failed dag instance.

#### Run the Airflow Docker Compose setup
```
# to add aws sdk dependency to airflow
docker compose build 

# to startup airflow
docker compose up
```
#### Access the Airflow UI at http://localhost:8080.

###  Configuring Airflow Connections
In the Airflow UI, go to Admin > Connections.
Add a new connection:
-   Connection ID: rds_conn
-   Connection Type: Postgres
-   Host: your_rds_endpoint
-   Login: your_rds_username
-   Password: your_rds_password
-   Port: 5432

###  Configuring Airflow Variables
In the Airflow UI, go to Admin > Variables.
#### Set environment variable for:
-  access_key
-  secret_key
-  email_sender
-  email_password
-  email_receiver

### DAGs
-   Capstone

![capstone_dag](/images/dag_capstone.png)


-   create_country_table

![capstone_dag](/images/dag_create_table.png)


### Trigger your airflow DAG
-   Turn on the DAGs by toggling the "ON" switch.
-   Trigger a manual run by clicking the play icon (▶️) next to the DAG name.


