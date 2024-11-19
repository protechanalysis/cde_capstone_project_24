import logging


import awswrangler as wr
import boto3
import pandas as pd
import requests
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


def country_api_request():
    """
    this function extract country data from a rest api
    """
    url = "https://restcountries.com/v3.1/all"
    data = None

    try:
        logging.info('GET request initiating')
        get_api = requests.get(url)
        
        # Check if the status code indicates success
        if get_api.status_code == 200:
            logging.info('API request connection is okay')
            
            # Try to retrieve and log JSON response data
            try:
                rest_data = get_api.json()
                data = pd.json_normalize(rest_data)
                logging.info('Response data received successfully')
                #data.to_csv('output.csv')
                # Convert the DataFrame to an Arrow Table
                # table = pa.Table.from_pandas(data)

                # # Write the Table to a Parquet file
                # pq.write_table(table, 'output.parquet')


                #print(data)  # Print or further process the data as needed
            except ValueError:
                raise Exception('Failed to parse JSON response')
        else:
            raise Exception(f'API request failed with status code {get_api.status_code}')
            
    except requests.ConnectionError as e:
        raise Exception(f'Connection error: {e}')
    except requests.RequestException as e:
        raise Exception(f'An unexpected error occurred: {e}')

    return data
# country_data = country_api_request()


def aws_session():
    """
    setting up aws boto3 session credentials
    """
    session = boto3.Session(
                    aws_access_key_id=Variable.get('access_key'),
                    aws_secret_access_key=Variable.get('secret_key'),
                    region_name="eu-west-2"
    )
    return session


def country_to_s3_parquet():
    """
    This function fetches country data, processes it, and writes it to S3 in Parquet format.
    """
    try:
        # Fetch the country data (ensure this is a pandas DataFrame)
        country_data = country_api_request()

        # Check if the data is not None before proceeding
        if country_data is not None and not country_data.empty:
            # Write the data to S3 in Parquet format
            wr.s3.to_parquet(
                df=country_data,
                path="s3://capstone-tourist/count/",
                boto3_session=aws_session(),
                mode="overwrite", 
                dataset=True
            )
            logging.info("Data successfully written to S3 in Parquet format.")
        else:
            logging.warning("No data available to write to S3.")

    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")
        # return f"Error: {str(e)}"
    
    # return "Data successfully written to S3"

# country_to_s3_parquet()


def read_s3_parquet():
    """
    This function reads a Parquet file from S3 and returns the result as a Pandas DataFrame.
    """
    try:
        # Attempt to read the parquet file from S3
        read_result = wr.s3.read_parquet(
            path="s3://capstone-tourist/count/",
            boto3_session=aws_session(),
            dataset=True
        )
        
        # Log the result if reading was successful
        if not read_result.empty:
            logging.info("Successfully read Parquet file from S3.")
        else:
            logging.warning("The Parquet file is empty.")
        
    except Exception as e:
        raise Exception(f"Error reading Parquet from S3: {str(e)}")
    
    return read_result

# country_read = read_s3_parquet()

# Select specific columns and create a copy of the DataFrame to avoid SettingWithCopyWarning
def column_selections():
    """
    This function selects the required columns from the S3 data.
    It first reads the data from the Parquet file in S3 and then selects specific columns.
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
            'independent', 'name.common', 'unMember', 'startOfWeek', 'name.official',
            'name.nativeName.eng.common', 'capital', 'region', 'area', 'population',
            'continents', 'idd.root', 'idd.suffixes', 'subregion'
        ]

        # Select the required columns
        dif = country_read[required_columns]

        logging.info("Column selection completed successfully.")
        return dif
    
    except Exception as e:
        logging.error(f"Error during column selection: {str(e)}")
        return None


# country_column_select = column_selections()



def extract_currency_code_symbol():
    """
    This function extracts the currency code and symbol from the country_read function.
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
        if 'name.common' not in country_read.columns:
            logging.error("'name.common' column not found in data.")
            return None, "Error: 'name.common' column not found in data."

        # Filter and join currency symbol columns with country name
        logging.info("Filtering currency symbol columns and joining with country names.")
        symbol = country_read.filter(items=['name.common']).join(country_read.filter(regex='^currencies.*symbol$'))

        # Melt the data to get currency code and symbol
        logging.info("Melting the data to extract currency codes and symbols.")
        symbol_code = symbol.melt(id_vars=['name.common'], var_name='currency_code', value_name='currency_symbol')

        # Remove rows with null values in the 'currency_symbol' column
        logging.info("Removing rows with null currency symbols.")
        syms = symbol_code[symbol_code['currency_symbol'].apply(lambda x: pd.notna(x))].copy()

        # Clean up currency codes by splitting
        logging.info("Cleaning up currency codes.")
        syms['currency_code'] = syms['currency_code'].str.split('.').str[1]

        # Group by country name and aggregate the symbols and codes
        logging.info("Grouping by country and aggregating currency symbols and codes.")
        sym_combine = syms.groupby('name.common').agg({
            'currency_symbol': lambda x: ','.join(x),
            'currency_code': lambda x: ','.join(x)
        }).reset_index()

        logging.info("Currency extraction completed successfully.")
        return sym_combine
    
    except Exception as e:
        raise Exception(f"Error during currency extraction: {str(e)}")
        # return None

# currency_code_symbol = extract_currency_code_symbol()


def extract_currency_name():
    """
    This function extracts the currency name from the country_read function.
    It reads data from S3, filters the relevant currency columns, processes them, and returns the results.
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
        cur_nam = country_read.filter(items=['name.common']).join(country_read.filter(regex='^currencies.*name$'))

        # Melt the data to extract currency names
        logging.info("Melting data to extract currency names.")
        currencies_name = cur_nam.melt(id_vars=['name.common'], value_name='currency_name')

        # Remove rows with null values in 'currency_name'
        logging.info("Removing rows with null currency names.")
        name_currency = currencies_name[currencies_name['currency_name'].apply(lambda x: pd.notna(x))]

        # Select the relevant columns
        name_currencies = name_currency[['name.common', 'currency_name']]

        # Group the data by country and aggregate currency names
        logging.info("Grouping by country and aggregating currency names.")
        cur_combined = name_currencies.groupby('name.common').agg({'currency_name': lambda x: ', '.join(map(str, x))}).reset_index()

        logging.info("Currency name extraction completed successfully.")
        return cur_combined

    except Exception as e:
        raise Exception(f"Error during currency name extraction: {str(e)}")
        # return None


# currency_name = extract_currency_name()
# cur_combined.to_csv('cur_combined.csv', index=False)


def extract_languages():
    """
    This function extracts country language information from the country_read function.
    It reads data from S3, processes the relevant language columns, and returns the result.
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
        language = country_read.filter(items=['name.common']).join(country_read.filter(regex='^languages.*'))

        # Melt the data to extract language names
        logging.info("Melting data to extract languages.")
        language_code = language.melt(id_vars=['name.common'], value_name='languages')

        # Remove rows with null values in 'languages'
        logging.info("Removing rows with null language values.")
        languages_clean = language_code[language_code['languages'].apply(lambda x: pd.notna(x))]

        # Select the relevant columns
        language_select = languages_clean[['name.common', 'languages']]

        # Group the data by country and aggregate languages
        logging.info("Grouping by country and aggregating languages.")
        languages = language_select.groupby('name.common').agg({'languages': lambda x: ', '.join(map(str, x))}).reset_index()

        logging.info("Language extraction completed successfully.")
        return languages

    except Exception as e:
        raise Exception(f"Error during language extraction: {str(e)}")
        # return None


# language = extract_languages()

def tables_joining():
    """
    This function joins multiple data tables (country information, currency data, and language data).
    It first selects columns, then joins the data from multiple sources and returns the combined table.
    """
    try:
        logging.info("Starting the table joining process.")
        
        # Fetch the individual tables
        country_column_select = column_selections()
        # if country_column_select is None:
        #     logging.error(f"Error in column selection: {status}")
        #     return None, f"Error in column selection: {status}"

        # logging.info("Country columns selected successfully.")

        currency_code_symbol = extract_currency_code_symbol()
        # if currency_code_symbol is None:
        #     logging.error("Error in extracting currency code and symbol.")
        #     return None, "Error in extracting currency code and symbol."

        logging.info("Currency code and symbol extracted successfully.")

        currency_name = extract_currency_name()
        # if currency_name is None:
        #     logging.error("Error in extracting currency name.")
        #     return None, "Error in extracting currency name."

        logging.info("Currency name extracted successfully.")

        language = extract_languages()
        # if language is None:
        #     logging.error("Error in extracting languages.")
        #     return None, "Error in extracting languages."

        logging.info("Languages extracted successfully.")

        # Perform the joins
        logging.info("Joining tables.")
        country_currency = pd.merge(country_column_select, currency_code_symbol, on='name.common', how='right')
        country_currency_name = pd.merge(country_currency, currency_name, on='name.common', how='right')
        country_language = pd.merge(country_currency_name, language, on='name.common', how='right')

        logging.info("Tables joined successfully.")
        
        return country_language
    
    except Exception as e:
        raise Exception(f"Error during table joining: {str(e)}")
        # return None

# country_join = tables_joining()

# print(country_join.dtypes)

def table_transformation():
    """
    This function transforms the country data by cleaning specific columns, 
    creating new columns, and dropping unnecessary ones.
    """
    try:
        logging.info("Starting table transformation process.")

        # Retrieve the joined data from the previous function
        # country_join, status = tables_joining()
        country_join = tables_joining()
        # if country_join is None:
        #     logging.error(f"Error in table joining: {status}")
        #     return None, f"Error in table joining: {status}"

        logging.info("Data joined successfully for transformation.")

        # List of columns to clean and transform
        columns_to_check = ['continents', 'idd.suffixes', 'capital']

        for col in columns_to_check:
            if col in country_join.columns:
                # Ensure the column is of string type
                country_join[col] = country_join[col].astype('string')
                # Remove unwanted characters (e.g., brackets, quotes)
                country_join[col] = country_join[col].str.replace(r'[\[\]\']', '', regex=True)
                logging.info(f"Column {col} transformed successfully.")
            else:
                logging.warning(f"Column {col} not found in the data.")

        # Create the 'country_code' column by combining 'idd.root' and 'idd.suffixes'
        if 'idd.root' in country_join.columns and 'idd.suffixes' in country_join.columns:
            country_join['country_code'] = country_join['idd.root'] + country_join['idd.suffixes']
            logging.info("'country_code' column created successfully.")
        else:
            logging.warning("Columns 'idd.root' or 'idd.suffixes' not found to create 'country_code'.")

        # Drop the 'idd.root' and 'idd.suffixes' columns as they are no longer needed
        country_transform = country_join.drop(columns=['idd.root', 'idd.suffixes'], errors='ignore')
        logging.info("Unnecessary columns dropped successfully.")

        return country_transform

    except Exception as e:
        raise Exception(f"Error during table transformation: {str(e)}")
        # return None


def renaming_column():
    """
    This function renames specific columns in a DataFrame.
    It ensures proper logging and handles exceptions during the renaming process.
    """
    try:
        logging.info("Starting the column renaming process.")
        
        # Fetch table_transformation
        to_rename = table_transformation()
        # Renaming columns
        to_rename.rename(columns={
            'name.common': 'country_name', 
            'independent': 'independence', 
            'unMember': 'united_nation_members', 
            'startOfWeek': 'start_of_week', 
            'name.official': 'official_country_name', 
            'name.nativeName.eng.common': 'common_native_name'}, inplace=True)

        logging.info("Column renaming completed successfully.")
        return to_rename

    except KeyError as e:
        logging.error(f"KeyError during column renaming: {e}")
        raise Exception(f"KeyError during column renaming: {str(e)}")
    
    except AttributeError as e:
        logging.error(f"AttributeError during column renaming: {e}")
        raise Exception(f"AttributeError during column renaming: {str(e)}")
    
    except Exception as e:
        logging.error(f"Unexpected error during column renaming: {e}")
        raise Exception(f"Unexpected error during column renaming: {str(e)}")


def load_to_database():
    """
    This function loads a DataFrame into a PostgreSQL database.
    It uses Airflow's PostgresHook to connect to the database and SQLAlchemy for data insertion.
    """
    try:
        logging.info("Starting the database load process.")
        data = renaming_column()
        # Ensure the DataFrame is valid
        if data is None or data.empty:
            logging.error("The DataFrame is empty or None.")
            return None, "Error: The DataFrame is empty or None."

        logging.info("DataFrame validation successful. Proceeding to database connection.")

        # Connect to PostgreSQL using Airflow's PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='rds_connect')
        engine = postgres_hook.get_sqlalchemy_engine()

        # Define table name
        table_name = 'country_data'  # Replace with your target table name

        logging.info(f"Loading data into the table '{table_name}' in the PostgreSQL database.")

        # Load the DataFrame into the PostgreSQL table
        data.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  # Replace the table if it exists
            index=False  # Do not include the DataFrame's index as a column
        )

        logging.info(f"Data successfully loaded into the table '{table_name}'.")
        return "Data loaded successfully."

    except Exception as e:
        raise Exception(f"Error during database load process: {str(e)}")
        # return None, f"Error: {str(e)}"

# dty = table_tansformation()

# rt = table_cleaning()
# print(rt)
# dty.to_csv('dty.csv', index=False)


# def country_transform_s3_parquet():
#     """
#     This function writes the transformed country data into an S3 bucket in Parquet format.
#     It first transforms the data and then writes it to S3.
#     """
#     try:
#         # Transform the data
#         count_transform = table_transformation()

#         # Write the transformed data to S3 in Parquet format
#         wr.s3.to_parquet(df=count_transform,
#                          path="s3://capstone-tourist/tra",
#                          boto3_session=aws_session(),
#                          mode="overwrite", dataset=True)

#         logging.info("Data successfully written to S3 in Parquet format.")
#         return "Transformed data successfully written to S3 in Parquet format."

#     except Exception as e:
#         raise Exception(f"Error during writing to S3: {str(e)}")
        # return f"Error: {str(e)}"

# def doen_load():
#     ta = table_transformation()
#     csv_path = "s3://capstone-tourist/tr5/country_transformed.csv"
#     wr.s3.to_csv(
#         df=ta,
#         path=csv_path,
#         boto3_session=aws_session(),
#         index=False  # Exclude the index in the CSV file
#         )
# country_transform_s3_parquet()

#     print(country_language.isnull().sum())

# # print(country_language.fillna('unspecified'))
# print(tables_joining().isnull().sum())
