# # # from airflow import DAG
# # # from airflow.utils import dates
# # # from airflow.operators.python_operator import PythonOperator

# # # from datetime import datetime, timedelta  


# # # default_args = {'owner': 'adewunmi',
# # #         'depends_on_past': False,
# # #         'retries': 2,
# # #         'retry_delay': timedelta(minutes=5)
# # #         }

# # # with DAG(dag_id='capstone',
# # #         default_args=default_args,
# # #         start_date=datetime(2024, 11, 15),
# # #         schedule_interval=None
# # # ) as dag:
    
# # #     yi


# # def renaming_column():
# #     to_rename = table_transformation

# #     to_rename.rename(columns={'name.common': 'country_name', 
# #                               'independent': 'independence', 
# #                               'unMember': 'united_nation_members', 
# #                               'startOfWeek': 'start_of_week', 
# #                               'name.official': 'official_country_name', 
# #                               'name.nativeName.eng.common': 'common_native_name'}, 
# #                               inplace=True)


# # def renaming_column(table_transformation):
# #     """
# #     This function renames specific columns in a DataFrame.
# #     It ensures proper logging and handles exceptions during the renaming process.
# #     """
# #     try:
# #         logging.info("Starting the column renaming process.")
        
# #         # Fetch table_transformation
# #         to_rename = table_transformation
# #         # Renaming columns
# #         to_rename.rename(columns={
# #             'name.common': 'country_name', 
# #             'independent': 'independence', 
# #             'unMember': 'united_nation_members', 
# #             'startOfWeek': 'start_of_week', 
# #             'name.official': 'official_country_name', 
# #             'name.nativeName.eng.common': 'common_native_name'
# #         }, inplace=True)

# #         logging.info("Column renaming completed successfully.")
# #         return table_transformation

# #     except KeyError as e:
# #         logging.error(f"KeyError during column renaming: {e}")
# #         raise Exception(f"KeyError during column renaming: {str(e)}")
    
# #     except AttributeError as e:
# #         logging.error(f"AttributeError during column renaming: {e}")
# #         raise Exception(f"AttributeError during column renaming: {str(e)}")
# #     # 
# #     except Exception as e:
# #         logging.error(f"Unexpected error during column renaming: {e}")
# #         raise Exception(f"Unexpected error during column renaming: {str(e)}")












# # 'independent', 'name.common', 'unMember', 'startOfWeek', 'name.official',
# #             'name.nativeName.eng.common', 'capital', 'region', 'area', 'population',
# #             'continents', 'idd.root', 'idd.suffixes', 'subregion'



# def load_to_datbase():
#     # Create or load your DataFrame
#     df = renaming_column()

#     # Connect to PostgreSQL using Airflow's PostgresHook
#     postgres_hook = PostgresHook(postgres_conn_id='rds_postgres')

#     # SQLAlchemy engine for bulk insert
#     engine = postgres_hook.get_sqlalchemy_engine()

#     # Insert DataFrame into PostgreSQL table
#     df.to_sql(
#         name='country_data',  # Replace with your table name
#         con=engine,
#         if_exists='replace',  # Options: 'fail', 'replace', 'append'
#         index=False  # Do not include DataFrame's index as a column
#     )




