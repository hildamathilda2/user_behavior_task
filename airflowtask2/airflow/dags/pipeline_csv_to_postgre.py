from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd

#define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,11,9)
}

#function to read csv and producing insert queries
def producing_insert_queries():
    CSV_FILE_PATH = '/home/hfrnssc/airflow/sample_files/dataset_user_behavior_for_test.csv'
    #read csv file
    df = pd.read_csv(CSV_FILE_PATH)

    