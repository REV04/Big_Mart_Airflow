'''
Milestone 3

Name    : Richard Edgina Virgo
Batch   : 006

This program was built for automation system which transform and load data from PostgreSQL, clean the data, and then post to Elasticsearch. This program also schedule the data every 06:30.

'''

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2 as db
import numpy as np
from elasticsearch import Elasticsearch

def postgre():
    '''
    This function is used to load the data from postgres and transform into csv file
    '''
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)

    # Executing SQL query using pandas
    df = pd.read_sql("SELECT * FROM table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_Richard_Edgina_data_raw.csv', index=False)
    print("-------Data Saved------")

def clean_scooter():
    '''
    This function is used to read data raw ann clean the data 
    '''
    df = pd.read_csv('/opt/airflow/dags/P2M3_Richard_Edgina_data_raw.csv')
    df.columns = df.columns.str.lower()
    df = df.rename(columns=lambda x: x.strip().lower().replace(" ", ""))
    
    df["fatcontent"] = df["fatcontent"].replace(["low fat"], "Low Fat")
    df["fatcontent"] = df["fatcontent"].replace(["LF"], "Low Fat")
    df["fatcontent"] = df["fatcontent"].replace(["reg"], "Regular")
    # Handling missing values for categorical columns
    categorical_columns = df.select_dtypes(include=['object', 'category']).columns
    for col in categorical_columns:
        df[col] = df[col].fillna(df[col].mode()[0])
    
    # Handling missing values for numerical columns
    numerical_columns = df.select_dtypes(include=['number']).columns
    for col in numerical_columns:
        df[col] = df[col].fillna(df[col].median())
    
    df = df.drop_duplicates()
    df.to_csv('/opt/airflow/dags/P2M3_Richard_Edgina_data_clean.csv', index=False)
    print("-------Data Cleaned------")

def post_to_elasticsearch():
    '''
    This function is used to post the cleaned data into Elasticsearch
    '''
    es = Elasticsearch('http://elasticsearch:9200')
    df = pd.read_csv('/opt/airflow/dags/P2M3_Richard_Edgina_data_clean.csv')
    for _, row in df.iterrows():
        doc = row.to_json()
        res = es.index(index="bigmart", doc_type="doc", body=doc)
        print(res)

default_args = {
    'owner': 'Richard Edgina Virgo',
    'start_date': dt.datetime(2024, 7, 21),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

with DAG('CleanData',
         default_args=default_args,
         schedule_interval='30 6 * * *',
         ) as dag:
    
    fetch_data = PythonOperator(task_id='Fetch',
                                python_callable=postgre)
    
    clean_data = PythonOperator(task_id='Clean',
                                python_callable=clean_scooter)
    
    post_data = PythonOperator(task_id='Post',
                               python_callable=post_to_elasticsearch)

fetch_data >> clean_data >> post_data
