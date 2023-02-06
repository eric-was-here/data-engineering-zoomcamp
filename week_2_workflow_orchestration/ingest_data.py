#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
from datetime import timedelta

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url, csv_name):
    os.system(f'wget -O - {url} | gunzip -c > {csv_name}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    # if 'yellow' in table_name:
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    # elif 'green' in table_name:
    #     df.lpep_pickup_datetime = pd.to_datetime(x.lpep_pickup_datetime)
    #     df.lpep_dropoff_datetime = pd.to_datetime(x.lpep_dropoff_datetime)

    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    connection_block = SqlAlchemyConnector.load('postgres-connector')
    with connection_block.get_connection(begin=False) as engine:

    # user = params.user
    # password = params.password
    # host = params.host
    # port = params.port
    # db = params.db
    # table_name = params.table_name
    # url = params.url
    # csv_name = 'output.csv'


    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # pd.read_csv(csv_name, nrows=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

    # for x in df_iter:
    #     t_start = time()

    #     if 'yellow' in table_name:
    #         x.tpep_pickup_datetime = pd.to_datetime(x.tpep_pickup_datetime)
    #         x.tpep_dropoff_datetime = pd.to_datetime(x.tpep_dropoff_datetime)
    #     elif 'green' in table_name:
    #         x.lpep_pickup_datetime = pd.to_datetime(x.lpep_pickup_datetime)
    #         x.lpep_dropoff_datetime = pd.to_datetime(x.lpep_dropoff_datetime)

    #     x.to_sql(name=table_name, con=engine, if_exists='append')

    #     t_end = time()

        # print('inserted chunk, took %.3f seconds' % (t_end - t_start))
@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f'Logging Subflow for {table_name}')

@flow(name="Ingest flow", retries=3)
def main_flow(table_name: str):
    # user = 'root'
    # password = 'root'
    # host = 'localhost'
    # port = 5432
    # db = 'ny_taxi'
    # # table_name = 'yellow_taxi_trips_100k'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    csv_name = 'output.csv'
    # log_subflow(table_name)
    raw_data = extract_data(url, csv_name)
    data = transform_data(raw_data)
    # ingest_data(user, password, host, port, db, table_name, data)
    ingest_data(table_name, data)
    # parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # parser.add_argument('--user')
    # parser.add_argument('--password')
    # parser.add_argument('--host')
    # parser.add_argument('--port')
    # parser.add_argument('--db')
    # parser.add_argument('--table_name')
    # parser.add_argument('--url')
    
    # args = parser.parse_args()

    # main(args)
    

if __name__ == '__main__':
    main_flow('yellow_taxi_trips_100k')
