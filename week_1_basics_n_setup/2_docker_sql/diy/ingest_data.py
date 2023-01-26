#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    os.system(f'wget -O - {url} | gunzip -c > {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    pd.read_csv(csv_name, nrows=0).to_sql(name=table_name, con=engine, if_exists='replace')

    for x in df_iter:
        t_start = time()

        if 'yellow' in table_name:
            x.tpep_pickup_datetime = pd.to_datetime(x.tpep_pickup_datetime)
            x.tpep_dropoff_datetime = pd.to_datetime(x.tpep_dropoff_datetime)
        elif 'green' in table_name:
            x.lpep_pickup_datetime = pd.to_datetime(x.lpep_pickup_datetime)
            x.lpep_dropoff_datetime = pd.to_datetime(x.lpep_dropoff_datetime)

        x.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted chunk, took %.3f seconds' % (t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--host')
    parser.add_argument('--port')
    parser.add_argument('--db')
    parser.add_argument('--table_name')
    parser.add_argument('--url')
    
    args = parser.parse_args()

    main(args)
