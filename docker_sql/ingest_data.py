#!/usr/bin/env python
# coding: utf-8

import argparse
import os

from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table
    url = params.url

    print("url", user)

    # download the csv 
    csv_name = 'output.csv'
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df = pd.read_csv(csv_name)

    # divide the dataframe into chunks of 100000
    # df_iter = df = pd.read_csv('yellow_tripdata_2021-01.csv', iterator = True, chunksize = 100000)


    # Insert only the headers to the table 
    df.head(n=0).to_sql(name = table_name, con = engine, if_exists = 'replace')

    df.to_sql(name=table_name, con = engine, if_exists = 'append')

    # for i in df_iter:
    #     t_start = time()
        
    #     i["tpep_pickup_datetime"] = pd.to_datetime(i["tpep_pickup_datetime"])
    #     i["tpep_dropoff_datetime"] = pd.to_datetime(i["tpep_dropoff_datetime"])
        
    #     i.to_sql(name='yellow_taxi_data', con = engine, if_exists = 'append')
        
    #     t_end = time()
        
    #     print(f'Inserted another chunk, took {t_end - t_start}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

    parser.add_argument('--user', help = 'user name for postgres')
    parser.add_argument('--password', help = 'password for postgres')
    parser.add_argument('--host', help = 'host for postgres')
    parser.add_argument('--port', help = 'port number for postgres')
    parser.add_argument('--db', help = 'database for postgres')
    parser.add_argument('--table', help = 'name of the table to write the results to')
    parser.add_argument('--url', help = 'url for csv')
                        
    args = parser.parse_args()
    
    main(args)


