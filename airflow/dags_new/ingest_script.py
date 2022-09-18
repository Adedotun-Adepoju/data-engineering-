import os 

from time import time 

import pandas as pd
import sqlalchemy from create_engine

def ingest_callable(user, password, host, port, db, table_name, file_name):
    print(table_name, file_name)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # df_iter = pd.read_parquet(file_name, iterator=True, chunksize=100000)

    # df = next(df_iter)

    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # df.head(n=0).to_sql(name=table_name, con = engine, if_exists = 'replace')

    # df.to_sql(name=table_name, con=engine, if_exists='append')

    # for i in df_iter:
    #     t_start = time()
        
    #     i["tpep_pickup_datetime"] = pd.to_datetime(i["tpep_pickup_datetime"])
    #     i["tpep_dropoff_datetime"] = pd.to_datetime(i["tpep_dropoff_datetime"])
        
    #     i.to_sql(name='yellow_taxi_data', con = engine, if_exists = 'append')
        
    #     t_end = time()
        
    #     print(f'Inserted another chunk, took {t_end - t_start}')
