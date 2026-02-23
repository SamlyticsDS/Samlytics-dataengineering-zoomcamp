#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm #to track increamental loading

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

#engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')

#df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# Increamental loading

def run():
    year=2021
    month=1
    chuck_size=100000
    target_table = 'yellow_taxi_data'

    pg_user = "root"
    pg_pass = "root"
    pg_host = "localhost"
    pg_port = 5432
    pg_db = "ny_taxi"

    prefix='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    file=(f'yellow_tripdata_{year}-{month:02d}.csv.gz')
    url =(f'{prefix}/{file}')
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


    df_iter= pd.read_csv(url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,     # Makes csv loading increamental
        chunksize=chuck_size   # Specify the iteration size
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                 con=engine, 
                 if_exists='replace'
            )
            first = False
        else:
            df_chunk.to_sql(
                name=target_table,
                con=engine, 
                if_exists='append'
            )


if __name__ == "__main__":
    run()
