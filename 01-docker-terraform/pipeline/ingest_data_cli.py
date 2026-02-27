#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm #to track increamental loading
import click

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

@click.command()
@click.option('--year', '-y', type=int, default=2021, help='Year of the dataset (e.g. 2021)')
@click.option('--month', '-m', type=int, default=1, help='Month of the dataset (1-12)')
@click.option('--chunk-size', '-c', type=int, default=100000, help='Number of rows per chunk')
@click.option('--table', '-t', default='yellow_taxi_data', help='Target DB table name')
@click.option('--user', default='root', help='Postgres username')
@click.option('--password', prompt=False, default='root', hide_input=True, help='Postgres password')
@click.option('--host', default='localhost', help='Postgres host')
@click.option('--port', default=5432, type=int, help='Postgres port')
@click.option('--db', default='ny_taxi', help='Postgres database name')
@click.option('--prefix', default='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow', help='URL prefix for dataset')
def run(year, month, chunk_size, table, user, password, host, port, db, prefix):
    file = f'yellow_tripdata_{year}-{month:02d}.csv.gz'
    url = f'{prefix}/{file}'
    engine = create_engine(f'postgresql+psycopg://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,     # Makes csv loading increamental
        chunksize=chunk_size   # Specify the iteration size
    )

    first = True

    for df_chunk in tqdm(df_iter, desc=f'Loading {file}'):
        if first:
            df_chunk.head(0).to_sql(
                name=table,
                con=engine, 
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=table,
            con=engine, 
            if_exists='append'
        )


if __name__ == "__main__":
    run()
