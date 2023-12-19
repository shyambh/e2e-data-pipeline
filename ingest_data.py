"""
Ingest the NY Taxi Data 2022 to Postgres
"""
from time import time
import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task

def get_filename(filename):
    return '.'.join(filename.split('.')[:-1])

@task(log_prints=True)
def connect_to_db(username, password, host, port, database):
    # Establish a connection to the database
    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
    
    return engine

@task(log_prints=True)
def download_csv(url_to_csv, output_file_name):
    """Function to download the csv file from remote"""
    os.system(f"curl -L -o {output_file_name} {url_to_csv}")
    os.system(f"gzip --decompress {output_file_name}")

@flow(name="Dump To DB", description="Dumps the transformed data into the Database")
def dump_to_db(df, db_engine, table_name, filename):
    # create table
    df.head(n=0).to_sql(table_name, con=db_engine, if_exists='replace')
    
    # Iteratively insert the csv rows into the DB in a batchsize of 100000
    df_iter = pd.read_csv(filename, iterator=True, chunksize=100000)

    # Iteratively dump the csv rows to the Database
    while True:
        try:
            t_start = time()
            
            transformed_df = transform_data(df)
            
            transformed_df.tpep_pickup_datetime = pd.to_datetime(transformed_df.tpep_pickup_datetime)
            transformed_df.tpep_dropoff_datetime = pd.to_datetime(transformed_df.tpep_dropoff_datetime)

            transformed_df.to_sql(table_name, con=db_engine, if_exists='append')
            
            t_end = time()
            print('inserted another chunk... took %.3f second' % (t_end - t_start))
            
            df = next(df_iter)
        
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
    
@task(log_prints=True)
def transform_data(df):
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    
    df = df[df['passenger_count'] != 0]
    
    print(f"post:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    
    return df

    
@flow(name="Ingest Flow")
def main_flow():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", help="Postgres username")
    parser.add_argument("--password", help="Postgres password")
    parser.add_argument("--host", help="Postgres hostname")
    parser.add_argument("--port", help="Postgres port number")
    parser.add_argument("--db", help="Postgres database name")
    parser.add_argument("--tbl_name", help="name of the destination table")
    parser.add_argument("--csv_url", help="url of the csv file")
    parser.add_argument("--output", help="name of the downloaded csv file")

    args = parser.parse_args()
    filename = '.'.join(args.output.split('.')[:-1])

    download_csv(args.csv_url, args.output)
    
    df = pd.read_csv(filename, nrows=100000)
    
    db_engine = connect_to_db(args.user, args.password, args.host, args.port, args.db)
    
    dump_to_db(df, db_engine, args.tbl_name, filename)

if(__name__ == "__main__"):
    main_flow()