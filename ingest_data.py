"""
Ingest the NY Taxi Data 2022 to Postgres
"""
from time import time
import argparse
import os
import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector

def get_filename(filename):
    return '.'.join(filename.split('.')[:-1])


@task(log_prints=True)
def download_csv(url_to_csv, output_file_name):
    """Function to download the csv file from remote"""
    os.system(f"curl -L -o {output_file_name} {url_to_csv}")
    os.system(f"gzip --decompress {output_file_name}")

 
@task(log_prints=True)
def transform_data(df):
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    
    df = df[df['passenger_count'] != 0]
    
    print(f"post:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    
    return df

@flow(name="Dump To DB Subflow", description="Dumps the transformed data into the Database")
def dump_to_db(table_name, filename):
    
    connection_block = SqlAlchemyConnector.load("pg-sql-connector")
    
    with connection_block.get_connection(begin=False) as db_engine:
        df = pd.read_csv(filename, nrows=100000)
        
        # create table
        df.head(n=0).to_sql(table_name, con=db_engine, if_exists='replace')
        
        df_iter = pd.read_csv(filename, iterator=True, chunksize=100000)

        # Iteratively dump the csv rows to the Database
        while True:
            try:
                t_start = time()
                
                df = next(df_iter)
                
                transformed_df = transform_data(df)
                
                transformed_df.tpep_pickup_datetime = pd.to_datetime(transformed_df.tpep_pickup_datetime)
                transformed_df.tpep_dropoff_datetime = pd.to_datetime(transformed_df.tpep_dropoff_datetime)

                transformed_df.to_sql(table_name, con=db_engine, if_exists='append')
                
                t_end = time()
                print('inserted another chunk... took %.3f second' % (t_end - t_start))
            
            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break
   
@flow(name="Ingest Flow")
def main_flow():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tbl_name", help="name of the destination table")
    parser.add_argument("--csv_url", help="url of the csv file")
    parser.add_argument("--output", help="name of the downloaded csv file")

    args = parser.parse_args()
    filename = '.'.join(args.output.split('.')[:-1])

    download_csv(args.csv_url, args.output)
    
    dump_to_db(args.tbl_name, filename)

if(__name__ == "__main__"):
    main_flow()