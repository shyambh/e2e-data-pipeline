"""
Ingest the NY Taxi Data 2022 to Postgres
"""
from time import time
import argparse
import os
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    username = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.db
    table_name = params.tbl_name
    url_to_csv = params.csv_url
    output_file_name = params.output
    
    # Download the csv file from the url 
    os.system(f"curl -L -o {output_file_name} {url_to_csv}")
    
    os.system(f"gzip --decompress {output_file_name}")
    
    filename = output_file_name.replace(".gz","")
    
    # Establish a connection to the database
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
    df = pd.read_csv(filename, nrows=100000)

    # Print out the create table query. Just for info.
    print(pd.io.sql.get_schema(df, table_name, con=engine))

    # create table
    df.head(n=0).to_sql(table_name, con=engine, if_exists='replace')
    
    # Iteratively insert the csv rows into the DB in a batchsize of 100000
    df_iter = pd.read_csv(filename, iterator=True, chunksize=100000)

    # This while loop throws an error in the last iteration since the last iteration has < 100000 rows to input. 
    # todo: refactor the code to handle this error.
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(table_name, con=engine, if_exists='append')
            
            t_end = time()
            print('inserted another chunk... took %.3f second' % (t_end - t_start))
        
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if(__name__ == "__main__"):
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

    main(args)