"""
Ingest the NY Taxi Data 2022 to Postgres
"""
from time import time
import argparse
import os
import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_gcp.cloud_storage import GcsBucket, DataFrameSerializationFormat
from prefect_gcp import GcpCredentials


def get_filename(filename):
    return ".".join(filename.split(".")[:-1])


@task(log_prints=True)
def download_csv(url_to_csv, output_file_name):
    """Function to download the csv file from remote"""
    os.system(f"curl -L -o {output_file_name} {url_to_csv}")
    os.system(f"gzip --decompress --force {output_file_name}")


@task(name="", log_prints=True)
def transform_data(df):
    """Transform the data before dumping in the DB"""
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    df = df[df["passenger_count"] != 0]

    print(f"post:missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df


@flow(
    name="Dump To DB Subflow",
    description="Dumps the transformed data into the Database",
)
def dump_to_db(table_name, filename):
    connection_block = SqlAlchemyConnector.load("pg-sql-connector")

    with connection_block.get_connection(begin=False) as db_engine:
        df = pd.read_csv(filename, nrows=100000)

        # create table
        df.head(n=0).to_sql(table_name, con=db_engine, if_exists="replace")

        df_iter = pd.read_csv(filename, iterator=True, chunksize=100000)

        # Iteratively dump the csv rows to the Database
        while True:
            try:
                t_start = time()

                df = next(df_iter)

                transformed_df = transform_data(df)

                transformed_df.tpep_pickup_datetime = pd.to_datetime(
                    transformed_df.tpep_pickup_datetime
                )
                transformed_df.tpep_dropoff_datetime = pd.to_datetime(
                    transformed_df.tpep_dropoff_datetime
                )

                transformed_df.to_sql(table_name, con=db_engine, if_exists="append")

                t_end = time()
                print("inserted another chunk... took %.3f second" % (t_end - t_start))

            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break


@flow(
    name="Store to Cloud",
    description="Upload the csv file in the parquet format in GCP cloud bucket & BigQuery",
)
def store_to_cloud() -> None:
    """Load the first 100,000 records into GCS Bucket & BigQuery"""
    sql_connection_block = SqlAlchemyConnector.load("pg-sql-connector")
    gcp_bucket_block = GcsBucket.load("gcp-zoomcamp-bucket")
    gcp_credentials_block = GcpCredentials.load("gcp-de-zoomcamp-creds")

    with sql_connection_block.get_connection() as sql_con:
        df = pd.read_sql_table("yellow_taxi_table", con=sql_con, chunksize=100000)

        df_trimmed = next(df)
        df_trimmed.to_parquet("./.sample_data/output.parquet", compression="gzip")

    df_trimmed.to_gbq(
        "ny_taxi_data_2021.rides",
        "data-eng-practice007",
        chunksize=100_000,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
    )
    gcp_bucket_block.upload_from_dataframe(
        df_trimmed,
        to_path="data/output.parquet",
        serialization_format=DataFrameSerializationFormat.PARQUET_GZIP,
    )

    download_from_gcs_to_local()


@task(name="Download file from GCS to local")
def download_from_gcs_to_local():
    """Download the uploaded parquet file from GCS"""
    gcp_bucket_block = GcsBucket.load("gcp-zoomcamp-bucket")
    gcp_bucket_block.get_directory(
        "data/output.gz.parquet",
        ".sample_data/downloaded/data/output.gz.parquet",
    )


@flow(name="Ingest Flow")
def main_flow():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tbl_name", help="name of the destination table")
    parser.add_argument("--csv_url", help="url of the csv file")
    parser.add_argument("--output", help="name of the downloaded csv file")

    args = parser.parse_args()
    filename = ".".join(args.output.split(".")[:-1])

    download_csv(args.csv_url, args.output)

    dump_to_db(args.tbl_name, filename)

    store_to_cloud()


if __name__ == "__main__":
    main_flow()
