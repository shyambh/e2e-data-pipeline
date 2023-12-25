"""
Ingest the NY Taxi Data 2022 to Postgres
"""
from pathlib import Path
from time import time
import argparse
import os
import pandas as pd
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_gcp.cloud_storage import GcsBucket, DataFrameSerializationFormat
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def download_csv(taxi_color: str, year: str, month: str, output_dir: str) -> str:
    """Function to download the csv file from remote"""
    url_to_csv = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_color}/{taxi_color}_tripdata_{year}-{month}.csv.gz"

    os.makedirs(f"{output_dir}/{taxi_color}_taxi/{year}/{month}", exist_ok=True)

    path = Path(f"{output_dir}/{taxi_color}_taxi/{year}/{month}")

    os.system(f"curl -L -o ./{path}/trip_data.gz.csv {url_to_csv}")

    return f"{path}/trip_data.gz.csv"


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
        df = pd.read_csv(filename, nrows=100000, compression="gzip")

        # create table
        df.head(n=0).to_sql(table_name, con=db_engine, if_exists="replace")

        df_iter = pd.read_csv(
            filename, iterator=True, chunksize=100000, compression="gzip"
        )

        # Iteratively dump the csv rows to the Database
        while True:
            try:
                t_start = time()

                df = next(df_iter)

                transformed_df = transform_data(df)

                if (
                    "tpep_pickup_datetime" in transformed_df
                    and "tpep_dropoff_datetime" in transformed_df
                ):
                    transformed_df.tpep_pickup_datetime = pd.to_datetime(
                        transformed_df.tpep_pickup_datetime
                    )
                    transformed_df.tpep_dropoff_datetime = pd.to_datetime(
                        transformed_df.tpep_dropoff_datetime
                    )

                transformed_df.to_sql(table_name, con=db_engine, if_exists="append")

                t_end = time()
                print(f"inserted another chunk... took {t_end - t_start:.3f} second")

            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break


@flow(
    name="Store to Cloud",
    description="Upload the csv file in the parquet format in GCP cloud bucket & BigQuery",
)
def store_to_cloud(table_name: str, path: Path, dataset: str) -> None:
    """Load the first 100,000 records from the Database into GCS Bucket & BigQuery"""
    sql_connection_block = SqlAlchemyConnector.load("pg-sql-connector")
    gcp_bucket_block = GcsBucket.load("gcp-zoomcamp-bucket")
    gcp_credentials_block = GcpCredentials.load("gcp-de-zoomcamp-creds")

    # Read the data from database and export to parquet file
    with sql_connection_block.get_connection() as sql_con:
        df = pd.read_sql_table(table_name, con=sql_con, chunksize=100000)

        df_trimmed = next(df)
        df_trimmed.to_parquet(
            f"./{path}/downloaded_from_gcs.parquet",
            compression="gzip",
        )

    # Store the local parquet file to GCP Bucket
    gcp_bucket_block.upload_from_dataframe(
        df_trimmed,
        to_path=f"{path}/output.parquet",
        serialization_format=DataFrameSerializationFormat.PARQUET_GZIP,
    )

    # Store the local parquet file to BigQuery
    df_trimmed.to_gbq(
        f"{dataset}.taxi_data",
        "data-eng-practice007",
        chunksize=100_000,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
    )

    # Download the uploaded parquet file to local
    # this is redundant. But doing this for practice
    download_from_gcs_to_local(path)


@task(name="Download file from GCS to local")
def download_from_gcs_to_local(path: str) -> None:
    """Download the uploaded parquet file from GCS"""
    gcp_bucket_block = GcsBucket.load("gcp-zoomcamp-bucket")
    gcp_bucket_block.get_directory(
        f"{path}/output.gz.parquet",
        f".sample_data/downloaded/{path}/output.gz.parquet",
    )


@flow(name="Ingest Flow")
def main_flow():
    """The main flow which is the master flow"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", help="name of the downloaded csv file")
    parser.add_argument("--taxi_color", help="color of the taxi")
    parser.add_argument("--year", help="year of the taxi data")
    parser.add_argument("--month", help="month of the taxi data")

    args = parser.parse_args()

    taxi_color = args.taxi_color
    year = args.year
    month = args.month

    table_name = f"{taxi_color}-{year}-{month}"

    # Download the csv from the remote URL
    filename = download_csv(taxi_color, year, month, args.output_dir)

    # Dump the downloaded csv into the Database
    dump_to_db(table_name, filename)

    # Store the local data to GCS Bucket and BigQuery
    path = Path("/".join(filename.split("/")[:-1]))
    dataset = f"{taxi_color}_taxi_tripdata_{year}_{month}"
    store_to_cloud(table_name, path, dataset)


if __name__ == "__main__":
    main_flow()
