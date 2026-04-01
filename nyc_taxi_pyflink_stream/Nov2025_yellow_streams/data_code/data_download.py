"""Download and persist a deterministic parquet subset for streaming demos.

Deterministic behavior:
- Reads exactly the requested `columns` from the source parquet URL.
- Writes output to a fixed local path used by the producer script.
"""

import pandas as pd


def download_data(url, columns):
    """Download parquet data from `url` and save selected columns locally.

    Deterministic behavior:
    - Always reads only the columns specified in `columns` — no extra fields
      are downloaded or persisted.
    - Always writes the output to a fixed relative path:
        ../nyc_taxi_pyflink_stream/data/downloaded/yellow_tripdata_2025-11.parquet
    - Output file will always have the same schema (column names and dtypes)
      as the subset of the source parquet file matching `columns`.
    - Does not filter, shuffle, or transform rows — all rows from the source
      are written as-is to the local file.

    Args:
        url:     URL of the remote parquet file to download.
        columns: List of column names to select from the source file.

    Side effects:
        Writes a parquet file to the fixed local output path.
        Prints progress messages to stdout.
    """

    print(f"Downloading data from {url}...")
    # Only fetch the requested columns to minimize download size.
    df = pd.read_parquet(url, columns=columns)

    # Fixed output path consumed by producer.py — must not change.
    output_path = "../nyc_taxi_pyflink_stream/data/downloaded/yellow_tripdata_2025-11.parquet"
    df.to_parquet(output_path)
    print(f"Data downloaded and saved to {output_path}")


if __name__ == "__main__":
    # Fixed source URL and column selection for the November 2025 NYC taxi dataset.
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
    # These 5 columns form the fixed schema expected by data_cleanup.py and producer.py.
    columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
    download_data(url, columns)
