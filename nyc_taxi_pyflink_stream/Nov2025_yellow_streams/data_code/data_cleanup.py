"""Data cleanup utilities for taxi ride stream preparation.

Deterministic behavior:
- `data_cleanup` always reads the parquet file from the provided path and returns
  a pandas DataFrame.
- `ride_from_row` always maps one DataFrame row into a `Ride` dataclass using a
  fixed schema and converts pickup time to epoch milliseconds.
"""

import pandas as pd
from dataclasses import dataclass


@dataclass
class Ride:
    """Typed representation of one taxi ride message sent to Kafka.

    Deterministic behavior:
    - Field types are always cast explicitly: PULocationID and DOLocationID as int,
      trip_distance and total_amount as float, tpep_pickup_datetime as int (epoch ms).
    - Field order is fixed and matches the Kafka message schema consumed by Flink.
    """

    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds


def ride_from_row(row):
    """Convert a DataFrame row into a `Ride` instance.

    Deterministic behavior:
    - Always reads the same 5 fixed columns: PULocationID, DOLocationID,
      trip_distance, total_amount, tpep_pickup_datetime.
    - tpep_pickup_datetime is always converted to epoch milliseconds using
      pandas Timestamp.timestamp() * 1000, then cast to int.
    - All numeric fields are explicitly cast to their target Python types
      (int or float) regardless of the source dtype in the DataFrame.

    Args:
        row: A pandas Series representing a single DataFrame row.

    Returns:
        A Ride dataclass instance with typed fields.
    """

    return Ride(
        PULocationID=int(row["PULocationID"]),
        DOLocationID=int(row["DOLocationID"]),
        trip_distance=float(row["trip_distance"]),
        total_amount=float(row["total_amount"]),
        # Convert pandas Timestamp to Unix epoch milliseconds (int).
        # Flink expects event-time fields in millisecond precision.
        tpep_pickup_datetime=int(row["tpep_pickup_datetime"].timestamp() * 1000),
    )


def data_cleanup(path):
    """Read parquet data from `path` and return it as a DataFrame.

    Deterministic behavior:
    - Always reads all rows and all columns from the parquet file at `path`.
    - Returns a pandas DataFrame with the same schema as the source file.
    - Does not filter, sort, or mutate rows — output is a direct reflection
      of the parquet file contents.

    Args:
        path: Absolute or relative path to the local parquet file.

    Returns:
        A pandas DataFrame containing all rows and columns from the file.
    """

    print(f"Reading data from {path}...")
    data = pd.read_parquet(path)
    print("Data read successfully:")
    return data


if __name__ == "__main__":
    # Entry point for manual testing. Path is fixed relative to this file's location.
    path = "../nyc_taxi_pyflink_stream/data/downloaded/yellow_tripdata_2025-11.parquet"
    data_cleanup(path)
