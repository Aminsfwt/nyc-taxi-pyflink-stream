"""
Kafka producer for streaming cleaned NYC taxi rides.

Deterministic behavior:
- Reads rides from a fixed local parquet file path resolved relative to this file.
- Serializes each ride dataclass as JSON bytes with a fixed schema.
- Publishes messages to topic `rides` on `localhost:9092`.
- Sends one message every 0.01 seconds to simulate a live stream at a fixed pace.
"""

import json
import os
import sys
from kafka import KafkaProducer
import dataclasses
import time

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

# Add the project root (folder containing `nyc_taxi_pyflink_stream`) to PYTHONPATH
# so that the `nyc_taxi_pyflink_stream` package can be imported regardless of
# the current working directory when the script is launched.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
import nyc_taxi_pyflink_stream.data.data_code.data_cleanup as dc

# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------

def json_serializer(data):
    """Serialize a dictionary-like object into UTF-8 JSON bytes.

    Deterministic behavior:
    - Always uses json.dumps with default settings (no sorting, no indentation).
    - Always encodes the result as UTF-8 bytes.

    Args:
        data: Any JSON-serializable Python object (dict, list, etc.).

    Returns:
        UTF-8 encoded JSON bytes.
    """
    return json.dumps(data).encode('utf-8')


def ride_serializer(ride):
    """Serialize a Ride dataclass into UTF-8 JSON bytes.

    Deterministic behavior:
    - Always converts the Ride dataclass to a dict using dataclasses.asdict(),
      which produces the same fixed set of keys in the same order every time:
      PULocationID, DOLocationID, trip_distance, total_amount, tpep_pickup_datetime.
    - Always encodes the resulting JSON string as UTF-8 bytes.
    - Field values are serialized as their native Python types (int, float).

    Args:
        ride: A Ride dataclass instance.

    Returns:
        UTF-8 encoded JSON bytes representing the ride.
    """
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')


# ---------------------------------------------------------------------------
# Kafka producer setup
# ---------------------------------------------------------------------------

# Fixed broker address — must match the Redpanda/Kafka service on localhost.
server = 'localhost:9092'

# Final producer used for all message publishing.
# Deterministic behavior: always connects to `server` and uses `ride_serializer`
# to encode every message value before sending.
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

# Fixed topic name consumed by the Flink job downstream.
topic_name = 'rides'

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

# Resolve the parquet file path relative to this script's location so the
# script works correctly regardless of the working directory it is launched from.
data_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../downloaded/yellow_tripdata_2025-11.parquet",
    )
)

if not os.path.exists(data_path):
    raise FileNotFoundError(f"Parquet file not found: {data_path}")

# DataFrame generated from the deterministic cleanup stage.
# Always reads all rows and all columns from the fixed parquet file.
df = dc.data_cleanup(data_path)

# ---------------------------------------------------------------------------
# Streaming loop
# ---------------------------------------------------------------------------

t0 = time.time()

# Iterate every row in the DataFrame and publish one Ride message per row.
# Deterministic behavior:
# - Rows are sent in the order they appear in the DataFrame (no shuffling).
# - Each row is converted to a Ride via dc.ride_from_row() before sending.
# - A fixed sleep of 0.01 seconds is applied between each message to simulate
#   a real-time stream at ~100 messages/second.
for _, row in df.iterrows():
    ride = dc.ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
    time.sleep(0.01)  # Fixed pacing interval — do not change without updating Flink watermarks.

# Flush ensures all buffered messages are delivered before the script exits.
producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
