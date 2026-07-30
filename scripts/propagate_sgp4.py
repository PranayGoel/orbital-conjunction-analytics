#!/usr/bin/env python3

import argparse
import logging
import math
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col
from pyspark import StorageLevel
from sgp4.api import Satrec, jday
import os

# --- CLI arguments ---
parser = argparse.ArgumentParser(description="Propagate TLEs using SGP4")
parser.add_argument("--input", required=True, help="Path to input TLE parquet folder (HDFS path)")
parser.add_argument("--output", required=True, help="Path to output propagated positions (HDFS path)")
parser.add_argument("--days", type=int, default=7, help="Duration in days (default: 7)")
parser.add_argument("--step", type=int, default=10, help="Time step in minutes (default: 10)")
args = parser.parse_args()

# --- Logging ---
today = datetime.now().strftime("%d-%m-%Y")
log_dir = os.path.expanduser("~/bda_proj/logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"propagationlog_{today}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Spark init ---
spark = SparkSession.builder \
    .appName("SGP4 Propagation") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# --- Read processed TLEs ---
logger.info(f"Reading TLE data from {args.input} ...")
tle_df = spark.read.parquet(args.input)
num_sats = tle_df.count()
logger.info(f"Total satellites: {num_sats}")

# --- Time params ---
TIME_STEP_MIN = args.step
DURATION_DAYS = args.days
start_time = datetime.now(timezone.utc)
end_time = start_time + timedelta(days=DURATION_DAYS)

timestamps = []
current = start_time
while current <= end_time:
    timestamps.append(current)
    current += timedelta(minutes=TIME_STEP_MIN)

num_timesteps = len(timestamps)
logger.info(f"Total timesteps: {num_timesteps} (from {start_time} to {end_time})")
logger.info(f"Expected total records: {num_sats * num_timesteps}")

# Broadcast timestamps
broadcast_timestamps = spark.sparkContext.broadcast(timestamps)

# --- Schema ---
positions_schema = StructType([
    StructField("satnum", LongType(), False),
    StructField("name", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("x_km", DoubleType(), False),
    StructField("y_km", DoubleType(), False),
    StructField("z_km", DoubleType(), False),
    StructField("vx_km_s", DoubleType(), False),
    StructField("vy_km_s", DoubleType(), False),
    StructField("vz_km_s", DoubleType(), False)
])

# --- Propagation function ---
def propagate_row(row):
    sat = Satrec.twoline2rv(row['line1'], row['line2'])
    out = []
    for ts in broadcast_timestamps.value:
        try:
            jd, fr = jday(ts.year, ts.month, ts.day, ts.hour, ts.minute,
                          ts.second + ts.microsecond * 1e-6)
            e, r, v = sat.sgp4(jd, fr)
            if e != 0:
                continue

            if any(math.isnan(x) or math.isinf(x) for x in r + v):
                continue

            distance = math.sqrt(r[0]**2 + r[1]**2 + r[2]**2)
            if distance < 6371:
                continue

            out.append(Row(
                satnum=row['satnum'],
                name=row['name'],
                object_type=row['object_type'],
                timestamp=ts.isoformat(),
                x_km=float(r[0]),
                y_km=float(r[1]),
                z_km=float(r[2]),
                vx_km_s=float(v[0]),
                vy_km_s=float(v[1]),
                vz_km_s=float(v[2])
            ))
        except Exception:
            continue
    return out

# --- Start propagation ---
logger.info("Starting SGP4 propagation...")
positions_rdd = tle_df.rdd.flatMap(propagate_row)
positions_df = spark.createDataFrame(positions_rdd, schema=positions_schema)
positions_df = positions_df.repartition(200, col("satnum"))
positions_df.persist(StorageLevel.MEMORY_AND_DISK)

# --- Write to HDFS ---
logger.info(f"Writing results to {args.output} ...")
positions_df.write.mode("overwrite").parquet(args.output)

total_positions = positions_df.count()
logger.info(f"✓ Propagation complete: {total_positions} position records saved")
logger.info(f"Average records per satellite: {total_positions / num_sats:.0f}")
positions_df.unpersist()

spark.stop()
logger.info("Spark job finished successfully.")
logger.info(f"Log saved to {log_file}")
