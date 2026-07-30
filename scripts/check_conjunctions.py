#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col, sqrt, pow, lag, when, dense_rank, min as spark_min, min_by, max as spark_max, broadcast, sum as spark_sum
from pyspark.sql.window import Window
import logging
import os
from datetime import datetime
import argparse

# --- CLI args ---
parser = argparse.ArgumentParser(description="Conjunction Check")
parser.add_argument("--input", type=str, required=True,
                    help="HDFS path for positions parquet")
parser.add_argument("--tle", type=str, required=True,
                    help="HDFS path for TLE parquet")
parser.add_argument("--output", type=str, required=True,
                    help="HDFS path to save conjunction results")
args = parser.parse_args()

# --- Logging setup ---
today = datetime.now().strftime("%d-%m-%Y")
log_dir = os.path.expanduser("~/bda_proj/logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"conjunctionlog_{today}.log")

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
    .appName("ConjunctionCheck") \
    .config("spark.sql.shuffle.partitions", "300") \
    .getOrCreate()

# --- Configuration ---
DISTANCE_THRESHOLD_KM = 5    # Initial proximity filter
VELOCITY_THRESHOLD_KM_S = 0.1  # To filter only meaningful encounters
TIME_WINDOW_MINUTES = 60        # Cluster within 1 hour

logger.info(f"Reading position data from {args.input} ...")
positions = spark.read.parquet(args.input).withColumn("timestamp", col("timestamp").cast("timestamp"))
logger.info(f"Total position records: {positions.count()}")

logger.info(f"Reading TLE data from {args.tle} ...")
tle_df = spark.read.parquet(args.tle).select("satnum", "name")

# --- Aliases for self-join ---
p1 = positions.alias("p1")
p2 = positions.alias("p2")

# --- STEP 1: Self-join on timestamp, different satellites ---
logger.info("Joining satellite positions on timestamp...")
joined = p1.join(
    p2,
    (col("p1.timestamp") == col("p2.timestamp")) &
    (col("p1.satnum") < col("p2.satnum")) &
    ~(
        (col("p1.object_type").contains("DEB") | col("p1.object_type").contains("DEBRIS")) &
        (col("p2.object_type").contains("DEB") | col("p2.object_type").contains("DEBRIS"))
    )
)

# --- STEP 2: Compute Euclidean distance ---
logger.info("Computing pairwise distances...")
with_distance = joined.withColumn(
    "distance_km",
    sqrt(
        pow(col("p1.x_km") - col("p2.x_km"), 2) +
        pow(col("p1.y_km") - col("p2.y_km"), 2) +
        pow(col("p1.z_km") - col("p2.z_km"), 2)
    )
)

# --- STEP 3: Filter pairs within distance threshold ---
logger.info("Filtering by distance and velocity thresholds...")
potential_conjunctions = with_distance.filter(
    (col("distance_km") < DISTANCE_THRESHOLD_KM)
).select(
    col("p1.satnum").alias("sat1"),
    col("p1.object_type").alias("sat1_object_type"),
    col("p2.satnum").alias("sat2"),
    col("p2.object_type").alias("sat2_object_type"),
    col("p1.timestamp").alias("timestamp"),
    col("p1.vx_km_s").alias("sat1_vx"),
    col("p1.vy_km_s").alias("sat1_vy"),
    col("p1.vz_km_s").alias("sat1_vz"),
    col("p2.vx_km_s").alias("sat2_vx"),
    col("p2.vy_km_s").alias("sat2_vy"),
    col("p2.vz_km_s").alias("sat2_vz"),
    col("distance_km")
)

# --- STEP 4: Compute relative velocity ---
logger.info("Computing relative velocities for filtered pairs...")
with_velocity = potential_conjunctions.withColumn(
    "rel_velocity_km_s",
    sqrt(
        pow(col("sat1_vx") - col("sat2_vx"), 2) +
        pow(col("sat1_vy") - col("sat2_vy"), 2) +
        pow(col("sat1_vz") - col("sat2_vz"), 2)
    )
)

# --- STEP 5: Apply velocity threshold ---
logger.info("Filtering by velocity threshold...")
close_approaches = with_velocity.filter(
    col("rel_velocity_km_s") > VELOCITY_THRESHOLD_KM_S
).select(
    "sat1", "sat2", "timestamp", "distance_km", "rel_velocity_km_s",
    "sat1_object_type", "sat2_object_type"
)

# --- STEP 6: Add satellite names ---
logger.info("Adding satellite names...")
close_with_names = close_approaches \
    .join(tle_df.select(col("satnum").alias("sat1"), col("name").alias("sat1_name")), on="sat1") \
    .join(tle_df.select(col("satnum").alias("sat2"), col("name").alias("sat2_name")), on="sat2")

# --- STEP 7: Cluster conjunctions over time ---
logger.info("Clustering conjunctions over time...")
window_sort = Window.partitionBy("sat1", "sat2").orderBy("timestamp")
cluster_window = window_sort.rowsBetween(Window.unboundedPreceding, Window.currentRow)

clustered = close_with_names.withColumn(
    "prev_timestamp", lag(col("timestamp")).over(window_sort)
).withColumn(
    "time_gap_minutes",
    when(
        col("prev_timestamp").isNotNull(),
        (col("timestamp").cast("long") - col("prev_timestamp").cast("long")) / 60
    ).otherwise(0)
).withColumn(
    "new_cluster",
    when(col("prev_timestamp").isNull() | (col("time_gap_minutes") > TIME_WINDOW_MINUTES), 1).otherwise(0)
).withColumn(
    "cluster_id",
    spark_sum("new_cluster").over(cluster_window)
)

# --- STEP 8: Aggregate per conjunction cluster ---
conjunction_events = clustered.groupBy(
    "sat1", "sat2", "sat1_name", "sat2_name", "sat1_object_type", "sat2_object_type", "cluster_id"
).agg(
    min_by("timestamp", "distance_km").alias("closest_approach_time"),
    spark_min("distance_km").alias("min_distance_km"),
    spark_max("rel_velocity_km_s").alias("max_relative_velocity_km_s")
).drop("cluster_id")

# --- STEP 9: Rank by severity ---
severity_window = Window.orderBy("min_distance_km")
conjunctions = conjunction_events.withColumn(
    "severity_rank",
    dense_rank().over(severity_window)
).select(
    col("sat1").alias("obj1"),
    col("sat1_name").alias("obj1_name"),
    col("sat1_object_type").alias("obj1_type"),
    col("sat2").alias("obj2"),
    col("sat2_name").alias("obj2_name"),
    col("sat2_object_type").alias("obj2_type"),
    col("closest_approach_time"),
    col("min_distance_km"),
    col("max_relative_velocity_km_s"),
    col("severity_rank")
)

# --- STEP 9.1: Enrich with country information ---
# Placeholder path for SATCAT CSV; replace with actual path
SATCAT_PATH = "hdfs:///user/bda/satcat/satcat.csv"  

logger.info(f"Reading SATCAT data from {SATCAT_PATH} ...")
satcat_df = spark.read.csv(SATCAT_PATH, header=True, inferSchema=True).select(
    col("NORAD_CAT_ID").alias("satnum"), col("OWNER").alias("country")
)

# Broadcast if small for efficiency
satcat_b = broadcast(satcat_df)

# Join for obj1 country
conjunctions = conjunctions.join(
    satcat_b.withColumnRenamed("satnum", "obj1"),
    on="obj1",
    how="left"
).withColumnRenamed("country", "obj1_country")

# Join for obj2 country
conjunctions = conjunctions.join(
    satcat_b.withColumnRenamed("satnum", "obj2"),
    on="obj2",
    how="left"
).withColumnRenamed("country", "obj2_country")

# --- STEP 10: Save results ---
logger.info(f"Saving conjunction analysis results to {args.output} ...")
conjunctions.persist(StorageLevel.MEMORY_AND_DISK)
conjunctions.write.mode("overwrite").parquet(args.output)

logger.info(f"Total unique conjunction events: {conjunctions.count()}")
logger.info("Top 20 closest approaches:")
conjunctions.orderBy("min_distance_km").limit(20).show(20, truncate=False)
conjunctions.unpersist()

spark.stop()
