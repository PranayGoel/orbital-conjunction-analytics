#!/usr/bin/env python3

from pyspark.sql import SparkSession
import pandas as pd
from sgp4.api import Satrec
import os
import logging
import argparse
from datetime import datetime

# --- Logging ---
today = datetime.now().strftime("%d-%m-%Y")
log_dir = os.path.expanduser("~/bda_proj/logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"parserlog_{today}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Argparse ---
parser = argparse.ArgumentParser(description="Parse TLE files and save to HDFS with daily folder structure.")
parser.add_argument("--input", required=True, help="Local folder containing TLE files")
parser.add_argument("--output", required=True, help="Base HDFS output folder (no date suffix)")
args = parser.parse_args()

# --- Date-based output folder ---
today = datetime.now().strftime("%d-%m-%Y")
output_path = args.output

# --- Spark init ---
spark = SparkSession.builder.appName("TLE Parser").getOrCreate()

# --- File paths ---
tle_folder = os.path.expanduser(args.input)

sat_tle_files = ["active_gp.tle"]
debris_tle_files = [
    "cosmos1408_debris.tle",
    "cosmos2251_debris.tle",
    "fengyun1c_debris.tle"
]

# --- Helper: classify from TLE name ---
def classify_object_type(name: str) -> str:
    n = name.upper()
    if "DEB" in n:
        return "DEBRIS"
    elif "R/B" in n or "ROCKET" in n:
        return "ROCKET_BODY"
    elif any(marker in n for marker in ["SAT", "COM", "NOAA", "ISS", "HUBBLE"]):
        return "SATELLITE"
    else:
        return "UNKNOWN"


# --- Parser ---
def parse_tle(lines, object_type_hint: str = "UNKNOWN", source_file: str = ""):
    results = []
    skipped = 0
    i = 0

    while i < len(lines):
        if i + 2 >= len(lines):
            logger.warning(f"Incomplete TLE group at line {i}, skipping")
            break

        name = lines[i].strip()
        line1 = lines[i + 1].strip()
        line2 = lines[i + 2].strip()

        if not line1.startswith("1 "):
            i += 1
            skipped += 1
            continue
        if not line2.startswith("2 "):
            i += 3
            skipped += 1
            continue

        try:
            sat = Satrec.twoline2rv(line1, line2)
            obj_type = classify_object_type(name)
            if obj_type == "UNKNOWN" and object_type_hint != "UNKNOWN":
                obj_type = object_type_hint

            results.append({
                "name": name,
                "satnum": sat.satnum,
                "object_type": obj_type,
                "line1": line1,
                "line2": line2
            })
        except Exception as e:
            logger.debug(f"Skipped invalid TLE at line {i}: {str(e)}")
            skipped += 1

        i += 3

    logger.info(f"Parsed {len(results)} TLEs ({object_type_hint}), skipped {skipped} from {source_file}")
    return results


# --- Read and parse all satellite TLE files ---
all_parsed = []

for f in sat_tle_files:
    hdfs_fp = os.path.join(tle_folder, f)
    try:
        lines = spark.sparkContext.textFile(hdfs_fp).collect()  # read all lines from HDFS file
        lines = [l.strip() for l in lines if l.strip()]
        all_parsed.extend(parse_tle(lines, object_type_hint="SATELLITE", source_file=f))
    except Exception as e:
        logger.warning(f"Failed to read {hdfs_fp}: {e}")

# --- Read and parse all debris TLE files from HDFS ---
for f in debris_tle_files:
    hdfs_fp = os.path.join(tle_folder, f)
    try:
        lines = spark.sparkContext.textFile(hdfs_fp).collect()
        lines = [l.strip() for l in lines if l.strip()]
        all_parsed.extend(parse_tle(lines, object_type_hint="DEBRIS", source_file=f))
    except Exception as e:
        logger.warning(f"Failed to read {hdfs_fp}: {e}")

# --- Convert to DataFrames ---
if len(all_parsed) == 0:
    logger.error("No TLEs parsed. Exiting.")
    spark.stop()
    exit(1)

pdf = pd.DataFrame(all_parsed)
logger.info(f"Parsed DataFrame shape: {pdf.shape}")

sdf = spark.createDataFrame(pdf)

# --- Save to HDFS ---
logger.info(f"Saving to {output_path}")
sdf.write.mode("overwrite").parquet(output_path)

logger.info(f"✓ Saved {len(all_parsed)} TLEs to {output_path}")

# --- Optional: verify ---
try:
    df_check = spark.read.parquet(output_path)
    logger.info(f"Verification: {df_check.count()} rows written successfully")
except Exception as e:
    logger.warning(f"Verification failed: {e}")

spark.stop()

