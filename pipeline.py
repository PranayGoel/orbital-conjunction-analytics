#!/usr/bin/env python3

import subprocess
import logging
import os
from datetime import datetime

# --- Logging setup ---
today = datetime.now().strftime("%d-%m-%Y")
log_dir = os.path.expanduser("~/bda_proj/logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"pipeline_{today}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Define HDFS paths ---
raw_data_hdfs = f"hdfs:///user/bda/tle/raw/{today}"
processed_hdfs = f"hdfs:///user/bda/tle/processed/{today}"
positions_hdfs = f"hdfs:///user/bda/tle/positions/{today}"
conjunctions_hdfs = f"hdfs:///user/bda/tle/conjunctions/{today}"

logger.info("HDFS paths for today:")
logger.info(f"Raw data: {raw_data_hdfs}")
logger.info(f"Processed TLE: {processed_hdfs}")
logger.info(f"Propagated positions: {positions_hdfs}")
logger.info(f"Conjunctions: {conjunctions_hdfs}")

# --- Helper to run scripts ---
def run_script(cmd_list, step_name):
    logger.info(f"--- Running {step_name} ---")
    try:
        subprocess.run(cmd_list, check=True)
        logger.info(f"✓ {step_name} completed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ {step_name} failed with exit code {e.returncode}")
        exit(1)
        
        
spark_home = os.environ.get("SPARK_HOME", "/usr/local/spark")
spark_submit = os.path.join(spark_home, "bin", "spark-submit")
    
# --- Step 0: Download latest TLEs ---
run_script([
    "python3", "scripts/download_tle.py",
    "--output_hdfs", raw_data_hdfs
], "Download TLEs")

# --- Step 1: Parse TLE ---
run_script([
    spark_submit,
    "--master", "spark://master:7077",
    "scripts/parse_tle.py",
    "--input", raw_data_hdfs,
    "--output", processed_hdfs
], "TLE Parsing")

# --- Step 2: Propagate SGP4 ---
run_script([
    spark_submit,
    "--master", "spark://master:7077",
    "scripts/propagate_sgp4.py",
    "--input", processed_hdfs,
    "--output", positions_hdfs
], "SGP4 Propagation")


# --- Step 3: Check Conjunctions ---
run_script([
    spark_submit,
    "--master", "spark://master:7077",
    "scripts/check_conjunctions.py",
    "--input", positions_hdfs,
    "--tle", processed_hdfs,
    "--output", conjunctions_hdfs
], "Conjunction Check")

# --- Step 4: Update HDFS 'latest' symlink for conjunctions ---
logger.info("Updating 'latest' marker file for conjunctions...")

latest_marker = "/user/bda/tle/latest.txt"

try:
    # Write the current folder path into latest.txt on HDFS
    subprocess.run([
        "hdfs", "dfs", "-rm", "-f", latest_marker
    ], check=False)

    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", "-", latest_marker],
        input=f"{conjunctions_hdfs}\n",
        text=True,
        check=True
    )

    logger.info(f"✓ Updated marker file to {conjunctions_hdfs}")
except subprocess.CalledProcessError as e:
    logger.error(f"✗ Failed to update 'latest' marker file: {e}")
    
logger.info("=== Pipeline finished successfully ===")
