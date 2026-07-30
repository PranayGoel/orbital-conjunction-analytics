#!/usr/bin/env python3

import requests
import os
import subprocess
import logging
from datetime import datetime
import argparse

# --- CLI args ---
parser = argparse.ArgumentParser(description="Download latest TLE files and upload to HDFS")
parser.add_argument("--output_hdfs", type=str, required=True, help="HDFS folder to upload TLEs")
args = parser.parse_args()

# --- Logging ---
today = datetime.now().strftime("%d-%m-%Y")
log_dir = os.path.expanduser("~/bda_proj/logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"download_tle_{today}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- URLs and filenames ---
tle_sources = {
    "active_gp.tle": "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle",
    "cosmos1408_debris.tle": "https://celestrak.org/NORAD/elements/gp.php?GROUP=cosmos-1408-debris&FORMAT=tle",
    "cosmos2251_debris.tle": "https://celestrak.org/NORAD/elements/gp.php?GROUP=cosmos-2251-debris&FORMAT=tle",
    "fengyun1c_debris.tle": "https://celestrak.org/NORAD/elements/gp.php?GROUP=fengyun-1c-debris&FORMAT=tle",
}

# --- Local folder ---
local_folder = os.path.expanduser(f"~/bda_proj/tle/{today}")
os.makedirs(local_folder, exist_ok=True)

# --- Download TLEs ---
for filename, url in tle_sources.items():
    try:
        logger.info(f"Downloading {filename} from {url}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(os.path.join(local_folder, filename), "w") as f:
            f.write(r.text)
        logger.info(f"Saved {filename} locally")
    except Exception as e:
        logger.error(f"Failed to download {filename}: {e}")

# --- Upload to HDFS ---
try:
    logger.info(f"Copying downloaded TLEs to HDFS folder: {args.output_hdfs}")
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", args.output_hdfs], check=True)
    for filename in tle_sources:
        local_path = os.path.join(local_folder, filename)
        if os.path.exists(local_path):
            subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, args.output_hdfs], check=True)
    logger.info("Upload to HDFS completed successfully")
except subprocess.CalledProcessError as e:
    logger.error(f"Failed to copy to HDFS: {e}")
