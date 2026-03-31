import os
import sys
import argparse
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data_from_hdfs(year, month):
    month_str = str(month).zfill(2)
    hdfs_dir = f"/nyc_taxi/processed/{year}/{month_str}"
    local_data_dir = "data"
    os.makedirs(local_data_dir, exist_ok=True)
    
    logging.info(f"Fetching Parquet from {hdfs_dir} to {local_data_dir}")
    try:
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", hdfs_dir],
            capture_output=True, text=True, check=True
        )
        lines = result.stdout.strip().split('\n')
        parquet_files = [line.split()[-1] for line in lines if line.endswith('.parquet')]
        if not parquet_files:
            logging.warning("No Parquet files found in target HDFS partition.")
            return

        for index, hdfs_path in enumerate(parquet_files, 1):
            filename = hdfs_path.split('/')[-1]
            local_path = os.path.join(local_data_dir, f"{year}_{month_str}_{filename}")
            
            if os.path.exists(local_path):
                logging.info(f"[{index}/{len(parquet_files)}] File already exists: {local_path}. Skipping.")
                continue
                
            logging.info(f"[{index}/{len(parquet_files)}] Downloading: {filename}")
            subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-get", hdfs_path, f"/tmp/{filename}"], check=True)
            subprocess.run(["docker", "cp", f"namenode:/tmp/{filename}", local_path], check=True)
            subprocess.run(["docker", "exec", "namenode", "rm", f"/tmp/{filename}"], check=True)
            
        logging.info("Data fetch completed successfully.")
        
    except subprocess.CalledProcessError as e:
        logging.error(f"HDFS fetch error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    fetch_data_from_hdfs(args.year, args.month)