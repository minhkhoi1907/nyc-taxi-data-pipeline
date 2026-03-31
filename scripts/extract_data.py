import os
import sys
import argparse
import requests
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_and_upload_to_hdfs(year, month):
    month_str = str(month).zfill(2)
    filename = f"yellow_tripdata_{year}-{month_str}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    
    local_dir = "data"
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, filename)
    hdfs_dir = "/raw/nyc_taxi"
    hdfs_path = f"{hdfs_dir}/{filename}"
    
    logging.info(f"Downloading data from: {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logging.info(f"Download completed: {local_path}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Download failed: {e}")
        sys.exit(1)

    logging.info(f"Uploading to HDFS: {hdfs_path}")
    try:
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "cp", local_path, f"namenode:/{filename}"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", f"/{filename}", hdfs_path], check=True)
        subprocess.run(["docker", "exec", "namenode", "rm", f"/{filename}"], check=True)
        logging.info("HDFS upload completed.")
    except subprocess.CalledProcessError as e:
        logging.error(f"HDFS upload failed: {e}")
        sys.exit(1)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
            logging.info(f"Cleaned up local file: {local_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    download_and_upload_to_hdfs(args.year, args.month)