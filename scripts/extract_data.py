import os
import requests
import subprocess

def download_and_upload_to_hdfs():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    filename = "yellow_tripdata_2024-01.parquet"
    local_path = os.path.join("data", filename)
    hdfs_dir = "/raw/nyc_taxi"
    hdfs_path = f"{hdfs_dir}/{filename}"
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk) 
    try:
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
        subprocess.run(["docker", "cp", local_path, f"namenode:/{filename}"], check=True)
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", f"/{filename}", hdfs_path], check=True)
        subprocess.run(["docker", "exec", "namenode", "rm", f"/{filename}"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Có lỗi xảy ra khi giao tiếp với Docker: {e}")
if __name__ == "__main__":
    download_and_upload_to_hdfs()