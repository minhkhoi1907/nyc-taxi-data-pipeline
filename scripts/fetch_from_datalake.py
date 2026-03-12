import os
import subprocess

def fetch_data_from_hdfs():
    hdfs_dir = "/raw/nyc_taxi"
    local_data_dir = "data"
    os.makedirs(local_data_dir, exist_ok=True)
    try:
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", hdfs_dir],
            capture_output=True, text=True, check=True
        )
        lines = result.stdout.strip().split('\n')
        parquet_files = [line.split()[-1] for line in lines if line.endswith('.parquet')]
        if not parquet_files:
            print("Không tìm thấy file Parquet nào trong HDFS.")
            return
        for index, hdfs_path in enumerate(parquet_files, 1):
            filename = hdfs_path.split('/')[-1]
            local_path = os.path.join(local_data_dir, filename)
            if os.path.exists(local_path):
                print(f"[{index}/{len(parquet_files)}] ⏭️ Đã có {filename}, bỏ qua.")
                continue
            subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-get", hdfs_path, f"/tmp/{filename}"], check=True)
            subprocess.run(["docker", "cp", f"namenode:/tmp/{filename}", local_path], check=True)
            subprocess.run(["docker", "exec", "namenode", "rm", f"/tmp/{filename}"], check=True)     
    except subprocess.CalledProcessError as e:
        print(f"Lỗi giao tiếp với HDFS: {e}")

if __name__ == "__main__":
    fetch_data_from_hdfs()