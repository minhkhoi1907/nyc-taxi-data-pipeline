import os
import subprocess

def bulk_upload_to_hdfs():
    local_data_dir = r"C:\Users\HP\Downloads"
    hdfs_dir = "/raw/nyc_taxi"
    try:
        subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
    except subprocess.CalledProcessError:
        print("Thư mục HDFS đã tồn tại hoặc có lỗi kết nối.")
    if not os.path.exists(local_data_dir):
        return
    files = [f for f in os.listdir(local_data_dir) if f.endswith('.parquet')]
    
    if len(files) == 0:
        return
    for index, filename in enumerate(files, 1):
        local_file_path = os.path.join(local_data_dir, filename)
        hdfs_file_path = f"{hdfs_dir}/{filename}"
        try:
            subprocess.run(["docker", "cp", local_file_path, f"namenode:/{filename}"], check=True)
            subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", f"/{filename}", hdfs_file_path], check=True)
            subprocess.run(["docker", "exec", "namenode", "rm", f"/{filename}"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Lỗi khi xử lý file {filename}: {e}")

if __name__ == "__main__":
    bulk_upload_to_hdfs()