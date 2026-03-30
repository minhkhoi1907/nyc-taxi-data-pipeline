import subprocess
import os
import sys

def run_command(command, cwd=None):
    try:
        print(f"\n🚀 Running: {' '.join(command)}")
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error during execution: {e}")
        sys.exit(1)

def main():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dbt_dir = os.path.join(root_dir, "nyc_taxi_dbt")
    
    print("=== BẮT ĐẦU NYC TAXI PIPELINE ===")
    
    # 1. Chạy Spark Processing
    print("\n1️⃣ Đang chạy Spark Processing job (HDFS -> Spark -> HDFS)...")
    run_command(["docker", "exec", "spark-master", "spark-submit", "/scripts/spark_process.py"])
    
    # 2. Kéo Parquet data về Local cho DuckDB
    fetch_script = os.path.join(root_dir, "scripts", "fetch_from_datalake.py")
    if os.path.exists(fetch_script):
        print("\n2️⃣ Đang tải Parquet data từ HDFS về Local...")
        run_command(["python", fetch_script])
        
    # 3. Tải lookup files (taxi_zone_lookup.csv)
    print("\n3️⃣ Đang nạp Seed/Lookup Data...")
    download_zones_script = os.path.join(root_dir, "scripts", "download_taxi_zones.py")
    if os.path.exists(download_zones_script):
        run_command(["python", download_zones_script])
        
    # 4. Chạy DBT Build
    print("\n4️⃣ Đang xây dựng Data Warehouse Models bằng dbt...")
    run_command(["dbt", "seed"], cwd=dbt_dir)
    run_command(["dbt", "run"], cwd=dbt_dir)
    
    # 5. Chạy DBT Test
    print("\n5️⃣ Chạy Data Quality Tests...")
    run_command(["dbt", "test"], cwd=dbt_dir)

    print("\n✅ PIPELINE CHẠY THÀNH CÔNG! Báo cáo (Marts) đã sẵn sàng.")

if __name__ == "__main__":
    main()
