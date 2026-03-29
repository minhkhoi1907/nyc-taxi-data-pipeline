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
    
    # 1. (Tuỳ chọn) Chạy script HDFS fetch - giả sử data đã ở đúng chỗ
    fetch_script = os.path.join(root_dir, "scripts", "fetch_from_datalake.py")
    if os.path.exists(fetch_script):
        print("\n1️⃣ Đang tải data từ Data Lake (HDFS)...")
        # subprocess.run(["python", fetch_script]) # Bỏ comment khi HDFS cluster đang chạy
        print("⏭️ Dữ liệu đã sẵn sàng.")
        
    # 2. Tải lookup files (taxi_zone_lookup.csv)
    print("\n2️⃣ Đang nạp Seed/Lookup Data...")
    download_zones_script = os.path.join(root_dir, "scripts", "download_taxi_zones.py")
    if os.path.exists(download_zones_script):
        run_command(["python", download_zones_script])
        
    # 3. Chạy DBT Build
    print("\n3️⃣ Đang xây dựng Data Warehouse Models bằng dbt...")
    # dbt seed (dành cho file csv nếu có thêm vào seeds)
    run_command(["dbt", "seed"], cwd=dbt_dir)
    
    # dbt run
    run_command(["dbt", "run"], cwd=dbt_dir)
    
    # dbt test
    print("\n4️⃣ Chạy Data Quality Tests...")
    run_command(["dbt", "test"], cwd=dbt_dir)

    print("\n✅ PIPELINE CHẠY THÀNH CÔNG! Báo cáo (Marts) đã sẵn sàng.")

if __name__ == "__main__":
    main()
