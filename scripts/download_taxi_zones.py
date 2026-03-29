import os
import urllib.request

def download_taxi_zones():
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    
    # Lấy đường dẫn rễ (root dir là nyc_taxi_pipeline)
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(root_dir, "nyc_taxi_dbt", "seeds")
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, "taxi_zone_lookup.csv")
    
    if os.path.exists(output_path):
        print(f"File {output_path} đã tồn tại! Bỏ qua tải xuống.")
        return

    print(f"Đang tải taxi_zone_lookup.csv về {output_path}...")
    try:
        urllib.request.urlretrieve(url, output_path)
        print("Tải thành công!")
    except Exception as e:
        print(f"Lỗi khi tải file: {e}")

if __name__ == "__main__":
    download_taxi_zones()
