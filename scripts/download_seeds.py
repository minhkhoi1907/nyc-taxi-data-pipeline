import os
import urllib.request

def download_taxi_zones():
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    output_dir = os.path.join("nyc_taxi_dbt", "seeds")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "taxi_zone_lookup.csv")
    
    print(f"Downloading taxi zones lookup to {output_path}...")
    try:
        urllib.request.urlretrieve(url, output_path)
        print("Download successful!")
    except Exception as e:
        print(f"Failed to download: {e}")

if __name__ == "__main__":
    download_taxi_zones()
