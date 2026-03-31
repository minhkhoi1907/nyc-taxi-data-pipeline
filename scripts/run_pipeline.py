import subprocess
import os
import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_command(command, cwd=None):
    try:
        logging.info(f"Running: {' '.join(command)}")
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing command: {e}")
        sys.exit(1)

def main(year, month):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dbt_dir = os.path.join(root_dir, "nyc_taxi_dbt")
    
    logging.info(f"Starting NYC Taxi Pipeline ({year}-{str(month).zfill(2)})")
    
    extract_script = os.path.join(root_dir, "scripts", "extract_data.py")
    if os.path.exists(extract_script):
        logging.info("Step 0: Invoking HTTP Extractor (Web -> HDFS)")
        run_command(["python", extract_script, "--year", str(year), "--month", str(month)])
    
    logging.info("Step 1: Invoking Spark Job (Processing & Schema Evolution)")
    run_command(["docker", "exec", "spark-master", "spark-submit", "/scripts/spark_process.py", "--year", str(year), "--month", str(month)])
    
    fetch_script = os.path.join(root_dir, "scripts", "fetch_from_datalake.py")
    if os.path.exists(fetch_script):
        logging.info("Step 2: Fetching Processed Parquet data to Local Storage")
        run_command(["python", fetch_script, "--year", str(year), "--month", str(month)])
        
    download_zones_script = os.path.join(root_dir, "scripts", "download_taxi_zones.py")
    if os.path.exists(download_zones_script):
        logging.info("Step 3: Loading Lookup Tables")
        run_command(["python", download_zones_script])
        
    logging.info("Step 4: Executing dbt models (Seed & Run)")
    run_command(["dbt", "seed"], cwd=dbt_dir)
    run_command(["dbt", "run"], cwd=dbt_dir)
    
    logging.info("Step 5: Executing dbt tests")
    run_command(["dbt", "test"], cwd=dbt_dir)

    logging.info("Pipeline execution completed successfully. Mart tables are ready.")
    
    dashboard_script = os.path.join(root_dir, "app", "Home.py")
    if os.path.exists(dashboard_script):
        try:
            logging.info("Step 6: Launching Streamlit Dashboard on port 8501")
            subprocess.Popen(["streamlit", "run", dashboard_script], cwd=root_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            logging.info("Access the dashboard at: http://localhost:8501")
        except Exception as e:
            logging.error(f"Failed to start Streamlit: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    main(args.year, args.month)
