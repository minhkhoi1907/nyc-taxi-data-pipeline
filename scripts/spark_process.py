import sys
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit
from pyspark.sql.types import IntegerType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(year, month):
    spark = SparkSession.builder \
        .appName("NYCTaxi_Spark_Processing") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    month_str = str(month).zfill(2)
    logging.info(f"Starting Spark processing for {year}-{month_str}")

    hdfs_namenode = "hdfs://namenode:9000"
    filename = f"yellow_tripdata_{year}-{month_str}.parquet"
    input_path = f"{hdfs_namenode}/raw/nyc_taxi/{filename}"
    output_path = f"{hdfs_namenode}/nyc_taxi/processed/{year}/{month_str}"

    try:
        logging.info(f"Reading data from: {input_path}")
        df = spark.read.parquet(input_path)
        
        column_mapping = {
            "Trip_Pickup_DateTime": "tpep_pickup_datetime",
            "Trip_Dropoff_DateTime": "tpep_dropoff_datetime",
            "Passenger_Count": "passenger_count",
            "Trip_Distance": "trip_distance",
            "Total_Amt": "total_amount",
            "Fare_Amt": "fare_amount",
            "Tip_Amt": "tip_amount"
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and new_col not in df.columns:
                logging.info(f"Standardizing column: {old_col} -> {new_col}")
                df = df.withColumnRenamed(old_col, new_col)
                
        if "PULocationID" not in df.columns:
            logging.info("Legacy data detected. Adding PULocationID as NULL.")
            df = df.withColumn("PULocationID", lit(None).cast(IntegerType()))
        if "DOLocationID" not in df.columns:
            df = df.withColumn("DOLocationID", lit(None).cast(IntegerType()))

        required_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "total_amount"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns after standardization: {missing_cols}")

        clean_df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
                     .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
                     .filter(col("total_amount") > 0)

        logging.info(f"Writing Parquet data to: {output_path}")
        clean_df.write \
            .mode("overwrite") \
            .parquet(output_path)
            
        logging.info("Spark processing completed successfully.")
        
    except Exception as e:
        logging.error(f"Spark processing failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    main(args.year, args.month)
