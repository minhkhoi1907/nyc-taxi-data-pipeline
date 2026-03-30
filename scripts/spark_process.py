from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import sys

def main():
    # Khởi tạo SparkSession
    # bitnami/spark:3.5.1 image comes with Hadoop 3 dependencies
    spark = SparkSession.builder \
        .appName("NYCTaxi_Spark_Processing") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Cấu hình logging level để đỡ rác terminal
    spark.sparkContext.setLogLevel("WARN")

    print("🚀 Bắt đầu quá trình xử lý Spark...")

    # HDFS path (truy cập từ trong Docker network)
    hdfs_namenode = "hdfs://namenode:9000"
    
    # Ở các bước trước, file csv được load vào thư mục /nyc_taxi/yellow/raw hoặc /nyc_taxi/raw
    # Ta sẽ đọc thư mục chứa dữ liệu raw
    input_path = f"{hdfs_namenode}/nyc_taxi/yellow_tripdata_*.csv"
    output_path = f"{hdfs_namenode}/nyc_taxi/processed/yellow_tripdata.parquet"

    try:
        print(f"📥 Đang đọc dữ liệu từ: {input_path}")
        # Đọc dữ liệu CSV
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        
        # In ra schema ban đầu
        print("Schema ban đầu:")
        df.printSchema()
        
        # Làm sạch dữ liệu cơ bản (Basic Cleaning)
        # 1. Ép kiểu datetime cho tpep_pickup_datetime và tpep_dropoff_datetime
        # 2. Lọc các cuốc xe không hợp lệ (passenger_count <= 0, trip_distance <= 0)
        clean_df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
                     .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
                     .filter(col("passenger_count") > 0) \
                     .filter(col("trip_distance") > 0) \
                     .filter(col("total_amount") > 0)

        # In số lượng dòng sau khi lọc
        # print("Số dòng dữ liệu sau khi làm sạch:", clean_df.count()) # Bỏ comment nếu chạy dữ liệu nhỏ

        print(f"📤 Đang ghi dữ liệu (định dạng Parquet) ra: {output_path}")
        
        # Ghi ra định dạng Parquet với mode="overwrite"
        clean_df.write \
            .mode("overwrite") \
            .parquet(output_path)
            
        print("✅ Xử lý qua Spark và ghi file Parquet thành công!")
        
    except Exception as e:
        print(f"❌ Lỗi trong quá trình Spark processing: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
