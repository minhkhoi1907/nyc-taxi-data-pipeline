import duckdb
con = duckdb.connect("dev.duckdb")
count_df = con.sql("SELECT COUNT(*) as total_trips FROM stg_yellow_tripdata").df()
print(f"TỔNG SỐ CHUYẾN XE 2009-2010 ĐÃ XỬ LÝ LÀ: {count_df['total_trips'][0]:,} chuyến")